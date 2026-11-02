"""数据集构建的三个步骤：seed → clean → shard。

用一套通用的"原始记录"形态演示清洗链路，不绑定任何具体业务::

    {"id": "p1-0001", "text": "……", "source": "api"}

seed    生成确定性的原始样本（含重复、含坏行），本地与 CI 都能跑，不依赖外部数据
clean   schema 校验 + 去重 + 文本规范化；坏样本进隔离区（quarantine），带原因可回放
shard   按确定性规则分片，产出 manifest（条数 / 校验和 / 分片清单）并自校验

设计要点：
- 清洗的产出必须**可复现**：同样的输入跑两次，clean/shard 的结果逐字节一致
- 坏数据**不丢弃**：进隔离区，带 (run 维度由上层给、规则编号、样本 ID)，便于修复后重跑
- 分片**确定性**：按 id 的 sha256 取模，重跑/换机器都切回同一份（见 manifest.py）
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from manifest import Manifest, ShardInfo, file_sha256, shard_of  # noqa: E402

_WS = re.compile(r"\s+")


# --------------------------------------------------------------------------
# 通用工具
# --------------------------------------------------------------------------

def _read_jsonl(path: str | Path) -> list[dict]:
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                # 解析失败也是"坏数据"，交给调用方隔离，而不是让整批任务崩掉
                rows.append({"__parse_error__": str(exc), "__line__": ln})
                continue
            if not isinstance(obj, dict):
                rows.append({"__parse_error__": "不是 JSON 对象", "__line__": ln})
                continue
            rows.append(obj)
    return rows


def _write_jsonl(rows: list[dict], path: str | Path) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    return len(rows)


# --------------------------------------------------------------------------
# seed：确定性样本（含重复与坏行，模拟真实脏数据）
# --------------------------------------------------------------------------

def cmd_seed(args: argparse.Namespace) -> int:
    part = args.partition
    rows: list[dict] = []
    for i in range(args.rows):
        rid = "%s-%04d" % (part, i)
        rows.append({
            "id": rid,
            "text": "  分区 %s 的第 %d 条样本，用于验证清洗与分片链路。 " % (part, i),
            "source": "synthetic",
        })

    # 故意制造三类脏数据，让 clean 有活可干（数量固定，保证可复现）
    rows.append(dict(rows[0]))                      # 重复 id
    rows.append(dict(rows[1]))                      # 重复 id
    rows.append({"id": "", "text": "空 id 的坏样本"})          # 缺主键
    rows.append({"id": "%s-bad" % part})                     # 缺必填字段 text
    rows.append({"id": "%s-ws" % part, "text": "  多个   空白\t需要   折叠  "})

    _write_jsonl(rows, args.out)
    print("[seed] %s -> %s（%d 条，含重复与坏行）" % (part, args.out, len(rows)))
    return 0


# --------------------------------------------------------------------------
# clean：schema 校验 + 去重 + 规范化，坏样本进隔离区
# --------------------------------------------------------------------------

_REQUIRED = ("id", "text")
_RULE_MISSING = "R001"      # 缺必填字段
_RULE_BAD_TYPE = "R002"     # 类型不对
_RULE_PARSE = "R003"        # JSON 解析失败
_RULE_DUP = "R004"          # 重复主键


def cmd_clean(args: argparse.Namespace) -> int:
    raw = _read_jsonl(args.inp)
    clean: list[dict] = []
    quarantine: list[dict] = []
    seen: set[str] = set()

    for row in raw:
        if "__parse_error__" in row:
            quarantine.append({**row, "rule": _RULE_PARSE})
            continue

        missing = [k for k in _REQUIRED if k not in row]
        if missing:
            quarantine.append({**row, "rule": _RULE_MISSING, "missing": missing})
            continue

        bad_type = [k for k in _REQUIRED if not isinstance(row[k], str)]
        if bad_type:
            quarantine.append({**row, "rule": _RULE_BAD_TYPE, "fields": bad_type})
            continue

        rid = row["id"].strip()
        text = _WS.sub(" ", row["text"]).strip()

        if not rid or not text:
            quarantine.append({**row, "rule": _RULE_MISSING, "why": "empty after normalize"})
            continue

        if rid in seen:
            quarantine.append({**row, "rule": _RULE_DUP})
            continue

        seen.add(rid)
        clean.append({"id": rid, "text": text, "source": row.get("source", "")})

    # 排序输出，保证同样的输入产出逐字节一致
    clean.sort(key=lambda r: r["id"])
    quarantine.sort(key=lambda r: (str(r.get("id", "")), str(r.get("rule", ""))))

    _write_jsonl(clean, args.out)
    if args.quarantine:
        _write_jsonl(quarantine, args.quarantine)

    print(
        "[clean] 输入 %d -> 合格 %d / 隔离 %d（去重 %d）"
        % (len(raw), len(clean), len(quarantine),
           sum(1 for q in quarantine if q.get("rule") == _RULE_DUP))
    )
    if not clean:
        print("[clean] 没有任何合格记录，任务应视为失败", file=sys.stderr)
        return 1
    return 0


# --------------------------------------------------------------------------
# shard：确定性分片 + manifest
# --------------------------------------------------------------------------

def cmd_shard(args: argparse.Namespace) -> int:
    rows = _read_jsonl(args.inp)
    if not rows:
        print("[shard] 输入为空", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    buckets: list[list[dict]] = [[] for _ in range(args.shards)]
    for r in rows:
        buckets[shard_of(str(r["id"]), args.shards)].append(r)

    base = Path(args.base_dir).resolve()

    shard_infos: list[ShardInfo] = []
    for idx, bucket in enumerate(buckets):
        bucket.sort(key=lambda r: str(r["id"]))
        f = out_dir / ("shard-%02d.jsonl" % idx)
        _write_jsonl(bucket, f)
        # path 一律记录为相对 base_dir 的形式，manifest 才能跟着目录整体搬运
        try:
            rel = str(f.resolve().relative_to(base))
        except ValueError:
            print("[shard] %s 不在 base_dir %s 内" % (f, base), file=sys.stderr)
            return 1
        shard_infos.append(ShardInfo(
            index=idx,
            path=rel,
            rows=len(bucket),
            bytes=f.stat().st_size,
            sha256=file_sha256(f),
        ))

    m = Manifest(
        dataset=args.dataset,
        version=args.version,
        shard_count=args.shards,
        rows=len(rows),
        shards=shard_infos,
        source_fingerprint=file_sha256(args.inp),
    )
    m.save(args.manifest)

    problems = m.verify(args.base_dir)
    if problems:
        for p in problems:
            print("[shard] manifest 自校验失败: %s" % p, file=sys.stderr)
        return 1

    print(
        "[shard] %d 条 -> %d 片，manifest %s（自校验通过）"
        % (len(rows), args.shards, args.manifest)
    )
    return 0


# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="数据集构建步骤")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed", help="生成确定性原始样本")
    s.add_argument("--partition", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--rows", type=int, default=20)
    s.set_defaults(func=cmd_seed)

    c = sub.add_parser("clean", help="schema 校验 + 去重 + 规范化")
    c.add_argument("--in", dest="inp", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--quarantine", default="")
    c.set_defaults(func=cmd_clean)

    h = sub.add_parser("shard", help="确定性分片 + manifest")
    h.add_argument("--in", dest="inp", required=True)
    h.add_argument("--out-dir", required=True)
    h.add_argument("--shards", type=int, default=2)
    h.add_argument("--manifest", required=True)
    h.add_argument("--dataset", required=True)
    h.add_argument("--version", default="v0")
    h.add_argument("--base-dir", default=".")
    h.set_defaults(func=cmd_shard)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
