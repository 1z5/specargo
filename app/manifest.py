"""数据集 manifest 与确定性分片。

为什么需要这个模块
------------------
训练 / 推理侧有一条硬约束：**同一份输入必须能切出同一份分片**。断点续训、数据并行、
可复现评测都依赖这一点——分片如果是随机切的，重跑就切不回同一份，模型会重复看某些
样本、漏掉另一些。

所以分片规则必须是确定性的：``shard = sha256(key) % N``，而不是随机数或遍历顺序。
（Python 内置 ``hash()`` 受 ``PYTHONHASHSEED`` 影响，跨进程不一致，不能用于分片。）

另外每次产出都要带 manifest：条数、校验和、分片清单、输入指纹。没有 manifest 的交付
等于没交付——下游无法对账，也无法判断"我收到的是不是全量、是不是同一版本"。

用法::

    python3 manifest.py verify --manifest out/manifest/p1.json --base-dir out
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

_CHUNK = 1 << 20


# --------------------------------------------------------------------------
# 确定性分片
# --------------------------------------------------------------------------

def stable_hash(key: str) -> int:
    """跨进程/跨机器一致的哈希（不依赖 PYTHONHASHSEED）。"""
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)


def shard_of(key: str, shard_count: int) -> int:
    """返回 key 所属分片下标；同样的 key 永远落到同一片。"""
    if shard_count <= 0:
        raise ValueError("shard_count 必须为正整数")
    return stable_hash(key) % shard_count


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(items: list[str]) -> str:
    """输入清单指纹（顺序无关），用于判断"输入有没有变"——断点续跑的前提。"""
    return hashlib.sha256("\n".join(sorted(items)).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------

@dataclass
class ShardInfo:
    index: int
    path: str          # 相对 base_dir
    rows: int
    bytes: int
    sha256: str


@dataclass
class Manifest:
    dataset: str
    version: str
    shard_count: int
    rows: int
    shards: list[ShardInfo] = field(default_factory=list)
    schema_version: str = "1"
    source_fingerprint: str = ""
    extra: dict = field(default_factory=dict)

    # ---------- 序列化 ----------
    def to_dict(self) -> dict:
        return asdict(self)

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path) -> "Manifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        raw["shards"] = [ShardInfo(**s) for s in raw.get("shards", [])]
        return cls(**raw)

    # ---------- 校验 ----------
    def verify(self, base_dir: str | Path) -> list[str]:
        """返回问题清单，空列表表示通过。

        检查三件事：分片齐全、校验和一致、条数合计对得上。
        任何一条不满足，下游就不该相信这份数据。
        """
        base = Path(base_dir)
        problems: list[str] = []

        if self.shard_count != len(self.shards):
            problems.append(
                "分片数不符：manifest 声明 %d，实际列出 %d"
                % (self.shard_count, len(self.shards))
            )

        total = 0
        for s in self.shards:
            f = base / s.path
            if not f.is_file():
                problems.append("分片缺失：%s" % s.path)
                continue
            size = f.stat().st_size
            if size != s.bytes:
                problems.append(
                    "分片大小不符：%s（声明 %d，实际 %d）" % (s.path, s.bytes, size)
                )
            digest = file_sha256(f)
            if digest != s.sha256:
                problems.append("分片校验和不符：%s" % s.path)
            total += s.rows

        if total != self.rows:
            problems.append("条数不符：manifest 声明 %d，分片合计 %d" % (self.rows, total))

        return problems


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="数据集 manifest 校验")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_verify = sub.add_parser("verify", help="校验 manifest 与分片是否一致")
    p_verify.add_argument("--manifest", required=True)
    p_verify.add_argument("--base-dir", default=".")

    args = parser.parse_args(argv)

    if args.cmd == "verify":
        m = Manifest.load(args.manifest)
        problems = m.verify(args.base_dir)
        if problems:
            for p in problems:
                print("[manifest] 不一致: %s" % p)
            print("[manifest] 校验失败（%d 项）" % len(problems))
            return 1
        print(
            "[manifest] 校验通过：dataset=%s version=%s rows=%d shards=%d"
            % (m.dataset, m.version, m.rows, m.shard_count)
        )
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
