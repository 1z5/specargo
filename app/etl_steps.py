"""ETL demo 的三个步骤（供 pipelines/etl-demo.yaml 调用）。

存在的意义是**证明框架普适性**：这段代码没有任何 TTS/ASR 知识，
却能和 TTS 管线共用同一个 runner、同一套断点续跑/状态登记/质量闸门/对账。
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path


def extract(args) -> int:
    """按分区产出 JSONL（模拟从上游拉数）。"""
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"order_id": f"O{args.partition}-{i:04d}", "amount": round(random.uniform(1, 500), 2),
         "dt": f"2026-11-{int(args.partition):02d}"}
        for i in range(50)
    ]
    with open(args.out, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"extract 分区 {args.partition}: {len(rows)} 行 -> {args.out}")
    return 0


def transform(args) -> int:
    """字段规范化：金额保留两位、补默认字段。"""
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(args.src, encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            row = json.loads(line)
            row["amount"] = round(float(row["amount"]), 2)
            row.setdefault("currency", "CNY")
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    print(f"transform: {n} 行 -> {args.out}")
    return 0


def load(args) -> int:
    """装载：写完成标记（真实场景是写入目标库）。"""
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    n = sum(1 for line in open(args.src, encoding="utf-8") if line.strip())
    Path(args.out).write_text(f"loaded {n} rows\n", encoding="utf-8")
    print(f"load: {n} 行 -> {args.out}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL demo 步骤")
    sub = parser.add_subparsers(dest="step", required=True)

    p = sub.add_parser("extract")
    p.add_argument("--partition", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("transform")
    p.add_argument("--in", dest="src", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("load")
    p.add_argument("--in", dest="src", required=True)
    p.add_argument("--out", required=True)

    args = parser.parse_args()
    return {"extract": extract, "transform": transform, "load": load}[args.step](args)


if __name__ == "__main__":
    sys.exit(main())
