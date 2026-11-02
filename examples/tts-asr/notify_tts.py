"""批次收口通知（onExit 用）：汇总 run 级结果并告警。

onExit 无论上游成败都执行，所以这里是"绿着但少数据"与"真失败"的统一出口：
    - 有漏数/未过检/重复 -> 明确说"哪几本书哪几章"，@负责人
    - 全部通过 -> 一行摘要，不打扰
告警通道可插拔：默认只打日志（NOTIFY_WEBHOOK 未配置时），
配了 Webhook（钉钉/飞书/Slack）才真发，避免本地演练误触发。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  (把仓库 app/ 加入 sys.path)

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

from specargo.envcompat import LEGACY_STATE_DB, getenv
from specargo.state import VERIFIED, connect


def build_summary(conn, run_id: str) -> dict:
    rows = conn.execute("SELECT * FROM pipeline_item WHERE run_id=?", (run_id,)).fetchall()
    by_book: dict[str, dict[str, int]] = {}
    for r in rows:
        entry = by_book.setdefault(f"{r['dataset']}[{r['variant']}]",
                                   {"verified": 0, "failed": 0, "generated": 0})
        entry[r["status"]] = entry.get(r["status"], 0) + 1
    run = conn.execute("SELECT * FROM pipeline_run WHERE run_id=?", (run_id,)).fetchone()
    return {
        "run_id": run_id,
        "run_status": run["status"] if run else "unknown",
        "note": run["note"] if run else "",
        "items": len(rows),
        "by_book": by_book,
    }


def render(summary: dict) -> str:
    lines = [
        f"TTS 批次 {summary['run_id']} 结果: {summary['run_status']}",
        f"item 总数 {summary['items']} | {summary['note']}",
    ]
    for book, counts in summary["by_book"].items():
        lines.append(
            f"  {book}: 过检 {counts.get(VERIFIED, 0)} / "
            f"失败 {counts.get('failed', 0)} / 已生成 {counts.get('generated', 0)}"
        )
    return "\n".join(lines)


def send_webhook(text: str) -> bool:
    url = os.getenv("NOTIFY_WEBHOOK", "")
    if not url:
        return False
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode()
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:   # noqa: S310 (用户自配 webhook)
        return resp.status == 200


def main() -> int:
    parser = argparse.ArgumentParser(description="批次收口通知")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-db",
                        default=getenv("PIPELINE_STATE_DB", LEGACY_STATE_DB,
                                       "/state/pipeline.db"))
    args = parser.parse_args()

    if not Path(args.state_db).exists():
        print(f"状态库不存在: {args.state_db}（可能整批还没开始就失败了）")
        return 0

    conn = connect(args.state_db)
    summary = build_summary(conn, args.run_id)
    conn.close()

    text = render(summary)
    print(text)
    if send_webhook(text):
        print("已发送 Webhook 告警")
    else:
        print("未配置 NOTIFY_WEBHOOK，仅打印（本地演练安全）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
