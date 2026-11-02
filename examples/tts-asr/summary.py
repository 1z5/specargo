"""对账汇总：把"跑完"升级成"跑对"（原实现缺的最后一环）。

要解决的三件事：
    - 数据遗漏：expected（管线声明应有）vs actual（状态表已登记）
    - 异常定位：输出可执行的差集清单，而不是一堆日志
    - 可判定：run 级汇总 + 退出码，供 CI / 工作流判断是否放行

作为 Argo 的 onExit / 末节点运行：无论上游成败都收口，输出
    /argo/outputs/parameters/{expected,generated,verified,failed,missing,ok}
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  (把仓库 app/ 加入 sys.path)

import argparse
import os
import sys
from pathlib import Path

from argo_io import publish_artifact, write_params
from book_reader import read_book
from domain_state import reconcile, scan_duplicate_files
from envcompat import LEGACY_RUN_ID, getenv
from state import close_run, connect


def summarize_book(conn, book_path: Path, voice: str) -> dict:
    book = read_book(book_path)
    # 口径统一：用 JSON 文件名 stem（与 pipeline 产物命名、状态登记一致）
    book_slug = book_path.stem
    result = reconcile(conn, book_slug, book, voice)
    # 磁盘级重复检测（状态表有唯一键，DB 层不会重复；重复来自重复落文件）
    dupes = scan_duplicate_files(
        Path(os.getenv("TTS_OUTPUT_DIR", "output")) / book_slug, book_slug, voice)
    result["duplicates"] = dupes
    result["ok"] = result["ok"] and not dupes
    return result


def render_markdown(results: list[dict]) -> str:
    lines = ["# TTS 批次对账报告", ""]
    lines.append("| 书 | 音色 | 期望 | 已过检 | 漏生成 | 未过检 | 重复 | 结论 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in results:
        lines.append(
            f"| {r['book_slug']} | {r['voice']} | {r['expected']} | {r['verified']} "
            f"| {len(r['missing_generated'])} | {len(r['not_verified'])} "
            f"| {len(r['duplicates'])} | {'✅' if r['ok'] else '❌'} |"
        )
    bad = [r for r in results if not r["ok"]]
    if bad:
        lines.append("")
        lines.append("## 待处理清单")
        for r in bad:
            if r["missing_generated"]:
                lines.append(f"- {r['book_slug']}[{r['voice']}] 漏生成章节: "
                             f"{', '.join(r['missing_generated'])}")
            if r["not_verified"]:
                lines.append(f"- {r['book_slug']}[{r['voice']}] 未过检章节: "
                             f"{', '.join(r['not_verified'])}")
            for d in r["duplicates"]:
                lines.append(f"- {r['book_slug']}[{r['voice']}] 重复产物: {d}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="批次对账汇总（expected vs actual）")
    parser.add_argument("--state-db", required=True, help="item 状态库（SQLite）")
    parser.add_argument("--books", nargs="+", required=True,
                        help="本次批次的书籍 JSON 路径（可多个）")
    parser.add_argument("--voice", required=True)
    parser.add_argument("--run-id", default=getenv("PIPELINE_RUN_ID", LEGACY_RUN_ID, ""))
    parser.add_argument("--report", default="", help="Markdown 报告输出路径")
    args = parser.parse_args()

    conn = connect(args.state_db)
    run_id = args.run_id or f"summary-{int(__import__('time').time())}"

    results = [summarize_book(conn, Path(b), args.voice) for b in args.books]
    md = render_markdown(results)

    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(md, encoding="utf-8")
        publish_artifact("reconcile-report.md", Path(args.report))

    expected = sum(r["expected"] for r in results)
    verified = sum(r["verified"] for r in results)
    missing = sum(len(r["missing_generated"]) for r in results)
    not_verified = sum(len(r["not_verified"]) for r in results)
    duplicates = sum(len(r["duplicates"]) for r in results)

    print(md)
    write_params({
        "expected": expected,
        "verified": verified,
        "missing": missing,
        "not-verified": not_verified,
        "duplicates": duplicates,
        "ok": "true" if all(r["ok"] for r in results) else "false",
    })
    close_run(conn, run_id,
              "succeeded" if all(r["ok"] for r in results) else "failed",
              f"expected={expected} verified={verified} missing={missing} "
              f"not_verified={not_verified} duplicates={duplicates}")
    conn.close()

    # 有漏数/未过检 -> 非零退出码，让工作流失败并告警（而不是"绿着但少数据"）
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
