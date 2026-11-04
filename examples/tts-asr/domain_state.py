"""TTS 领域的状态辅助：书籍/章节/音色语义 <-> 框架通用 (dataset, partition, variant)。

框架状态层只认通用四元组；本文件把领域口径（book_slug/chapter_num/voice）
映射过去，并保留领域特有的对账/重复文件检测逻辑。方向是
``examples -> framework``，框架不反向依赖本文件。
"""
from __future__ import annotations

import re
from pathlib import Path

import _bootstrap  # noqa: F401  (把仓库 src/ 加入 sys.path，供脚本方式运行)

from models import Book
from specargo.state import VERIFIED, items_for, upsert_item


def upsert_tts_item(conn, *, book_slug: str, chapter_num: str, voice: str,
                    status: str, stage: str = "", run_id: str | None = None,
                    coverage: float | None = None,
                    accuracy: float | None = None) -> None:
    """领域口径 -> 通用 (dataset, partition, variant) 后写入状态表。"""
    upsert_item(conn, dataset=book_slug, partition=chapter_num, variant=voice,
                stage=stage, status=status, run_id=run_id,
                coverage=coverage, accuracy=accuracy)


def items_for_book(conn, book_slug: str, voice: str):
    return items_for(conn, book_slug, voice)


# ---------------------------------------------------------------------------
# 期望清单：从书籍 JSON 推出"本应有哪些 item"
# ---------------------------------------------------------------------------

def expected_item_keys(book: Book, voice: str) -> list[str]:
    """按 pipeline 的文件名约定推出期望的 chapter_num 列表。

    opening -> "0"；章节 -> str(chapter_id) 或序号；closing -> str(total+1)。
    """
    keys = ["0"]
    for idx, chapter in enumerate(book.chapters):
        keys.append(str(chapter.chapter_id) if chapter.chapter_id else str(idx + 1))
    if book.chapter_count > 0:
        keys.append(str(book.chapter_count + 1))
    return keys


def reconcile(conn, book_slug: str, book: Book, voice: str) -> dict:
    """返回该书的对账结果。

    ⚠️ ``book_slug`` 必须与**产物文件名/状态登记**用的口径一致（JSON 文件名 stem），
    不能用 ``book.slug``（标题派生）——两者在日常数据里经常不同
    （如 ``book_a.json`` vs 标题 "Some Book Title" -> ``some_book_title``）。
    口径不统一是对账最常见的错位来源，联调里真实踩到过。

    - missing_generated: 期望有但状态表里没有 -> 漏生成（任务没跑到）
    - not_verified:     生成了但没通过校验 -> 漏检/质量不合格
    - verified:         已交付
    """
    expected = expected_item_keys(book, voice)
    rows = {r["partition"]: r for r in items_for_book(conn, book_slug, voice)}

    missing_generated = [k for k in expected if k not in rows]
    not_verified = [
        k for k in expected
        if k in rows and rows[k]["status"] != VERIFIED
    ]
    verified = [k for k in expected if k in rows and rows[k]["status"] == VERIFIED]

    return {
        "book_slug": book.slug,
        "voice": voice,
        "expected": len(expected),
        "verified": len(verified),
        "missing_generated": missing_generated,   # 漏
        "not_verified": not_verified,             # 未过检
        "ok": not missing_generated and not not_verified,
    }


def scan_duplicate_files(output_dir: Path, book_slug: str, voice: str) -> list[str]:
    """磁盘级重复检测（重复、错位）。

    两类重复：
      A. 同一 (chapter_num, voice) 出现多个产物文件 —— 重复写入 / 拷贝残留
         （如 ``book_a_1_cedar.mp3`` 与 ``book_a_1_cedar (1).mp3``）
      B. 不同文件但输入指纹相同 —— 内容重复，通常是上游数据错位
    """
    root = Path(output_dir)
    if not root.exists():
        return []

    shadow = (".input.txt", ".prompt.txt", ".input.sha256",
              ".transcript.txt", ".orig.bak")
    by_item: dict[tuple[str, str], list[str]] = {}
    by_fingerprint: dict[str, list[str]] = {}

    for p in sorted(root.iterdir()):
        if not p.is_file() or p.name.endswith(shadow):
            continue
        # 归一化：去掉 " (1)" 拷贝标记与 .bak/.orig 后缀，再按 <slug>_<num>_<voice> 解析
        stem = re.sub(r"\s*\(\d+\)$", "", p.stem)
        stem = re.sub(r"\.(bak|orig)$", "", stem)
        parts = stem.rsplit("_", 2)
        if len(parts) != 3:
            continue
        slug, num, v = parts
        if slug != book_slug or v != voice:
            continue
        by_item.setdefault((num, v), []).append(p.name)

        fp_file = p.parent / (p.name + ".input.sha256")
        if fp_file.exists():
            fp = fp_file.read_text(encoding="utf-8").strip()
            if fp:
                by_fingerprint.setdefault(fp, []).append(p.name)

    problems: list[str] = []
    for (num, _v), names in by_item.items():
        if len(names) > 1:
            problems.append(f"章节 {num} 有多个产物: {names}")
    for fp, names in by_fingerprint.items():
        if len(names) > 1:
            problems.append(f"输入相同(指纹 {fp[:8]})的多个产物: {names}")
    return problems
