"""item 级状态登记与对账（原实现完全没有的一层）。

对应 JD：
    - 职责 2 "排查数据遗漏" -> ``reconcile()`` 做 expected vs actual 差集
    - 职责 4 "结果登记"    -> 每个 item 有终态 + run_id + 证据路径
    - 要求 6 "数据遗漏、重复、错位" -> 漏生成 / 未过检 / 重复文件三类检查

为什么用 SQLite 而不是文件：
    Argo 的每个 task 是一个独立 Pod，Pod 之间只能通过**外部存储**共享状态。
    emptyDir 随 Pod 消失，所以状态必须落在 Pod 之外：
    单机/测试用 SQLite，上集群换同一个 schema 的 MySQL/PostgreSQL 即可
    （迁移成本只剩连接串——这是我会在面试里说的设计取舍）。

状态机（与 _legacy 的 ExpandJob.status 同构，粒度更细到 item）：
    pending -> generated -> verified
                  \\-> failed
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from models import Book

# item 状态
PENDING = "pending"
GENERATED = "generated"
VERIFIED = "verified"
FAILED = "failed"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tts_run (
    run_id      TEXT PRIMARY KEY,
    started_at  TEXT,
    finished_at TEXT,
    status      TEXT,
    note        TEXT
);
CREATE TABLE IF NOT EXISTS tts_item (
    book_slug   TEXT NOT NULL,
    chapter_num TEXT NOT NULL,      -- "0"=opening, "N"=章节, "N+1"=closing（与文件名一致）
    voice       TEXT NOT NULL,
    status      TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    coverage    REAL,
    accuracy    REAL,
    run_id      TEXT,
    updated_at  TEXT,
    PRIMARY KEY (book_slug, chapter_num, voice)
);
CREATE INDEX IF NOT EXISTS idx_item_run ON tts_item(run_id);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str | Path) -> sqlite3.Connection:
    """打开（必要时创建）状态库。WAL 便于多进程读写。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# 批次（run）级登记
# ---------------------------------------------------------------------------

def open_run(conn: sqlite3.Connection, run_id: str, note: str = "") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO tts_run(run_id, started_at, finished_at, status, note)"
        " VALUES(?,?,?,?,?)",
        (run_id, now(), None, "running", note),
    )
    conn.commit()


def close_run(conn: sqlite3.Connection, run_id: str, status: str, note: str = "") -> None:
    conn.execute(
        "UPDATE tts_run SET finished_at=?, status=?, note=? WHERE run_id=?",
        (now(), status, note, run_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# item 级登记
# ---------------------------------------------------------------------------

def upsert_item(
    conn: sqlite3.Connection,
    *,
    book_slug: str,
    chapter_num: str,
    voice: str,
    status: str,
    run_id: str | None = None,
    attempt: int | None = None,
    coverage: float | None = None,
    accuracy: float | None = None,
) -> None:
    """幂等写入：同一 (book, chapter, voice) 只保留一行，重跑即更新。"""
    existing = conn.execute(
        "SELECT attempt FROM tts_item WHERE book_slug=? AND chapter_num=? AND voice=?",
        (book_slug, chapter_num, voice),
    ).fetchone()
    next_attempt = (existing["attempt"] if existing else 0) + (1 if attempt is None else attempt)
    conn.execute(
        """
        INSERT INTO tts_item(book_slug, chapter_num, voice, status, attempt,
                             coverage, accuracy, run_id, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(book_slug, chapter_num, voice) DO UPDATE SET
            status=excluded.status,
            attempt=excluded.attempt,
            coverage=COALESCE(excluded.coverage, tts_item.coverage),
            accuracy=COALESCE(excluded.accuracy, tts_item.accuracy),
            run_id=excluded.run_id,
            updated_at=excluded.updated_at
        """,
        (book_slug, chapter_num, voice, status, next_attempt,
         coverage, accuracy, run_id, now()),
    )
    conn.commit()


def get_item(conn: sqlite3.Connection, book_slug: str, chapter_num: str, voice: str):
    return conn.execute(
        "SELECT * FROM tts_item WHERE book_slug=? AND chapter_num=? AND voice=?",
        (book_slug, chapter_num, voice),
    ).fetchone()


def items_for(conn: sqlite3.Connection, book_slug: str, voice: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM tts_item WHERE book_slug=? AND voice=? ORDER BY chapter_num",
        (book_slug, voice),
    ).fetchall()


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


# ---------------------------------------------------------------------------
# 对账：expected vs actual（JD "排查数据遗漏"的落地）
# ---------------------------------------------------------------------------

def reconcile(conn: sqlite3.Connection, book_slug: str, book: Book, voice: str) -> dict:
    """返回该书的对账结果。

    ⚠️ ``book_slug`` 必须与**产物文件名/状态登记**用的口径一致（JSON 文件名 stem），
    不能用 ``book.slug``（标题派生）——两者在日常数据里经常不同
    （如 ``book_a.json`` vs 标题 "Some Book Title" -> ``some_book_title``）。
    口径不统一是对账最常见的错位来源，我在联调里真实踩到过。

    - missing_generated: 期望有但状态表里没有 -> 漏生成（任务没跑到）
    - not_verified:     生成了但没通过校验 -> 漏检/质量不合格
    - verified:         已交付
    """
    expected = expected_item_keys(book, voice)
    rows = {r["chapter_num"]: r for r in items_for(conn, book_slug, voice)}

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
    """磁盘级重复检测（对应 JD 要求 6 的"重复、错位"）。

    两类重复：
      A. 同一 (chapter_num, voice) 出现多个产物文件 —— 重复写入 / 拷贝残留
         （如 ``book_a_1_cedar.mp3`` 与 ``book_a_1_cedar (1).mp3``）
      B. 不同文件但输入指纹相同 —— 内容重复，通常是上游数据错位
    """
    import re

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
