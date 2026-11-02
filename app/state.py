"""item 级状态登记与对账（原实现完全没有的一层）。

要解决的三件事：
    - 数据遗漏 -> ``reconcile()`` 做 expected vs actual 差集
    - 结果登记 -> 每个 item 有终态 + run_id + 证据路径
    - 重复/错位 -> 漏生成 / 未过检 / 重复文件三类检查

为什么用 SQLite 而不是文件：
    Argo 的每个 task 是一个独立 Pod，Pod 之间只能通过**外部存储**共享状态。
    emptyDir 随 Pod 消失，所以状态必须落在 Pod 之外：
    单机/测试用 SQLite，上集群换同一个 schema 的 MySQL/PostgreSQL 即可
    （迁移成本只剩连接串）。

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
    stage       TEXT NOT NULL DEFAULT '',   -- 框架化新增：多 stage 管线必须带 stage 维度
    status      TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    coverage    REAL,
    accuracy    REAL,
    run_id      TEXT,
    updated_at  TEXT,
    PRIMARY KEY (book_slug, chapter_num, voice, stage)
);
CREATE INDEX IF NOT EXISTS idx_item_run ON tts_item(run_id);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str | Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    """打开（必要时创建）状态库。WAL 便于多进程读写。

    ``check_same_thread=False`` 只给"一个连接被多个线程串行使用"的场景用
    （如常驻服务用一把锁保护同一连接的读写）；默认保持 sqlite3 的线程约束，
    避免把跨线程误用静默放过。
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=check_same_thread)
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
    stage: str = "",
) -> None:
    """幂等写入：同一 (dataset, partition, variant, stage) 只保留一行，重跑即更新。

    ⚠️ stage 必须进主键：多 stage 管线里，若只有 (dataset, partition, variant)，
    后一个 stage 的状态会覆盖前一个，"生成成功但校验没过"就查不出来了。
    （这是把单stage管线框架化成多stage时真实踩到的坑。）
    """
    existing = conn.execute(
        "SELECT attempt FROM tts_item WHERE book_slug=? AND chapter_num=? AND voice=? AND stage=?",
        (book_slug, chapter_num, voice, stage),
    ).fetchone()
    next_attempt = (existing["attempt"] if existing else 0) + (1 if attempt is None else attempt)
    conn.execute(
        """
        INSERT INTO tts_item(book_slug, chapter_num, voice, stage, status, attempt,
                             coverage, accuracy, run_id, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(book_slug, chapter_num, voice, stage) DO UPDATE SET
            status=excluded.status,
            attempt=excluded.attempt,
            coverage=COALESCE(excluded.coverage, tts_item.coverage),
            accuracy=COALESCE(excluded.accuracy, tts_item.accuracy),
            run_id=excluded.run_id,
            updated_at=excluded.updated_at
        """,
        (book_slug, chapter_num, voice, stage, status, next_attempt,
         coverage, accuracy, run_id, now()),
    )
    conn.commit()


def get_item(conn: sqlite3.Connection, book_slug: str, chapter_num: str, voice: str,
             stage: str = ""):
    return conn.execute(
        "SELECT * FROM tts_item WHERE book_slug=? AND chapter_num=? AND voice=? AND stage=?",
        (book_slug, chapter_num, voice, stage),
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
# 对账：expected vs actual
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
    """磁盘级重复检测（重复、错位）。

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


# ---------------------------------------------------------------------------
# 通用别名（框架化）：item key 泛化为 (dataset, partition, variant)
#    列名沿用 book_slug/chapter_num/voice，语义上是 dataset/partition/variant
#    ——不重建表是为了兼容既有数据与测试；新代码一律用下面这组别名
# ---------------------------------------------------------------------------

def upsert_generic_item(conn, *, dataset: str, partition: str, variant: str = "",
                        status: str, run_id: str | None = None, attempt: int | None = None,
                        coverage=None, accuracy=None, stage: str = "") -> None:
    upsert_item(conn, book_slug=dataset, chapter_num=partition, voice=variant,
                status=status, run_id=run_id, attempt=attempt,
                coverage=coverage, accuracy=accuracy, stage=stage)


def items_for_generic(conn, dataset: str, variant: str = "") -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM tts_item WHERE book_slug=? AND voice=? ORDER BY chapter_num",
        (dataset, variant),
    ).fetchall()


def reconcile_generic(conn, dataset: str, expected, final_stage: str | None = None,
                      terminal_statuses: Sequence[str] = (VERIFIED,)) -> dict:
    """对账：expected 由**调用方（管线定义）**给出，不再从领域数据推导。

    ``expected`` 是 (dataset, partition, variant) 三元组列表——
    TTS 管线来自书籍 JSON 推导，ETL 管线来自时间窗，两者走同一套对账。
    ``final_stage``：只按最后一个 stage 判定是否交付（多 stage 管线的正确语义：
    中间 stage 成功不代表整条管线成功）。
    ``terminal_statuses``：末 stage 的"可交付终态"。有质量闸门的 stage 只认
    ``verified``；没有配置 checks 的 stage 由 Runner 传入 ``generated`` 也算交付
    ——否则"不配质量闸门"的管线永远无法通过，等于框架隐性强制每个 stage 都要 checks。

    ⚠️ variant 必须参与对账：TTS 这类"同内容多音色"的管线，
    若按 (dataset, stage) 聚合，A 音色缺的章节会被 B 音色的记录掩盖 → 假绿。
    这里按 (dataset, variant) 分组分别对账，再合并结论。
    """
    terminal = tuple(terminal_statuses) or (VERIFIED,)
    groups: dict[str, dict] = {}
    for ds, key, variant in expected:
        if ds != dataset:
            continue
        g = groups.setdefault(variant, {"expected": [], "rows": {}})
        g["expected"].append(key)

    # 一次性取回该 dataset 的所有行，按 variant 分组（避免 N 次查询）
    all_rows = conn.execute(
        "SELECT * FROM tts_item WHERE book_slug=?", (dataset,)).fetchall()
    by_variant: dict[str, dict] = {}
    for r in all_rows:
        if final_stage and r["stage"] != final_stage:
            continue
        by_variant.setdefault(r["voice"], {})[r["chapter_num"]] = r

    missing: list[str] = []
    not_verified: list[str] = []
    expected_total = 0
    for variant, g in groups.items():
        rows = by_variant.get(variant, {})
        expected_total += len(g["expected"])
        for key in g["expected"]:
            if key not in rows:
                missing.append(f"{variant or '-'}:{key}" if variant else key)
            elif rows[key]["status"] not in terminal:
                not_verified.append(f"{variant or '-'}:{key}" if variant else key)

    return {
        "dataset": dataset,
        "expected": expected_total,
        "verified": expected_total - len(missing) - len(not_verified),
        "missing": missing,
        "not_verified": not_verified,
        "ok": not missing and not not_verified,
    }
