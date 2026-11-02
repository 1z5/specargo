"""旧状态库自动迁移（一次性兼容层，下一版本删除）。

历史版本用领域命名的状态表（旧前缀记在 ``envcompat.py``）。框架已统一为
``pipeline_run`` / ``pipeline_item(dataset, partition, variant, ...)``。
这里做原地升级，保证已有本地/部署状态库不需要手工迁移。

兼容层集中在本文件 + ``envcompat.py``；CI 的框架纯净度守卫对这两个文件例外。
"""
from __future__ import annotations

import sqlite3
import sys

LEGACY_RUN_TABLE = "tts_run"
LEGACY_ITEM_TABLE = "tts_item"
LEGACY_COLUMNS = (
    ("book_slug", "dataset"),
    ("chapter_num", "partition"),
    ("voice", "variant"),
)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f'PRAGMA table_info("{table}")')}


def migrate_legacy_schema(conn: sqlite3.Connection) -> None:
    """幂等迁移：旧表改名 + 旧列改名（SQLite >= 3.25 支持 RENAME COLUMN）。"""
    for old, new in ((LEGACY_RUN_TABLE, "pipeline_run"),
                     (LEGACY_ITEM_TABLE, "pipeline_item")):
        if _table_exists(conn, old) and not _table_exists(conn, new):
            conn.execute(f'ALTER TABLE "{old}" RENAME TO "{new}"')
        elif _table_exists(conn, old) and _table_exists(conn, new):
            # 旧表与新表并存：不自动合并（避免误判数据归属），但必须提示人工处理
            print(f"[state] 警告：旧表 {old} 与新表 {new} 同时存在，旧数据未合并；"
                  f"请人工确认后清理旧表", file=sys.stderr)
    conn.execute("DROP INDEX IF EXISTS idx_item_run")   # 旧索引名一并清理
    if _table_exists(conn, "pipeline_item"):
        columns = _columns(conn, "pipeline_item")
        for old, new in LEGACY_COLUMNS:
            if old in columns and new not in columns:
                conn.execute(
                    f'ALTER TABLE "pipeline_item" RENAME COLUMN "{old}" TO "{new}"')
    conn.commit()
