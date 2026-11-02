"""item 级状态登记与对账（框架可靠性层）。

要解决的三件事：
    - 数据遗漏 -> ``reconcile_generic()`` 做 expected vs actual 差集
    - 结果登记 -> 每个 item 有终态 + run_id + 证据指标
    - 可追溯   -> (dataset, partition, variant, stage) 四元组主键 + run_id

为什么用 SQLite 而不是文件：
    编排层每个 task 是独立 Pod，Pod 之间只能通过**外部存储**共享状态。
    emptyDir 随 Pod 消失，所以状态必须落在 Pod 之外：
    单机/测试用 SQLite；接口稳定后，上集群可换同 schema 的 MySQL/PostgreSQL
    （目前只提供 SQLite 实现，迁移路径见 ROADMAP）。

状态机::

    pending -> generated -> verified
                  -> failed

    配了质量闸门（checks）的 stage 只认 ``verified`` 为交付终态；
    没配 checks 的 stage 成功执行后的 ``generated`` 即终态（由调用方传入）。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from legacy_state_migration import migrate_legacy_schema

# item 状态
PENDING = "pending"
GENERATED = "generated"
VERIFIED = "verified"
FAILED = "failed"

SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id      TEXT PRIMARY KEY,
    started_at  TEXT,
    finished_at TEXT,
    status      TEXT,
    note        TEXT
);
CREATE TABLE IF NOT EXISTS pipeline_item (
    dataset     TEXT NOT NULL,
    partition   TEXT NOT NULL,
    variant     TEXT NOT NULL DEFAULT '',
    stage       TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    coverage    REAL,
    accuracy    REAL,
    run_id      TEXT,
    updated_at  TEXT,
    PRIMARY KEY (dataset, partition, variant, stage)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_item_run ON pipeline_item(run_id);
CREATE TABLE IF NOT EXISTS schema_version (
    version TEXT PRIMARY KEY
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str | Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    """打开（必要时创建/迁移）状态库。WAL 便于多进程读写。

    ``check_same_thread=False`` 只给"一个连接被多个线程串行使用"的场景用
    （如常驻服务用一把锁保护同一连接的读写）；默认保持 sqlite3 的线程约束，
    避免把跨线程误用静默放过。
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    migrate_legacy_schema(conn)
    conn.executescript(SCHEMA)
    # schema_version：记录当前版本；已有更高版本时不要倒退插入低版本
    existing = [row["version"] for row in conn.execute("SELECT version FROM schema_version")]
    numeric = [int(v) for v in existing if str(v).isdigit()]
    if not numeric or max(numeric) < int(SCHEMA_VERSION):
        conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(?)",
                     (SCHEMA_VERSION,))
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# 批次（run）级登记
# ---------------------------------------------------------------------------

def open_run(conn: sqlite3.Connection, run_id: str, note: str = "") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_run(run_id, started_at, finished_at, status, note)"
        " VALUES(?,?,?,?,?)",
        (run_id, now(), None, "running", note),
    )
    conn.commit()


def close_run(conn: sqlite3.Connection, run_id: str, status: str, note: str = "") -> None:
    conn.execute(
        "UPDATE pipeline_run SET finished_at=?, status=?, note=? WHERE run_id=?",
        (now(), status, note, run_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# item 级登记
# ---------------------------------------------------------------------------

def upsert_item(
    conn: sqlite3.Connection,
    *,
    dataset: str,
    partition: str,
    status: str,
    variant: str = "",
    stage: str = "",
    run_id: str | None = None,
    attempt: int | None = None,
    coverage: float | None = None,
    accuracy: float | None = None,
) -> None:
    """幂等写入：同一 (dataset, partition, variant, stage) 只保留一行，重跑即更新。

    ⚠️ stage 必须进主键：多 stage 管线里，若只有 (dataset, partition, variant)，
    后一个 stage 的状态会覆盖前一个，"生成成功但校验没过"就查不出来了。
    （这是把单 stage 管线框架化成多 stage 时真实踩到的坑。）
    """
    existing = conn.execute(
        "SELECT attempt FROM pipeline_item"
        " WHERE dataset=? AND partition=? AND variant=? AND stage=?",
        (dataset, partition, variant, stage),
    ).fetchone()
    next_attempt = (existing["attempt"] if existing else 0) + (1 if attempt is None else attempt)
    conn.execute(
        """
        INSERT INTO pipeline_item(dataset, partition, variant, stage, status, attempt,
                                  coverage, accuracy, run_id, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(dataset, partition, variant, stage) DO UPDATE SET
            status=excluded.status,
            attempt=excluded.attempt,
            coverage=COALESCE(excluded.coverage, pipeline_item.coverage),
            accuracy=COALESCE(excluded.accuracy, pipeline_item.accuracy),
            run_id=excluded.run_id,
            updated_at=excluded.updated_at
        """,
        (dataset, partition, variant, stage, status, next_attempt,
         coverage, accuracy, run_id, now()),
    )
    conn.commit()


def get_item(conn: sqlite3.Connection, dataset: str, partition: str,
             variant: str = "", stage: str = ""):
    return conn.execute(
        "SELECT * FROM pipeline_item"
        " WHERE dataset=? AND partition=? AND variant=? AND stage=?",
        (dataset, partition, variant, stage),
    ).fetchone()


def items_for(conn: sqlite3.Connection, dataset: str,
              variant: str = "") -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM pipeline_item WHERE dataset=? AND variant=? ORDER BY partition",
        (dataset, variant),
    ).fetchall()


# ---------------------------------------------------------------------------
# 对账：expected vs actual（expected 由管线定义给出，不由框架推导领域数据）
# ---------------------------------------------------------------------------

def evaluate_items(dataset: str, expected, rows, final_stage: str | None = None,
                   terminal_statuses: Sequence[str] = (VERIFIED,)) -> dict:
    """纯函数对账：expected 与已登记 rows 比对（不依赖存储后端）。

    本地 SQLite 与集群对象存储状态日志共用这一份判定逻辑——否则两个执行目标
    的"什么算交付"会各写一遍，迟早漂移。

    - ``expected``: (dataset, partition, variant) 三元组列表
    - ``rows``: 可迭代的 item 记录（dict 或 sqlite3.Row，需有
      dataset/partition/variant/stage/status 字段）
    - ``final_stage``: 只按末 stage 判定交付；None 表示不过滤
    - ``terminal_statuses``: 末 stage 的可交付终态（有闸门只认 verified）
    """
    terminal = tuple(terminal_statuses) or (VERIFIED,)
    groups: dict[str, dict] = {}
    for ds, key, variant in expected:
        if ds != dataset:
            continue
        g = groups.setdefault(variant, {"expected": []})
        g["expected"].append(key)

    by_variant: dict[str, dict] = {}
    for row in rows:
        if row["dataset"] != dataset:
            continue
        if final_stage and row["stage"] != final_stage:
            continue
        by_variant.setdefault(row["variant"], {})[row["partition"]] = row

    missing: list[str] = []
    not_verified: list[str] = []
    expected_total = 0
    for variant, g in groups.items():
        variant_rows = by_variant.get(variant, {})
        expected_total += len(g["expected"])
        for key in g["expected"]:
            if key not in variant_rows:
                missing.append(f"{variant or '-'}:{key}" if variant else key)
            elif variant_rows[key]["status"] not in terminal:
                not_verified.append(f"{variant or '-'}:{key}" if variant else key)

    return {
        "dataset": dataset,
        "expected": expected_total,
        "verified": expected_total - len(missing) - len(not_verified),
        "missing": missing,
        "not_verified": not_verified,
        "ok": not missing and not not_verified,
    }


def reconcile_generic(conn, dataset: str, expected, final_stage: str | None = None,
                      terminal_statuses: Sequence[str] = (VERIFIED,)) -> dict:
    """从 SQLite 状态表对账（语义见 ``evaluate_items``）。

    ⚠️ variant 必须参与对账：同一内容多 variant 的管线，若按 (dataset, stage)
    聚合，A variant 缺的条目会被 B variant 的记录掩盖 → 假绿。
    """
    rows = conn.execute(
        "SELECT * FROM pipeline_item WHERE dataset=?", (dataset,)).fetchall()
    return evaluate_items(dataset, expected, rows, final_stage=final_stage,
                          terminal_statuses=terminal_statuses)


def reconcile_datasets(datasets, expected, rows, final_stage: str | None = None,
                       terminal_statuses: Sequence[str] = (VERIFIED,)) -> list[dict]:
    """按 dataset 分组对账（Runner 与集群 onExit 共用）：返回每组结果列表。"""
    return [
        evaluate_items(ds, expected, rows, final_stage=final_stage,
                       terminal_statuses=terminal_statuses)
        for ds in datasets
    ]
