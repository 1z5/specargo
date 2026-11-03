"""规模压测：千级分区下 journal 对账的正确性、耗时与对象存储读次数。

背景：onExit 对账跑在集群里，每次对象存储 GET 都是网络往返；分区数上千时
"读多少对象"直接决定收口是秒级还是分钟级。本套件锁住三件事：

1. **读次数**：``reconcile-journal`` 只读末 stage 的 N 个状态，不是全部 3N 个
   （用计数 store 实测 GET 次数——这是优化的硬证据，防止回归成全量读）；
2. **正确性**：直读路径与全量读 + 过滤的结果**完全一致**（含缺失/损坏/非终态）；
3. **规模**：1000 分区 × 3 stage = 3000 个状态对象，对账通过且有界。

同时验证 ``state check-skip`` 是 O(1) 单点读（不随分区数增长）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"
import importlib.util                                                  # noqa: E402
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

import specargo                                                        # noqa: E402
from specargo import journal                                           # noqa: E402
from specargo.state import evaluate_items, reconcile_datasets          # noqa: E402

VERIFIED = "verified"
GENERATED = "generated"


class CountingStore:
    """包一层 store，统计 get/list 次数（用于证明读次数优化）。"""

    def __init__(self, inner):
        self.inner = inner
        self.get_calls = 0
        self.list_calls = 0

    def get(self, key, dest):
        self.get_calls += 1
        return self.inner.get(key, dest)

    def list(self, prefix=""):
        self.list_calls += 1
        return self.inner.list(prefix)

    def exists(self, key):
        return self.inner.exists(key)

    def put(self, src, key):
        return self.inner.put(src, key)


def _with_counting_store():
    """把 journal._store 换成计数包装，返回 (store, 恢复函数)。"""
    original = journal._store
    inner = original()
    counting = CountingStore(inner)
    journal._store = lambda: counting
    return counting, lambda: setattr(journal, "_store", original)


# ---------------------------------------------------------------------------
# 1. 正确性：直读 == 全量读 + 过滤
# ---------------------------------------------------------------------------

def test_direct_read_equals_full_load_filtered():
    """末 stage 直读必须与 load_statuses+final_stage 过滤完全一致。"""
    with tempfile.TemporaryDirectory() as d:
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(Path(d) / "a")})
        for i in range(40):
            for stage in ("extract", "transform", "load"):
                status = VERIFIED if i % 4 else (GENERATED if i % 8 == 1 else "failed")
                journal.put_status(run_id="eq", dataset="ds", partition=str(i),
                                   variant="v" if i % 5 == 0 else "", stage=stage,
                                   status=status)
        # 制造一个损坏的状态对象（内容不是 JSON）
        from specargo.objstore import _store
        store = _store()
        bad_key = journal.status_key("eq", "load", "ds", "999", "")
        bad = Path(d) / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        store.put(bad, bad_key)

        items = [("ds", str(i), "v" if i % 5 == 0 else "") for i in range(40)]
        items.append(("ds", "999", ""))            # 损坏对象
        items.append(("ds", "12345", ""))          # 完全缺失

        direct = journal.get_statuses_for("eq", "load", items)
        full = [r for r in journal.load_statuses("eq") if r.get("stage") == "load"]

        by_key = {(r["dataset"], r["partition"], r["variant"]): r
                  for r in full if r.get("dataset")}
        for row in direct:
            key = (row["dataset"], row["partition"], row["variant"])
            ref = by_key.get(key)
            if ref is None:
                # 缺失/损坏：全量读里没有对应条目，直读必须标记为 missing/corrupt
                assert row["status"] in ("missing", "corrupt"), row
            else:
                assert row["status"] == ref["status"], (row, ref)

        # 对账结论也必须一致
        a = reconcile_datasets(["ds"], items, direct, final_stage="load",
                               terminal_statuses=(VERIFIED,))
        b = reconcile_datasets(["ds"], items, full, final_stage="load",
                               terminal_statuses=(VERIFIED,))
        assert a == b, (a, b)


# ---------------------------------------------------------------------------
# 2. 读次数：末 stage 直读是 N 次 GET，不是 3N，且不 LIST
# ---------------------------------------------------------------------------

def test_corrupt_status_record_is_missing_in_both_read_paths():
    """损坏的状态对象：两条读路径都必须判"漏数"，且对账结论完全一致。

    这是**有意的设计取舍**，不是遗漏：status key 是 (dataset, partition, variant)
    的摘要、不携带身份，load_statuses 无法把损坏记录挂回具体 item。若直读路径
    按已知身份记成"未过检"、全量读路径记成"漏数"，同一次故障会出现两种告警。
    要让损坏也精确归因，得改 key 设计（见 docs/ROADMAP.md）。
    """
    with tempfile.TemporaryDirectory() as d:
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(Path(d) / "a")})
        for i in range(3):
            journal.put_status(run_id="c1", dataset="ds", partition=str(i),
                               variant="", stage="load", status=VERIFIED)
        # 把 partition 1 的状态对象写坏
        from specargo.objstore import _store
        bad = Path(d) / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        _store().put(bad, journal.status_key("c1", "load", "ds", "1", ""))

        items = [("ds", str(i), "") for i in range(3)]
        direct = journal.get_statuses_for("c1", "load", items)
        full = [r for r in journal.load_statuses("c1") if r.get("stage") == "load"]

        a = reconcile_datasets(["ds"], items, direct, final_stage="load",
                               terminal_statuses=(VERIFIED,))
        b = reconcile_datasets(["ds"], items, full, final_stage="load",
                               terminal_statuses=(VERIFIED,))
        assert a == b, (a, b)
        assert a[0]["missing"] == ["1"], a          # 损坏 → 漏数
        assert a[0]["not_verified"] == [], a        # 不是"未过检"
        assert a[0]["ok"] is False, a


def test_reconcile_reads_only_final_stage():
    """对账只读末 stage：GET 次数 == 分区数，且不做 LIST。"""
    with tempfile.TemporaryDirectory() as d:
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(Path(d) / "a")})
        n = 60
        for i in range(n):
            for stage in ("extract", "transform", "load"):
                journal.put_status(run_id="cnt", dataset="ds", partition=str(i),
                                   variant="", stage=stage, status=VERIFIED)
        counting, restore = _with_counting_store()
        try:
            rows = journal.get_statuses_for(
                "cnt", "load", [("ds", str(i), "") for i in range(n)])
        finally:
            restore()
        assert counting.list_calls == 0, "直读不需要 LIST 操作"
        assert counting.get_calls == n, \
            f"对账应只读 {n} 个末 stage 状态，实际 GET {counting.get_calls} 次"
        assert len(rows) == n
        assert all(r["status"] == VERIFIED for r in rows)


# ---------------------------------------------------------------------------
# 3. 规模：1000 分区
# ---------------------------------------------------------------------------

def test_thousand_partitions_reconcile_passes_in_time():
    """1000 分区 × 3 stage：状态全部落盘、对账通过、耗时与读次数有界。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(root / "art")})
        n = 1000
        t0 = time.time()
        for i in range(n):
            for stage in ("extract", "transform", "load"):
                journal.put_status(run_id="big", dataset="ds", partition=str(i),
                                   variant="", stage=stage, status=VERIFIED)
        write_s = time.time() - t0

        items = [("ds", str(i), "") for i in range(n)]
        counting, restore = _with_counting_store()
        try:
            t0 = time.time()
            rows = journal.get_statuses_for("big", "load", items)
            read_s = time.time() - t0
        finally:
            restore()

        t0 = time.time()
        recons = reconcile_datasets(["ds"], items, rows, final_stage="load",
                                    terminal_statuses=(VERIFIED,))
        rec_s = time.time() - t0

        assert recons[0]["ok"] and recons[0]["verified"] == n, recons[0]
        assert counting.get_calls == n, counting.get_calls
        assert counting.list_calls == 0
        # 本地文件系统上的上界；集群 S3 上每次 GET 是网络往返，
        # 优化后 GET 次数已是 N（不是 3N），这是可接受的量级。
        assert read_s < 30, f"读取 {n} 个状态耗时 {read_s:.1f}s"
        assert write_s < 60, f"写入 {3 * n} 个状态耗时 {write_s:.1f}s"
        print(f"    [规模] 写 3×{n}={write_s:.1f}s / 读 {n}={read_s:.2f}s / "
              f"对账={rec_s:.3f}s / GET 次数={counting.get_calls} / LIST={counting.list_calls}")


def test_check_skip_is_o1_single_read():
    """resume 判定是单点读：读次数不随分区数增长（1 次 GET）。"""
    with tempfile.TemporaryDirectory() as d:
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(Path(d) / "a")})
        for i in range(200):
            journal.put_status(run_id="o1", dataset="ds", partition=str(i),
                               variant="", stage="load", status=VERIFIED)
        counting, restore = _with_counting_store()
        try:
            row = journal.get_item_status("o1", dataset="ds", partition="100",
                                          variant="", stage="load")
            missing = journal.get_item_status("o1", dataset="ds", partition="100",
                                              variant="", stage="extract")
        finally:
            restore()
        assert row["status"] == VERIFIED
        assert missing is None, "该分区没有 extract 状态，应返回 None"
        assert counting.get_calls <= 2, f"单点判定应 ≤2 次 GET，实际 {counting.get_calls}"
        assert counting.list_calls == 0, "单点判定不应 LIST"


def test_scale_cli_reconcile_reports_all_verified():
    """CLI 层（onExit 真正调用的路径）在千级分区下输出 PASS。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(root / "art")})
        n = 1000
        for i in range(n):
            journal.put_status(run_id="cli", dataset="ds", partition=str(i),
                               variant="", stage="load", status=VERIFIED)
        parts = root / "parts.json"
        parts.write_text(json.dumps(
            [{"key": str(i), "dataset": "ds", "variant": ""} for i in range(n)]),
            encoding="utf-8")
        env = dict(os.environ)
        env["PIPELINE_RUN_ID"] = "cli"
        p = subprocess.run(
            [sys.executable, "-m", "specargo.pipeline_cli", "reconcile-journal",
             "--run-id", "cli", "--partitions-file", str(parts),
             "--final-stage", "load", "--terminal-statuses", '["verified"]'],
            capture_output=True, text=True, env=env, cwd=ROOT)
        assert p.returncode == 0, p.stdout + p.stderr
        assert "expected=1000" in p.stdout and "verified=1000" in p.stdout, p.stdout
        assert "PASS" in p.stdout


def test_cli_separates_missing_from_not_verified():
    """漏数与未过检必须分开报告（不同故障：没跑 vs 跑了没过检）。

    回归：直读优化最初把"读不到 key"合成一行 status=missing，导致漏数被
    归进未过检——告警文案分不清两类故障。
    """
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        os.environ.update({"OBJSTORE_KIND": "local", "OBJSTORE_ROOT": str(root / "art")})
        for i in list(range(10)) + [20]:
            journal.put_status(run_id="cls", dataset="ds", partition=str(i),
                               variant="", stage="load",
                               status=VERIFIED if i != 20 else GENERATED)
        parts = root / "parts.json"
        parts.write_text(json.dumps(
            [{"key": str(i), "dataset": "ds", "variant": ""}
             for i in list(range(10)) + [20, 777]]), encoding="utf-8")
        env = dict(os.environ)
        env["PIPELINE_RUN_ID"] = "cls"
        p = subprocess.run(
            [sys.executable, "-m", "specargo.pipeline_cli", "reconcile-journal",
             "--run-id", "cls", "--partitions-file", str(parts),
             "--final-stage", "load", "--terminal-statuses", '["verified"]'],
            capture_output=True, text=True, env=env, cwd=ROOT)
        assert p.returncode == 1, p.stdout + p.stderr
        assert "missing=1" in p.stdout and "not_verified=1" in p.stdout, p.stdout
        assert "漏数: 777" in p.stdout, "没有状态对象的分区必须算漏数"
        assert "未过检: 20" in p.stdout, "跑了但没过检的分区必须算未过检"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"  FAIL {name}: {exc}")
            except Exception as exc:                     # noqa: BLE001
                failures += 1
                print(f"  ERROR {name}: {exc!r}")
    print(f"\n{'规模测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
