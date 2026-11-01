"""框架层测试：声明式 spec、通用 runner、插件检查、对账。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from spec import Partition, PipelineSpec, Stage, RetryPolicy, build_context, load_spec, parse_duration, render  # noqa: E402
from checks import available, run_check  # noqa: E402
import checks_tts  # noqa: E402,F401  注册领域检查
from runner import Runner  # noqa: E402
import state as st  # noqa: E402


# ---------------- spec ----------------

def test_parse_duration():
    assert parse_duration("30m") == 1800
    assert parse_duration("1h") == 3600
    assert parse_duration("45s") == 45
    assert parse_duration("2h30m") == 9000


def test_render_supports_dotted_paths():
    ctx = {"partition": {"key": "book_a", "variant": "cedar"}, "params": {"n": 3}}
    assert render("out/{{partition.key}}_{{partition.variant}}.jsonl", ctx) == "out/book_a_cedar.jsonl"
    assert render("limit={{params.n}}", ctx) == "limit=3"
    assert render("{{missing}}x", ctx) == "x"


def test_load_spec_etl_demo():
    spec = load_spec("pipelines/etl-demo.yaml")
    assert spec.name == "etl-demo"
    assert [s.name for s in spec.stages] == ["extract", "transform", "load"]
    assert [p.key for p in spec.partitions] == ["1", "2", "3"], "range 分区"
    assert spec.stages[0].retry.limit == 2
    assert spec.stages[1].retry.limit == 0, "数据问题不重试"
    assert "json_schema" in spec.stages[1].checks


def test_load_spec_tts():
    spec = load_spec("pipelines/tts.yaml")
    assert spec.name == "tts"
    assert len(spec.partitions) == 2
    assert spec.partitions[0].variant == "cedar"
    assert spec.stages[1].checks == ["asr_three_state"]


def test_domain_check_registered_but_framework_agnostic():
    """框架内置检查不含领域知识；ASR 检查由插件注入。"""
    assert "json_schema" in available() and "row_count" in available()
    assert "asr_three_state" in available(), "领域检查通过插件注册"


# ---------------- 通用检查 ----------------

def test_json_schema_check():
    with tempfile.TemporaryDirectory() as d:
        good = Path(d) / "a.jsonl"
        good.write_text('{"order_id":"1","amount":1,"dt":"x"}\n', encoding="utf-8")
        bad = Path(d) / "b.jsonl"
        bad.write_text('{"order_id":"1"}\n', encoding="utf-8")
        assert run_check("json_schema", good, {"required": ["order_id", "amount", "dt"]}, {}).passed
        r = run_check("json_schema", bad, {"required": ["order_id", "amount", "dt"]}, {})
        assert not r.passed and "amount" in r.detail


def test_row_count_check():
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "a.jsonl"
        f.write_text("1\n2\n3\n", encoding="utf-8")
        assert run_check("row_count", f, {"expect": 3}, {}).passed
        assert not run_check("row_count", f, {"expect": 5}, {}).passed
        assert run_check("row_count", f, {"min": 1, "max": 3}, {}).passed
        assert not run_check("row_count", f, {"min": 5}, {}).passed


def test_file_nonempty_check():
    with tempfile.TemporaryDirectory() as d:
        empty = Path(d) / "e.txt"; empty.write_text("", encoding="utf-8")
        full = Path(d) / "f.txt"; full.write_text("x", encoding="utf-8")
        assert not run_check("file_nonempty", empty, {}, {}).passed
        assert run_check("file_nonempty", full, {}, {}).passed


# ---------------- 通用 runner（用假命令，不依赖外部服务） ----------------

def _fake_spec(workdir: Path, fail_extract: bool = False) -> PipelineSpec:
    return PipelineSpec(
        name="fake",
        partitions=[Partition("ds", "1"), Partition("ds", "2")],
        stages=[
            Stage(name="extract", command="mkdir -p out/extract && echo row > out/extract/{{partition.key}}.jsonl",
                  outputs=["raw"], artifacts={"raw": "out/extract/{{partition.key}}.jsonl"},
                  retry=RetryPolicy(limit=2), checks=["file_nonempty", "row_count"],
                  check_config=None),
            # 末 stage 也要配一个廉价检查：否则对账无法区分"跑完"和"跑对"
            Stage(name="load", command="mkdir -p out/load && echo done > out/load/{{partition.key}}.done",
                  dependencies=["extract"], outputs=["marker"],
                  artifacts={"marker": "out/load/{{partition.key}}.done"},
                  retry=RetryPolicy(limit=0), checks=["file_nonempty"]),
        ],
    )


def test_runner_happy_path_with_reconcile():
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        db = work / "state.db"
        spec = _fake_spec(work)
        # check_config 需要挂在 stage 上（框架读 stage.check_config）
        spec.stages[0].check_config = {"row_count": {"expect": 1}}
        r = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert r.ok, r.summary()
        assert len(r.runs) == 4, "2 stage × 2 partition"
        conn = st.connect(db)
        recon = st.reconcile_generic(conn, "ds", [(p.dataset, p.key, p.variant) for p in spec.partitions], final_stage="load")
        assert recon["ok"] and recon["expected"] == 2, recon
        conn.close()


def test_runner_skips_completed_partition():
    """断点续跑：第二次跑同类任务，已登记 completed 的不再执行（由命令层跳过）。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        db = work / "state.db"
        spec = _fake_spec(work)
        spec.stages[0].check_config = {"row_count": {"expect": 1}}
        # 第一次：只跑分区 1
        spec.partitions = [Partition("ds", "1")]
        Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        # 第二次：跑分区 1+2，对账应只缺 2（1 已在状态表里）
        spec.partitions = [Partition("ds", "1"), Partition("ds", "2")]
        r = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        conn = st.connect(db)
        recon = st.reconcile_generic(conn, "ds", [("ds", "1", ""), ("ds", "2", "")], final_stage="load")
        assert recon["missing"] == [], recon
        conn.close()


def test_runner_check_failure_blocks():
    """质量闸门不过 -> failed，对账抓得到（不是"绿着但不对"）。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        db = work / "state.db"
        spec = _fake_spec(work)
        spec.stages[0].check_config = {"row_count": {"expect": 99}}   # 故意不达标
        r = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert not r.ok
        assert any(x.status == "failed" for x in r.runs)
        conn = st.connect(db)
        recon = st.reconcile_generic(conn, "ds", [(p.dataset, p.key, p.variant) for p in spec.partitions], final_stage="load")
        # 上游 extract 检查不过 -> load 被依赖语义跳过 -> 末 stage 无记录 -> 对账报缺失
        assert not recon["ok"], recon
        assert len(recon["missing"]) == 2, recon
        # 下游必须被跳过，不能被下游成功掩盖上游失败
        assert {x.stage: x.status for x in r.runs}.get("load") == "skipped"
        conn.close()


def test_runner_retries_transient_failure():
    """瞬时失败按策略重试后成功。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        counter = work / "count"
        # 前两次失败、第三次成功（用文件计数模拟闪断）
        # ⚠️ 这里必须用字符串拼接，不能用 f-string：f-string 会把 {{partition.key}}
        #    转义成单花括号，导致模板渲染不到（框架侧无误，是测试自身的坑）
        cmd = ("n=$(cat " + str(counter) + " 2>/dev/null || echo 0); n=$((n+1)); "
               "echo $n > " + str(counter) + "; "
               "if [ $n -lt 3 ]; then echo boom >&2; exit 1; fi; "
               "mkdir -p out/extract && echo row > out/extract/{{partition.key}}.jsonl")
        spec = PipelineSpec(
            name="retry", partitions=[Partition("ds", "1")],
            stages=[Stage(name="extract", command=cmd, outputs=["raw"],
                          artifacts={"raw": "out/extract/{{partition.key}}.jsonl"},
                          retry=RetryPolicy(limit=3, duration="1s"), checks=["file_nonempty"])],
        )
        r = Runner(spec, workdir=work, verbose=False).run()
        assert r.ok, [x.detail for x in r.runs]
        assert counter.read_text().strip() == "3", "应重试到第 3 次才成功"


def test_runner_resume_skips_verified():
    """框架级断点续跑：已 verified 的 (stage×partition) 第二次跑必须跳过。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        db = work / "state.db"
        spec = _fake_spec(work)
        spec.stages[0].check_config = {"row_count": {"expect": 1}}
        # 第一次：全量
        r1 = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert r1.ok and len(r1.runs) == 4
        # 第二次：应全部 resume 跳过，且不产生新执行
        r2 = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert all(x.status == "skipped" for x in r2.runs), [x.status for x in r2.runs]
        assert all("already verified" in x.detail for x in r2.runs)
        # 跳过不算失败
        assert r2.ok
        # --force 应重新执行
        r3 = Runner(spec, state_db=str(db), workdir=work, verbose=False, force=True).run()
        assert any(x.status == "verified" for x in r3.runs), "force 必须重跑"


def test_runner_dependency_blocks_downstream():
    """上游失败 -> 下游被依赖语义阻断（不能被下游成功掩盖）。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        db = work / "state.db"
        spec = _fake_spec(work)
        spec.stages[0].check_config = {"row_count": {"expect": 99}}   # extract 必失败
        r = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert not r.ok
        by_stage = {}
        for x in r.runs:
            by_stage.setdefault(x.stage, set()).add(x.status)
        assert by_stage["extract"] == {"failed"}
        assert by_stage["load"] == {"skipped"}, "下游必须被阻断"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"  PASS {name}")
            except AssertionError as exc:
                failures += 1; print(f"  FAIL {name}: {exc}")
    print(f"\n{'框架测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
