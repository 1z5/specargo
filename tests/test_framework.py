"""框架层测试：声明式 spec、通用 runner、插件检查、对账。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from spec import Partition, PipelineSpec, Stage, RetryPolicy, build_context, load_spec, parse_duration, render  # noqa: E402
from checks import CheckResult, available, register, run_check  # noqa: E402
from runner import Runner  # noqa: E402
import state as st  # noqa: E402

# 测试里多处需要仓根与应用目录
ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"


# ---------------- spec ----------------

def test_parse_duration():
    assert parse_duration("30m") == 1800
    assert parse_duration("1h") == 3600
    assert parse_duration("45s") == 45
    assert parse_duration("2h30m") == 9000


def test_render_supports_dotted_paths():
    ctx = {"partition": {"key": "p1", "variant": "v1"}, "params": {"n": 3}}
    assert render("out/{{partition.key}}_{{partition.variant}}.jsonl", ctx) == "out/p1_v1.jsonl"
    assert render("limit={{params.n}}", ctx) == "limit=3"


def test_render_strict_rejects_unknown_variable():
    """未定义变量必须报错，不能静默替换成空串（残缺命令最难查）。"""
    from spec import SpecError
    ctx = {"params": {"n": 3}}
    try:
        render("--run-id {{params.run_id}}", ctx)
        raise AssertionError("应抛出 SpecError")
    except SpecError as exc:
        assert "params.run_id" in str.exc if False else "params.run_id" in str(exc), exc
    # 显式关闭严格模式时才退回空串（兼容旧调用）
    assert render("{{missing}}x", ctx, strict=False) == "x"


def test_load_spec_rejects_undeclared_param():
    """YAML 用了未声明参数必须在加载期炸掉。"""
    import tempfile
    from spec import SpecError, load_spec

    bad = """
name: bad
params: {python: python3}
partitions: {from: list, items: [{dataset: d, key: k}]}
stages:
  - name: s1
    command: "{{params.python}} x.py --run-id {{params.run_id}}"
"""
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "bad.yaml"
        f.write_text(bad, encoding="utf-8")
        try:
            load_spec(f)
            raise AssertionError("应抛出 SpecError")
        except SpecError as exc:
            assert "run_id" in str(exc), exc


def test_load_spec_etl_demo():
    spec = load_spec("pipelines/etl-demo.yaml")
    assert spec.name == "etl-demo"
    assert [s.name for s in spec.stages] == ["extract", "transform", "load"]
    assert [p.key for p in spec.partitions] == ["1", "2", "3"], "range 分区"
    assert spec.stages[0].retry.limit == 2
    assert spec.stages[1].retry.limit == 0, "数据问题不重试"
    assert "json_schema" in spec.stages[1].checks


def test_domain_plugins_are_not_imported_by_framework():
    """框架不 import 领域插件；领域检查只能通过 spec.plugins 显式加载。"""
    assert "json_schema" in available() and "row_count" in available()
    # 框架模块 import 完不应带着领域检查（领域插件在 examples/ 下，由 plugins 显式加载）
    assert "asr_three_state" not in available(), "框架层不应反向 import 领域插件"

    @register("test_only_check")
    def _test_only(artifact, config, ctx):
        return CheckResult(True, metric="ok")

    assert "test_only_check" in available()


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
        assert all("already completed" in x.detail for x in r2.runs)
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


def test_reconcile_groups_by_variant():
    """多 variant 必须分组对账：A variant 缺的条目不能被 B variant 的记录掩盖（假绿回归）。"""
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        expected = [("ds", "0", "male"), ("ds", "1", "male"),
                    ("ds", "0", "female"), ("ds", "1", "female")]
        # 只登记 female 的全部 + male 的 0（male 缺 1）
        for k in ("0", "1"):
            st.upsert_item(conn, dataset="ds", partition=k, variant="female",
                                   status=st.VERIFIED, stage="verify")
        st.upsert_item(conn, dataset="ds", partition="0", variant="male",
                               status=st.VERIFIED, stage="verify")

        r = st.reconcile_generic(conn, "ds", expected, final_stage="verify")
        assert r["expected"] == 4, r
        # male:1 缺失必须被报出来，且带 variant 前缀便于定位
        assert r["missing"] == ["male:1"], r
        assert r["ok"] is False

        # 补齐 male:1 后应通过
        st.upsert_item(conn, dataset="ds", partition="1", variant="male",
                               status=st.VERIFIED, stage="verify")
        r2 = st.reconcile_generic(conn, "ds", expected, final_stage="verify")
        assert r2["ok"] is True and r2["verified"] == 4, r2


def test_reconcile_single_variant_unchanged():
    """单 variant 行为不回退。"""
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        expected = [("ds", "0", ""), ("ds", "1", "")]
        st.upsert_item(conn, dataset="ds", partition="0", variant="",
                               status=st.VERIFIED, stage="load")
        r = st.reconcile_generic(conn, "ds", expected, final_stage="load")
        assert r["missing"] == ["1"] and r["ok"] is False, r


def test_runner_records_real_attempt_count():
    """attempt 必须是真实重试次数，不是恒为 0（否则状态表该列无意义）。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d); (work / "out").mkdir()
        counter = work / "n"
        # 前两次失败、第三次成功 -> 应记录 attempt=3
        cmd = ("n=$(cat " + str(counter) + " 2>/dev/null || echo 0); n=$((n+1)); "
               "echo $n > " + str(counter) + "; "
               "if [ $n -lt 3 ]; then exit 1; fi; "
               "mkdir -p out/e && echo x > out/e/1.jsonl")
        spec = PipelineSpec(
            name="a", partitions=[Partition("ds", "1")],
            stages=[Stage(name="e", command=cmd, outputs=["raw"],
                          artifacts={"raw": "out/e/1.jsonl"},
                          retry=RetryPolicy(limit=3, duration="1s"),
                          checks=["file_nonempty"])],
        )
        r = Runner(spec, workdir=work, verbose=False).run()
        assert r.ok, [x.detail for x in r.runs]
        assert r.runs[0].attempts == 3, r.runs[0]


# ===================== spec2argo 编译器 =====================

def _load_etl_spec():
    return load_spec(str(Path(__file__).resolve().parents[1] / "pipelines" / "etl-demo.yaml"))


def test_compile_produces_valid_dag_shape():
    """编译产物：DAG 形状、模板集合、依赖关系正确。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    assert wf["kind"] == "Workflow"
    assert wf["spec"]["entrypoint"] == "main"

    tmpls = {t["name"] for t in wf["spec"]["templates"]}
    # 每个 stage 一个模板 + 每个带 checks 的 stage 一个 gate 模板 + discover + main
    assert {"main", "discover-scan", "extract", "transform", "load",
            "gate-extract", "gate-transform", "gate-load"} <= tmpls, tmpls

    dag = next(t for t in wf["spec"]["templates"] if t["name"] == "main")["dag"]
    tasks = {t["name"]: t for t in dag["tasks"]}
    assert tasks["extract"]["dependencies"] == ["discover"]
    assert tasks["transform"]["dependencies"] == ["discover", "extract"]
    # 扇出引用 discover 输出，不是写死列表
    assert "tasks.discover.outputs.parameters.items" in tasks["extract"]["withParam"]


def test_compile_preserves_retry_policy():
    """retryStrategy 分层必须保留：extract 可重试，transform limit 0。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    by_name = {t["name"]: t for t in wf["spec"]["templates"]}
    assert by_name["extract"]["retryStrategy"]["limit"] == "2"
    assert by_name["extract"]["retryStrategy"]["retryPolicy"] == "OnTransientError"
    assert by_name["transform"]["retryStrategy"]["limit"] == "0"
    # gate 永远不重试（检查失败是数据问题）
    assert by_name["gate-extract"]["retryStrategy"]["limit"] == "0"


def test_compile_wires_object_store_contract():
    """跨 Pod 数据必须接线成 get→执行→put，且 key 一致。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    by_name = {t["name"]: t for t in wf["spec"]["templates"]}

    extract_script = by_name["extract"]["container"]["args"][0]
    assert "objstore.py put" in extract_script and "raw.jsonl" in extract_script

    transform_script = by_name["transform"]["container"]["args"][0]
    assert "objstore.py get" in transform_script and "raw.jsonl" in transform_script
    assert "objstore.py put" in transform_script and "clean.jsonl" in transform_script

    # 上下游 key 必须一致（否则下游取不到上游产物）
    assert 'runs/$PIPELINE_RUN_ID/{{inputs.parameters.partition}}/raw.jsonl' in extract_script
    assert 'runs/$PIPELINE_RUN_ID/{{inputs.parameters.partition}}/raw.jsonl' in transform_script

    # 产物路径翻到集群挂载点，不能用相对路径
    assert "/output/" in transform_script
    assert "out/" not in transform_script.replace("/output/", "")


def test_compile_emits_gate_for_checks():
    """有 checks 的 stage 必须生成独立 gate 任务，且调用框架 CLI 跑真实检查。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    by_name = {t["name"]: t for t in wf["spec"]["templates"]}
    gate = by_name["gate-extract"]["container"]["args"][0]
    # gate 不再是内联 Python 片段，而是调用本地也可单测的 CLI
    assert "pipeline_cli.py gate" in gate
    assert "row_count" in gate and "file_nonempty" in gate
    assert "--check-config-file" in gate
    assert "from checks import run_check" not in gate


def test_compile_discover_outputs_json_array():
    """discover 必须输出 JSON 数组字符串，否则 withParam 展不开。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    disc = next(t for t in wf["spec"]["templates"] if t["name"] == "discover-scan")
    outs = disc["outputs"]["parameters"]
    assert outs[0]["name"] == "items"
    assert outs[0]["valueFrom"]["path"] == "/tmp/out/items.json"
    assert "json.dumps" in disc["script"]["args"][0]


def test_compile_is_deterministic():
    """同一 spec 编译两次结果一致（可做 drift 检测）。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    a = compile_workflow(spec)
    b = compile_workflow(spec)
    assert a == b


# ===================== 真实 pipeline 的回归防线 =====================
# 这几条是为了防住"CI 绿但旗舰 pipeline 跑不通"这类回归：
# 之前的 bug 是 YAML 声明了 app_dir: "" 而 Runner 用 setdefault 注入不覆盖，
# 导致渲染出 `python3 /etl_steps.py`，但当时没有任何测试真跑过真实 pipeline。

def test_runner_overrides_empty_injected_params():
    """Runner 必须覆盖 YAML 里声明的空占位（app_dir/run_id），不能用 setdefault。"""
    from runner import Runner

    spec = _load_etl_spec()
    # 前置：YAML 确实声明了空串占位（这是 bug 的触发条件）
    assert spec.params.get("app_dir") == "", spec.params
    assert spec.params.get("run_id") == "", spec.params

    Runner(spec, verbose=False)

    assert spec.params["app_dir"], "app_dir 未被注入（渲染会得到 /etl_steps.py）"
    assert Path(spec.params["app_dir"]).is_dir(), spec.params["app_dir"]
    assert spec.params["run_id"], "run_id 未被注入（状态表追溯会失效）"


def test_real_pipeline_commands_render_without_empty_paths():
    """真实 pipeline 的每条命令渲染后，不能出现空路径/未解析变量。"""
    from runner import Runner
    from spec import build_context, render

    spec = _load_etl_spec()
    Runner(spec, verbose=False)
    part = spec.partitions[0]
    ctx = build_context(spec, part, spec.params)

    for stage in spec.stages:
        cmd = render(stage.command, ctx)
        assert "{{" not in cmd, (stage.name, cmd)
        # 命令里出现的 .py 脚本路径必须真实存在：
        # app_dir 为空时会渲染成 `/etl_steps.py`（根目录），该文件不存在
        import re
        for script in re.findall(r"(\S+\.py)", cmd):
            assert Path(script).is_file(), (stage.name, f"脚本路径不存在: {script}", cmd)


def test_real_pipeline_runs_end_to_end_green():
    """端到端：真正执行 pipelines/etl-demo.yaml，必须对账全绿。

    这是最重要的一条——它把"旗舰 demo 能跑"变成 CI 断言。
    """
    from runner import Runner
    from state import connect, reconcile_generic

    with tempfile.TemporaryDirectory() as d:
        work = Path(d)
        db = work / "state.db"
        spec = _load_etl_spec()
        result = Runner(spec, state_db=str(db), workdir=work, verbose=False).run()
        assert result.ok, [(r.stage, r.status, r.detail[:120]) for r in result.runs]

        conn = connect(db)
        recon = reconcile_generic(
            conn, "dwd_order",
            [(p.dataset, p.key, p.variant) for p in spec.partitions],
            final_stage=spec.stages[-1].name,
        )
        assert recon["ok"], recon
        assert recon["expected"] == 3 and recon["verified"] == 3, recon
        conn.close()


def test_compile_workflow_uses_spec_name():
    """编译产物的 workflow 名必须是 spec.name（曾因循环变量遮蔽变成 consumed artifact 名）。"""
    from spec2argo import compile_workflow

    spec = _load_etl_spec()
    wf = compile_workflow(spec)
    assert wf["metadata"]["name"] == spec.name == "etl-demo", wf["metadata"]["name"]

    # 显式传 name 必须生效（不被内部循环变量覆盖）
    wf2 = compile_workflow(spec, name="custom-name")
    assert wf2["metadata"]["name"] == "custom-name", wf2["metadata"]["name"]


# ===================== 通用数据集构建（manifest / 清洗 / 分片）=====================

def test_shard_assignment_is_deterministic():
    """分片必须确定性：同一个 key 永远同一片，且与遍历顺序无关（训练可复现的前提）。"""
    from manifest import shard_of

    keys = ["p1-%04d" % i for i in range(200)]
    first = {k: shard_of(k, 4) for k in keys}
    # 换顺序重算，结果必须一致
    second = {k: shard_of(k, 4) for k in reversed(keys)}
    assert first == second
    # 分片号落在合法区间，且没有被完全打偏（4 片应有基本均匀的分布）
    assert set(first.values()) <= {0, 1, 2, 3}
    assert len(set(first.values())) == 4, set(first.values())


def test_manifest_verify_detects_corruption_and_missing_shard():
    """manifest 校验必须能抓出：分片被改、分片缺失、条数对不上。"""
    import json
    import tempfile
    from manifest import Manifest, ShardInfo, file_sha256

    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        (base / "shards").mkdir()
        f0 = base / "shards" / "s0.jsonl"
        f1 = base / "shards" / "s1.jsonl"
        f0.write_text('{"id":"a"}\n{"id":"b"}\n', encoding="utf-8")
        f1.write_text('{"id":"c"}\n', encoding="utf-8")

        m = Manifest(dataset="t", version="v1", shard_count=2, rows=3, shards=[
            ShardInfo(0, "shards/s0.jsonl", 2, f0.stat().st_size, file_sha256(f0)),
            ShardInfo(1, "shards/s1.jsonl", 1, f1.stat().st_size, file_sha256(f1)),
        ])
        assert m.verify(base) == [], m.verify(base)

        # 改一个字节 -> 校验和/大小不符
        f0.write_text('{"id":"a"}\n{"id":"X"}\n', encoding="utf-8")
        assert m.verify(base), "内容被改但校验通过"

        # 删掉分片 -> 分片缺失
        f0.write_text('{"id":"a"}\n{"id":"b"}\n', encoding="utf-8")
        f1.unlink()
        problems = m.verify(base)
        assert any("缺失" in p for p in problems), problems


def test_clean_dedups_and_quarantines():
    """清洗：去重按 id、坏行进隔离区且不带进合格集。"""
    import json
    import subprocess
    import sys as _sys

    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        raw = base / "raw.jsonl"
        rows = [
            {"id": "a", "text": "  多余   空白  "},
            {"id": "a", "text": "重复主键"},
            {"id": "", "text": "空主键"},
            {"id": "b"},
            {"id": "c", "text": "正常一条"},
        ]
        raw.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
                       encoding="utf-8")
        out, quar = base / "clean.jsonl", base / "quarantine.jsonl"
        rc = subprocess.run(
            [_sys.executable, str(APP_DIR / "dataset_steps.py"), "clean",
             "--in", str(raw), "--out", str(out), "--quarantine", str(quar)],
            capture_output=True, text=True,
        )
        assert rc.returncode == 0, rc.stderr
        kept = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert [r["id"] for r in kept] == ["a", "c"], kept          # 去重 + 排序
        assert kept[0]["text"] == "多余 空白", kept[0]              # 空白已折叠
        bad = [json.loads(l) for l in quar.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(bad) == 3 and all("rule" in b for b in bad), bad  # 坏行带规则编号


def test_dataset_build_pipeline_end_to_end():
    """真实跑 dataset-build 管线：清洗→分片→manifest 自校验，全绿。"""
    from runner import Runner

    spec = load_spec(str(ROOT / "pipelines" / "dataset-build.yaml"))
    with tempfile.TemporaryDirectory() as d:
        result = Runner(spec, state_db=str(Path(d) / "s.db"), workdir=Path(d), verbose=False).run()
        assert result.ok, [(r.stage, r.status, r.detail[:100]) for r in result.runs]

        # manifest 落盘且能被独立校验（模拟下游对账）
        import subprocess, sys as _sys
        manifests = sorted((Path(d) / "out" / "manifest").glob("*.json"))
        assert manifests, "没有产出 manifest"
        for mf in manifests:
            rc = subprocess.run(
                [_sys.executable, str(APP_DIR / "manifest.py"), "verify",
                 "--manifest", str(mf), "--base-dir", str(Path(d) / "out")],
                capture_output=True, text=True,
            )
            assert rc.returncode == 0, rc.stdout + rc.stderr


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
