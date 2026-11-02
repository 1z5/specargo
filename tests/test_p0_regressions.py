"""P0 回归测试：把"静默失败"变成可复现的断言。

这些用例对应一次只读审计里实测到的框架缺陷（每条都能在旧代码上复现失败）：

1. spec 写错（空 stages / 拼错字段 / 依赖名拼错 / 依赖成环 / 非法时长）时，
   旧实现静默当成"0 个任务"并退出码 0 —— 对用户是"跑完了"，对框架是假绿。
2. 没配 checks 的 stage：旧实现永远无法 resume（状态是 generated，只认 verified），
   且最终对账必然 not_verified → 管线永远失败。等于隐性强制每 stage 配闸门。
3. 编译器：spec.params 不进 workflow arguments、variant/dataset 扇出恒为空、
   check_config 不解析 → tts/dataset-build 的编译产物不可用。
4. 集群 gate：读节点本地路径、不 objstore get（靠调度恰好同节点才过）。

运行方式与其他套件一致（零依赖）：
    python3 tests/test_p0_regressions.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from runner import Runner                                    # noqa: E402
from spec import Partition, PipelineSpec, SpecError, Stage, load_spec  # noqa: E402
from spec2argo import compile_workflow, translate            # noqa: E402
import state as st                                           # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "pipelines"


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _write_spec(text: str) -> Path:
    d = tempfile.mkdtemp()
    f = Path(d) / "spec.yaml"
    f.write_text(text, encoding="utf-8")
    return f


def _expect_spec_error(text: str, needle: str = "") -> SpecError:
    spec_file = _write_spec(text)
    try:
        load_spec(spec_file)
    except SpecError as exc:
        if needle:
            assert needle in str(exc), f"错误信息里没有 {needle!r}: {exc}"
        return exc
    raise AssertionError(f"该 spec 应被拒绝，但 load_spec 成功了：{text[:100]}")


BASE_STAGE = "partitions: {from: list, items: [{dataset: ds, key: k1}]}\n"


# ---------------------------------------------------------------------------
# 1. spec 结构校验：配置错了必须喊出来
# ---------------------------------------------------------------------------

def test_reject_empty_spec():
    _expect_spec_error("name: empty\n", "stages")
    _expect_spec_error("", "")


def test_reject_unknown_top_level_and_stage_fields():
    """拼错的字段不能被静默丢弃（旧实现 stages 拼成 stage 会得到 0 个任务且成功）。"""
    _expect_spec_error(
        "name: x\nstage: [{name: s, command: echo}]\n", "未知字段")
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n  - {name: s, command: echo, retreys: {limit: 3}}\n",
        "retreys")
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n  - {name: s, command: echo, timeout: 30x}\n",
        "不是有效时长")


def test_reject_bad_dependencies_and_cycles():
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n  - {name: s, command: echo, dependencies: [frist]}\n",
        "不存在的 stage")
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n"
        "  - {name: a, command: echo, dependencies: [b]}\n"
        "  - {name: b, command: echo, dependencies: [a]}\n",
        "成环")


def test_reject_unsupported_when():
    """when 只被解析、没有任何执行方消费——必须显式拒绝，不能留半个特性。"""
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n  - {name: s, command: echo, when: 'a == b'}\n",
        "when")


def test_reject_bad_retry_policy_and_check_target():
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n"
        "  - {name: s, command: echo, retry: {policy: Sometimes}}\n",
        "retry.policy")
    _expect_spec_error(
        "name: x\n" + BASE_STAGE + "stages:\n"
        "  - {name: s, command: echo, checks: [file_nonempty]}\n",
        "checks")


# ---------------------------------------------------------------------------
# 2. 无 checks 的 stage：可 resume、可对账（旧实现两件事都做不到）
# ---------------------------------------------------------------------------

def _no_check_spec() -> PipelineSpec:
    return PipelineSpec(
        name="nocheck",
        partitions=[Partition("ds", "1")],
        stages=[Stage(
            name="work",
            command="mkdir -p out && date +%s%N > out/marker.txt",
            outputs=["marker"],
            artifacts={"marker": "out/marker.txt"},
        )],
    )


def test_no_checks_pipeline_resumes_and_reconciles():
    with tempfile.TemporaryDirectory() as d:
        work = Path(d)
        db = work / "state.db"
        spec = _no_check_spec()

        r1 = Runner(spec, state_db=str(db), workdir=work, verbose=False)
        r1.run()
        assert r1.result.ok, r1.result.summary()
        assert r1.reconcile_ok, "没有质量闸门的管线也应按 generated 判交付"
        marker1 = (work / "out" / "marker.txt").read_text()

        time.sleep(0.02)
        r2 = Runner(spec, state_db=str(db), workdir=work, verbose=False)
        r2.run()
        assert all(x.status == "skipped" for x in r2.result.runs), \
            [x.status for x in r2.result.runs]
        assert r2.result.ok and r2.reconcile_ok
        assert (work / "out" / "marker.txt").read_text() == marker1, \
            "resume 跳过后不应重新执行命令"

        # --force 必须仍然能重跑
        r3 = Runner(spec, state_db=str(db), workdir=work, verbose=False, force=True)
        r3.run()
        assert any(x.status == "generated" for x in r3.result.runs), \
            [x.status for x in r3.result.runs]


def test_checks_stage_still_requires_verified():
    """有 checks 的 stage 不能因为"generated 也算终态"而放松对账。"""
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "s.db")
        st.upsert_generic_item(conn, dataset="ds", partition="1", variant="",
                               status=st.GENERATED, stage="gated")
        recon = st.reconcile_generic(conn, "ds", [("ds", "1", "")],
                                     final_stage="gated")
        assert not recon["ok"] and recon["not_verified"] == ["1"], recon
        conn.close()


# ---------------------------------------------------------------------------
# 3. 编译器：params / dataset / variant / check_config
# ---------------------------------------------------------------------------

def _compile(path: Path, **kwargs):
    return compile_workflow(load_spec(path), **kwargs)


def test_compile_emits_workflow_arguments():
    wf = _compile(PIPELINES / "dataset-build.yaml")
    args = {p["name"]: p["value"] for p in wf["spec"]["arguments"]["parameters"]}
    assert args["python"] == "python3"
    assert args["app_dir"] == "/app"
    assert args["work_dir"] == "/output"
    assert args["shards"] == "2", args


def test_compile_has_no_unresolved_spec_templates():
    """CI 的 argo lint 不解析模板变量，所以必须在编译期自己断言零残留。"""
    for f in sorted(PIPELINES.glob("*.yaml")):
        wf = _compile(f)
        text = yaml.safe_dump(wf, allow_unicode=True, sort_keys=False)
        for bad in ("{{params.", "{{partition.", "{{artifact}}"):
            assert bad not in text, f"{f.name} 编译产物残留 {bad}"


def test_compile_list_mode_preserves_dataset_and_variant():
    """list 模式扇出必须传对象（key/dataset/variant），不能只传 key 丢维度。"""
    spec_file = _write_spec(
        "name: listdemo\n"
        "k8s: {image: demo:ci}\n"
        "params: {python: python3, work_dir: out}\n"
        "partitions:\n"
        "  from: list\n"
        "  items: [{dataset: ds_a, key: k1}, {dataset: ds_b, key: k2}]\n"
        "variants: ['v1', 'v2']\n"
        "stages:\n"
        "  - name: s1\n"
        "    command: \"echo {{partition.dataset}} {{partition.key}} {{partition.variant}} > "
        "{{params.work_dir}}/{{partition.key}}.txt\"\n"
        "    outputs: [o]\n"
        "    artifacts: {o: \"{{params.work_dir}}/{{partition.key}}.txt\"}\n"
    )
    wf = compile_workflow(load_spec(spec_file))
    main = next(t for t in wf["spec"]["templates"] if t["name"] == "main")
    task = next(t for t in main["dag"]["tasks"] if t["name"] == "s1")
    items = json.loads(task["withParam"])
    assert len(items) == 4, items      # 2 dataset × 2 variant
    assert {(i["dataset"], i["key"], i["variant"]) for i in items} == {
        ("ds_a", "k1", "v1"), ("ds_a", "k1", "v2"),
        ("ds_b", "k2", "v1"), ("ds_b", "k2", "v2"),
    }
    values = {p["name"]: p["value"] for p in task["arguments"]["parameters"]}
    assert values["partition"] == "{{item.key}}"
    assert values["dataset"] == "{{item.dataset}}"
    assert values["variant"] == "{{item.variant}}"


def test_compile_discover_emits_object_items():
    wf = _compile(PIPELINES / "etl-demo.yaml")
    disc = next(t for t in wf["spec"]["templates"] if t["name"] == "discover-scan")
    script = disc["script"]["args"][0]
    assert "json.dumps(items" in script
    assert "'key': k" in script and "'variant': v" in script
    env = {e["name"]: e["value"] for e in disc["script"]["env"]}
    assert env["SPEC_DATASET"] == "dwd_order"
    assert json.loads(env["SPEC_VARIANTS"]) == [""]


def test_compile_gate_fetches_artifact_before_check():
    """gate 必须先 objstore get，再跑 checks——不能读节点本地文件。"""
    wf = _compile(PIPELINES / "etl-demo.yaml")
    gate = next(t for t in wf["spec"]["templates"] if t["name"] == "gate-extract")
    script = gate["container"]["args"][0]
    get_pos = script.find("objstore.py get")
    check_pos = script.find("run_check")
    assert get_pos >= 0 and check_pos > get_pos, script
    assert "runs/$TTS_RUN_ID/{{inputs.parameters.partition}}/raw.jsonl" in script
    env = {e["name"]: e["value"] for e in gate["container"]["env"]}
    assert "TTS_RUN_ID" in env and "OBJSTORE_ENDPOINT" in env


def test_compile_resolves_check_config_params():
    """gate 配置里的 {{params.*}}/{{artifact}} 必须在编译期解析（gate 没有 runner context）。"""
    spec_file = _write_spec(
        "name: gatecfg\n"
        "k8s: {image: demo:ci}\n"
        "params: {python: python3, app_dir: '', work_dir: out, marker: ok}\n"
        "partitions: {from: list, items: [{dataset: ds, key: k1}]}\n"
        "stages:\n"
        "  - name: s1\n"
        "    command: \"mkdir -p out && echo x > out/k1.txt\"\n"
        "    outputs: [o]\n"
        "    artifacts: {o: \"{{params.work_dir}}/k1.txt\"}\n"
        "    checks: [command]\n"
        "    check_config:\n"
        "      command:\n"
        "        run: \"test -s {{artifact}} && grep -q {{params.marker}} {{params.app_dir}}/x\"\n"
    )
    wf = compile_workflow(load_spec(spec_file))
    gate = next(t for t in wf["spec"]["templates"] if t["name"] == "gate-s1")
    script = gate["container"]["args"][0]
    cfg_text = script.split("<<'JSON'\n", 1)[1].split("\nJSON\n", 1)[0]
    run_cmd = json.loads(cfg_text)["command"]["run"]
    assert "{{params." not in run_cmd and "{{artifact}}" not in run_cmd, run_cmd
    assert "test -s /output/k1.txt" in run_cmd, run_cmd      # {{artifact}} 已解析
    assert "grep -q ok /app/x" in run_cmd, run_cmd           # params 已解析为字面量


def test_compile_quotes_artifact_paths_with_spaces():
    """artifact 路径含空格时，自动生成的 objstore --file 必须被引号保护。"""
    spec_file = _write_spec(
        "name: spacepath\n"
        "k8s: {image: demo:ci}\n"
        "params: {work_dir: out}\n"
        "partitions: {from: list, items: [{dataset: ds, key: k1}]}\n"
        "stages:\n"
        "  - name: s1\n"
        "    command: \"mkdir -p 'out dir' && echo x > 'out dir/k1.txt'\"\n"
        "    outputs: [o]\n"
        "    artifacts: {o: \"out dir/{{partition.key}}.txt\"}\n"
    )
    wf = compile_workflow(load_spec(spec_file))
    tmpl = next(t for t in wf["spec"]["templates"] if t["name"] == "s1")
    script = tmpl["container"]["args"][0]
    assert "--file 'out dir/{{inputs.parameters.partition}}.txt'" in script, script


def test_compile_gate_declares_dataset_and_variant_inputs():
    """gate 可能引用 dataset/variant，模板 inputs 与 task args 必须都声明。"""
    wf = _compile(PIPELINES / "etl-demo.yaml")
    gate = next(t for t in wf["spec"]["templates"] if t["name"] == "gate-extract")
    declared = [p["name"] for p in gate["inputs"]["parameters"]]
    assert declared == ["partition", "dataset", "variant"], declared
    main = next(t for t in wf["spec"]["templates"] if t["name"] == "main")
    gate_task = next(t for t in main["dag"]["tasks"] if t["name"] == "gate-extract")
    args = {p["name"]: p["value"] for p in gate_task["arguments"]["parameters"]}
    assert args == {"partition": "{{item.key}}", "dataset": "{{item.dataset}}",
                    "variant": "{{item.variant}}"}, args


def test_dataset_pipeline_self_verifies_in_stage_for_cluster_gate():
    """dataset-build 的深度自校验必须在 stage 命令内，gate 只依赖可跨 Pod 取回的 manifest。

    回归：旧版把 manifest.py verify 放在 gate 的 command check 里，而它依赖 shards
    文件——shards 不是单文件 artifact、不会被 gate get，跨 Pod 必红。
    """
    spec = load_spec(PIPELINES / "dataset-build.yaml")
    shard_stage = next(s for s in spec.stages if s.name == "shard")
    assert "manifest.py verify" in shard_stage.command, shard_stage.command
    assert shard_stage.checks == ["file_nonempty", "json_schema"], shard_stage.checks
    wf = compile_workflow(spec)
    gate = next(t for t in wf["spec"]["templates"] if t["name"] == "gate-shard")
    script = gate["container"]["args"][0]
    assert "manifest.py verify" not in script, script


def test_unicode_partition_keys_allowed():
    """分区 key 允许 Unicode 字母（中文分区名可用），但拒绝 shell 元字符。"""
    spec_file = _write_spec(
        "name: zh-keys\n"
        "k8s: {image: demo:ci}\n"
        "params: {work_dir: out}\n"
        "partitions: {from: list, items: [{dataset: 数据, key: 甲}]}\n"
        "stages:\n"
        "  - name: s\n"
        "    command: \"echo {{partition.key}} > {{params.work_dir}}/{{partition.key}}.txt\"\n"
        "    outputs: [o]\n"
        "    artifacts: {o: \"{{params.work_dir}}/{{partition.key}}.txt\"}\n"
    )
    spec = load_spec(spec_file)
    assert spec.partitions[0].key == "甲"
    wf = compile_workflow(spec)
    main = next(t for t in wf["spec"]["templates"] if t["name"] == "main")
    task = next(t for t in main["dag"]["tasks"] if t["name"] == "s")
    items = json.loads(task["withParam"])
    assert items == [{"key": "甲", "dataset": "数据", "variant": ""}], items


def test_reject_unsafe_param_names_and_partition_keys():
    _expect_spec_error(
        "name: x\nparams: {'foo-bar': 1}\npartitions: {from: list, items: [k]}\n"
        "stages:\n  - {name: s, command: echo}\n", "参数名")
    _expect_spec_error(
        "name: x\npartitions: {from: list, items: [{dataset: ds, key: 'a b'}]}\n"
        "stages:\n  - {name: s, command: echo}\n", "不安全字符")
    _expect_spec_error(
        "name: x\nvariants: []\npartitions: {from: list, items: [k]}\n"
        "stages:\n  - {name: s, command: echo}\n", "variants")


def test_reject_dot_partition_keys_and_whitespace_discover():
    """路径段类 key（. / ..）与含空格的 discover 输出必须在加载期拒绝。

    - "." / ".." 作为 key 会成为路径段（本地 store 拒绝、S3 当字面 key，行为不一致）；
    - discover 输出含空格时，加载期 split() 会误拆成两个"合法"分区，与集群脚本
      按行拒绝的语义不一致——统一改成按行解析 + charset 校验。
    """
    for bad_key in (".", ".."):
        _expect_spec_error(
            f"name: x\npartitions: {{from: list, items: [{{dataset: ds, key: '{bad_key}'}}]}}\n"
            "stages:\n  - {name: s, command: echo}\n", "不安全字符")
    _expect_spec_error(
        "name: x\npartitions: {from: discover, command: \"printf 'a b\\n'\"}\n"
        "stages:\n  - {name: s, command: echo}\n", "不安全字符")


def test_reject_non_integer_retry_numbers():
    """retry.limit/factor 必须是整数：2.5 不能被 int() 静默截断成 2。"""
    for retry in ("{limit: 2.5}", "{backoff: {factor: 1.5}}", "{policy: 3}"):
        _expect_spec_error(
            f"name: x\npartitions: {{from: list, items: [k]}}\n"
            f"stages:\n  - {{name: s, command: echo, retry: {retry}}}\n",
            "retry")
    # 2.0 这类整数值浮点仍可接受（等价于 2）
    spec_file = _write_spec(
        "name: x\npartitions: {from: list, items: [k]}\n"
        "stages:\n  - {name: s, command: echo, retry: {limit: 2.0, backoff: {factor: 2.0}}}\n")
    spec = load_spec(spec_file)
    assert spec.stages[0].retry.limit == 2 and spec.stages[0].retry.factor == 2


def test_reject_non_topological_consumes_and_unknown_k8s():
    _expect_spec_error(
        "name: x\nk8s: {imag: demo:ci}\npartitions: {from: list, items: [k]}\n"
        "stages:\n  - {name: s, command: echo}\n", "未知字段")
    _expect_spec_error(
        "name: x\npartitions: {from: list, items: [k]}\n"
        "stages:\n"
        "  - {name: a, command: echo, outputs: [o], artifacts: {o: out/o.txt}}\n"
        "  - {name: b, command: echo, consumes: [o]}\n",
        "不是它的上游依赖")


def test_runner_orders_stages_topologically_and_cascades_blocking():
    """非拓扑序声明 + 上游失败时：按依赖顺序执行，且阻断要级联到隔代下游。"""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d)
        spec = PipelineSpec(
            name="order",
            partitions=[Partition("ds", "1")],
            stages=[
                Stage(name="c", command="echo c", dependencies=["b"]),
                Stage(name="b", command="exit 1", dependencies=["a"]),
                Stage(name="a", command="echo a"),
            ],
        )
        r = Runner(spec, workdir=work, verbose=False)
        r.run()
        # 执行顺序必须是 a -> b -> c
        assert [x.stage for x in r.result.runs] == ["a", "b", "c"], \
            [x.stage for x in r.result.runs]
        by_stage = {x.stage: x for x in r.result.runs}
        assert by_stage["a"].status == "generated"
        assert by_stage["b"].status == "failed"
        # c 依赖 b，而 b 被 a 阻断；阻断必须级联，c 不能照跑
        assert by_stage["c"].status == "skipped", by_stage["c"]
        assert by_stage["c"].detail.startswith("blocked by b"), by_stage["c"].detail


def test_translate_maps_partition_key_to_partition_param():
    ctx = "runs/x/{{partition.key}}/{{partition.dataset}}/{{partition.variant}}"
    assert translate(ctx) == (
        "runs/x/{{inputs.parameters.partition}}/"
        "{{inputs.parameters.dataset}}/{{inputs.parameters.variant}}")


# ---------------------------------------------------------------------------
# 自测入口（与其他套件一致，不依赖 pytest）
# ---------------------------------------------------------------------------

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
            except Exception as exc:                 # noqa: BLE001 - 自测入口要报出来
                failures += 1
                print(f"  ERROR {name}: {exc!r}")
    print(f"\n{'P0 回归测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
