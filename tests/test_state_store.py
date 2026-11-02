"""StateStore 协议 + 集群 resume：终态判定只有一份，两个执行目标共用。

本套件锁住三件事：

1. **判定唯一**：本地 runner 与集群编译产物都用 ``resume_policy`` 的
   ``stage_terminal_statuses``，不各写一份（历史上正是双份实现导致漂移）；
2. **协议一致**：``SqliteStateStore`` 与 ``JournalStateStore`` 的 item 读写
   语义相同（同四元组主键、同状态值），可互换；
3. **集群 resume 可用**：``k8s.resume: true`` 的编译产物会在 stage 前查状态日志，
   已终态则跳过（退出码 0），未终态/无记录照常执行（退出码 1）。
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"
import importlib.util                                                  # noqa: E402
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

import specargo                                                        # noqa: E402
from specargo.journal import get_item_status, put_status               # noqa: E402
from specargo.resume_policy import (PLAIN_TERMINAL, GATED_TERMINAL,     # noqa: E402
                                    describe_terminal, is_terminal,
                                    stage_terminal_statuses)
from specargo.state import (GENERATED, VERIFIED, JournalStateStore,     # noqa: E402
                            SqliteStateStore, connect)
from specargo.spec import load_spec                                    # noqa: E402
from specargo.spec2argo import compile_workflow                         # noqa: E402


class local_objstore:
    def __init__(self, root: Path):
        self.root = root
        self.old = {k: os.environ.get(k) for k in
                    ("OBJSTORE_KIND", "OBJSTORE_ROOT", "OBJSTORE_PREFIX",
                     "PIPELINE_RUN_ID")}

    def __enter__(self):
        os.environ["OBJSTORE_KIND"] = "local"
        os.environ["OBJSTORE_ROOT"] = str(self.root)
        os.environ.pop("OBJSTORE_PREFIX", None)
        return self

    def __exit__(self, *exc):
        for key, value in self.old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# ---------------------------------------------------------------------------
# 1. 终态判定：一份定义
# ---------------------------------------------------------------------------

def test_terminal_status_rule_is_explicit():
    """有闸门只认 verified；无闸门 generated 即终态（不能隐性强制配闸门）。"""
    assert stage_terminal_statuses(True) == (VERIFIED,) == GATED_TERMINAL
    assert stage_terminal_statuses(False) == (GENERATED, VERIFIED) == PLAIN_TERMINAL
    assert is_terminal(VERIFIED, True) and not is_terminal(GENERATED, True)
    assert is_terminal(GENERATED, False) and is_terminal(VERIFIED, False)
    assert not is_terminal("failed", True) and not is_terminal("failed", False)
    assert "verified" in describe_terminal(True)


def test_runner_and_compiler_share_the_same_rule():
    """本地 Runner 与编译产物必须引用同一个判定函数。"""
    import inspect

    from specargo.runner import Runner
    from specargo import spec2argo

    runner_src = inspect.getsource(Runner._stage_terminal_statuses)
    assert "resume_policy" in runner_src, "Runner 仍在用自己的判定实现"
    assert "stage_terminal_statuses" in inspect.getsource(spec2argo), \
        "编译器没有引用共享判定"


# ---------------------------------------------------------------------------
# 2. StateStore：两端语义一致
# ---------------------------------------------------------------------------

def test_sqlite_state_store_matches_module_functions():
    """SqliteStateStore 只是既有函数的薄封装，行为必须一致。"""
    from specargo.state import get_item, items_for, upsert_item

    with tempfile.TemporaryDirectory() as d:
        conn = connect(Path(d) / "s.db")
        store = SqliteStateStore(conn)
        store.upsert_item(dataset="ds", partition="1", variant="", stage="load",
                          status=VERIFIED, run_id="r1", coverage=1.0)
        row_store = store.get_item("ds", "1", "", "load")
        row_fn = get_item(conn, "ds", "1", "", stage="load")
        assert row_store["status"] == row_fn["status"] == VERIFIED
        assert len(store.items_for("ds")) == len(items_for(conn, "ds")) == 1
        assert len(store.all_items()) == 1
        conn.close()


def test_journal_state_store_roundtrip_and_isolation():
    """JournalStateStore：写后读得到；不同 dataset/partition/variant 互不串。"""
    with tempfile.TemporaryDirectory() as d:
        with local_objstore(Path(d) / "art"):
            store = JournalStateStore("r1")
            store.open_run("r1", note="smoke")
            store.upsert_item(dataset="ds", partition="1", variant="", stage="load",
                              status=VERIFIED, run_id="r1")
            store.upsert_item(dataset="ds", partition="2", variant="v2", stage="load",
                              status=GENERATED, run_id="r1")
            store.upsert_item(dataset="other", partition="1", variant="", stage="load",
                              status=VERIFIED, run_id="r1")

            assert store.get_item("ds", "1", "", "load")["status"] == VERIFIED
            assert store.get_item("ds", "2", "v2", "load")["status"] == GENERATED
            assert store.get_item("other", "1", "", "load")["status"] == VERIFIED
            # 四元组必须都参与定位：variant 不同的同 partition 不是同一条
            assert store.get_item("ds", "2", "", "load") is None
            assert len(store.items_for("ds")) == 2
            assert len(store.items_for("other")) == 1
            assert len(store.all_items()) == 3

            store.close_run("r1", "succeeded")
            assert store._runs["r1"]["status"] == "succeeded"


def test_journal_store_read_failure_does_not_look_completed():
    """读状态失败必须按"未完成"处理——不能把异常当成已终态（会假跳过）。"""
    with tempfile.TemporaryDirectory() as d:
        with local_objstore(Path(d) / "art"):
            # 没有 PIPELINE_RUN_ID 且 store.run_id 为空 → 读不到
            store = JournalStateStore("")
            try:
                store.get_item("ds", "1", "", "load")
                raise AssertionError("空 run_id 应报错")
            except ValueError:
                pass                              # 期望：显式失败，不返回 None 冒充完成


# ---------------------------------------------------------------------------
# 3. 集群 resume：CLI 判定 + 编译产物 + 端到端模拟
# ---------------------------------------------------------------------------

def test_check_skip_cli_exit_codes():
    """check-skip 退出码：0=已终态应跳过，1=未完成，2=用法错误。"""
    with tempfile.TemporaryDirectory() as d:
        with local_objstore(Path(d) / "art"):
            env = dict(os.environ)
            env["PIPELINE_RUN_ID"] = "r1"
            cmd = [sys.executable, "-m", "specargo.pipeline_cli", "state", "check-skip",
                   "--dataset", "ds", "--partition", "1", "--stage", "extract"]
            put = [sys.executable, "-m", "specargo.pipeline_cli", "state", "put-remote",
                   "--dataset", "ds", "--partition", "1", "--stage", "extract"]

            p = subprocess.run(cmd, capture_output=True, text=True, env=env)
            assert p.returncode == 1, p.stdout + p.stderr        # 无记录 → 需执行

            subprocess.run(put + ["--status", GENERATED], env=env,
                           capture_output=True, check=True)
            p = subprocess.run(cmd, capture_output=True, text=True, env=env)
            assert p.returncode == 0, p.stdout + p.stderr        # 无闸门：generated 即终态

            subprocess.run(put + ["--status", GENERATED], env=env,
                           capture_output=True, check=True)
            p = subprocess.run(cmd + ["--has-checks"], capture_output=True,
                               text=True, env=env)
            assert p.returncode == 1, p.stdout + p.stderr        # 有闸门：generated 非终态

            subprocess.run(put + ["--status", VERIFIED], env=env,
                           capture_output=True, check=True)
            p = subprocess.run(cmd + ["--has-checks"], capture_output=True,
                               text=True, env=env)
            assert p.returncode == 0, p.stdout + p.stderr        # verified → 跳过

            env.pop("PIPELINE_RUN_ID")
            p = subprocess.run(cmd, capture_output=True, text=True, env=env)
            assert p.returncode == 2, p.stdout + p.stderr        # 缺 run_id


def test_compile_resume_emits_skip_check_only_when_enabled():
    """resume 默认关闭（CI 不变）；开启后 stage 前插 check-skip。"""
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    plain = compile_workflow(spec)
    extract_plain = next(t for t in plain["spec"]["templates"] if t["name"] == "extract")
    assert "check-skip" not in extract_plain["container"]["args"][0]

    spec.k8s = dict(spec.k8s)
    spec.k8s.update({"resume": True, "journal": True})
    wf = compile_workflow(spec, name="etl-demo-resume")
    extract = next(t for t in wf["spec"]["templates"] if t["name"] == "extract")
    script = extract["container"]["args"][0]
    assert "state check-skip" in script
    assert "--stage extract --has-checks" in script          # 该 stage 配了闸门
    assert '&& { echo "[resume] extract 已完成，跳过"; exit 0; }' in script
    # check-skip 必须在业务命令之前（否则没有跳过意义）
    assert script.index("check-skip") < script.index("etl_steps extract")


def test_compile_resume_has_checks_flag_tracks_stage_checks():
    """没配 checks 的 stage 不应加 --has-checks（否则永远无法 resume）。"""
    import tempfile

    spec_text = (
        "name: plain\n"
        "runtime: {image: demo:ci, resume: true, journal: true}\n"
        "params: {python: python3, work_dir: out}\n"
        "partitions: {from: list, items: [{dataset: ds, key: k1}]}\n"
        "stages:\n"
        "  - name: build\n"
        "    command: \"echo build\"\n"
        "    outputs: [o]\n"
        "    artifacts: {o: out/o.txt}\n"
    )
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "spec.yaml"
        f.write_text(spec_text, encoding="utf-8")
        wf = compile_workflow(load_spec(str(f)))
        build = next(t for t in wf["spec"]["templates"] if t["name"] == "build")
        script = build["container"]["args"][0]
        assert "check-skip" in script, "resume 开启就必须有跳过判定"
        assert "--has-checks" not in script, "无闸门 stage 不应加 --has-checks"


def test_cluster_resume_skips_only_completed_partitions():
    """端到端（按 Pod 语义）：预置分区 1 终态 → 只跳过分区 1。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        out = root / "output"
        out.mkdir()
        spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
        spec.k8s = dict(spec.k8s)
        spec.k8s.update({"resume": True, "journal": True})
        wf = compile_workflow(spec, name="etl-demo-resume")
        templates = {t["name"]: t for t in wf["spec"]["templates"]}

        env = dict(os.environ)
        env.update({"OBJSTORE_KIND": "local",
                    "OBJSTORE_ROOT": str(root / "artifacts"),
                    "PIPELINE_RUN_ID": "wf-resume"})

        def fix(script: str, partition: str) -> str:
            return (script.replace("{{inputs.parameters.partition}}", partition)
                          .replace("{{inputs.parameters.dataset}}", "dwd_order")
                          .replace("{{inputs.parameters.variant}}", "")
                          .replace("/app/", "").replace("/output/", str(out) + "/"))

        def run_stage(name: str, partition: str):
            work = root / f"{name}-{partition}"
            work.mkdir(exist_ok=True)
            return subprocess.run(
                ["bash", "-c", fix(templates[name]["container"]["args"][0], partition)],
                capture_output=True, text=True, env=env, cwd=work)

        # 预置：分区 1 的 extract 上轮已 verified
        pre = subprocess.run(
            [sys.executable, "-m", "specargo.pipeline_cli", "state", "put-remote",
             "--dataset", "dwd_order", "--partition", "1", "--stage", "extract",
             "--status", VERIFIED], capture_output=True, text=True, env=env)
        assert pre.returncode == 0, pre.stderr

        p1 = run_stage("extract", "1")
        assert p1.returncode == 0, p1.stdout + p1.stderr
        assert "[resume] extract 已完成，跳过" in p1.stdout
        assert "extract 分区 1" not in p1.stdout, "已终态的分区不应再执行业务命令"

        p2 = run_stage("extract", "2")
        assert p2.returncode == 0, p2.stdout + p2.stderr
        assert "extract 分区 2" in p2.stdout, "未完成的分区应正常执行"
        assert "[state] remote" in p2.stdout, "执行后要登记状态"

        # generated 不是终态（extract 配了闸门，只认 verified）→ 仍要执行。
        # 这一步同时验证"闸门没过就不能跳过"，否则会把未质检数据当完成。
        p2_again = run_stage("extract", "2")
        assert p2_again.returncode == 0
        assert "未完成（generated）" in p2_again.stdout
        assert "extract 分区 2" in p2_again.stdout

        # gate 通过后（verified）再跑 → 才允许跳过
        gated = subprocess.run(
            [sys.executable, "-m", "specargo.pipeline_cli", "state", "put-remote",
             "--dataset", "dwd_order", "--partition", "2", "--stage", "extract",
             "--status", VERIFIED], capture_output=True, text=True, env=env)
        assert gated.returncode == 0, gated.stderr
        p2_done = run_stage("extract", "2")
        assert p2_done.returncode == 0
        assert "[resume] extract 已完成，跳过" in p2_done.stdout
        assert "extract 分区 2" not in p2_done.stdout


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
    print(f"\n{'StateStore/resume 测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
