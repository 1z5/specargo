"""集群侧语义测试：状态日志（journal）、onExit 对账、gate CLI、插件加载。

这些是"双目标语义对等"的核心：本地 runner 与集群执行共用同一套
checks / 状态 / 对账实现，而不是各写一份。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
sys.path.insert(0, str(APP))

from checks import run_check                                    # noqa: E402
from spec import load_spec                                      # noqa: E402
from spec2argo import compile_workflow                          # noqa: E402
import journal                                                  # noqa: E402
import notify                                                   # noqa: E402
import state as st                                              # noqa: E402


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

class local_objstore:
    """临时把对象存储切到本地目录模式（测试隔离）。"""

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


def _write_spec(text: str) -> Path:
    d = tempfile.mkdtemp()
    f = Path(d) / "spec.yaml"
    f.write_text(text, encoding="utf-8")
    return f


JOURNAL_SPEC = """\
name: jdemo
k8s:
  image: specargo:ci
  journal: true
  notify_webhook: ""
  objstore_env: {OBJSTORE_BUCKET: argo-artifacts}
params: {python: python3, run_id: "", app_dir: "", work_dir: "out"}
partitions:
  from: list
  items: [{dataset: ds, key: "1"}]
variants: [""]
stages:
  - name: extract
    command: "{{params.python}} {{params.app_dir}}/etl_steps.py extract --partition {{partition.key}} --out {{params.work_dir}}/extract/{{partition.key}}.jsonl"
    outputs: [raw]
    artifacts: {raw: "{{params.work_dir}}/extract/{{partition.key}}.jsonl"}
    checks: [file_nonempty]
    timeout: 5m
  - name: load
    dependencies: [extract]
    command: "{{params.python}} {{params.app_dir}}/etl_steps.py load --in {{params.work_dir}}/extract/{{partition.key}}.jsonl --out {{params.work_dir}}/load/{{partition.key}}.done"
    outputs: [marker]
    artifacts: {marker: "{{params.work_dir}}/load/{{partition.key}}.done"}
    consumes: [raw]
    timeout: 5m
"""


# ---------------------------------------------------------------------------
# 1. CLI：gate / state / reconcile
# ---------------------------------------------------------------------------

def test_cli_gate_builtin_checks():
    with tempfile.TemporaryDirectory() as d:
        good = Path(d) / "good.jsonl"
        good.write_text('{"order_id":"1","amount":1,"dt":"x"}\n', encoding="utf-8")
        bad = Path(d) / "bad.jsonl"
        bad.write_text("", encoding="utf-8")
        cmd = [sys.executable, str(APP / "pipeline_cli.py"), "gate",
               "--stage", "s1", "--artifact", str(good),
               "--checks", '["file_nonempty"]']
        assert subprocess.run(cmd, capture_output=True).returncode == 0
        cmd[cmd.index("--artifact") + 1] = str(bad)
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert p.returncode == 1 and "未通过检查" in p.stderr, p.stderr


def test_cli_gate_loads_domain_plugin_from_env():
    with tempfile.TemporaryDirectory() as d:
        plug_dir = Path(d)
        (plug_dir / "my_domain_checks.py").write_text(
            "from checks import CheckResult, register\n"
            "@register('my_domain_check')\n"
            "def my_domain_check(artifact, config, ctx):\n"
            "    return CheckResult(True, metric='ok')\n", encoding="utf-8")
        art = Path(d) / "a.txt"
        art.write_text("x", encoding="utf-8")
        env = dict(os.environ)
        env["PIPELINE_PLUGINS"] = '["my_domain_checks"]'
        env["PIPELINE_PLUGIN_PATH"] = str(plug_dir)
        p = subprocess.run(
            [sys.executable, str(APP / "pipeline_cli.py"), "gate",
             "--stage", "s1", "--artifact", str(art),
             "--checks", '["my_domain_check"]'],
            capture_output=True, text=True, env=env)
        assert p.returncode == 0, p.stdout + p.stderr
        assert "passed=True" in p.stdout


def test_cli_state_put_writes_sqlite():
    with tempfile.TemporaryDirectory() as d:
        db = Path(d) / "s.db"
        cmd = [sys.executable, str(APP / "pipeline_cli.py"), "state", "put",
               "--state-db", str(db), "--dataset", "ds", "--partition", "1",
               "--stage", "load", "--status", "verified", "--run-id", "r1"]
        assert subprocess.run(cmd, capture_output=True).returncode == 0
        conn = st.connect(db)
        assert st.get_item(conn, "ds", "1", "", "load")["status"] == st.VERIFIED
        conn.close()


def test_journal_roundtrip_and_reconcile_cli():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        with local_objstore(root / "artifacts"):
            journal.put_status(run_id="r1", dataset="ds", partition="1",
                               variant="", stage="extract", status="generated")
            journal.put_status(run_id="r1", dataset="ds", partition="1",
                               variant="", stage="load", status="verified")
            journal.put_partitions("r1", [{"key": "1", "dataset": "ds",
                                           "variant": ""}])
            rows = journal.load_statuses("r1")
            assert len(rows) == 2 and {r["stage"] for r in rows} == {"extract", "load"}
            assert journal.get_partitions("r1")[0]["key"] == "1"

            parts = root / "parts.json"
            parts.write_text(json.dumps(journal.get_partitions("r1")), encoding="utf-8")
            env = dict(os.environ)
            env.update({"OBJSTORE_KIND": "local",
                        "OBJSTORE_ROOT": str(root / "artifacts")})
            cmd = [sys.executable, str(APP / "pipeline_cli.py"),
                   "reconcile-journal", "--run-id", "r1",
                   "--partitions-file", str(parts), "--final-stage", "load",
                   "--report-json", str(root / "report.json")]
            env["PIPELINE_RUN_ID"] = "r1"
            ok = subprocess.run(cmd, capture_output=True, text=True, env=env)
            assert ok.returncode == 0 and "PASS" in ok.stdout, ok.stdout + ok.stderr
            assert json.loads((root / "report.json").read_text())["reconcile"][0]["ok"]

            # 缺数反例：新 run 只写了 extract
            journal.put_status(run_id="r2", dataset="ds", partition="1",
                               variant="", stage="extract", status="generated")
            bad = subprocess.run(cmd[:-4] + ["--run-id", "r2", "--partitions-file",
                                             str(parts), "--final-stage", "load"],
                                 capture_output=True, text=True, env=env)
            assert bad.returncode == 1 and "FAIL" in bad.stdout, bad.stdout + bad.stderr


def test_notify_renders_and_webhook_disabled():
    recons = [{"dataset": "ds", "expected": 2, "verified": 1,
               "missing": ["2"], "not_verified": [], "ok": False}]
    text = notify.render_summary("r1", recons)
    assert "FAIL" in text and "漏数: 2" in text
    assert notify.send_webhook(text, "") is False


# ---------------------------------------------------------------------------
# 2. 编译器：journal 开/关的产物差异
# ---------------------------------------------------------------------------

def test_compile_journal_emits_on_exit_and_state_puts():
    wf = compile_workflow(load_spec(_write_spec(JOURNAL_SPEC)))
    assert wf["spec"]["onExit"] == "on-exit"
    names = {t["name"] for t in wf["spec"]["templates"]}
    assert {"main", "extract", "gate-extract", "load", "on-exit"} <= names

    extract = next(t for t in wf["spec"]["templates"] if t["name"] == "extract")
    script = extract["container"]["args"][0]
    assert "state put-remote" in script and "--status generated" in script
    assert "PIPELINE_RUN_ID" in script

    gate = next(t for t in wf["spec"]["templates"] if t["name"] == "gate-extract")
    gate_script = gate["container"]["args"][0]
    assert "--journal" in gate_script and "pipeline_cli.py gate" in gate_script

    on_exit = next(t for t in wf["spec"]["templates"] if t["name"] == "on-exit")
    ox = on_exit["container"]["args"][0]
    assert "reconcile-journal" in ox
    assert '--final-stage load' in ox
    assert '--terminal-statuses \'["generated", "verified"]\'' in ox


def test_compile_pod_gc_strategy_is_configurable():
    """podGC 默认成功后清理 Pod；CI 可设为空保留 Pod 做跨节点断言。"""
    default = compile_workflow(load_spec(str(ROOT / "pipelines" / "etl-demo.yaml")))
    assert default["spec"]["podGC"]["strategy"] == "OnWorkflowSuccess"
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    spec.k8s["pod_gc_strategy"] = ""
    assert "podGC" not in compile_workflow(spec)["spec"]
    spec.k8s["pod_gc_strategy"] = "Never"
    assert compile_workflow(spec)["spec"]["podGC"]["strategy"] == "Never"


def test_compile_without_journal_is_unchanged():
    """默认不带 journal 的管线（CI 用的就是这种）不得多出 onExit/状态写入。"""
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    wf = compile_workflow(spec)
    assert "onExit" not in wf["spec"]
    for tmpl in wf["spec"]["templates"]:
        script = (tmpl.get("container", {}).get("args") or [""])[0]
        if isinstance(script, str):
            assert "state put-remote" not in script
            assert "reconcile-journal" not in script


def test_discover_journal_writes_partitions_manifest():
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    spec.k8s = {"image": "specargo:ci", "journal": True}
    wf = compile_workflow(spec)
    disc = next(t for t in wf["spec"]["templates"] if t["name"] == "discover-scan")
    script = disc["script"]["args"][0]
    assert "_run/partitions.json" in script and "objstore.py put" in script
    # onExit 走 discover 分支：从对象存储取分区清单，取不到则跳过
    on_exit = next(t for t in wf["spec"]["templates"] if t["name"] == "on-exit")
    ox = on_exit["container"]["args"][0]
    assert "_run/partitions.json" in ox and "跳过对账" in ox


# ---------------------------------------------------------------------------
# 3. 端到端模拟：按 Pod 语义执行 stage → gate → onExit
# ---------------------------------------------------------------------------

ETL_DISCOVER_SPEC = """\
name: discover-demo
k8s:
  image: specargo:ci
params: {python: python3, run_id: "", app_dir: "", work_dir: "out"}
partitions:
  from: discover
  dataset: dwd_order
  command: "printf '%s\\n' 1"
stages:
  - name: extract
    command: "{{params.python}} {{params.app_dir}}/etl_steps.py extract --partition {{partition.key}} --out {{params.work_dir}}/extract/{{partition.key}}.jsonl"
    outputs: [raw]
    artifacts: {raw: "{{params.work_dir}}/extract/{{partition.key}}.jsonl"}
    checks: [file_nonempty]
    timeout: 5m
"""


def _fix(script: str, partition: str, output_dir: Path) -> str:
    return (script.replace("{{inputs.parameters.partition}}", partition)
                  .replace("{{inputs.parameters.dataset}}", "ds")
                  .replace("{{inputs.parameters.variant}}", "")
                  .replace("/app/", str(APP) + "/")
                  .replace("/output/", str(output_dir) + "/"))


def test_journal_pipeline_end_to_end_with_on_exit():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        out = root / "output"
        out.mkdir()
        env = dict(os.environ)
        env.update({"OBJSTORE_KIND": "local",
                    "OBJSTORE_ROOT": str(root / "artifacts"),
                    "PIPELINE_RUN_ID": "wf-e2e"})
        wf = compile_workflow(load_spec(_write_spec(JOURNAL_SPEC)))
        templates = {t["name"]: t for t in wf["spec"]["templates"]}

        def run_tmpl(name: str) -> int:
            script = templates[name]["container"]["args"][0]
            work = root / name
            work.mkdir()
            p = subprocess.run(["bash", "-c", _fix(script, "1", out)],
                               capture_output=True, text=True, env=env, cwd=work)
            assert p.returncode == 0, (name, p.stdout, p.stderr)
            return p.returncode

        run_tmpl("extract")
        run_tmpl("gate-extract")
        run_tmpl("load")
        on_exit = subprocess.run(
            ["bash", "-c", _fix(templates["on-exit"]["container"]["args"][0], "1", out)],
            capture_output=True, text=True, env=env, cwd=root)
        assert on_exit.returncode == 0, on_exit.stdout + on_exit.stderr
        assert "PASS" in on_exit.stdout

        # 删掉 load 的状态 → onExit 必须让工作流失败（"跑完≠跑对"）
        for f in (root / "artifacts").rglob("*.json"):
            if f"/{'_status/load'}/" in str(f):
                f.unlink()
        bad = subprocess.run(
            ["bash", "-c", _fix(templates["on-exit"]["container"]["args"][0], "1", out)],
            capture_output=True, text=True, env=env, cwd=root)
        assert bad.returncode == 1 and "FAIL" in bad.stdout, bad.stdout + bad.stderr


def test_journal_many_statuses_do_not_leak_file_descriptors():
    """journal 临时文件 fd 必须关闭：大批量状态读取不能 EMFILE。"""
    import resource
    with tempfile.TemporaryDirectory() as d:
        with local_objstore(Path(d) / "artifacts"):
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (256, hard))
                for i in range(300):
                    journal.put_status(run_id="r", dataset="ds", partition=str(i),
                                       variant="", stage="s", status="verified")
                rows = journal.load_statuses("r")
                assert len(rows) == 300, f"fd 泄漏导致状态读不全: {len(rows)}"
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def test_webhook_failure_does_not_change_reconcile_verdict():
    """告警通道不可达不能把对账 PASS 变成失败。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        with local_objstore(root / "artifacts"):
            journal.put_status(run_id="r1", dataset="ds", partition="1",
                               variant="", stage="load", status="verified")
            journal.put_partitions("r1", [{"key": "1", "dataset": "ds", "variant": ""}])
            parts = root / "parts.json"
            parts.write_text(json.dumps(journal.get_partitions("r1")), encoding="utf-8")
            env = dict(os.environ)
            env.update({"OBJSTORE_KIND": "local",
                        "OBJSTORE_ROOT": str(root / "artifacts")})
            p = subprocess.run(
                [sys.executable, str(APP / "pipeline_cli.py"), "reconcile-journal",
                 "--run-id", "r1", "--partitions-file", str(parts),
                 "--final-stage", "load",
                 "--notify-webhook", "http://127.0.0.1:1/hook"],
                capture_output=True, text=True, env=env)
            assert p.returncode == 0 and "PASS" in p.stdout, p.stdout + p.stderr


def test_cli_bad_inputs_fail_cleanly():
    """缺文件/空 checks 必须非零退出且不给 traceback。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        art = root / "a.txt"
        art.write_text("x", encoding="utf-8")
        p = subprocess.run(
            [sys.executable, str(APP / "pipeline_cli.py"), "gate", "--stage", "s",
             "--artifact", str(art), "--checks", "[]"],
            capture_output=True, text=True)
        assert p.returncode != 0 and "Traceback" not in p.stderr, p.stderr
        p = subprocess.run(
            [sys.executable, str(APP / "pipeline_cli.py"), "gate", "--stage", "s",
             "--artifact", str(art), "--checks", '["file_nonempty"]',
             "--check-config-file", str(root / "missing.json")],
            capture_output=True, text=True)
        assert p.returncode != 0 and "Traceback" not in p.stderr, p.stderr
        p = subprocess.run(
            [sys.executable, str(APP / "pipeline_cli.py"), "reconcile-journal",
             "--run-id", "r", "--partitions-file", str(root / "missing.json"),
             "--final-stage", "load"],
            capture_output=True, text=True)
        assert p.returncode != 0 and "Traceback" not in p.stderr, p.stderr


def test_on_exit_missing_partitions_on_success_path_fails():
    """成功路径缺分区清单必须失败（否则假绿）；失败路径才允许跳过。"""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        spec = load_spec(_write_spec(ETL_DISCOVER_SPEC.replace(
            "k8s:", "k8s:\n  journal: true")))
        wf = compile_workflow(spec)
        on_exit = next(t for t in wf["spec"]["templates"] if t["name"] == "on-exit")
        script = on_exit["container"]["args"][0].replace("/app/", str(APP) + "/")
        env = dict(os.environ)
        env.update({"OBJSTORE_KIND": "local",
                    "OBJSTORE_ROOT": str(root / "artifacts"),
                    "PIPELINE_RUN_ID": "missing-run",
                    "PIPELINE_WORKFLOW_STATUS": "Succeeded"})
        p = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                           env=env, cwd=root)
        assert p.returncode == 1, p.stdout + p.stderr
        env["PIPELINE_WORKFLOW_STATUS"] = "Failed"
        p2 = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                            env=env, cwd=root)
        assert p2.returncode == 0, p2.stdout + p2.stderr


def test_non_journal_discover_env_is_unchanged():
    """默认（非 journal）产物里 discover 的 env 仍是原来的两个 SPEC_*，避免行为漂移。"""
    wf = compile_workflow(load_spec(str(ROOT / "pipelines" / "etl-demo.yaml")))
    disc = next(t for t in wf["spec"]["templates"] if t["name"] == "discover-scan")
    names = [e["name"] for e in disc["script"]["env"]]
    assert names == ["SPEC_DATASET", "SPEC_VARIANTS"], names


def test_journal_rejects_bad_key_components_and_webhook_scheme():
    """journal 组件必须无路径分隔符；webhook 只允许 http/https。"""
    for bad in ("..", "a/b", "", "."):
        for fn in (lambda v: journal.status_key(v, "s", "ds", "1"),
                   lambda v: journal.status_key("r", v, "ds", "1"),
                   lambda v: journal.partitions_key(v)):
            try:
                fn(bad)
                raise AssertionError(f"应拒绝非法 journal 组件: {bad!r}")
            except ValueError:
                pass
    assert notify.send_webhook("x", "file:///etc/passwd") is False
    assert notify.send_webhook("x", "ftp://example.invalid/x") is False


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
    print(f"\n{'集群语义测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
