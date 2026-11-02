"""RuntimeConfig：执行目标配置的优先级 / 校验 / 契约常量。

编译器不应持有"业务默认值"——运行时假设（镜像、挂载、对象存储端点、deadline、
podGC、插件路径）必须可配置且默认值明确。本套件锁住三件事：

1. **优先级**：内置默认 < spec ``runtime:``/``k8s:`` 节 < CLI ``--runtime`` 文件；
2. **校验**：类型/未知字段/预留 env 在 load 期报错，而不是运行时炸；
3. **契约固定**：run_id env 名与对象存储 key 根是框架语义，编译产物必须引用常量。
"""
from __future__ import annotations
import importlib.util

import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
# 优先用已安装的包（pip install -e .）；未安装时退回 src/（不依赖安装也能跑测试）
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

import specargo  # noqa: E402  (供 specargo.__file__ 定位包目录)

from specargo.runtime import (OBJSTORE_KEY_ROOT, RESERVED_ENV, RUN_ID_ENV,  # noqa: E402
                     RuntimeConfig)
from specargo.spec import SpecError, load_spec                        # noqa: E402
from specargo.spec2argo import compile_workflow                        # noqa: E402


def _write_spec(text: str) -> Path:
    d = tempfile.mkdtemp()
    f = Path(d) / "spec.yaml"
    f.write_text(text, encoding="utf-8")
    return f


BASE = ("name: x\npartitions: {from: list, items: [{dataset: ds, key: k1}]}\n"
        "stages:\n  - {name: s, command: echo, outputs: [o], "
        "artifacts: {o: out/o.txt}}\n")


# ---------------------------------------------------------------------------
# 1. 默认值与 spec 节
# ---------------------------------------------------------------------------

def test_defaults_are_explicit_and_neutral():
    cfg = RuntimeConfig()
    assert cfg.image == "pipeline-image:latest"      # 明确占位，不猜业务镜像
    assert cfg.image_pull_policy == "IfNotPresent"
    assert cfg.service_account == "default"
    assert cfg.namespace == "argo"
    assert cfg.active_deadline_seconds == 1800
    assert cfg.pod_gc_strategy == "OnWorkflowSuccess"
    assert cfg.journal is False
    assert cfg.objstore_endpoint.startswith("http://")
    assert cfg.objstore_env == {} and cfg.mounts == {} and cfg.volumes == {}


def test_spec_runtime_section_overrides_defaults():
    spec = load_spec(_write_spec(
        BASE.replace("name: x\n", "name: x\nruntime:\n  image: my/image:v1\n"
                                  "  service_account: argo-workflow\n"
                                  "  namespace: data-platform\n"
                                  "  active_deadline_seconds: 600\n"
                                  "  pod_gc_strategy: Never\n")))
    cfg = RuntimeConfig.from_spec(spec)
    assert cfg.image == "my/image:v1"
    assert cfg.service_account == "argo-workflow"
    assert cfg.namespace == "data-platform"
    assert cfg.active_deadline_seconds == 600
    assert cfg.pod_gc_strategy == "Never"


def test_k8s_block_still_works_as_alias():
    """旧名 k8s: 仍可用（存量 spec 不破坏）。"""
    spec = load_spec(_write_spec(
        BASE.replace("name: x\n", "name: x\nk8s: {image: legacy/image:v2}\n")))
    cfg = RuntimeConfig.from_spec(spec)
    assert cfg.image == "legacy/image:v2"


def test_runtime_and_k8s_together_is_rejected():
    """两个执行目标节同时非空 → 报错（避免"改了没生效"的困惑）。

    注意：报错发生在 load_spec 的校验期（单一事实来源），不是编译期。
    """
    spec_file = _write_spec(
        BASE.replace("name: x\n", "name: x\nruntime: {image: a:1}\n"
                                  "k8s: {image: b:2}\n"))
    try:
        load_spec(spec_file)
        raise AssertionError("runtime 与 k8s 同时存在应被拒绝")
    except SpecError as exc:
        assert "k8s" in str(exc)


def test_cli_runtime_file_overrides_spec():
    """CLI --runtime 按 key 覆盖 spec 节（同一份 spec 多环境部署）。"""
    spec_file = _write_spec(
        BASE.replace("name: x\n", "name: x\nruntime:\n  image: spec/image:v1\n"
                                  "  namespace: spec-ns\n"))
    with tempfile.TemporaryDirectory() as d:
        rt = Path(d) / "runtime.yaml"
        rt.write_text("image: cli/image:v2\n", encoding="utf-8")
        out = Path(d) / "wf.yaml"
        p = subprocess.run(
            [sys.executable, "-m", "specargo.spec2argo", str(spec_file),
             "--runtime", str(rt), "-o", str(out)],
            capture_output=True, text=True)
        assert p.returncode == 0, p.stdout + p.stderr
        wf = yaml.safe_load(out.read_text(encoding="utf-8"))
        images = {t["container"]["image"] for t in wf["spec"]["templates"]
                  if "container" in t}
        assert images == {"cli/image:v2"}, images            # CLI 覆盖生效
        assert wf["metadata"]["namespace"] == "spec-ns"      # 未覆盖的 key 保留 spec 值


# ---------------------------------------------------------------------------
# 2. 校验
# ---------------------------------------------------------------------------

def test_runtime_validation_rejects_bad_types_and_keys():
    bad_cases = [
        ("image: 123", "image"),
        ("journal: 'false'", "journal"),
        ("active_deadline_seconds: 1.5", "active_deadline_seconds"),
        ("objstore_env: [A]", "objstore_env"),
        ("objstore_env: {BAD NAME: x}", "变量名"),
        ("objstore_env: {PIPELINE_RUN_ID: x}", "保留变量"),
        ("objstore_env: {OBJSTORE_ENDPOINT: x}", "保留变量"),
        ("unknown_key: 1", "未知字段"),
        ("mounts: {a: 1}", "mounts"),
    ]
    for body, needle in bad_cases:
        try:
            RuntimeConfig.from_mapping(yaml.safe_load(body), where="runtime")
            raise AssertionError(f"应被拒绝: {body}")
        except SpecError as exc:
            assert needle in str(exc), (body, str(exc))


def test_reserved_env_set_is_documented():
    """预留 env 集合应包含框架自己注入的全部变量。"""
    assert "PIPELINE_RUN_ID" in RESERVED_ENV
    assert "OBJSTORE_ENDPOINT" in RESERVED_ENV
    assert "PIPELINE_PLUGINS" in RESERVED_ENV
    assert "PIPELINE_WORKFLOW_STATUS" in RESERVED_ENV


# ---------------------------------------------------------------------------
# 3. 契约常量：编译产物必须引用固定 env 名 / key 根
# ---------------------------------------------------------------------------

def test_compiled_output_uses_contract_constants():
    assert RUN_ID_ENV == "PIPELINE_RUN_ID"
    assert OBJSTORE_KEY_ROOT == "runs"
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    wf = compile_workflow(spec)
    text = yaml.safe_dump(wf, allow_unicode=True)
    assert RUN_ID_ENV in text
    assert f"runs/${RUN_ID_ENV}/" in text


def test_non_mapping_runtime_and_volumes_are_rejected():
    """runtime/k8s 非映射、volumes 结构非法 → SpecError（不是原生异常）。

    回归：load_spec 的 dict(raw.get("runtime")) 曾对字符串直接抛 ValueError；
    volumes 只校验了"是 dict"，字符串值/覆盖 name 会在编译期炸原生异常。
    """
    from specargo.spec import load_spec
    base = ("name: x\npartitions: {from: list, items: [{dataset: ds, key: k1}]}\n"
            "stages:\n  - {name: s, command: echo, outputs: [o], "
            "artifacts: {o: out/o.txt}}\n")
    bad_bodies = [
        "runtime: x\n",
        "runtime: [a]\n",
        "runtime: 3\n",
        "k8s: x\n",
        "runtime: {volumes: {output: hello}}\n",
        "runtime: {volumes: {output: {name: hijack}}}\n",
        "runtime: {volumes: {output: {}}}\n",
    ]
    for body in bad_bodies:
        spec_file = _write_spec("name: x\n" + body + base.split("\n", 1)[1])
        try:
            load_spec(spec_file)
            raise AssertionError(f"应被拒绝: {body!r}")
        except SpecError:
            pass                      # 期望：load 期 SpecError


def test_runtime_roundtrip_as_dict():
    cfg = RuntimeConfig.from_mapping({"image": "a:1", "journal": True})
    assert cfg.as_dict()["image"] == "a:1"
    assert cfg.as_dict()["journal"] is True


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
    print(f"\n{'RuntimeConfig 测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
