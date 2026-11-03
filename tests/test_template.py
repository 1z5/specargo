"""WorkflowTemplate 编译：同一份 spec 既能直接提交 Workflow，也能发布成模板。

价值：模板注册到集群后，多份 Workflow 引用同一个模板；模板更新（灰度）或
回滚只改模板，不改提交侧。本套件锁住：

1. 结构正确：kind/name/spec 与直接编译的 Workflow 一致（只是 CRD 类型不同）；
2. 引用可生成：`--reference` 产出的最小 Workflow 能被 Argo 解析；
3. 不泄漏：模板里不能带 ``status`` 之类只属于运行实例的字段。
"""
from __future__ import annotations

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
from specargo.spec import load_spec                                    # noqa: E402
from specargo.spec2argo import (compile_template, compile_workflow,    # noqa: E402
                               render_reference)
import yaml                                                            # noqa: E402


def test_template_shares_spec_with_workflow():
    """模板与 Workflow 的 spec 部分必须完全一致（同一份定义两个用途）。"""
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    wf = compile_workflow(spec)
    tmpl = compile_template(spec)
    assert tmpl["kind"] == "WorkflowTemplate"
    assert wf["kind"] == "Workflow"
    assert tmpl["apiVersion"] == wf["apiVersion"]
    assert tmpl["spec"] == wf["spec"], "模板与 Workflow 的 spec 不应有差异"
    assert tmpl["metadata"] == wf["metadata"]


def test_template_has_no_runtime_status():
    """模板是静态发布物，不能带 status / 运行期字段。"""
    tmpl = compile_template(load_spec(str(ROOT / "pipelines" / "etl-demo.yaml")))
    assert "status" not in tmpl
    assert "status" not in tmpl["spec"]


def test_reference_workflow_is_valid():
    """引用模板的最小 Workflow：有 workflowTemplateRef，且不带 templates。"""
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    tmpl = compile_template(spec, name="etl-demo-template")
    ref = render_reference(tmpl, name="etl-via-template")
    assert ref["kind"] == "Workflow"
    assert ref["spec"]["workflowTemplateRef"] == {"name": "etl-demo-template"}
    assert "templates" not in ref["spec"]
    assert "entrypoint" not in ref["spec"]
    # 往返 YAML 仍合法
    assert yaml.safe_load(yaml.safe_dump(ref))["spec"]["workflowTemplateRef"]["name"] \
        == "etl-demo-template"


def test_cli_as_template_and_reference():
    """CLI 的两个开关都要落盘成可解析的 YAML（CI 直接 kubectl apply）。"""
    with tempfile.TemporaryDirectory() as d:
        tmpl_path = Path(d) / "wt.yaml"
        ref_path = Path(d) / "ref.yaml"
        p = subprocess.run(
            [sys.executable, "-m", "specargo.spec2argo", "pipelines/etl-demo.yaml",
             "--as-template", "-o", str(tmpl_path)],
            capture_output=True, text=True, cwd=ROOT)
        assert p.returncode == 0, p.stdout + p.stderr
        tmpl = yaml.safe_load(tmpl_path.read_text(encoding="utf-8"))
        assert tmpl["kind"] == "WorkflowTemplate"
        assert tmpl["spec"]["entrypoint"] == "main"

        p = subprocess.run(
            [sys.executable, "-m", "specargo.spec2argo", "pipelines/etl-demo.yaml",
             "--as-template", "--reference", "etl-via-template", "-o", str(ref_path)],
            capture_output=True, text=True, cwd=ROOT)
        assert p.returncode == 0, p.stdout + p.stderr
        ref = yaml.safe_load(ref_path.read_text(encoding="utf-8"))
        assert ref["kind"] == "Workflow"
        assert ref["spec"]["workflowTemplateRef"]["name"] == "etl-demo"


def test_template_keeps_runtime_overrides():
    """模板同样吃 runtime 覆盖（同一份 spec 多环境发布）。"""
    spec = load_spec(str(ROOT / "pipelines" / "etl-demo.yaml"))
    spec.k8s = dict(spec.k8s)
    spec.k8s["image"] = "registry.local/etl:v9"
    tmpl = compile_template(spec)
    images = {t["container"]["image"] for t in tmpl["spec"]["templates"]
              if "container" in t}
    assert images == {"registry.local/etl:v9"}, images


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
    print(f"\n{'模板编译测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
