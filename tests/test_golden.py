"""Golden 编译产物：锁住 spec2argo 的编译行为（防编译器静默漂移）。

编译器是"一份 spec、两个执行目标"的核心，任何无意的行为变化都会直接改变
集群执行语义。本套件把编译产物固化成 golden 文件：

- 编译器**有意**变更 → 跑 ``python3 tests/update_golden.py`` 并提交 golden 变更；
- 编译器**无意**变更（重构碰坏） → 本测试以 drift 失败，CI 变红。

比较按解析后的 YAML 结构进行（不受 dump 宽度/键序影响），同时断言
"编译产物零残留模板变量"这个不变量。
"""
from __future__ import annotations
import importlib.util

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
# 优先用已安装的包（pip install -e .）；未安装时退回 src/（不依赖安装也能跑测试）
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))
GOLDEN = ROOT / "tests" / "golden"

from specargo.spec import load_spec                                    # noqa: E402
from specargo.spec2argo import compile_workflow                        # noqa: E402


def _cases() -> list[dict]:
    return json.loads((GOLDEN / "cases.json").read_text(encoding="utf-8"))


def _compile(case: dict):
    spec = load_spec(str(ROOT / case["spec"]))
    overrides = case.get("overrides") or {}
    if overrides:
        spec.k8s = dict(spec.k8s)
        spec.k8s.update(overrides)
    return compile_workflow(spec, name=case.get("name"))


def _load_golden(case: dict) -> dict:
    text = (GOLDEN / case["golden"]).read_text(encoding="utf-8")
    return yaml.safe_load(text)


def test_golden_matches_current_compiler():
    """编译产物必须与 golden 完全一致（结构级比较）。"""
    cases = _cases()
    assert cases, "golden cases.json 为空"
    for case in cases:
        wf = _compile(case)
        golden = _load_golden(case)
        assert wf == golden, (
            f"{case['golden']} 与 golden 不一致：编译器行为发生了 drift。\n"
            f"  若这是有意变更：python3 tests/update_golden.py 并提交 golden；\n"
            f"  若是无意回归：检查本次对 spec2argo 的改动。"
        )


def test_golden_has_no_unresolved_templates():
    """golden 自身也必须零残留（防止 golden 文件被手改坏）。"""
    import re
    for path in sorted(GOLDEN.glob("*.yaml")):
        text = path.read_text(encoding="utf-8")
        leftover = re.findall(
            r"\{\{\s*(?:params\.[^{}\s]*|partition\.[^{}\s]*|artifact)\s*\}\}", text)
        assert not leftover, f"{path.name} 残留模板变量: {sorted(set(leftover))}"


def test_golden_covers_critical_shapes():
    """golden 必须覆盖关键形态：普通、journal、podGC 关闭、领域插件。"""
    names = {case["golden"] for case in _cases()}
    assert "etl-demo.yaml" in names, "缺少普通管线 golden"
    assert "etl-demo-journal.yaml" in names, "缺少 journal golden（onExit 对账路径）"
    assert "etl-demo-podgc-off.yaml" in names, "缺少 podGC 关闭 golden（CI 变体）"
    assert "tts-example.yaml" in names, "缺少领域插件 golden（plugins 注入）"

    journal = _load_golden({"golden": "etl-demo-journal.yaml"})
    assert journal["spec"]["onExit"] == "on-exit", "journal golden 缺少 onExit"
    tts = _load_golden({"golden": "tts-example.yaml"})
    gate_env = next(
        t["container"]["env"] for t in tts["spec"]["templates"]
        if t["name"] == "gate-verify")
    assert any(e["name"] == "PIPELINE_PLUGINS" for e in gate_env), \
        "领域插件 golden 缺少 PIPELINE_PLUGINS 注入"


def test_update_golden_script_is_idempotent():
    """再生成脚本必须幂等：跑一次不应产生变更（否则 golden 与编译器已漂移）。"""
    import subprocess
    p = subprocess.run([sys.executable, str(ROOT / "tests" / "update_golden.py"),
                        "--diff"], capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    assert "golden 变更: 0 个" in p.stdout, (
        "update_golden.py 产生了变更：golden 与当前编译器不一致\n" + p.stdout)


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
    print(f"\n{'Golden 测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
