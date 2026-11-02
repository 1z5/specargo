"""显式再生成 golden 编译产物。

编译器行为**有意变更**时才跑这个脚本（会覆盖 tests/golden/*.yaml），
然后提交 golden 变更。目的：让"编译器变了"成为一次显式、可 review 的动作，
而不是被静默带进某次功能提交。

用法::

    python3 tests/update_golden.py            # 重新生成全部 golden
    python3 tests/update_golden.py --diff     # 只显示与现有 golden 的差异摘要
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
GOLDEN = ROOT / "tests" / "golden"
sys.path.insert(0, str(APP))

from spec import load_spec                                    # noqa: E402
from spec2argo import compile_workflow                         # noqa: E402

HEADER = """# Golden 编译产物（由 tests/update_golden.py 生成，请勿手改）
# 用途：锁住 spec2argo 的编译行为——编译器变更必须显式更新 golden，
#       否则 tests/test_golden.py 会以 drift 失败（防止"顺手改坏了编译器"）。
# 比较按解析后的 YAML 结构进行（不受 dump 宽度/键序影响）。
"""


def _load_cases() -> list[dict]:
    return json.loads((GOLDEN / "cases.json").read_text(encoding="utf-8"))


def _compile(case: dict):
    spec = load_spec(str(ROOT / case["spec"]))
    overrides = case.get("overrides") or {}
    if overrides:
        spec.k8s = dict(spec.k8s)
        spec.k8s.update(overrides)
    return compile_workflow(spec, name=case.get("name"))


def update() -> list[str]:
    changed: list[str] = []
    for case in _load_cases():
        wf = _compile(case)
        text = HEADER + yaml.safe_dump(wf, allow_unicode=True, sort_keys=False,
                                       width=120)
        path = GOLDEN / case["golden"]
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
            changed.append(case["golden"])
    return changed


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    changed = update()
    if "--diff" in argv:
        print(f"golden 变更: {len(changed)} 个")
        for name in changed:
            print(f"  ~ {name}")
        return 0
    if changed:
        print(f"✅ 已更新 {len(changed)} 个 golden: {', '.join(changed)}")
    else:
        print("✅ golden 已是最新（无变更）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
