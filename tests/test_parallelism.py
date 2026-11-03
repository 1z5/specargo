"""workflow 级并发上限：扇出管线的第一道闸。

没有它的时候，N 个分区 × M 个 stage/gate 会在提交瞬间一起进队列——真实的瓶颈不是
"跑得多慢"，而是 controller 被 Pod 请求打爆、调度排队、甚至 kind 单节点直接拒绝。
所以它必须是一个**显式的环境配置**（``runtime.parallelism``），而不是代码里写死。

本套件锁住三件事：
1. 默认不写该字段（Argo 默认"不限"的行为不能被悄悄改掉）；
2. 配置了就必须精确出现在编译产物 ``spec.parallelism``（不是模板里，也不是每个 task 上）；
3. 非法值（0 / 负数 / 字符串 / bool / 小数）与拼错的键名都必须报错，不能静默忽略。
"""
from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

from specargo.runtime import RuntimeConfig                             # noqa: E402
from specargo.spec import SpecError, load_spec                        # noqa: E402
from specargo.spec2argo import compile_template, compile_workflow     # noqa: E402

_SPEC = """\
version: 1
name: parallel-demo
{block}
partitions:
  from: list
  items: ["1", "2", "3"]
stages:
  - name: only
    command: "true"
"""


def _load(tmpdir: str, block: str = ""):
    path = Path(tmpdir) / "spec.yaml"
    path.write_text(_SPEC.format(block=block), encoding="utf-8")
    return load_spec(str(path))


def test_default_compilation_has_no_parallelism_field():
    """默认不写：保持 Argo 原生"不限"语义，别让编译器替用户做主。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = _load(tmpdir)
        wf = compile_workflow(spec)
    assert "parallelism" not in wf["spec"], wf["spec"].get("parallelism")
    assert RuntimeConfig.from_spec(spec).parallelism == 0


def test_runtime_parallelism_is_emitted_at_workflow_level():
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = _load(tmpdir, "runtime:\n  parallelism: 4\n")
        wf = compile_workflow(spec)
    assert wf["spec"]["parallelism"] == 4, wf["spec"].get("parallelism")
    # 只在 workflow 顶层：不能跑到 template / task 上去
    for template in wf["spec"]["templates"]:
        assert "parallelism" not in template, template


def test_legacy_k8s_block_accepts_parallelism():
    """旧名 k8s: 与 runtime: 行为一致（迁移期不能只有新名生效）。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = _load(tmpdir, "k8s:\n  parallelism: 3\n")
        cfg = RuntimeConfig.from_spec(spec)
        wf = compile_workflow(spec)
    assert cfg.parallelism == 3, cfg.parallelism
    assert wf["spec"]["parallelism"] == 3


def test_cli_runtime_override_wins_over_spec_value():
    """同一份 spec 多环境部署：--runtime 文件按 key 覆盖 spec 里的值。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = _load(tmpdir, "runtime:\n  parallelism: 2\n")
        cfg = RuntimeConfig.from_spec(spec)
        assert cfg.parallelism == 2
        cfg.apply_mapping(yaml.safe_load("parallelism: 16"), where="--runtime")
        assert cfg.parallelism == 16, cfg.parallelism
        wf = compile_workflow(spec, runtime=cfg)
    assert wf["spec"]["parallelism"] == 16


def test_workflow_template_carries_same_parallelism():
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = _load(tmpdir, "runtime:\n  parallelism: 5\n")
        wf = compile_workflow(spec)
        tmpl = compile_template(spec)
    assert tmpl["spec"]["parallelism"] == 5
    assert tmpl["spec"] == wf["spec"], "模板与 Workflow 的 spec 必须完全一致"


def test_invalid_parallelism_values_are_rejected():
    """0 / 负数 / 字符串 / bool / 小数：报错并说清楚"不写就是不限"。"""
    for raw in ("0", "-1", '"4"', "true", "1.5"):
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                RuntimeConfig.from_mapping({"parallelism": yaml.safe_load(raw)})
            except SpecError as exc:
                message = str(exc)
            else:
                raise AssertionError(f"parallelism: {raw} 应该被拒绝")
        assert "parallelism 必须是正整数" in message, (raw, message)


def test_typo_in_parallelism_key_is_rejected():
    """拼错不能静默忽略——否则"我配了并发上限却没生效"会静默发生。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            RuntimeConfig.from_mapping({"paralellism": 8})
        except SpecError as exc:
            message = str(exc)
        else:
            raise AssertionError("拼错的键名必须报错")
    assert "未知字段" in message, message
    assert "paralellism" in message, message


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
    print(f"\n{'并发上限测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
