"""spec 契约版本与兼容策略：版本缺失/越界/别名误写/迁移链/产物打标。

为什么单独一套：没有版本号的 spec 是"一次性契约"——改一个字段，所有旧文件一起红，
且报错只会说"未知字段"。这一套锁住四件事：

1. **缺省不逼人回填**：老 spec 不写 version 就是当前版本，不告警（否则一升级全库告警）；
2. **越界可行动**：version 高于框架支持时，报错必须说"升级 specargo"，而不是"未知字段"；
3. **迁移是代码强制的承诺**：有迁移实现就自动迁移并**可见地**告警；没有就明确报错不猜；
4. **版本跟着产物走**：编译出的 Workflow/WorkflowTemplate 带 spec 版本注解。
"""
from __future__ import annotations

import contextlib
import importlib.util
import sys
import tempfile
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

import specargo.spec as spec_mod                                       # noqa: E402
from specargo.spec import (SpecDeprecationWarning, SpecError,          # noqa: E402
                           load_spec)
from specargo.spec2argo import compile_template, compile_workflow      # noqa: E402

_CURRENT = spec_mod.SPEC_VERSION_CURRENT

_MINIMAL = """\
name: demo
{version}partitions:
  from: list
  items: ["1"]
stages:
  - name: only
    command: "echo {{{{partition.key}}}}"
"""


def _write(tmp: Path, body: str) -> str:
    path = tmp / "spec.yaml"
    path.write_text(body, encoding="utf-8")
    return str(path)


def _minimal(tmp: Path, version_line: str = "") -> str:
    return _write(tmp, _MINIMAL.format(version=version_line))


@contextlib.contextmanager
def _patched_version(current: int, migrations: dict):
    """临时改契约版本与迁移链（模拟"框架升级到 vN"），退出时严格还原。"""
    old_current = spec_mod.SPEC_VERSION_CURRENT
    old_migrations = dict(spec_mod._MIGRATIONS)
    spec_mod.SPEC_VERSION_CURRENT = current
    spec_mod._MIGRATIONS.clear()
    spec_mod._MIGRATIONS.update(migrations)
    try:
        yield
    finally:
        spec_mod.SPEC_VERSION_CURRENT = old_current
        spec_mod._MIGRATIONS.clear()
        spec_mod._MIGRATIONS.update(old_migrations)


def _load_capturing(path: str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spec = load_spec(path)
    return spec, caught


def test_missing_version_defaults_to_current_without_warning():
    """老 spec 不写 version：按当前版本加载，且**不**告警（升级框架不该吵醒所有用户）。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        spec, caught = _load_capturing(_minimal(Path(tmpdir)))
    assert spec.version == _CURRENT, spec.version
    assert not caught, [str(w.message) for w in caught]


def test_explicit_current_version_loads():
    with tempfile.TemporaryDirectory() as tmpdir:
        spec, caught = _load_capturing(
            _minimal(Path(tmpdir), f"version: {_CURRENT}\n"))
    assert spec.version == _CURRENT
    assert not caught


def test_future_version_rejected_with_actionable_message():
    """版本高于框架支持：必须指出"升级 specargo"，不能落成"未知字段"。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _minimal(Path(tmpdir), f"version: {_CURRENT + 1}\n")
        try:
            load_spec(path)
        except SpecError as exc:
            message = str(exc)
        else:
            raise AssertionError("高版本 spec 必须加载失败")
    assert "升级 specargo" in message, message
    assert str(_CURRENT + 1) in message, message


def test_version_must_be_positive_int():
    """0 / 负数 / 字符串 / 小数 / bool / 空值 全部拒绝。

    ``version: true`` 是重点：bool 是 int 的子类，不显式拦就会被当成 1 放行。
    """
    bad_values = ["0", "-1", '"1"', "1.5", "true", ""]
    with tempfile.TemporaryDirectory() as tmpdir:
        for raw in bad_values:
            line = "version:\n" if raw == "" else f"version: {raw}\n"
            path = _minimal(Path(tmpdir), line)
            try:
                load_spec(path)
            except SpecError as exc:
                assert "version 必须是正整数" in str(exc), (raw, str(exc))
            else:
                raise AssertionError(f"version: {raw!r} 应该被拒绝")


def test_version_field_alias_gets_explicit_hint():
    """写成 spec_version / schema_version 时，报错要直接说正确字段名。"""
    for alias in ("spec_version", "schema_version", "apiVersion"):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _minimal(Path(tmpdir), f"{alias}: {_CURRENT}\n")
            try:
                load_spec(path)
            except SpecError as exc:
                message = str(exc)
            else:
                raise AssertionError(f"{alias} 应该报错")
        assert alias in message, message
        assert "version" in message, message


def test_old_version_migrates_and_warns_visibly():
    """有迁移实现时自动迁移，并且告警**默认可见**（UserWarning 子类，不是静默的 Deprecation）。"""
    def v1_to_v2(raw: dict) -> dict:
        # 真实场景：v2 把 k8s: 正式改名为 runtime:
        data = dict(raw)
        if "k8s" in data:
            data["runtime"] = data.pop("k8s")
        return data

    with tempfile.TemporaryDirectory() as tmpdir:
        body = _MINIMAL.format(version="version: 1\n") + \
            "k8s:\n  image: legacy:1\n"
        path = _write(Path(tmpdir), body)
        with _patched_version(2, {1: v1_to_v2}):
            spec, caught = _load_capturing(path)
        assert spec.version == 2, spec.version
        assert spec.runtime == {"image": "legacy:1"}, spec.runtime
        assert not spec.k8s, spec.k8s
        warned = [w for w in caught if issubclass(w.category, SpecDeprecationWarning)]
        assert len(warned) == 1, [str(w.message) for w in caught]
        assert "1->2" in str(warned[0].message), str(warned[0].message)
        # 默认可见：不带 simplefilter 也必须能收到（模拟用户直接跑 CLI 的情形）
        with warnings.catch_warnings(record=True) as default_caught:
            warnings.resetwarnings()
            with _patched_version(2, {1: v1_to_v2}):
                load_spec(path)
    assert any(issubclass(w.category, SpecDeprecationWarning) for w in default_caught), \
        "spec 版本过时告警默认不可见 = 静默容忍旧写法"


def test_missing_migration_raises_instead_of_guessing():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _minimal(Path(tmpdir), "version: 1\n")
        with _patched_version(2, {}):
            try:
                load_spec(path)
            except SpecError as exc:
                message = str(exc)
            else:
                raise AssertionError("缺迁移实现时必须报错")
    assert "迁移实现" in message, message
    assert "1" in message and "2" in message, message


def test_multi_step_migration_runs_in_order():
    """跨多个版本时按 N->N+1 顺序逐级迁移，不能跳级。"""
    order: list[str] = []

    def v1_to_v2(raw: dict) -> dict:
        order.append("1->2")
        return {**raw, "description": "step-2"}

    def v2_to_v3(raw: dict) -> dict:
        order.append("2->3")
        return {**raw, "description": "step-3"}

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _minimal(Path(tmpdir), "version: 1\n")
        with _patched_version(3, {1: v1_to_v2, 2: v2_to_v3}):
            spec, caught = _load_capturing(path)
    assert order == ["1->2", "2->3"], order
    assert spec.description == "step-3", spec.description
    assert spec.version == 3, spec.version
    assert "1->2, 2->3" in str(caught[0].message), str(caught[0].message)


def test_compiled_outputs_carry_spec_version():
    """编译产物带版本注解：线上工作流能回答"按哪版契约编译"。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _minimal(Path(tmpdir))
        spec = load_spec(path)
    wf = compile_workflow(spec)
    tmpl = compile_template(spec)
    assert wf["metadata"]["annotations"]["specargo.dev/spec-version"] == str(_CURRENT)
    assert tmpl["metadata"]["annotations"]["specargo.dev/spec-version"] == str(_CURRENT)
    # 注解不能反向污染 spec 段（模板与工作流的 spec 必须仍然完全一致）
    assert tmpl["spec"] == wf["spec"]


def test_repo_pipelines_load_cleanly_at_current_version():
    """仓库自带三条管线必须无告警地按当前版本加载（版本机制不能误伤自己）。"""
    paths = sorted((ROOT / "pipelines").glob("*.yaml")) + \
        sorted((ROOT / "examples" / "tts-asr" / "pipelines").glob("*.yaml"))
    assert paths, "没找到仓库自带的管线"
    for path in paths:
        spec, caught = _load_capturing(str(path))
        assert spec.version == _CURRENT, (path, spec.version)
        assert not caught, (str(path), [str(w.message) for w in caught])


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
    print(f"\n{'spec 版本测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
