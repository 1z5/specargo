"""CLI 的错误呈现与统一入口：退出码要能区分"写错了"和"跑挂了"。

为什么值得单独一套：这两类错误在 CI 里长得一样是最坏的体验——
- spec 拼错 / 版本写高 / 文件不存在 = **用法错误** → 退出码 2，一行可行动的 stderr；
- 管线真的跑失败 / 对账不过 = **执行失败** → 退出码 1，带完整现场。

同时锁住统一入口（``specargo run`` / ``specargo compile`` / ``specargo gate``）——
它之前没有任何测试覆盖，于是"``specargo run x.yaml`` 直接 TypeError"这种
P0 级问题能一直躺着：``main()`` 不接受参数，而 ``cli.main`` 却把参数传了进去。
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"

_ENV = {**os.environ, "PYTHONPATH": str(_SRC) + os.pathsep + os.environ.get("PYTHONPATH", "")}

_GOOD_SPEC = """\
version: 1
name: cli-good
partitions:
  from: list
  items: ["1"]
stages:
  - name: only
    command: "true"
"""

_BAD_VERSION_SPEC = """\
version: 2
name: cli-bad-version
partitions:
  from: list
  items: ["1"]
stages:
  - name: only
    command: "true"
"""

_FAILING_SPEC = """\
version: 1
name: cli-failing
partitions:
  from: list
  items: ["1"]
stages:
  - name: only
    command: "false"
"""


def _run(args: list[str], cwd: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], capture_output=True, text=True,
                          env=_ENV, cwd=cwd, timeout=180)


def _write(tmp: Path, name: str, body: str) -> str:
    path = tmp / name
    path.write_text(body, encoding="utf-8")
    return str(path)


def _stderr(result: subprocess.CompletedProcess) -> str:
    return (result.stderr or "") + (result.stdout or "")


def test_spec_error_is_usage_exit_2_without_traceback():
    """spec 写错：退出码 2、stderr 一行可行动提示、不出现 traceback。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "bad.yaml", _BAD_VERSION_SPEC)
        result = _run(["-m", "specargo.runner", spec, "--workdir", tmpdir])
    assert result.returncode == 2, (result.returncode, _stderr(result)[-400:])
    assert "spec 错误" in result.stderr, result.stderr
    assert "升级 specargo" in result.stderr, result.stderr
    assert "Traceback" not in _stderr(result), _stderr(result)


def test_missing_spec_file_is_friendly():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = _run(["-m", "specargo.runner", str(Path(tmpdir) / "nope.yaml"),
                       "--workdir", tmpdir])
    assert result.returncode == 2, result.returncode
    assert "文件不存在" in result.stderr, result.stderr
    assert "Traceback" not in _stderr(result), _stderr(result)


def test_pipeline_failure_stays_exit_1_not_2():
    """真正的执行失败必须是 1：与"写错了"分开，否则 CI 无法按类分流。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "fail.yaml", _FAILING_SPEC)
        result = _run(["-m", "specargo.runner", spec,
                       "--workdir", tmpdir, "--state-db", str(tmp / "s.db")])
    assert result.returncode == 1, (result.returncode, _stderr(result)[-400:])
    assert "spec 错误" not in result.stderr, result.stderr


def test_compile_spec_error_is_exit_2():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "bad.yaml", _BAD_VERSION_SPEC)
        result = _run(["-m", "specargo.spec2argo", spec, "-o", str(tmp / "wf.yaml")])
    assert result.returncode == 2, (result.returncode, _stderr(result)[-300:])
    assert "spec 错误" in result.stderr


def test_unified_entry_dispatches_run_and_compile():
    """统一入口 ``specargo <cmd>`` 必须真的能跑（此前会把参数传给 main() 直接 TypeError）。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "good.yaml", _GOOD_SPEC)
        run = _run(["-m", "specargo", "run", spec,
                    "--workdir", tmpdir, "--state-db", str(tmp / "s.db")])
        assert run.returncode == 0, (run.returncode, _stderr(run)[-400:])
        assert "Traceback" not in _stderr(run), _stderr(run)

        compile_result = _run(["-m", "specargo", "compile", spec,
                              "-o", str(tmp / "wf.yaml")])
        assert compile_result.returncode == 0, (compile_result.returncode,
                                                _stderr(compile_result)[-400:])
        assert "Traceback" not in _stderr(compile_result)

        bad = _write(tmp, "bad.yaml", _BAD_VERSION_SPEC)
        bad_run = _run(["-m", "specargo", "run", bad, "--workdir", tmpdir])
        assert bad_run.returncode == 2, bad_run.returncode
        assert "spec 错误" in bad_run.stderr


def test_unified_entry_passes_help_through_to_subcommand():
    """``specargo gate --help`` 要能看到 gate 自己的选项，而不是外层那三行 usage。"""
    result = _run(["-m", "specargo", "gate", "--help"])
    assert result.returncode == 0, (result.returncode, _stderr(result)[-300:])
    assert "--stage" in result.stdout, result.stdout
    assert "--artifact" in result.stdout, result.stdout


def test_unified_entry_rejects_unknown_command_with_exit_2():
    result = _run(["-m", "specargo", "bogus"])
    assert result.returncode == 2, result.returncode
    assert "invalid choice" in result.stderr, result.stderr


def test_unified_entry_without_args_prints_help_and_exits_2():
    result = _run(["-m", "specargo"])
    assert result.returncode == 2, result.returncode
    assert "usage: specargo" in result.stdout, result.stdout


# ---------------- YAML 语法错误：必须是"一行提示 + 退出码 2"，不能吐 traceback ----------------
#
# 这是一个真实缺口：PyYAML 的 ScannerError/ParserError 之前三处裸 yaml.safe_load
# 都没捕获，于是"少写一个冒号"会打印整屏 traceback、退出码还是 1（和执行失败混在一起）。
# YAML 写错属于用户输入问题，和 SpecError 走同一个出口。

_BROKEN_YAML = """\
name: broken
stages:
  - name: s
    command: "x"
   bad-indent: 1
"""


def test_broken_yaml_run_is_usage_error_without_traceback():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "broken.yaml", _BROKEN_YAML)
        result = _run(["-m", "specargo.runner", spec])
        assert result.returncode == 2, result.returncode
        text = _stderr(result)
        assert "Traceback" not in text, text
        assert "YAML 语法错误" in text, text
        assert "行" in text and "列" in text, text      # 行列号可定位


def test_broken_yaml_compile_is_usage_error_without_traceback():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "broken.yaml", _BROKEN_YAML)
        result = _run(["-m", "specargo.spec2argo", spec, "-o", str(tmp / "wf.yaml")])
        assert result.returncode == 2, result.returncode
        text = _stderr(result)
        assert "Traceback" not in text, text
        assert "YAML 语法错误" in text, text


def test_broken_runtime_yaml_is_usage_error_without_traceback():
    """--runtime 入口同样不能漏 traceback（之前它自己裸读 YAML）。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        spec = _write(tmp, "good.yaml", _GOOD_SPEC)
        broken_runtime = _write(tmp, "runtime.yaml", "parallelism: [\n")
        result = _run(["-m", "specargo.spec2argo", spec,
                       "--runtime", broken_runtime, "-o", str(tmp / "wf.yaml")])
        assert result.returncode == 2, result.returncode
        text = _stderr(result)
        assert "Traceback" not in text, text
        assert "YAML 语法错误" in text, text


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
    print(f"\n{'CLI 错误呈现测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
