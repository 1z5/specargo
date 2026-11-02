"""质量检查插件注册表：把"怎么算合格"变成可插拔。

原实现的耦合点：``verify_audio.py`` 里写死了 ASR 三态判定。
框架化之后，检查是插件——同一个引擎可以给 TTS 挂 ASR 检查、
给 ETL 挂 schema/行数检查、给视频管线挂 ffprobe 检查。

内置三个**通用**检查（不含任何领域知识）：
    file_nonempty  产物存在且非空
    json_schema    JSON 必填字段（config: {required: [...]}）
    row_count      行数等于期望或在区间内（config: {expect: N} / {min: A, max: B}）
    command        执行 shell 命令，非零即失败——领域检查的逃生口
                   （TTS 的 ASR 三态判定就通过它接入，框架不需要知道 ASR 是什么）

自定义检查::

    from checks import register

    @register("my_check")
    def my_check(artifact: Path, config: dict, ctx: dict) -> CheckResult:
        return CheckResult(passed=True, metric="1.0", detail="ok")
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

@dataclass
class CheckResult:
    passed: bool
    metric: str = ""          # 可量化指标（覆盖率、行数、相似度……）
    detail: str = ""          # 失败原因 / 样本


_REGISTRY: dict[str, Callable] = {}


def register(name: str):
    def deco(fn: Callable):
        _REGISTRY[name] = fn
        return fn
    return deco


def available() -> list[str]:
    return sorted(_REGISTRY)


def run_check(name: str, artifact: Path, config: dict, ctx: dict) -> CheckResult:
    fn = _REGISTRY.get(name)
    if fn is None:
        return CheckResult(False, detail=f"未注册的检查: {name}")
    try:
        return fn(artifact, config or {}, ctx or {})
    except Exception as exc:                       # 检查自身异常 = 不合格，不能静默
        return CheckResult(False, detail=f"检查 {name} 抛异常: {exc!r}")


# ---------------------------------------------------------------------------
# 内置通用检查
# ---------------------------------------------------------------------------

@register("file_nonempty")
def _file_nonempty(artifact: Path, config: dict, ctx: dict) -> CheckResult:
    if not artifact.exists():
        return CheckResult(False, detail=f"产物不存在: {artifact}")
    size = artifact.stat().st_size
    min_size = int(config.get("min_bytes", 1))
    if size < min_size:
        return CheckResult(False, metric=str(size), detail=f"产物过小: {size} < {min_size}")
    return CheckResult(True, metric=str(size), detail=f"{size} bytes")


@register("json_schema")
def _json_schema(artifact: Path, config: dict, ctx: dict) -> CheckResult:
    """必填字段检查。同时支持 JSON 数组、单 JSON 对象、JSONL（每行一个 JSON）。

    ⚠️ JSONL 要逐行解析：直接 json.loads 整个文件会在第二行报 "Extra data"
    （这个 bug 在 ETL E2E 里真实踩到过）。
    """
    required = list(config.get("required", []))
    if not artifact.exists():
        return CheckResult(False, detail=f"产物不存在: {artifact}")
    text = artifact.read_text(encoding="utf-8")

    rows: list = []
    parse_error = ""
    try:
        data = json.loads(text)
        rows = data if isinstance(data, list) else [data]
    except Exception:
        # 回退 JSONL：逐行解析
        try:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        except Exception as exc:
            parse_error = str(exc)

    if parse_error:
        return CheckResult(False, detail=f"不是合法 JSON/JSONL: {parse_error}")
    if not rows:
        return CheckResult(False, detail="空文件，无记录可校验")

    missing = sorted({k for k in required for row in rows if isinstance(row, dict) and k not in row})
    if missing:
        sample = missing[:5]
        return CheckResult(False, metric=str(len(missing)),
                           detail=f"{len(rows)} 行中缺字段: {sample}"
                                  + (" ..." if len(missing) > 5 else ""))
    return CheckResult(True, metric=f"{len(rows)}行/{len(required)}字段", detail="字段齐全")


@register("row_count")
def _row_count(artifact: Path, config: dict, ctx: dict) -> CheckResult:
    if not artifact.exists():
        return CheckResult(False, detail=f"产物不存在: {artifact}")
    lines = [ln for ln in artifact.read_text(encoding="utf-8").splitlines() if ln.strip()]
    n = len(lines)
    if "expect" in config:
        want = int(config["expect"])
        ok = n == want
        return CheckResult(ok, metric=str(n),
                           detail="" if ok else f"行数 {n} != 期望 {want}")
    lo, hi = config.get("min"), config.get("max")
    ok = (lo is None or n >= int(lo)) and (hi is None or n <= int(hi))
    return CheckResult(ok, metric=str(n), detail="" if ok else f"行数 {n} 超出 [{lo}, {hi}]")


@register("command")
def _command(artifact: Path, config: dict, ctx: dict) -> CheckResult:
    """领域检查的逃生口：跑一条 shell 命令，非零退出码即不合格。

    命令里可用 {{artifact}} / {{params.*}} / {{partition.*}} 占位。
    TTS 的 ASR 三态判定就是这样接入的——框架本身不包含任何 ASR 知识。
    """
    from spec import render

    cmd = config.get("run", "")
    if not cmd:
        return CheckResult(False, detail="command 检查缺少 run")
    context = dict(ctx or {})
    context.setdefault("artifact", str(artifact))
    rendered = render(cmd, context)
    # 与任务同工作目录执行（框架在 ctx 里注入 workdir；单测可直接调 run_check 不带该键）
    cwd = context.get("workdir") or None
    proc = subprocess.run(rendered, shell=True, capture_output=True, text=True, cwd=cwd)
    ok = proc.returncode == 0
    return CheckResult(ok, metric=str(proc.returncode),
                       detail=(proc.stdout or "") + (proc.stderr or ""))


# 领域检查在各自模块里注册（避免框架依赖领域代码）：
#   app/checks_tts.py -> @register("asr_three_state")
try:  # pragma: no cover - 仅在 TTS 实例存在时生效
    import checks_tts  # noqa: F401
except Exception:
    pass
