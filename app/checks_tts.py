"""TTS 领域检查插件：把 ASR 三态判定注册成框架的一个 check。

框架（``checks.py``）不包含任何 ASR 知识；领域知识通过注册表注入。
这样同一套 runner 既能跑 TTS 管线，也能跑 ETL / 视频管线。

本插件只是**适配层**：真正判定逻辑仍在 ``verify_audio.py`` / ``tts_verify/``，
通过 command 检查（退出码 0/1）接入——保持"生成层不校验、校验层不生成"的分层。
"""
from __future__ import annotations

from pathlib import Path

from checks import CheckResult, register


@register("asr_three_state")
def asr_three_state(artifact: Path, config: dict, ctx: dict) -> CheckResult:
    """ASR 回检三态判定（FOUND / UNSURE / MISSING）。

    config:
        script: verify_audio.py 路径（默认 "verify_audio.py"）
        min_accuracy: 准确率阈值（默认 0.90）
        min_coverage: 覆盖率阈值（默认 0.95）
    约定：退出码 0 = 无确证漏读（PASS/WARN）；1 = 有 FAIL；2 = 用法/路径错误。
    """
    import subprocess

    from spec import render

    script = config.get("script", "verify_audio.py")
    min_accuracy = config.get("min_accuracy", 0.90)
    min_coverage = config.get("min_coverage", 0.95)
    target = config.get("target") or str(artifact)

    cmd = render(
        f"python {script} {target} --min-accuracy {min_accuracy} "
        f"--min-coverage {min_coverage} --state-db {{params.state_db}}",
        {**(ctx or {}), "params": {**(ctx.get("params", {}) if ctx else {}),
                                   "state_db": config.get("state_db", "")}},
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = (proc.stdout or "")[-800:]
    if proc.returncode == 2:
        return CheckResult(False, detail=f"校验器用法错误: {out}")
    passed = proc.returncode == 0
    # 从输出里抠出覆盖率/漏读数作为 metric（人工报告同样可读）
    metric = ""
    for line in out.splitlines():
        if "覆盖率" in line or "确证漏读" in line:
            metric = line.strip()[:80]
            break
    return CheckResult(passed, metric=metric, detail="" if passed else out[-300:])
