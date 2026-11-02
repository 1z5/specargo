"""环境变量兼容读取：新名优先，旧名回退一个发布周期。

框架契约统一用 ``PIPELINE_`` 前缀；历史上的 ``TTS_`` 前缀保留兼容读取，
方便已有部署平滑迁移（一个版本后删除回退逻辑）。
"""
from __future__ import annotations

import os


def getenv(name: str, legacy: str | None = None, default: str = "") -> str:
    """读环境变量：``name`` 非空则用；否则回退 ``legacy``；都没有用 ``default``。"""
    value = os.getenv(name)
    if value not in (None, ""):
        return value
    if legacy:
        value = os.getenv(legacy)
        if value not in (None, ""):
            return value
    return default

# 已废弃的旧前缀：集中在这里，便于一个版本后整体删除。
# 框架纯净度守卫（CI）对本文件做例外——它是唯一的兼容层。
LEGACY_RUN_ID = "TTS_RUN_ID"
LEGACY_STATE_DB = "TTS_STATE_DB"
LEGACY_ARGO_OUTPUTS = "TTS_ARGO_OUTPUTS"
LEGACY_ARGO_OUTPUT_DIR = "TTS_ARGO_OUTPUT_DIR"
LEGACY_ARGO_PARAM_DIR = "TTS_ARGO_PARAM_DIR"
LEGACY_ARGO_ARTIFACT_DIR = "TTS_ARGO_ARTIFACT_DIR"
