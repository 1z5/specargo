"""Argo 输出参数：把业务结果暴露给工作流下游。

Argo 的 executor 会采集业务容器写进以下路径的文件，作为 node 的 outputs：
    /argo/outputs/parameters/<name>   -> 文本，节点 outputs.parameters
    /argo/outputs/artifacts/<name>    -> 文件/目录，节点 outputs.artifacts
下游通过 ``{{tasks.<name>.outputs.parameters.<param>}}`` 或
``{{tasks.<name>.outputs.artifacts.<art>}}`` 引用。

本地/单机跑同样可用：设 ``PIPELINE_ARGO_OUTPUTS=1`` 即写入（默认目录可被
``PIPELINE_ARGO_OUTPUT_DIR`` 覆盖），便于离线验证"工作流契约"而不需要集群。
旧前缀（见 ``envcompat.py``）仍兼容读取一个版本。
"""
from __future__ import annotations

import os
from pathlib import Path

from .envcompat import (LEGACY_ARGO_ARTIFACT_DIR, LEGACY_ARGO_OUTPUT_DIR,
                       LEGACY_ARGO_OUTPUTS, LEGACY_ARGO_PARAM_DIR, getenv)

DEFAULT_PARAM_DIR = Path("/argo/outputs/parameters")
DEFAULT_ARTIFACT_DIR = Path("/argo/outputs/artifacts")


def _base_dir() -> Path:
    """输出根目录优先级：PIPELINE_ARGO_OUTPUT_DIR > PARAM_DIR 的父目录 > /argo/outputs。

    这样本地联调时只设 PIPELINE_ARGO_PARAM_DIR 也能让 artifacts 落到同一棵树，
    不会误写到只读的 /argo。
    """
    output_dir = getenv("PIPELINE_ARGO_OUTPUT_DIR", LEGACY_ARGO_OUTPUT_DIR)
    if output_dir:
        return Path(output_dir)
    param_dir_value = getenv("PIPELINE_ARGO_PARAM_DIR", LEGACY_ARGO_PARAM_DIR)
    if param_dir_value:
        return Path(param_dir_value).parent
    return Path("/argo/outputs")


def argo_enabled() -> bool:
    """在 Argo Pod 内（/argo 存在）或显式开启时启用。"""
    return Path("/argo").exists() or getenv(
        "PIPELINE_ARGO_OUTPUTS", LEGACY_ARGO_OUTPUTS) == "1"


def param_dir() -> Path:
    override = getenv("PIPELINE_ARGO_PARAM_DIR", LEGACY_ARGO_PARAM_DIR)
    return Path(override) if override else _base_dir() / "parameters"


def artifact_dir() -> Path:
    override = getenv("PIPELINE_ARGO_ARTIFACT_DIR", LEGACY_ARGO_ARTIFACT_DIR)
    return Path(override) if override else _base_dir() / "artifacts"


def write_param(name: str, value) -> bool:
    """写一个标量输出参数；未启用时静默跳过（返回 False）。"""
    if not argo_enabled():
        return False
    d = param_dir()
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(f"{value}\n", encoding="utf-8")
    return True


def write_params(mapping: dict) -> int:
    return sum(1 for k, v in mapping.items() if write_param(k, v))


def publish_artifact(name: str, source: Path) -> bool:
    """把文件/目录放到 artifacts 输出目录（Argo 会上传到 artifact repository）。"""
    if not argo_enabled():
        return False
    import shutil

    d = artifact_dir()
    d.mkdir(parents=True, exist_ok=True)
    target = d / name
    src = Path(source)
    if src.is_dir():
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(src, target)
    else:
        shutil.copy2(src, target)
    return True
