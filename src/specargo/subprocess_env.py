"""子进程环境：让框架启动的任务/检查能 ``import specargo``。

包化后 spec 里的业务命令写成 ``python3 -m specargo.examples.etl_steps ...``
（或用户自己的 ``-m your_package``）。runner 以 ``cwd=workdir`` 起子进程，
相对 PYTHONPATH 会失效；容器里虽然 ``pip install`` 过，但本地未安装时也要能跑。

因此框架在启动任何子进程前，把「specargo 包所在目录的父目录」加到
``PYTHONPATH`` 前面——用户不需要为了跑 demo 先 pip install，也不需要自己
设 PYTHONPATH。已安装（site-packages）时这个注入是无害的。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def package_root() -> Path:
    """返回包含 ``specargo`` 包的目录（src 布局下即 ``<repo>/src``）。"""
    return Path(__file__).resolve().parent.parent


def child_env(base: dict | None = None) -> dict:
    """在 base 环境基础上注入 PYTHONPATH（保留原有值，框架路径在前）。"""
    env = dict(base if base is not None else os.environ)
    existing = env.get("PYTHONPATH", "")
    parts = [str(package_root())] + ([existing] if existing else [])
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def child_python() -> str:
    """子进程使用的解释器（与当前进程一致，避免 PATH 里挑到别的 python）。"""
    return sys.executable or "python3"
