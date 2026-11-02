"""显式插件加载：框架不 import 任何领域模块。

领域能力（例如领域专用的质量检查）通过 spec 顶层 ``plugins`` 声明::

    plugins: ["my_domain_checks"]

加载时把 spec 所在目录及其父目录临时加入 ``sys.path``——领域示例放在
``examples/<name>/`` 下、spec 放在 ``examples/<name>/pipelines/``，
因此插件模块（如 ``my_domain_checks.py``）能被直接 import。

方向是单向的：``examples -> framework``，框架层永远不反向 import 领域代码。
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

from .spec import SpecError


def search_dirs_for(spec_path: str | None) -> list[Path]:
    """插件模块的候选目录：spec 所在目录 + 父目录（examples/<name>/）。"""
    if not spec_path:
        return []
    path = Path(spec_path).resolve()
    return [path.parent, path.parent.parent]


def load(modules: list[str], *, search_dirs=()) -> list[str]:
    """按声明顺序 import 插件模块（它们通过装饰器完成注册）。

    加载失败直接报 ``SpecError``：插件声明错时应该在启动期失败，
    而不是跑到一半发现"检查未注册"。
    """
    for directory in search_dirs:
        entry = str(Path(directory).resolve())
        if entry not in sys.path:
            sys.path.insert(0, entry)
    loaded: list[str] = []
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception as exc:
            raise SpecError(f"插件模块加载失败: {name!r}: {exc!r}") from exc
        loaded.append(name)
    return loaded
