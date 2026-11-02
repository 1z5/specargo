"""把仓库 ``src/``（框架包 specargo）加入 sys.path，供 examples 以脚本方式运行。

领域示例与框架是单向依赖：``examples -> framework``。
所有需要 import 框架模块（state/checks/argo_io/...）的示例文件，第一行
（``from __future__`` 之后）先 ``import _bootstrap`` 即可通过脚本直接运行。

包化后优先用已安装的 ``specargo`` 包（``pip install .``）；未安装时退回
仓库 ``src/`` 目录，因此本地跑示例不需要先安装。
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# 已安装（pip install / 镜像内 install）则什么都不用做
if importlib.util.find_spec("specargo") is None:
    # 仓库布局：examples/tts-asr/ -> ../../src
    # 镜像布局（examples Dockerfile）：/app/examples/tts-asr/ -> /app/src（由 PYTHONPATH 提供）
    _CANDIDATES = [Path(__file__).resolve().parents[2] / "src", Path("/app/src")]
    for _candidate in _CANDIDATES:
        if _candidate.is_dir():
            if str(_candidate) not in sys.path:
                sys.path.insert(0, str(_candidate))
            break
    else:  # pragma: no cover - 明确报错优于后续 ModuleNotFoundError
        raise RuntimeError(
            "找不到框架包 specargo：请 pip install .，或检查仓库 src/ 布局")
