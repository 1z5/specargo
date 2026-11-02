"""把仓库 ``app/``（框架层）加入 sys.path，供 examples 以脚本方式运行。

领域示例与框架是单向依赖：``examples -> framework``。
所有需要 import 框架模块（state/checks/argo_io/...）的示例文件，第一行
（``from __future__`` 之后）先 ``import _bootstrap`` 即可通过脚本直接运行。
"""
from __future__ import annotations

import sys
from pathlib import Path

# 仓库布局：examples/tts-asr/ -> ../../app
# 镜像布局（examples Dockerfile）：/app/examples/tts-asr/ -> /app/framework（由 PYTHONPATH 提供）
_CANDIDATES = [Path(__file__).resolve().parents[2] / "app", Path("/app/framework")]
for _candidate in _CANDIDATES:
    if _candidate.is_dir():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break
else:  # pragma: no cover - 明确报错优于后续 ModuleNotFoundError
    raise RuntimeError("找不到框架代码目录：请检查仓库布局，或按 examples/tts-asr/Dockerfile 设置 PYTHONPATH")

