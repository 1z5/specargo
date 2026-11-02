"""specargo：声明式批量数据管线框架。

一份 YAML 描述管线（stage / 依赖 / 分区 / 重试 / 质量闸门 / 产物契约），
两个执行目标：本地 runner 直接跑，或编译成 Argo Workflows CRD 上集群。

框架层不含任何领域知识；领域能力通过 spec 顶层 ``plugins:`` 显式加载
（方向：examples -> framework，框架永不反向 import 领域代码）。

命令行入口（也可用 ``python3 -m specargo.<module>``）::

    specargo run       pipelines/etl-demo.yaml     # 本地执行
    specargo compile   pipelines/etl-demo.yaml     # 编译成 Argo CRD
    specargo gate      --stage s --artifact a ...  # 质量闸门（集群 task 调用）
    specargo state     put|put-remote ...          # 状态登记
    specargo reconcile --journal ...               # 对账（onExit 用）
    specargo service                              # 常驻处理服务
"""
from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
