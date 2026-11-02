"""specargo.examples：框架自带的**示例步骤**（真实使用时替换为你自己的模块）。

这些模块的用途：
    - 让 ``pipelines/*.yaml`` 不依赖外部数据源就能跑通（CI 与本地演练）；
    - 演示"领域步骤如何接入框架"——它们只做通用 ETL / 数据集构建逻辑，
      不含任何领域知识。

换成你自己的步骤：把 pipelines 里的 ``-m specargo.examples.etl_steps``
换成 ``-m your_package.your_module``（或任何可执行命令），框架其余部分不变。
"""
from __future__ import annotations
