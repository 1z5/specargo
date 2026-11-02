"""断点续跑的终态判定：本地 runner 与集群编译产物**共用同一份定义**。

历史上"什么算这个 stage 成功了"在两个执行目标里各写一份：
本地 runner 判 ``verified``，集群侧曾经只判 ``generated``——于是同一条管线
本地会跳过、集群会重跑（或反过来），这类漂移在 CI 里看不出来。

规则（有意为之，理由见 docstring）::

    stage 配了 checks  → 只认 verified（闸门跑过且通过）
    stage 没配 checks  → generated 即终态（否则框架等于强制每个 stage 都配闸门）

``k8s.resume`` 的编译产物与本地 runner 都从这里取判定，杜绝双份实现。
"""
from __future__ import annotations

from .state import GENERATED, VERIFIED

#: 配了质量闸门时的成功终态
GATED_TERMINAL: tuple[str, ...] = (VERIFIED,)
#: 没配质量闸门时的成功终态
PLAIN_TERMINAL: tuple[str, ...] = (GENERATED, VERIFIED)


def stage_terminal_statuses(has_checks: bool) -> tuple[str, ...]:
    """返回该 stage 的成功终态集合（入参：是否配置了 checks）。"""
    return GATED_TERMINAL if has_checks else PLAIN_TERMINAL


def is_terminal(status: str, has_checks: bool) -> bool:
    """该状态是否意味着"这个 stage 对这个分区已经成功完成、可以跳过"。"""
    return status in stage_terminal_statuses(has_checks)


def describe_terminal(has_checks: bool) -> str:
    """给 CLI / 编译产物注释用的可读描述。"""
    statuses = stage_terminal_statuses(has_checks)
    return "/".join(statuses)
