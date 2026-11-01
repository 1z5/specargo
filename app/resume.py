"""断点续跑与幂等：输入指纹 + 跳过判定。

设计动机（原实现的缺口）：
    原 ``pipeline.py`` 中途失败只能整本重跑——它从不检查"这一章是否已经生成过"。
    而 ``tts_client.generate()`` 每次调用前都会落盘 ``.input.txt``（原文快照），
    这本身就是输入的指纹，只是没有被用来做跳过决策。
    本模块把"输入指纹"变成显式机制：

        输出存在 且 指纹匹配  -> 跳过（幂等重跑安全，= 断点续跑）
        输出存在 但 指纹变化  -> 重生成（输入改了，旧产物作废）
        输出不存在            -> 生成

    指纹同时用于"错位检测"：同一章节两次生成的输入不同，说明上游文案变过。
"""
from __future__ import annotations

import hashlib
from pathlib import Path

# 指纹文件名约定：<audio>.input.sha256（与 .input.txt / .prompt.txt 影子文件同一套风格）
FINGERPRINT_SUFFIX = ".input.sha256"


def fingerprint(text: str) -> str:
    """输入文本的稳定指纹（sha256）。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fingerprint_path(output_path: Path) -> Path:
    return output_path.parent / (output_path.name + FINGERPRINT_SUFFIX)


def write_fingerprint(output_path: Path, text: str) -> str:
    """生成成功后落盘指纹，供下次重跑判断是否可跳过。"""
    fp = fingerprint(text)
    fingerprint_path(output_path).write_text(fp + "\n", encoding="utf-8")
    return fp


def read_fingerprint(output_path: Path) -> str | None:
    p = fingerprint_path(output_path)
    if not p.exists():
        return None
    value = p.read_text(encoding="utf-8").strip()
    return value or None


def should_skip(output_path: Path, text: str) -> bool:
    """输出已存在且输入指纹未变 -> 可跳过。

    这是"断点续跑"的最小充分条件：任务可重入 + 结果可复用。
    """
    if not output_path.exists():
        return False
    recorded = read_fingerprint(output_path)
    return recorded is not None and recorded == fingerprint(text)


def skip_reason(output_path: Path, text: str) -> str:
    """给人看的跳过/不跳过原因（日志与排障用）。"""
    if not output_path.exists():
        return "output-missing"
    recorded = read_fingerprint(output_path)
    if recorded is None:
        return "no-fingerprint"          # 存量产物：无指纹，保守起见重生成
    if recorded != fingerprint(text):
        return "input-changed"           # 输入变了，旧产物作废
    return "skippable"
