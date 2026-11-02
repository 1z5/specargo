"""通用收口告警：run 级汇总渲染 + webhook（可选）。

框架层只负责"把对账结果变成一条人能读懂的消息 + 可插拔的 webhook"；
领域特有的分组/文案由调用方传入的对账结果决定，不含领域知识。
未配置 webhook 时只打印日志，避免本地演练误触发。
"""
from __future__ import annotations

import json
import os
import urllib.request


def render_summary(run_id: str, recons: list[dict]) -> str:
    """把多 dataset 的对账结果渲染成一段文本（钉钉/飞书/Slack 通用）。"""
    ok = all(r.get("ok") for r in recons)
    lines = [f"批次 {run_id} 对账结果: {'PASS' if ok else 'FAIL'}"]
    for r in recons:
        lines.append(
            f"  dataset={r['dataset']}: expected={r['expected']} "
            f"verified={r['verified']} missing={len(r['missing'])} "
            f"not_verified={len(r['not_verified'])}"
        )
        if r["missing"]:
            lines.append(f"    漏数: {', '.join(r['missing'][:10])}")
        if r["not_verified"]:
            lines.append(f"    未过检: {', '.join(r['not_verified'][:10])}")
    return "\n".join(lines)


def send_webhook(text: str, url: str | None = None) -> bool:
    """发送文本告警；未配置 URL 时返回 False（只打日志，不报错）。"""
    url = url or os.getenv("NOTIFY_WEBHOOK", "")
    if not url:
        return False
    from urllib.parse import urlparse
    scheme = urlparse(url).scheme.lower()
    if scheme not in ("http", "https"):
        print(f"[notify] 不支持的 webhook scheme: {scheme!r}（只允许 http/https）")
        return False
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode()
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:   # noqa: S310
            return resp.status == 200
    except Exception as exc:          # 告警通道失败不能改变对账结论
        print(f"[notify] webhook 发送失败（不影响对账判定）: {exc!r}")
        return False
