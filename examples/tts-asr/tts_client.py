"""OpenAI TTS 客户端（改造版）。

相对原版的三处改动：
    1. **断点续跑/幂等**：生成前用输入指纹判断能否跳过（resume.should_skip），
       成功后落盘 ``.input.sha256``。重跑不再重复花钱、重复写产物。
    2. **重试次数可配置**（``TTS_MAX_RETRIES``）：迁到 Argo 后，瞬时错误应由
       Argo 的 retryStrategy 负责；此时把容器内重试降到 1，避免两层重试相乘。
       （这是个真实的取舍：Pod 内重试 × Argo 重试 = 最坏 3×3=9 次调用。）
    3. **延迟导入 openai**：便于离线单测（不需要 API key 也能测跳过逻辑）。

兼容性注意：``generate()`` 现在返回 bool（True=新生成 / False=跳过），
调用方若忽略返回值行为不变；但 ``--fix`` 这类"必须重新生成"的场景
必须显式传 ``skip_existing=False``。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  (把仓库 app/ 加入 sys.path)

import os
import time
from pathlib import Path

from config import Config
from specargo.resume import should_skip, write_fingerprint


class TTSClient:
    def __init__(self, config: Config) -> None:
        # 延迟导入：单测与裁剪依赖（edge-tts 路径）时不需要 openai
        from openai import OpenAI

        kwargs: dict = {"api_key": config.openai_api_key}
        if config.openai_base_url:
            kwargs["base_url"] = config.openai_base_url
        self._client = OpenAI(**kwargs)
        self._model = config.tts_model
        self._fmt = config.tts_format
        self._instructions = config.tts_instructions
        self._speed = float(getattr(config, "tts_speed", 1.0) or 1.0)  # 0.25~4.0
        self._delay = config.api_delay_seconds
        self._max_retries = int(os.getenv("TTS_MAX_RETRIES", "3"))

    # ------------------------------------------------------------------
    def generate(
        self,
        text: str,
        voice: str,
        output_path: Path,
        max_retries: int | None = None,
        skip_existing: bool = True,
    ) -> bool:
        """生成音频。返回 True=本次新生成，False=跳过（已存在且输入未变）。

        先落盘影子文件（.prompt.txt 日志 / .input.txt 原文基准）——这一约定保留，
        因为 verify_audio.py 的原文提取依赖它；新增的 .input.sha256 指纹也在这里落盘。
        """
        retries = self._max_retries if max_retries is None else max_retries
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 1) 断点续跑：输入未变且产物已存在 -> 跳过
        if skip_existing and should_skip(output_path, text):
            print(f"      [skip] {output_path.name} 已存在且输入未变（指纹匹配）")
            return False

        # 2) 影子文件（原约定，保持兼容）
        log_path = output_path.with_suffix(".prompt.txt")
        log_path.write_text(
            f"model: {self._model}\nvoice: {voice}\nspeed: {self._speed}\n"
            f"instructions: {self._instructions}\n\n{text}",
            encoding="utf-8",
        )
        output_path.with_suffix(".input.txt").write_text(text, encoding="utf-8")

        # 3) 调用 + 重试（指数退避）
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                kwargs: dict = {
                    "model": self._model,
                    "voice": voice,
                    "input": text,
                    "response_format": self._fmt,
                    "speed": self._speed,
                }
                if self._instructions:
                    kwargs["instructions"] = self._instructions
                with self._client.audio.speech.with_streaming_response.create(**kwargs) as response:
                    response.stream_to_file(str(output_path))
                # 4) 成功后落盘输入指纹（下次重跑的跳过依据）
                write_fingerprint(output_path, text)
                return True
            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"TTS generation failed after {retries} attempts: {last_error}")
