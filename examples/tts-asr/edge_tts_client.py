"""Edge-TTS 客户端: 微软免费神经网络语音 (与 TTSClient 接口一致)。

接口约定 (与 tts_client.TTSClient 对齐, 供 pipeline 无差别调用):
    generate(text: str, voice: str, output_path: Path, max_retries: int) -> None

每次生成同时落盘两个 shadow 文件 (校验体系地基):
    {output}.prompt.txt   TTS 完整日志 (model/voice/rate/instructions + 输入文本)
    {output}.input.txt    纯文本原文 (供 verify_audio.py 漏读校验对比)

voice 命名: edge-tts 用微软音色名, 如 en-US-GuyNeural / en-US-AndrewNeural,
全部音色可用 `edge-tts --list-voices` 查看。
注意: edge-tts 只收录 Edge 朗读功能的部分 Azure 音色 (Davis/Jason 等不在其中)。

语速: config.tts_rate (env TTS_RATE, 默认 "+0%")。变慢用负值如 "-10%",
变快用正值如 "+15%"; 也兼容倍数写法 "0.9x"。格式与 Azure SSML rate 一致。
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

from config import Config

# edge-tts 无 instructions 概念 (声音风格由音色决定), 记录日志用
_ENGINE_NAME = "edge-tts"
_INSTRUCTIONS = "Microsoft Edge neural voice (edge-tts); style fixed by voice."


class EdgeTTSClient:
    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._fmt = "mp3"  # edge-tts 固定输出 mp3
        # 语速/音调/音量 (Azure SSML 格式, 也兼容 "0.9x" 倍数写法)
        self._rate = getattr(config, "tts_rate", "+0%") or "+0%"
        self._pitch = getattr(config, "tts_pitch", "+0Hz") or "+0Hz"
        self._volume = getattr(config, "tts_volume", "+0%") or "+0%"

    def generate(self, text: str, voice: str, output_path: Path,
                 max_retries: int = 3) -> None:
        log_path = output_path.with_suffix(".prompt.txt")
        log_path.write_text(
            f"model: {_ENGINE_NAME}\nvoice: {voice}\n"
            f"rate: {self._rate} | pitch: {self._pitch} | volume: {self._volume}\n"
            f"instructions: {_INSTRUCTIONS}\n\n{text}",
            encoding="utf-8",
        )
        # 纯文本原文落盘: 供 verify_audio.py 做漏读校验对比
        output_path.with_suffix(".input.txt").write_text(text, encoding="utf-8")

        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                asyncio.run(self._save(text, voice, output_path))
                return
            except Exception as exc:
                last_error = exc
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(
            f"edge-tts generation failed after {max_retries} attempts: {last_error}"
        )

    async def _save(self, text: str, voice: str, output_path: Path) -> None:
        import edge_tts
        communicate = edge_tts.Communicate(
            text, voice,
            rate=self._rate,
            pitch=self._pitch,
            volume=self._volume,
        )
        await communicate.save(str(output_path))
