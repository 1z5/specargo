from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent


def _load_dotenv(env_path: Path | None = None) -> dict[str, str]:
    if env_path is None:
        env_path = ROOT_DIR / ".env"
    result: dict[str, str] = {}
    if not env_path.exists():
        return result
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        result[key] = value
    return result


@dataclass
class Config:
    openai_api_key: str = ""
    openai_base_url: str | None = None
    tts_model: str = "gpt-4o-mini-tts"
    tts_voice_male: str = "cedar"
    tts_voice_female: str = "marin"
    tts_voice_test: str = "cedar"
    tts_format: str = "mp3"
    tts_rate: str = "+0%"    # edge-tts/Azure 语速 (SSML rate): "-10%" 变慢, "+15%" 变快
    tts_pitch: str = "+0Hz"  # edge-tts/Azure 音调 (SSML pitch): "-5Hz" 低沉, "+5Hz" 明亮
    tts_volume: str = "+0%"  # edge-tts/Azure 音量 (SSML volume): "+20%" / "-20%"
    tts_speed: float = 1.0   # OpenAI 语速 (0.25~4.0, 1.0 默认)
    tts_instructions: str = (
        "Voice Affect: Calm, composed, and thoughtful; speak with quiet authority.\n\n"
        "Tone: Warm, reflective, and engaging; sound like a professional audiobook narrator explaining valuable ideas.\n\n"
        "Pacing: Steady and natural; allow listeners to absorb key insights without slowing the flow.\n\n"
        "Pronunciation: Clear and articulate; emphasize key concepts and transitions.\n\n"
        "Pauses: Use brief natural pauses after sections and important ideas.\n\n"
        "Completion: Read all content fully. Do not skip, summarize, or stop early. Continue to the very last word."
    )
    api_delay_seconds: float = 3.0
    output_dir: Path = field(default_factory=lambda: ROOT_DIR / "output")
    books_dir: Path = field(default_factory=lambda: ROOT_DIR / "books")
    connect_timeout: float = 30.0
    read_timeout: float = 300.0

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> "Config":
        env = _load_dotenv(env_path)
        return cls(
            openai_api_key=(
                os.getenv("OPENAI_API_KEY")
                or env.get("OPENAI_API_KEY", "")
            ),
            openai_base_url=(
                os.getenv("OPENAI_BASE_URL")
                or env.get("OPENAI_BASE_URL")
                or None
            ),
            tts_voice_male=(
                os.getenv("TTS_VOICE_MALE")
                or env.get("TTS_VOICE_MALE", "cedar")
            ),
            tts_voice_female=(
                os.getenv("TTS_VOICE_FEMALE")
                or env.get("TTS_VOICE_FEMALE", "marin")
            ),
            tts_voice_test=(
                os.getenv("TTS_VOICE_TEST")
                or env.get("TTS_VOICE_MALE")
                or env.get("TTS_VOICE_MALE", "cedar")
            ),
            tts_format=(
                os.getenv("TTS_FORMAT")
                or env.get("TTS_FORMAT", "mp3")
            ),
            tts_rate=(
                os.getenv("TTS_RATE")
                or env.get("TTS_RATE", "+0%")
            ),
            tts_pitch=(
                os.getenv("TTS_PITCH")
                or env.get("TTS_PITCH", "+0Hz")
            ),
            tts_volume=(
                os.getenv("TTS_VOLUME")
                or env.get("TTS_VOLUME", "+0%")
            ),
            tts_speed=float(
                os.getenv("TTS_SPEED")
                or env.get("TTS_SPEED", "1.0")
            ),
            output_dir=Path(
                os.getenv("TTS_OUTPUT_DIR")
                or env.get("TTS_OUTPUT_DIR")
                or ROOT_DIR / "output"
            ),
            books_dir=Path(
                os.getenv("TTS_BOOKS_DIR")
                or env.get("TTS_BOOKS_DIR")
                or ROOT_DIR / "books"
            ),
        )


config = Config.from_env()
