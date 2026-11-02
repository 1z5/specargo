"""文本归一化: 消除 TTS 原文与 ASR 转录之间的格式差异, 让对比更准确。

处理的差异:
    - 大小写: HeLLo -> hello
    - 标点: 全部剥离 (don't -> dont, "hi," -> hi)
    - 数字词 -> 数字: fourteen -> 14, twenty four -> 24
    - 数字占位: 所有数字统一为 "#" (消除 "14" vs "fourteen" vs "0.6" 读法差异),
      且 "point" 也映射为 "#" (Whisper 常把 "Point 6" 听成 "0.6")
    - 连字符两种形态 (normalize_variants):
        split: fight-or-flight -> fight or flight (对齐转录读成空格的情况)
        join:  micro-emergencies -> microemergencies (对齐 TTS 连读粘连的情况)

用法:
    normalize(text)              -> split 形态, 用于词级覆盖率对比
    normalize_variants(text)     -> (split, join) 两形态, 用于句子级匹配取最高分
"""
from __future__ import annotations

import re

_NUMBER_WORDS: dict[str, str] = {
    "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
    "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
    "ten": "10", "eleven": "11", "twelve": "12", "thirteen": "13",
    "fourteen": "14", "fifteen": "15", "sixteen": "16", "seventeen": "17",
    "eighteen": "18", "nineteen": "19",
    "twenty": "20", "thirty": "30", "forty": "40", "fifty": "50",
    "sixty": "60", "seventy": "70", "eighty": "80", "ninety": "90",
    "hundred": "100", "thousand": "1000", "million": "1000000",
    "billion": "1000000000",
}

_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _pieces(token: str, mode: str) -> list[str]:
    """把 token 拆成词片段。

    mode="split": 连字符与句点拆分 ("micro-emergencies" -> micro/emergencies, "0.6" -> 0/6)
    mode="join":  连字符与句点粘连 ("micro-emergencies" -> microemergencies, "0.6" -> 06)
    """
    if mode == "join":
        parts = [token.replace(".", "").replace("-", "")]
    else:
        parts = re.split(r"[.\-]", token)
    return [p for p in (_ALNUM_RE.sub("", x) for x in parts) if p]


def _normalize_pieces(pieces: list[str]) -> str:
    """词片段 -> 归一化词串: 数字词转数字、复合数字、数字与 point 统一为 '#'。"""
    words: list[str] = []
    for piece in pieces:
        mapped = _NUMBER_WORDS.get(piece, piece)
        if mapped != piece:
            # 复合数字词: "twenty one" -> "21" (仅当后者也是数字词且前面是整十)
            if words and words[-1].isdigit():
                prev = int(words[-1])
                if prev % 10 == 0 and prev % 100 != 0 and len(words[-1]) <= 2:
                    words[-1] = str(prev + int(mapped))
                    continue
        words.append(mapped)
    # 数字占位: 消除数字读法差异; "point" 常被 Whisper 听成数字 (0.6), 一并占位
    return " ".join("#" if w.isdigit() or w == "point" else w for w in words)


def _normalize_impl(text: str, mode: str) -> str:
    pieces: list[str] = []
    for token in text.lower().split():
        pieces.extend(_pieces(token, mode))
    return _normalize_pieces(pieces)


def normalize(text: str) -> str:
    """归一化 (split 形态), 返回空格分隔的词串。"""
    return _normalize_impl(text, "split")


def normalize_variants(text: str) -> tuple[str, str]:
    """返回 (split, join) 两种形态的归一化词串。

    句子级匹配应分别尝试两种形态取最高相似度:
    split 对齐 "fight-or-flight" 读成 "fight or flight",
    join  对齐 "micro-emergencies" 连读成 "microemergencies"。
    """
    return _normalize_impl(text, "split"), _normalize_impl(text, "join")


def normalize_words(text: str) -> list[str]:
    """归一化并切词 (split 形态)。"""
    return normalize(text).split()
