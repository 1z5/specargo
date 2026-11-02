"""对比模块: 原文 vs 转录的词级比对, 定位漏读/错读片段。

核心思想: 归一化后按词做 SequenceMatcher, 从 opcodes 中提取:
    - 覆盖率 coverage  = 匹配词数 / 原文词数  (音频是否读全)
    - 准确率 accuracy  = 匹配词数 / 转录词数  (转录是否正确)
    - 漏读片段 gaps    = 原文中连续未读到的词 (delete opcode)
    - 错读片段 errors  = 原文与转录同位置但内容不同 (replace opcode)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher

from tts_verify.normalizer import normalize_words


@dataclass
class Gap:
    """原文中未被读到的连续片段。"""

    start: int          # 原文词索引(归一化后)
    end: int            # 结束索引(不含)
    n_words: int
    text: str           # 缺失原文摘录(归一化前原文词串)

    def __str__(self) -> str:
        return self.text


@dataclass
class Misread:
    """原文与转录对不上的片段(非纯格式差异)。"""

    src_start: int
    src_end: int
    tgt_start: int
    tgt_end: int
    src_text: str       # 原文摘录
    tgt_text: str       # 转录摘录

    def __str__(self) -> str:
        return f"原文[{self.src_text!r}] -> 转录[{self.tgt_text!r}]"


@dataclass
class Comparison:
    coverage: float = 0.0            # 0~1
    accuracy: float = 0.0            # 0~1
    gaps: list[Gap] = field(default_factory=list)
    misreads: list[Misread] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """是否有实质内容缺失(漏读)或大面积错读。"""
        return not self.gaps and len(self.misreads) <= 2


def compare(source_text: str, transcript_text: str,
            min_gap_words: int = 3) -> Comparison:
    """对比原文与转录。

    min_gap_words: 连续缺失词数达到该值才算"漏读片段"。
    更短的差异(标点/单词错读)归入 misreads。
    """
    src = source_text.split()
    tgt = transcript_text.split()
    ns = normalize_words(source_text)
    nt = normalize_words(transcript_text)

    if not ns or not nt:
        return Comparison()

    matcher = SequenceMatcher(None, ns, nt, autojunk=False)
    matched = 0
    gaps: list[Gap] = []
    misreads: list[Misread] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            matched += i2 - i1
        elif tag == "delete":
            n = i2 - i1
            if n >= min_gap_words:
                gaps.append(Gap(
                    start=i1, end=i2, n_words=n,
                    text=" ".join(src[i1:i2]),
                ))
        elif tag == "replace":
            n = i2 - i1
            m = j2 - j1
            if n >= min_gap_words or m >= min_gap_words:
                misreads.append(Misread(
                    src_start=i1, src_end=i2, tgt_start=j1, tgt_end=j2,
                    src_text=" ".join(src[i1:i2]),
                    tgt_text=" ".join(tgt[j1:j2]),
                ))

    return Comparison(
        coverage=matched / len(ns),
        accuracy=matched / len(nt) if nt else 0.0,
        gaps=gaps,
        misreads=misreads,
    )
