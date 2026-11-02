"""句子级漏读检测: 直接回答"哪句没读到", 容忍转录端的零星听错。

动机:
    词级严格对比会把 Whisper 的听错(如 Shallenberger -> Schellenberger,
    moved -> move)也算作"缺失", 导致覆盖率永远 <100%, 用户无法区分
    "个别词听错"与"整句漏读"。

方法:
    1. 原文按句切分
    2. 每句归一化后在转录全文中滑动窗口找最佳相似度
       (窗口长度 = 句长 ~ 2 倍句长, 容忍 Whisper 断句/词序差异)
    3. 相似度 >= threshold(默认 0.8)  -> 判为 FOUND(已读, 听错无妨)
       相似度 <  threshold             -> 判为 MISSING(这句没读到)

区分度: 听错 1~2 个词的句子相似度仍 >85%; 整句没读时相似度接近 0,
两者差距巨大, 不存在模糊地带。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

from tts_verify.normalizer import normalize_variants

_SENT_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass
class SentenceResult:
    text: str                 # 原文句子
    status: str               # "FOUND" | "MISSING"
    similarity: float         # 与转录的最佳相似度 (0~1)
    matched_at: int | None    # 转录中匹配起点 (词索引)

    @property
    def is_missing(self) -> bool:
        return self.status == "MISSING"


def split_sentences(text: str) -> list[str]:
    """按句号/问号/感叹号切分, 保留原句文本。"""
    parts = _SENT_SPLIT_RE.split(text.strip())
    return [p.strip() for p in parts if p.strip()]


def _best_match(sent_variants: tuple[list[str], list[str]],
                tgt_variants: tuple[list[str], list[str]],
                threshold: float) -> tuple[float, int | None]:
    """找句子的最佳相似度与位置 (split/join 两形态取最高分)。

    两级策略:
        1. 快速路径: 词集合重合 >= 85% 直接判已读 (听错 1~2 词的正常句)
        2. 细匹配: 可疑句用锚词定位窗口做 SequenceMatcher 精确对齐
    多数句子走快速路径, 性能与准确度兼顾。
    """
    sent_split, sent_join = sent_variants
    tgt_split, tgt_join = tgt_variants

    def _run(sw: list[str], tw: list[str], best: float, best_pos: int | None):
        n = len(sw)
        if n == 0 or len(tw) == 0:
            return best, best_pos

        # 快速路径: 多集交集 (数字占位 "#" 不虚高)
        overlap = sum(min(sw.count(w), tw.count(w)) for w in set(sw))
        if overlap >= n * 0.85:
            return 1.0, None

        # 细匹配: 锚定句子的第一个非占位词在转录中的出现位置, 只在这些位置开窗口
        anchor = next((w for w in sw if w != "#" and w in tw), None)
        if anchor is None:
            return best, best_pos
        max_wlen = min(n * 2 + 1, len(tw) + 1)
        for pos in (i for i, w in enumerate(tw) if w == anchor):
            for wlen in range(n, max_wlen, 2):
                for start in range(max(0, pos - wlen + 1),
                                   min(pos + 1, len(tw) - wlen + 1)):
                    seg = tw[start:start + wlen]
                    if sum(min(sw.count(w), seg.count(w)) for w in set(sw)) < n * 0.4:
                        continue
                    ratio = SequenceMatcher(None, sw, seg, autojunk=False).ratio()
                    if ratio > best:
                        best = ratio
                        best_pos = start
        return best, best_pos

    best, best_pos = _run(sent_split, tgt_split, 0.0, None)
    if best >= threshold:
        return best, best_pos
    # split 形态未达标, 补试 join 形态 (对齐 TTS 连读粘连)
    return _run(sent_join, tgt_join, best, best_pos)


def find_sentence_results(source: str, transcript: str,
                          threshold: float = 0.7,
                          sure_threshold: float = 0.5) -> list[SentenceResult]:
    """对原文每句做模糊匹配, 返回三态判定结果。

    三态判定 (基于实测相似度分布):
        sim >= threshold(0.7)                    -> FOUND   已读
        sim >= sure_threshold(0.5) 或 句长<=2词    -> UNSURE  疑似: 无法确证漏读
                                                        (听错严重或短句, 需人工抽听)
        否则                                       -> MISSING 确证漏读

    经验数据: 确证漏读(整句消失/截断) 相似度 0~0.36; 听错但已读 0.5~1.0;
    短句(<=2词)听错时相似度可能为 0 (如 "Breathe." 听成 "Brief."),
    无法从文本区分, 只能归入 UNSURE 人工确认。
    """
    tgt_split, tgt_join = normalize_variants(transcript)
    tgt_variants = (tgt_split.split(), tgt_join.split())
    results: list[SentenceResult] = []

    for sent in split_sentences(source):
        sent_split, sent_join = normalize_variants(sent)
        sent_variants = (sent_split.split(), sent_join.split())
        if not sent_variants[0]:
            continue
        best, pos = _best_match(sent_variants, tgt_variants, threshold)
        n_words = len(sent_variants[0])
        if best >= threshold:
            status = "FOUND"
        elif best >= sure_threshold or n_words <= 2:
            status = "UNSURE"
        else:
            status = "MISSING"
        results.append(SentenceResult(sent, status, best, pos))

    return results


# 兼容旧名 (早期只有 MISSING/FOUND 两态)
def find_missing_sentences(source: str, transcript: str,
                           threshold: float = 0.7) -> list[SentenceResult]:
    """旧接口: 等价于 find_sentence_results (建议改用新名)。"""
    return find_sentence_results(source, transcript, threshold=threshold)


def missing_only(results: list[SentenceResult]) -> list[SentenceResult]:
    """过滤出 MISSING (确证漏读) 的句子。"""
    return [r for r in results if r.is_missing]


def unsure_only(results: list[SentenceResult]) -> list[SentenceResult]:
    """过滤出 UNSURE (疑似, 需人工确认) 的句子。"""
    return [r for r in results if r.status == "UNSURE"]
