"""报告模块: 生成结构化 JSON 报告 + 人类可读 Markdown 报告。

JSON 报告用于机器消费(后续自动修复/CI), MD 报告用于人工查看。
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from tts_verify.comparator import Comparison, Gap, Misread
from tts_verify.sentence_matcher import SentenceResult

# 状态常量
PASS = "PASS"
WARN = "WARN"                   # 有疑似句(UNSURE): 无确证漏读, 需人工抽听
FAIL_MISSING = "FAIL_MISSING"   # 有句子确证未读到: 漏读
FAIL_ERRORS = "FAIL_ERRORS"     # 准确率不足: 错读过多
SKIPPED = "SKIPPED"             # 无原文可比
ERROR = "ERROR"                 # 转录/读取失败


@dataclass
class FileResult:
    audio: str
    status: str
    coverage: float | None = None
    accuracy: float | None = None
    duration_s: float = 0.0
    gaps: list[Gap] = field(default_factory=list)
    misreads: list[Misread] = field(default_factory=list)
    sentences: list[SentenceResult] = field(default_factory=list)
    note: str = ""
    fix_attempts: int = 0
    fixed: bool = False

    @property
    def missing_sentences(self) -> list[SentenceResult]:
        return [s for s in self.sentences if s.is_missing]

    @property
    def missing_count(self) -> int:
        return len(self.missing_sentences)

    @property
    def unsure_sentences(self) -> list[SentenceResult]:
        return [s for s in self.sentences if s.status == "UNSURE"]

    @property
    def unsure_count(self) -> int:
        return len(self.unsure_sentences)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["gaps"] = [
            {"start": g.start, "end": g.end, "n_words": g.n_words, "text": g.text}
            for g in self.gaps
        ]
        d["misreads"] = [
            {"src_start": m.src_start, "src_end": m.src_end,
             "tgt_start": m.tgt_start, "tgt_end": m.tgt_end,
             "src_text": m.src_text, "tgt_text": m.tgt_text}
            for m in self.misreads
        ]
        d["sentences"] = [
            {"text": s.text, "status": s.status,
             "similarity": round(s.similarity, 3), "matched_at": s.matched_at}
            for s in self.sentences
        ]
        d["missing_sentences"] = [
            {"text": s.text, "similarity": round(s.similarity, 3)}
            for s in self.missing_sentences
        ]
        d["unsure_sentences"] = [
            {"text": s.text, "similarity": round(s.similarity, 3)}
            for s in self.unsure_sentences
        ]
        return d


@dataclass
class Report:
    input_dir: str
    model: str
    language: str
    min_accuracy: float
    sentence_threshold: float = 0.7
    sure_threshold: float = 0.5
    generated_at: str = ""
    results: list[FileResult] = field(default_factory=list)

    # ---- 汇总 ----
    @property
    def pass_count(self) -> int:
        return sum(1 for r in self.results if r.status == PASS)

    @property
    def warn_count(self) -> int:
        return sum(1 for r in self.results if r.status == WARN)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if r.status in (FAIL_MISSING, FAIL_ERRORS))

    @property
    def fail_missing(self) -> list[FileResult]:
        return [r for r in self.results if r.status == FAIL_MISSING]

    @property
    def fail_errors(self) -> list[FileResult]:
        return [r for r in self.results if r.status == FAIL_ERRORS]

    @property
    def warns(self) -> list[FileResult]:
        """疑似待人工确认的文件。"""
        return [r for r in self.results if r.status == WARN]

    def to_dict(self) -> dict:
        return {
            "input_dir": self.input_dir,
            "model": self.model,
            "language": self.language,
            "sentence_threshold": self.sentence_threshold,
            "sure_threshold": self.sure_threshold,
            "generated_at": self.generated_at,
            "summary": {
                "total": len(self.results),
                "pass": self.pass_count,
                "warn": self.warn_count,
                "fail": self.fail_count,
                "fail_missing": len(self.fail_missing),
                "fail_errors": len(self.fail_errors),
            },
            "files": [r.to_dict() for r in self.results],
        }

    # ---- 输出 ----
    def write_json(self, path: Path) -> None:
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_markdown(self, path: Path) -> None:
        lines: list[str] = [
            "# 音频完整性校验报告",
            "",
            f"- 目录: `{self.input_dir}`",
            f"- 模型: {self.model} / 语言: {self.language}",
            f"- 判定: 句子相似度 ≥ {self.sentence_threshold:.0%} 已读; "
            f"{self.sure_threshold:.0%}~{self.sentence_threshold:.0%} 或短句 = 疑似待听; "
            f"< {self.sure_threshold:.0%} = 确证漏读; 准确率 < {self.min_accuracy:.0%} = 错读过多",
            f"- 生成时间: {self.generated_at}",
            "",
            "## 汇总",
            "",
            f"**PASS {self.pass_count} / WARN {self.warn_count} / "
            f"FAIL {self.fail_count} / 共 {len(self.results)}**",
            "",
            "| 文件 | 漏读句 | 疑似句 | 覆盖率 | 准确率 | 状态 | 说明 |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in self.results:
            cov = f"{r.coverage:.1%}" if r.coverage is not None else "-"
            acc = f"{r.accuracy:.1%}" if r.accuracy is not None else "-"
            lines.append(
                f"| {r.audio} | {r.missing_count} | {r.unsure_count} | "
                f"{cov} | {acc} | {r.status} | {r.note} |"
            )
        lines.append("")

        def _fmt_detail(result: FileResult) -> None:
            lines.append(f"### {result.audio}  (漏读 {result.missing_count} 句, "
                         f"疑似 {result.unsure_count} 句)")
            lines.append("")
            lines.append(f"状态: `{result.status}`" +
                         (f" | 已自动修复: 第 {result.fix_attempts} 次重生成后通过" if result.fixed else ""))
            lines.append("")
            if result.missing_sentences:
                lines.append("**确证未读到的句子 (原文):**")
                lines.append("")
                lines.append("```text")
                for i, s in enumerate(result.missing_sentences, 1):
                    lines.append(f"[漏读 {i}] 相似度 {s.similarity:.2f}:")
                    lines.append(f"    {s.text}")
                lines.append("```")
                lines.append("")
            if result.unsure_sentences:
                lines.append("**疑似句 (无法确证, 请人工抽听对应时间位置):**")
                lines.append("")
                lines.append("```text")
                for i, s in enumerate(result.unsure_sentences, 1):
                    lines.append(f"[疑似 {i}] 相似度 {s.similarity:.2f}:")
                    lines.append(f"    {s.text}")
                lines.append("```")
                lines.append("")
            if result.misreads:
                lines.append("**疑似错读片段 (原文 vs 转录, 不影响漏读判定):**")
                lines.append("")
                lines.append("```text")
                for m in result.misreads:
                    lines.append(f"    原文: {m.src_text}")
                    lines.append(f"    转录: {m.tgt_text}")
                    lines.append("")
                lines.append("```")
                lines.append("")

        for r in self.fail_missing:
            _fmt_detail(r)
        for r in self.fail_errors:
            _fmt_detail(r)
        for r in self.warns:
            _fmt_detail(r)

        if not self.fail_missing and not self.fail_errors and not self.warns:
            lines.append("所有文件均通过校验。")
            lines.append("")
        elif self.warns:
            lines.append("> ⚠️ WARN 表示存在无法确证的句子 (多为转录严重听错, "
                         "也可能真漏读), 需人工抽听确认; 不影响退出码。")
            lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("*由 verify_audio.py 生成*")
        path.write_text("\n".join(lines), encoding="utf-8")


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")
