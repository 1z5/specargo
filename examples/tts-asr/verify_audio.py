"""音频完整性校验工具: 转录 -> 句子级漏读检测 -> 报告 -> 自动修复。

核心判定 (句子级三态, 直接回答"哪句没读到"):
    原文按句切分, 每句在转录中模糊匹配 (容忍个别词听错):
        相似度 >= --sentence-threshold (默认 0.7)   -> FOUND   已读
        相似度 >= --sure-threshold (默认 0.5) 或短句 -> UNSURE  疑似 (严重听错或短句,
                                                            无法确证, 需人工抽听)
        否则                                        -> MISSING 确证漏读
    文件状态: 有 MISSING -> FAIL_MISSING; 仅有 UNSURE -> WARN;
             准确率不足 -> FAIL_ERRORS; 否则 PASS。
    退出码: 0 = 无确证漏读 (WARN 不阻塞), 1 = 存在 FAIL。

工程化设计:
    - 增量处理: 已有 transcript 缓存则跳过转录 (--force 强制重转)
    - 原文来源: 优先 {同名}.input.txt (TTS 输入纯文本), 回退解析 .prompt.txt;
      存量音频(仅 prompt.txt)会自动补写 .input.txt 以便后续精确对比
    - 结构化报告: verify_report/report.json (机器可读) + report.md (人工可读)
    - 自动修复: --fix 时对 FAIL 文件按原文重新生成音频并复检, 失败自动回滚

用法:
    python verify_audio.py <音频目录> [--model small] [--language en]
                           [--sentence-threshold 0.7] [--sure-threshold 0.5]
                           [--min-accuracy 0.90]
                           [--force] [--fix] [--fix-retries 3] [--report-dir verify_report]

    target 可以是目录(批量)或单个音频文件(单文件模式, 报告输出到文件所在目录)。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  (把仓库 app/ 加入 sys.path)

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

from tts_verify.comparator import compare
from tts_verify.reporter import (
    ERROR, FAIL_ERRORS, FAIL_MISSING, PASS, SKIPPED, WARN, FileResult, Report, now_str,
)
from tts_verify.sentence_matcher import find_sentence_results, missing_only, unsure_only

# item 级状态登记 + Argo 输出参数（框架层）
from specargo.argo_io import write_params
from domain_state import upsert_tts_item
from specargo.envcompat import LEGACY_RUN_ID, LEGACY_STATE_DB, getenv
from specargo.state import FAILED, VERIFIED, close_run, connect, open_run


def parse_item_from_path(audio: Path) -> tuple[str, str, str]:
    """从 <slug>_<num>_<voice>.<ext> 反解 item 标识。

    slug 由标题生成、可能含下划线，所以从右侧切：rsplit("_", 2)。
    """
    parts = audio.stem.rsplit("_", 2)
    if len(parts) != 3:
        return audio.stem, "?", "?"
    return parts[0], parts[1], parts[2]

AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac", ".mp4", ".wma", ".webm"}
HEADER_PREFIXES = (
    "model:", "voice:", "instructions:", "voice affect:",
    "tone:", "pacing:", "pronunciation:", "pauses:", "completion:",
)

# ---------------------------------------------------------------------------
# 原文提取
# ---------------------------------------------------------------------------

def load_source_text(audio: Path) -> tuple[str | None, str]:
    """返回 (原文文本, 来源说明)。优先 .input.txt, 回退解析 .prompt.txt。"""
    input_txt = audio.with_suffix(".input.txt")
    if input_txt.exists():
        return input_txt.read_text(encoding="utf-8").strip(), "input.txt"

    prompt_txt = audio.with_suffix(".prompt.txt")
    if not prompt_txt.exists():
        return None, "no-source"
    body: list[str] = []
    for line in prompt_txt.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower().startswith(HEADER_PREFIXES):
            continue
        body.append(stripped)
    return (" ".join(body) or None), "prompt.txt"


def parse_voice_from_prompt(audio: Path) -> str | None:
    """从 prompt.txt 提取 voice (供 --fix 重生成用)。"""
    prompt_txt = audio.with_suffix(".prompt.txt")
    if not prompt_txt.exists():
        return None
    for line in prompt_txt.read_text(encoding="utf-8").splitlines():
        if line.lower().startswith("voice:"):
            return line.split(":", 1)[1].strip()
    return None

# ---------------------------------------------------------------------------
# 转录
# ---------------------------------------------------------------------------

def transcribe_audio(model, audio: Path, language: str) -> tuple[list[dict], float]:
    """转录音频, 返回 (segments 列表, 耗时秒)。

    注意: vad_filter 必须为 False。VAD(静音过滤)会把 TTS 音频中
    停顿较长的短句(如 "Flip the paradigm.")当作噪音丢弃, 造成漏读误报
    (实测踩过: 某引擎生成的一章被 VAD 当作噪音丢弃, 造成漏读误报)。TTS 生成的是干净语音, 无需 VAD。
    """
    t0 = time.time()
    segments_iter, info = model.transcribe(
        str(audio), language=language, vad_filter=False, beam_size=5
    )
    segments = [
        {"start": s.start, "end": s.end, "text": s.text.strip()}
        for s in segments_iter
    ]
    return segments, time.time() - t0


def get_plain_text(segments: list[dict]) -> str:
    return " ".join(s["text"] for s in segments if s["text"]).strip()

# ---------------------------------------------------------------------------
# 检测 (转录 + 句子级判定)
# ---------------------------------------------------------------------------

def check_one(model, audio: Path, language: str, source: str | None,
              force: bool, sentence_threshold: float, sure_threshold: float,
              min_accuracy: float) -> FileResult:
    """对一个音频执行 转录(或读缓存) + 三态检测, 返回 FileResult。"""
    plain_out = audio.with_suffix(audio.suffix + ".transcript.plain.txt")

    # 1. 转录 (优先缓存)
    if plain_out.exists() and not force:
        print(f"      使用缓存转录")
        plain = plain_out.read_text(encoding="utf-8").strip()
        duration_s = 0.0
    else:
        segments, duration_s = transcribe_audio(model, audio, language)
        with_ts = "\n".join(
            f"[{s['start']:7.2f}s -> {s['end']:7.2f}s] {s['text']}" for s in segments
        ) + "\n"
        audio.with_suffix(audio.suffix + ".transcript.txt").write_text(with_ts, encoding="utf-8")
        plain = get_plain_text(segments)
        plain_out.write_text(plain + "\n", encoding="utf-8")

    # 2. 检测
    if not source:
        return FileResult(audio.name, SKIPPED, note="无原文 (.input.txt/.prompt.txt)")

    sentences = find_sentence_results(source, plain,
                                      threshold=sentence_threshold,
                                      sure_threshold=sure_threshold)
    missing = missing_only(sentences)
    unsure = unsure_only(sentences)
    comp = compare(source, plain)
    cov, acc = comp.coverage, comp.accuracy

    if missing:
        status = FAIL_MISSING
    elif acc < min_accuracy:
        status = FAIL_ERRORS
    elif unsure:
        status = WARN
    else:
        status = PASS

    return FileResult(audio.name, status, cov, acc,
                      duration_s=duration_s, gaps=comp.gaps,
                      misreads=comp.misreads, sentences=sentences)

# ---------------------------------------------------------------------------
# 修复
# ---------------------------------------------------------------------------

def try_fix(model, audio: Path, language: str, source: str, max_retries: int,
            sentence_threshold: float, sure_threshold: float,
            min_accuracy: float) -> tuple[FileResult, bool]:
    """对 FAIL 文件按原文重新生成音频并复检。

    返回 (最终 FileResult, 是否成功修复)。
    重生成覆盖原文件前先备份 .orig.bak; 复检仍失败则回滚。
    复检通过标准: 无确证漏读 (PASS 或 WARN 均视为修复成功)。
    """
    from config import Config
    from tts_client import TTSClient

    voice = parse_voice_from_prompt(audio)
    if not voice:
        return (FileResult(audio.name, ERROR, note="无法修复: 缺少 voice 信息"), False)

    cfg = Config.from_env()
    if not cfg.openai_api_key:
        return (FileResult(audio.name, ERROR, note="无法修复: 未配置 OPENAI_API_KEY"), False)

    client = TTSClient(cfg)
    backup = audio.with_suffix(audio.suffix + ".orig.bak")
    shutil.copy2(audio, backup)

    try:
        for attempt in range(1, max_retries + 1):
            print(f"      修复尝试 {attempt}/{max_retries}: 重新生成 {audio.name} ...")
            # skip_existing=False：修复必须真正重生成（默认值变化的兼容性修正）
            client.generate(source, voice, audio, skip_existing=False)
            result = check_one(model, audio, language, source, force=True,
                               sentence_threshold=sentence_threshold,
                               sure_threshold=sure_threshold,
                               min_accuracy=min_accuracy)
            print(f"      复检: 确证漏读 {result.missing_count} 句 | "
                  f"疑似 {result.unsure_count} 句 -> {result.status}")
            if result.status in (PASS, WARN):
                backup.unlink(missing_ok=True)
                result.fix_attempts = attempt
                result.fixed = True
                return (result, True)
    except Exception as exc:
        print(f"      修复失败: {exc}")

    # 全部尝试失败: 回滚
    shutil.copy2(backup, audio)
    backup.unlink(missing_ok=True)
    result = check_one(model, audio, language, source, force=True,
                       sentence_threshold=sentence_threshold,
                       sure_threshold=sure_threshold,
                       min_accuracy=min_accuracy)
    result.fix_attempts = max_retries
    result.note = "修复失败, 已回滚"
    return (result, False)

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="音频完整性校验 (漏读检测)")
    parser.add_argument("input_dir", type=Path,
                        help="音频所在目录, 或单个音频文件 (单文件模式)")
    parser.add_argument("--model", default="small", help="whisper 模型 (默认 small)")
    parser.add_argument("--language", default="en", help="音频语言 (默认 en)")
    parser.add_argument("--sentence-threshold", type=float, default=0.7,
                        help="已读阈值: 相似度 >= 此值视为已读 (默认 0.7)")
    parser.add_argument("--sure-threshold", type=float, default=0.5,
                        help="确证漏读阈值: 相似度 < 此值且非短句视为确证漏读; "
                             "介于两阈值之间或短句视为疑似待人工确认 (默认 0.5)")
    parser.add_argument("--min-accuracy", type=float, default=0.90,
                        help="准确率阈值, 低于判为错读过多 (默认 0.90)")
    parser.add_argument("--force", action="store_true", help="忽略已有转录缓存, 重新转录")
    parser.add_argument("--fix", action="store_true",
                        help="对 FAIL 文件按原文自动重新生成并复检")
    parser.add_argument("--fix-retries", type=int, default=3, help="单文件修复重试次数")
    parser.add_argument("--report-dir", type=Path, default=Path("verify_report"),
                        help="报告输出目录 (默认 <目标所在目录>/verify_report)")
    # --- 改造新增：与 Argo / 状态库对接 ---
    parser.add_argument("--run-id", default=getenv("PIPELINE_RUN_ID", LEGACY_RUN_ID, ""),
                        help="批次 ID (Argo 里传 {{workflow.name}}，用于追溯)")
    parser.add_argument("--state-db",
                        default=getenv("PIPELINE_STATE_DB", LEGACY_STATE_DB, ""),
                        help="item 状态库路径 (SQLite；不填则不登记)")
    args = parser.parse_args()

    # 支持单文件模式: 传入文件路径时只处理该文件
    single_file = args.input_dir.is_file()
    scan_root = args.input_dir
    if not scan_root.exists():
        print(f"错误: 路径不存在: {args.input_dir}", file=sys.stderr)
        return 2

    if single_file:
        files = [args.input_dir] if args.input_dir.suffix.lower() in AUDIO_EXTS else []
        if not files:
            print(f"错误: 不是支持的音频文件: {args.input_dir}", file=sys.stderr)
            return 2
    else:
        if not scan_root.is_dir():
            print(f"错误: 目录不存在: {args.input_dir}", file=sys.stderr)
            return 2
        files = sorted(p for p in scan_root.iterdir() if p.suffix.lower() in AUDIO_EXTS)
        if not files:
            print(f"错误: {args.input_dir} 下没有音频文件", file=sys.stderr)
            return 2

    report_dir = args.report_dir
    if not report_dir.is_absolute():
        base = args.input_dir.parent if single_file else args.input_dir
        report_dir = base / report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    # --- 改造新增：开批次 + 状态库 ---
    run_id = args.run_id or f"verify-{int(time.time())}"
    state = connect(args.state_db) if args.state_db else None
    if state is not None:
        open_run(state, run_id, note=f"verify {args.input_dir}")

    from faster_whisper import WhisperModel
    print(f"共 {len(files)} 个音频 | 模型 {args.model} | "
          f"判定: >= {args.sentence_threshold:.0%} 已读 / "
          f"{args.sure_threshold:.0%}~{args.sentence_threshold:.0%} 疑似 / "
          f"< {args.sure_threshold:.0%} 确证漏读")
    print("加载模型 ...")
    t0 = time.time()
    model = WhisperModel(args.model, device="cpu", compute_type="int8")
    print(f"模型加载完成, 耗时 {time.time() - t0:.1f}s\n")

    report = Report(
        input_dir=str(args.input_dir), model=args.model, language=args.language,
        min_accuracy=args.min_accuracy,
        sentence_threshold=args.sentence_threshold,
        sure_threshold=args.sure_threshold,
        generated_at=now_str(),
    )

    for i, audio in enumerate(files, 1):
        source, src_note = load_source_text(audio)
        print(f"[{i}/{len(files)}] 校验: {audio.name}")

        # 存量音频自愈: 仅有 prompt.txt 时自动补写 .input.txt (后续对比用精确原文)
        if src_note == "prompt.txt" and source:
            audio.with_suffix(".input.txt").write_text(source + "\n", encoding="utf-8")

        try:
            result = check_one(model, audio, args.language, source,
                               force=args.force,
                               sentence_threshold=args.sentence_threshold,
                               sure_threshold=args.sure_threshold,
                               min_accuracy=args.min_accuracy)
        except Exception as exc:
            print(f"      转录失败: {exc}")
            report.results.append(FileResult(audio.name, ERROR, note=str(exc)))
            continue

        if src_note == "prompt.txt":
            result.note = "存量音频: 已自动补写 .input.txt, 下次校验即用精确原文"

        # 自动修复 (仅对确证漏读/错读过多触发; 疑似 WARN 交人工)
        if args.fix and result.status in (FAIL_MISSING, FAIL_ERRORS) and source:
            print(f"      触发自动修复 ...")
            result, ok = try_fix(model, audio, args.language, source, args.fix_retries,
                                 args.sentence_threshold, args.sure_threshold,
                                 args.min_accuracy)
            print(f"      修复{'成功' if ok else '失败'}: {result.status}")

        print(f"      确证漏读 {result.missing_count} 句 | 疑似 {result.unsure_count} 句 | "
              f"覆盖率 {result.coverage:.1%} | 准确率 {result.accuracy:.1%} | "
              f"状态 {result.status}")
        if result.missing_sentences:
            for s in result.missing_sentences:
                print(f"        - [确证漏读 相似度 {s.similarity:.2f}] {s.text[:90]}")
        if result.unsure_sentences:
            for s in result.unsure_sentences:
                print(f"        ? [疑似 相似度 {s.similarity:.2f}] {s.text[:90]}")

        # --- 改造新增：item 级结果登记（PASS/WARN 都算过检；FAIL 隔离待处理）---
        if state is not None and result.status != SKIPPED:
            slug, num, voice = parse_item_from_path(audio)
            upsert_tts_item(
                state, book_slug=slug, chapter_num=num, voice=voice,
                status=VERIFIED if result.status in (PASS, WARN) else FAILED,
                run_id=run_id, coverage=result.coverage, accuracy=result.accuracy,
            )

        report.results.append(result)

    # 报告
    report.write_json(report_dir / "report.json")
    report.write_markdown(report_dir / "report.md")
    print(f"\n========== 汇总: PASS {report.pass_count} / WARN {report.warn_count} "
          f"/ FAIL {report.fail_count} / 共 {len(report.results)} ==========")
    for r in report.results:
        cov = f"{r.coverage:.1%}" if r.coverage is not None else "-"
        acc = f"{r.accuracy:.1%}" if r.accuracy is not None else "-"
        print(f"  {r.audio:<42} 漏读{r.missing_count:>2}句 疑似{r.unsure_count:>2}句 "
              f"{cov:>7} {acc:>7}  {r.status}" +
              ("  (已修复)" if r.fixed else "") + (f"  {r.note}" if r.note else ""))
    print(f"\n报告: {report_dir / 'report.json'} / {report_dir / 'report.md'}")
    if report.fail_count:
        print("存在确证漏读 (FAIL), 可用 --fix 自动重生成修复")
    elif report.warn_count:
        print("无确证漏读; 有疑似句 (WARN), 建议人工抽听报告列出的时间位置")

    # --- 改造新增：收口（Argo outputs + 批次状态），供 DAG 下游判断分支 ---
    total_missing = sum(r.missing_count for r in report.results)
    write_params({
        "total": len(report.results),
        "pass-count": report.pass_count,
        "warn-count": report.warn_count,
        "fail-count": report.fail_count,
        "missing-count": total_missing,
        "status": "fail" if report.fail_count else "pass",
    })
    if state is not None:
        close_run(state, run_id,
                  "failed" if report.fail_count else "succeeded",
                  f"fail={report.fail_count} missing={total_missing}")
        state.close()

    return 1 if report.fail_count else 0


if __name__ == "__main__":
    sys.exit(main())
