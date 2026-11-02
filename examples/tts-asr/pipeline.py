"""生成编排（改造版）。

相对原版的变化：
    1. **skip_existing**（默认开）：逐文件判断能否跳过，实现断点续跑；
    2. **run_id**：环境变量 ``PIPELINE_RUN_ID`` 注入（Argo 里用 ``{{workflow.name}}``；
       旧前缀 ``TTS_RUN_ID`` 仍兼容读取），全程贯穿日志与状态表，实现追溯；
    3. **状态登记**：每个 item 生成后写入框架状态表（见 ``domain_state.py``），
       跑完即可对账，不靠人翻目录；
    4. **单书 CLI**：``python pipeline.py --book <json> --voice <voice>``，
       供编排层每个 task 调用（一个 Pod 只干一本书，失败影响面最小）。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  (把仓库 app/ 加入 sys.path)

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

from config import Config, config as default_config
from domain_state import upsert_tts_item
from specargo.envcompat import LEGACY_RUN_ID, LEGACY_STATE_DB, getenv
from models import Book
from book_reader import read_book, find_book_json_files
from specargo.resume import skip_reason
from specargo.state import GENERATED, VERIFIED, connect, open_run
from text_composer import compose_opening, compose_chapter, compose_closing


def new_run_id() -> str:
    return getenv("PIPELINE_RUN_ID", LEGACY_RUN_ID) or f"local-{uuid.uuid4().hex[:8]}"


class TTSPipeline:
    def __init__(self, cfg: Config | None = None, engine: str = "openai") -> None:
        self.cfg = cfg or default_config
        if engine not in ("openai", "edge-tts"):
            raise ValueError(f"未知引擎: {engine} (可选 openai / edge-tts)")
        self.engine = engine
        if engine == "openai":
            from tts_client import TTSClient
            self.client = TTSClient(self.cfg)
        else:
            from edge_tts_client import EdgeTTSClient
            self.client = EdgeTTSClient(self.cfg)
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = new_run_id()
        engine_label = self.cfg.tts_model if engine == "openai" else "edge-tts"
        print(f"Engine: {engine_label} | run_id: {self.run_id}")
        print(f"Output: {self.cfg.output_dir}")

    # ------------------------------------------------------------------
    def _register(self, state, book_slug: str, chapter_num: str, voice: str,
                  status: str, coverage=None, accuracy=None) -> None:
        if state is None:
            return
        upsert_tts_item(
            state, book_slug=book_slug, chapter_num=chapter_num, voice=voice,
            status=status, run_id=self.run_id, coverage=coverage, accuracy=accuracy,
        )

    def generate_book(
        self,
        book: Book,
        voice: str,
        book_slug: str,
        skip_existing: bool = True,
        state=None,
    ) -> list[Path]:
        generated: list[Path] = []
        skipped: list[Path] = []
        total = book.chapter_count
        book_dir = self.cfg.output_dir / book_slug
        book_dir.mkdir(parents=True, exist_ok=True)

        # (label, chapter_num, text) —— opening / 各章 / closing
        plan: list[tuple[str, str, str]] = [("opening", "0", compose_opening(book))]
        for idx, chapter in enumerate(book.chapters):
            num = str(chapter.chapter_id) if chapter.chapter_id else str(idx + 1)
            plan.append((f"chapter-{num}", num, compose_chapter(chapter)))
        if total > 0:
            plan.append(("closing", str(total + 1), compose_closing(book)))

        for i, (label, chapter_num, text) in enumerate(plan):
            filename = f"{book_slug}_{chapter_num}_{voice}.{self.cfg.tts_format}"
            output_path = book_dir / filename
            print(f"  [{i}/{len(plan) - 1}] {label} -> {filename} ({len(text)} chars)")

            if skip_existing:
                reason = skip_reason(output_path, text)
                if reason == "skippable":
                    print(f"      [skip] 输入未变，复用已有产物")
                    skipped.append(output_path)
                    self._register(state, book_slug, chapter_num, voice, GENERATED)
                    continue
                if reason == "input-changed":
                    print(f"      [regen] 输入已变化，旧产物作废")

            self.client.generate(text, voice, output_path)
            generated.append(output_path)
            self._register(state, book_slug, chapter_num, voice, GENERATED)

            if i < len(plan) - 1:
                time.sleep(self.cfg.api_delay_seconds)

        print(f"  完成: 新生成 {len(generated)} / 跳过 {len(skipped)} / 共 {len(plan)}")
        return generated

    # ------------------------------------------------------------------
    def generate_single_json(self, json_path: Path, voice: str,
                             skip_existing: bool = True, state=None) -> list[Path]:
        book_slug = json_path.stem
        print(f"Reading: {json_path}")
        book = read_book(json_path)
        print(f"Book: {book.book_info.title} by {book.book_info.author}")
        print(f"Chapters: {book.chapter_count}, Voice: {voice}")
        return self.generate_book(book, voice, book_slug,
                                  skip_existing=skip_existing, state=state)

    def generate_from_dir(self, books_dir: Path, voice: str,
                          skip_existing: bool = True, state=None) -> dict[str, list[Path]]:
        json_files = find_book_json_files(books_dir)
        if not json_files:
            print(f"No JSON files found in {books_dir}")
            return {}

        results: dict[str, list[Path]] = {}
        total_files = len(json_files)
        for i, json_path in enumerate(json_files, 1):
            print(f"\n=== [{i}/{total_files}] {json_path.name} ===")
            try:
                generated = self.generate_single_json(
                    json_path, voice, skip_existing=skip_existing, state=state)
                results[json_path.name] = generated
            except Exception as exc:
                print(f"  ERROR: {exc}", file=sys.stderr)
                results[json_path.name] = []
        return results

    def generate_test_phase(self, json_path: Path, skip_existing: bool = True,
                            state=None) -> list[Path]:
        voice = self.cfg.tts_voice_test
        print("=== TEST PHASE ===")
        return self.generate_single_json(json_path, voice,
                                         skip_existing=skip_existing, state=state)

    def generate_production_phase(self, books_dir: Path, voice: str,
                                  skip_existing: bool = True, state=None):
        return self.generate_from_dir(books_dir, voice,
                                      skip_existing=skip_existing, state=state)


# ---------------------------------------------------------------------------
# 单书 CLI：Argo 的每个 generate task 调一次（一个 Pod 一本书）
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="TTS 生成（单书，供 Argo task 调用）")
    parser.add_argument("--book", required=True,
                        help="书籍 JSON 路径或 slug（相对 books_dir）")
    parser.add_argument("--voice", required=True, help="音色")
    parser.add_argument("--engine", default="openai", choices=["openai", "edge-tts"])
    parser.add_argument("--no-skip", action="store_true",
                        help="禁用断点续跑（强制全部重生成）")
    parser.add_argument("--state-db",
                        default=getenv("PIPELINE_STATE_DB", LEGACY_STATE_DB, ""),
                        help="item 状态库路径（SQLite；不填则不登记）")
    args = parser.parse_args()

    cfg = Config.from_env()
    book_path = Path(args.book)
    if not book_path.exists():
        name = args.book if args.book.endswith(".json") else f"{args.book}.json"
        candidate = cfg.books_dir / name
        if candidate.exists():
            book_path = candidate

    state = connect(args.state_db) if args.state_db else None
    if state is not None:
        open_run(state, new_run_id(), note=f"generate {book_path.name}")

    pipeline = TTSPipeline(cfg, engine=args.engine)
    try:
        pipeline.generate_single_json(book_path, args.voice,
                                      skip_existing=not args.no_skip, state=state)
    finally:
        if state is not None:
            state.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
