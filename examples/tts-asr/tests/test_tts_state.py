"""TTS 领域示例的离线测试：书籍口径 -> 通用状态表的映射、对账、重复检测。

只依赖标准库（不 import openai/faster-whisper）。运行：
    python3 examples/tts-asr/tests/test_tts_state.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_EXAMPLE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_EXAMPLE_DIR))
sys.path.insert(0, str(_EXAMPLE_DIR.parents[1] / "app"))

from domain_state import (expected_item_keys, reconcile,  # noqa: E402
                          scan_duplicate_files, upsert_tts_item)
from models import Book, BookInfo, Chapter               # noqa: E402
from specargo import state as st                                       # noqa: E402


def _book(title: str = "Do What Matters", n_chapters: int = 3) -> Book:
    return Book(
        book_info=BookInfo(title=title, author="A", hook_intro="hook"),
        chapters=[
            Chapter(chapter_id=i + 1, chapter_title=f"c{i}", chapter_intro="x",
                    breakdown={})
            for i in range(n_chapters)
        ],
        outro="bye",
    )


def test_expected_keys_match_filename_convention():
    book = _book(n_chapters=3)
    assert expected_item_keys(book, "voice-a") == ["0", "1", "2", "3", "4"]


def test_reconcile_detects_missing_and_unverified():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        book = _book(n_chapters=3)
        # 口径：产物与登记用 JSON 文件名 stem，不是标题派生的 book.slug
        slug = "book_a"
        assert slug != book.slug, "本测试就是要覆盖两个口径不同的情况"
        st.open_run(conn, "run-1")
        upsert_tts_item(conn, book_slug=slug, chapter_num="0", voice="voice-a",
                        status=st.GENERATED, run_id="run-1")
        upsert_tts_item(conn, book_slug=slug, chapter_num="1", voice="voice-a",
                        status=st.VERIFIED, run_id="run-1",
                        coverage=0.99, accuracy=0.99)

        r = reconcile(conn, slug, book, "voice-a")
        assert r["expected"] == 5
        assert r["verified"] == 1
        assert r["missing_generated"] == ["2", "3", "4"], r
        assert r["not_verified"] == ["0"], r
        assert r["ok"] is False

        for k in ["2", "3", "4"]:
            upsert_tts_item(conn, book_slug=slug, chapter_num=k, voice="voice-a",
                            status=st.VERIFIED, run_id="run-1",
                            coverage=1.0, accuracy=1.0)
        upsert_tts_item(conn, book_slug=slug, chapter_num="0", voice="voice-a",
                        status=st.VERIFIED, run_id="run-1",
                        coverage=1.0, accuracy=1.0)
        assert reconcile(conn, slug, book, "voice-a")["ok"] is True


def test_scan_duplicate_files():
    with tempfile.TemporaryDirectory() as d:
        out_dir = Path(d)
        (out_dir / "book_a_1_voice-a.mp3").write_bytes(b"1")
        (out_dir / "book_a_1_voice-a (1).mp3").write_bytes(b"2")   # 同名拷贝残留
        (out_dir / "book_a_1_voice-a.mp3.input.txt").write_text("x")
        dupes = scan_duplicate_files(out_dir, "book_a", "voice-a")
        assert dupes, "应检出重复文件"
        assert not any("input.txt" in x for x in dupes), "影子文件不算产物"


def test_scan_duplicate_by_identical_input():
    """不同文件但输入指纹相同 = 内容重复（错位信号）。"""
    with tempfile.TemporaryDirectory() as d:
        out_dir = Path(d)
        for name in ("book_a_1_voice-a.mp3", "book_a_2_voice-a.mp3"):
            (out_dir / name).write_bytes(b"x")
            (out_dir / (name + ".input.sha256")).write_text("same-fingerprint\n")
        dupes = scan_duplicate_files(out_dir, "book_a", "voice-a")
        assert any("输入相同" in x for x in dupes), dupes


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"  FAIL {name}: {exc}")
    print(f"\n{'领域状态测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
