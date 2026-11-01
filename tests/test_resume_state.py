"""断点续跑 / 幂等 / 对账的离线验证（不需要 API key、不需要集群）。

运行：
    cd ~/dev/tts-argo/app && python3 -m pytest ../tests -q
    或 python3 ../tests/test_resume_state.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from models import Book, BookInfo, Chapter          # noqa: E402
from resume import fingerprint, read_fingerprint, should_skip, skip_reason, write_fingerprint  # noqa: E402
import state as st                                   # noqa: E402


def _book(title: str = "Do What Matters", n_chapters: int = 3) -> Book:
    return Book(
        book_info=BookInfo(title=title, author="A", hook_intro="hook"),
        chapters=[
            Chapter(chapter_id=i + 1, chapter_title=f"c{i}", chapter_intro="x", breakdown={})
            for i in range(n_chapters)
        ],
        outro="bye",
    )


# ---------------------------------------------------------------------------
# 1. 输入指纹与跳过判定
# ---------------------------------------------------------------------------

def test_skip_when_output_and_fingerprint_match():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "book_a_1_cedar.mp3"
        out.write_bytes(b"fake-audio")
        text = "Point 1, hello world."
        assert should_skip(out, text) is False, "没有指纹时必须重生成（保守）"
        write_fingerprint(out, text)
        assert should_skip(out, text) is True, "指纹匹配应跳过"
        assert skip_reason(out, text) == "skippable"


def test_regenerate_when_input_changed():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "book_a_1_cedar.mp3"
        out.write_bytes(b"fake-audio")
        write_fingerprint(out, "old text")
        assert should_skip(out, "new text") is False
        assert skip_reason(out, "new text") == "input-changed"


def test_regenerate_when_output_missing():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "book_a_1_cedar.mp3"
        assert should_skip(out, "text") is False
        assert skip_reason(out, "text") == "output-missing"


def test_fingerprint_is_stable_and_sensitive():
    assert fingerprint("abc") == fingerprint("abc")
    assert fingerprint("abc") != fingerprint("abd")


# ---------------------------------------------------------------------------
# 2. item 状态表 + 对账
# ---------------------------------------------------------------------------

def test_expected_keys_match_filename_convention():
    book = _book(n_chapters=3)
    assert st.expected_item_keys(book, "cedar") == ["0", "1", "2", "3", "4"]


def test_reconcile_detects_missing_and_unverified():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        book = _book(n_chapters=3)
        # 口径：产物与登记用 JSON 文件名 stem，不是标题派生的 book.slug
        slug = "book_a"
        assert slug != book.slug, "本测试就是要覆盖两个口径不同的情况"
        st.open_run(conn, "run-1")
        # 只登记 2 个 item：一个 generated、一个 verified
        st.upsert_item(conn, book_slug=slug, chapter_num="0", voice="cedar",
                       status=st.GENERATED, run_id="run-1")
        st.upsert_item(conn, book_slug=slug, chapter_num="1", voice="cedar",
                       status=st.VERIFIED, run_id="run-1", coverage=0.99, accuracy=0.99)

        r = st.reconcile(conn, slug, book, "cedar")
        assert r["expected"] == 5
        assert r["verified"] == 1
        assert r["missing_generated"] == ["2", "3", "4"], r
        assert r["not_verified"] == ["0"], r
        assert r["ok"] is False

        # 补齐后应对账通过
        for k in ["2", "3", "4"]:
            st.upsert_item(conn, book_slug=slug, chapter_num=k, voice="cedar",
                           status=st.VERIFIED, run_id="run-1", coverage=1.0, accuracy=1.0)
        st.upsert_item(conn, book_slug=slug, chapter_num="0", voice="cedar",
                       status=st.VERIFIED, run_id="run-1", coverage=1.0, accuracy=1.0)
        r2 = st.reconcile(conn, slug, book, "cedar")
        assert r2["ok"] is True, r2


def test_upsert_is_idempotent_and_counts_attempts():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        for _ in range(3):
            st.upsert_item(conn, book_slug="b", chapter_num="1", voice="v",
                           status=st.GENERATED, run_id="r")
        row = st.get_item(conn, "b", "1", "v")
        assert row["attempt"] == 3
        assert row["status"] == st.GENERATED


def test_scan_duplicate_files():
    with tempfile.TemporaryDirectory() as d:
        book_dir = Path(d)
        (book_dir / "book_a_1_cedar.mp3").write_bytes(b"1")
        (book_dir / "book_a_1_cedar (1).mp3").write_bytes(b"2")   # 同名拷贝残留
        (book_dir / "book_a_1_cedar.mp3.input.txt").write_text("x")
        dupes = st.scan_duplicate_files(book_dir, "book_a", "cedar")
        assert dupes, "应检出重复文件"
        assert not any("input.txt" in x for x in dupes), "影子文件不算产物"


def test_scan_duplicate_by_identical_input():
    """不同文件但输入指纹相同 = 内容重复（错位信号）。"""
    with tempfile.TemporaryDirectory() as d:
        book_dir = Path(d)
        for name in ("book_a_1_cedar.mp3", "book_a_2_cedar.mp3"):
            (book_dir / name).write_bytes(b"x")
            (book_dir / (name + ".input.sha256")).write_text("same-fingerprint\n")
        dupes = st.scan_duplicate_files(book_dir, "book_a", "cedar")
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
    print(f"\n{'全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
