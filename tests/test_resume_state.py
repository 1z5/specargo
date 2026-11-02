"""断点续跑 / 幂等 / 通用状态表的离线验证（不需要 API key、不需要集群）。

只覆盖框架层通用能力；领域口径（书籍/章节/音色）的测试在
``examples/tts-asr/tests/test_tts_state.py``。

运行：``python3 tests/test_resume_state.py``
"""
from __future__ import annotations
import importlib.util

import sys
import tempfile
from pathlib import Path


from specargo.resume import (fingerprint, read_fingerprint, should_skip, skip_reason,  # noqa: E402
                    write_fingerprint)
from specargo import state as st                                   # noqa: E402


# ---------------------------------------------------------------------------
# 1. 输入指纹与跳过判定
# ---------------------------------------------------------------------------

def test_skip_when_output_and_fingerprint_match():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "item1.mp3"
        out.write_bytes(b"fake-output")
        text = "Point 1, hello world."
        assert should_skip(out, text) is False, "没有指纹时必须重生成（保守）"
        write_fingerprint(out, text)
        assert should_skip(out, text) is True, "指纹匹配应跳过"
        assert skip_reason(out, text) == "skippable"


def test_regenerate_when_input_changed():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "item1.mp3"
        out.write_bytes(b"fake-output")
        write_fingerprint(out, "old text")
        assert should_skip(out, "new text") is False
        assert skip_reason(out, "new text") == "input-changed"


def test_regenerate_when_output_missing():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "item1.mp3"
        assert should_skip(out, "text") is False
        assert skip_reason(out, "text") == "output-missing"


def test_fingerprint_is_stable_and_sensitive():
    assert fingerprint("abc") == fingerprint("abc")
    assert fingerprint("abc") != fingerprint("abd")
    assert read_fingerprint(Path("/nonexistent/x")) is None


# ---------------------------------------------------------------------------
# 2. 通用状态表（dataset/partition/variant/stage）
# ---------------------------------------------------------------------------

def test_upsert_is_idempotent_and_counts_attempts():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        for _ in range(3):
            st.upsert_item(conn, dataset="ds", partition="1", variant="v",
                           status=st.GENERATED, run_id="r")
        row = st.get_item(conn, "ds", "1", "v")
        assert row["attempt"] == 3
        assert row["status"] == st.GENERATED
        # 同一个 (dataset, partition, variant) 但不同 stage 是不同 item
        st.upsert_item(conn, dataset="ds", partition="1", variant="v",
                       stage="verify", status=st.VERIFIED, run_id="r")
        assert st.get_item(conn, "ds", "1", "v", stage="verify")["status"] == st.VERIFIED
        assert st.get_item(conn, "ds", "1", "v")["status"] == st.GENERATED


def test_reconcile_generic_detects_missing_and_unverified():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        expected = [("ds", "1", ""), ("ds", "2", "")]
        st.open_run(conn, "run-1")
        st.upsert_item(conn, dataset="ds", partition="1", status=st.VERIFIED,
                       run_id="run-1")
        recon = st.reconcile_generic(conn, "ds", expected)
        assert recon["missing"] == ["2"] and recon["not_verified"] == [], recon
        assert recon["ok"] is False

        st.upsert_item(conn, dataset="ds", partition="2", status=st.VERIFIED,
                       run_id="run-1")
        st.close_run(conn, "run-1", "succeeded")
        recon2 = st.reconcile_generic(conn, "ds", expected)
        assert recon2["ok"] is True and recon2["verified"] == 2, recon2
        run = conn.execute(
            "SELECT status FROM pipeline_run WHERE run_id='run-1'").fetchone()
        assert run["status"] == "succeeded"


def test_connect_creates_schema_version():
    with tempfile.TemporaryDirectory() as d:
        conn = st.connect(Path(d) / "state.db")
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        assert row["version"] == st.SCHEMA_VERSION


def test_legacy_schema_is_migrated():
    """旧版领域命名状态库能原地升级到通用 schema（数据保留）。"""
    import sqlite3
    with tempfile.TemporaryDirectory() as d:
        db = Path(d) / "old.db"
        conn = sqlite3.connect(db)
        conn.executescript("""
        CREATE TABLE tts_run (run_id TEXT PRIMARY KEY, started_at TEXT,
                              finished_at TEXT, status TEXT, note TEXT);
        CREATE TABLE tts_item (book_slug TEXT NOT NULL, chapter_num TEXT NOT NULL,
                               voice TEXT NOT NULL, stage TEXT NOT NULL DEFAULT '',
                               status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                               coverage REAL, accuracy REAL, run_id TEXT, updated_at TEXT,
                               PRIMARY KEY (book_slug, chapter_num, voice, stage));
        INSERT INTO tts_run(run_id, status) VALUES('old-run', 'succeeded');
        INSERT INTO tts_item(book_slug, chapter_num, voice, stage, status)
               VALUES('book_a', '0', 'voice-a', '', 'verified');
        """)
        conn.commit(); conn.close()

        migrated = st.connect(db)
        assert st.get_item(migrated, "book_a", "0", "voice-a")["status"] == st.VERIFIED
        run = migrated.execute(
            "SELECT status FROM pipeline_run WHERE run_id='old-run'").fetchone()
        assert run["status"] == "succeeded"
        # 再次 connect 应幂等（不重复迁移/报错）
        migrated.close()
        again = st.connect(db)
        assert again.execute("SELECT COUNT(*) FROM pipeline_item").fetchone()[0] == 1
        again.close()


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
