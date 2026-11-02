"""常驻服务 + 对象存储 + 指标 的测试（真实起服务，不用 mock）。"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from artifacts import LocalArtifactStore, make_store          # noqa: E402
from metrics import Counter, Gauge, Histogram, Metrics         # noqa: E402


# ===================== 对象存储 =====================

def test_local_artifact_store_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        store = LocalArtifactStore(Path(d) / "artifacts")
        src = Path(d) / "report.json"
        src.write_text('{"ok": true}', encoding="utf-8")
        key = store.put(src, "run-1/verify/report.json")
        assert store.exists(key)
        dest = Path(d) / "back.json"
        store.get(key, dest)
        assert json.loads(dest.read_text(encoding="utf-8"))["ok"] is True
        assert "run-1/verify/report.json" in store.list("run-1")
        assert store.digest(key) == store.digest(key), "digest 应稳定"
        assert store.delete(key) and not store.exists(key)


def test_artifact_store_rejects_path_traversal():
    with tempfile.TemporaryDirectory() as d:
        store = LocalArtifactStore(Path(d) / "art")
        try:
            store.put(Path(d) / "x", "../../etc/passwd")
            raise AssertionError("应拒绝目录穿越")
        except ValueError:
            pass
        # 任意路径段是 "." / ".." / 空段都拒绝（S3 实现共用同一套校验）
        for bad in ("runs/dotdot/../o.jsonl", "runs//o.jsonl", "..", "."):
            try:
                store.put(Path(d) / "x", bad)
                raise AssertionError(f"应拒绝非法 key: {bad}")
            except ValueError:
                pass
        import artifacts
        for bad in ("..", ".", "", "a/../b"):
            try:
                artifacts._normalize_key(bad)
                raise AssertionError(f"_normalize_key 应拒绝: {bad!r}")
            except ValueError:
                pass


def test_s3_list_prefix_keeps_trailing_slash_semantics():
    """S3 list 前缀必须保留尾斜杠：否则 "runs/r1/" 会 over-match "runs/r10/"。

    本地实现是目录语义、S3 是字符串前缀语义；两者对"带尾斜杠的目录前缀"
    必须表现一致。这里直接测前缀构造（S3 构造依赖 boto3，CI 不装）。
    """
    from artifacts import S3ArtifactStore

    f = S3ArtifactStore._list_prefix
    assert f("p", "runs/r1/") == "p/runs/r1/", f("p", "runs/r1/")
    assert f("p", "runs/r1") == "p/runs/r1"
    assert f("p", "") == "p/"
    assert f("", "runs/") == "runs/"
    assert f("p", "/runs/r2/") == "p/runs/r2/"
    # 关键断言：目录前缀不应 over-match 兄弟前缀
    assert not "p/runs/r10/b.jsonl".startswith(f("p", "runs/r1/"))
    assert "p/runs/r1/a.jsonl".startswith(f("p", "runs/r1/"))
    try:
        f("p", "../x/")
        raise AssertionError("应拒绝目录穿越前缀")
    except ValueError:
        pass


def test_artifact_store_directory_upload():
    with tempfile.TemporaryDirectory() as d:
        store = LocalArtifactStore(Path(d) / "art")
        src = Path(d) / "out"; (src / "sub").mkdir(parents=True)
        (src / "a.mp3").write_bytes(b"1"); (src / "sub" / "b.mp3").write_bytes(b"2")
        store.put(src, "run-1/audio")
        assert store.exists("run-1/audio/a.mp3")
        assert store.exists("run-1/audio/sub/b.mp3")
        dest = Path(d) / "restore"
        store.get("run-1/audio", dest)
        assert (dest / "sub" / "b.mp3").read_bytes() == b"2"


def test_make_store_factory():
    with tempfile.TemporaryDirectory() as d:
        assert isinstance(make_store("local", root=d), LocalArtifactStore)
        try:
            make_store("gcs")
            raise AssertionError("应拒绝未知类型")
        except ValueError:
            pass


# ===================== 指标 =====================

def test_metrics_render_prometheus_format():
    m = Metrics(namespace="test")
    m.stage_runs.inc(3, stage="generate", status="succeeded")
    m.stage_runs.inc(1, stage="generate", status="failed")
    m.backlog.set(7, stage="generate")
    with m.stage_duration.time(stage="generate"):
        time.sleep(0.01)
    text = m.render()
    assert "# TYPE test_stage_runs_total counter" in text
    assert 'test_stage_runs_total{stage="generate",status="succeeded"} 3' in text
    assert 'test_backlog{stage="generate"} 7' in text
    assert "test_stage_duration_seconds_bucket" in text
    assert text.endswith("\n")


def test_metrics_file_output_is_atomic():
    with tempfile.TemporaryDirectory() as d:
        m = Metrics(namespace="t")
        m.backlog.set(1, stage="x")
        p = m.write_file(Path(d) / "metrics.prom")
        assert p.exists() and 't_backlog{stage="x"} 1' in p.read_text()
        assert not Path(str(p) + ".tmp").exists(), "临时文件应被原子替换"


def test_module_level_singleton_is_usable():
    """模块级单例必须可直接用（防模块名与实例名碰撞的坑）。"""
    import metrics as m
    assert hasattr(m, "METRICS"), "单例名不能与模块名相同"
    m.METRICS.inflight.inc(2, stage="x")
    assert "pipeline_inflight" in m.render()
    assert "pipeline_inflight" in m.METRICS.render()


# ===================== 常驻服务（真实起进程） =====================

def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
        return r.status, json.loads(r.read().decode("utf-8"))


def _get_raw(port: int, path: str):
    """不抛异常的 GET：4xx/5xx 也是有效响应（探针 503 要用它判断）。

    ⚠️ urlopen 对 >=400 抛 HTTPError，会把"摘流量"误判成"端口已关"。
    """
    import urllib.error
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def _post(port: int, path: str, body: dict | None = None):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read().decode("utf-8"))


def test_service_lifecycle_probes_tasks_graceful_shutdown():
    port = _free_port()
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin", "SERVICE_VERSION": "test-1",
           "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "app")}
    proc = subprocess.Popen(
        [sys.executable, "-m", "service", "--port", str(port), "--workers", "2"],
        cwd=str(Path(__file__).resolve().parents[1] / "app"), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        # 等服务就绪
        for _ in range(50):
            try:
                code, body = _get(port, "/healthz")
                if code == 200:
                    break
            except Exception:
                time.sleep(0.1)
        else:
            raise AssertionError("服务未在超时内就绪")

        # 1) 探针
        code, body = _get(port, "/healthz")
        assert code == 200 and body["status"] == "ok"
        code, body = _get(port, "/readyz")
        assert code == 200 and body["status"] == "ready", body
        code, body = _get(port, "/version")
        assert body["version"] == "test-1"

        # 2) 异步任务：提交 -> 轮询到完成
        code, body = _post(port, "/tasks", {"payload": "hello", "delay": 0.05})
        assert code == 202 and body["status"] == "queued"
        job_id = body["job_id"]
        deadline = time.time() + 10
        while time.time() < deadline:
            code, job = _get(port, f"/tasks/{job_id}")
            if job["status"] in ("succeeded", "verified", "failed"):
                break
            time.sleep(0.1)
        assert job["status"] in ("succeeded", "verified"), job
        assert job["result"]["echo"] == "hello"
        assert job["progress_current"] == 1

        # 3) 同步调用
        code, body = _post(port, "/process", {"payload": "sync", "delay": 0.01})
        assert code == 200 and body["result"]["echo"] == "sync"

        # 4) 指标端点
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
            text = r.read().decode()
        assert "pipeline_stage_runs_total" in text and 'status="succeeded"' in text

        # 5) 任务列表 + 积压（同步 /process 不建任务记录，故异步任务为 1 条）
        code, body = _get(port, "/tasks")
        assert body["total"] >= 1 and body["inflight"] == 0, body
        assert body["backlog"] == 0, body

        # 6) 优雅停机：SIGTERM 后 /readyz 应变 503，进程应自行退出
        proc.send_signal(signal.SIGTERM)
        deadline = time.time() + 20
        saw_draining = False
        while time.time() < deadline:
            try:
                code, body = _get_raw(port, "/readyz")
                if code == 503:
                    saw_draining = True
                    break
            except Exception:
                break            # 连接被拒 = 端口已关，进程已退出
            time.sleep(0.1)
        assert saw_draining, "SIGTERM 后 /readyz 应立刻返回 503（先摘流量）"
        proc.wait(timeout=20)
        assert proc.returncode == 0, f"退出码应正常，实际 {proc.returncode}"
    finally:
        if proc.poll() is None:
            proc.kill()
        out = proc.stdout.read() if proc.stdout else ""
        if "Traceback" in out:
            print(out)


def test_service_with_state_db_is_ready_and_persists_items():
    """带 --state-db 起服务：/readyz 必须 200，任务状态必须真的落库。

    旧实现的 P0：sqlite 连接在主线程创建、被 worker 线程跨线程使用 →
    /readyz 恒 503、写库异常被 `except: pass` 吞掉（状态表永远 0 行）。
    现有用例都传 state_db=None，所以 CI 抓不到。
    """
    port = _free_port()
    app_dir = Path(__file__).resolve().parents[1] / "app"
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin",
           "PYTHONPATH": str(app_dir)}
    with tempfile.TemporaryDirectory() as d:
        db = Path(d) / "svc.db"
        proc = subprocess.Popen(
            [sys.executable, "-m", "service", "--port", str(port),
             "--state-db", str(db), "--workers", "2"],
            cwd=str(app_dir), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            for _ in range(60):
                try:
                    code, body = _get(port, "/readyz")
                    if code == 200:
                        break
                except Exception:
                    pass
                time.sleep(0.1)
            else:
                raise AssertionError("带状态库的服务未在超时内就绪（readyz 恒 503？）")
            assert code == 200 and body["checks"]["state_db"] == "ok", body

            code, body = _post(port, "/tasks", {"payload": "persist", "delay": 0.02})
            assert code == 202, body
            job_id = body["job_id"]
            deadline = time.time() + 10
            while time.time() < deadline:
                code, job = _get(port, f"/tasks/{job_id}")
                if job["status"] in ("succeeded", "verified", "failed"):
                    break
                time.sleep(0.1)
            assert job["status"] in ("succeeded", "verified"), job

            # 状态必须真的落库（旧实现这里是 0 行）
            import sqlite3
            conn = sqlite3.connect(str(db))
            rows = conn.execute(
                "SELECT status FROM tts_item WHERE chapter_num=?", (job_id,)).fetchall()
            conn.close()
            assert rows, "任务状态没有写入状态库（跨线程写库失败被吞？）"
            assert rows[0][0] in ("verified", "generated"), rows
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()


def test_service_rejects_new_tasks_while_draining():
    """停机中拒绝新任务（对应 K8s 滚动升级时不能丢流量到旧 Pod）。"""
    import importlib
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    service_mod = importlib.import_module("service")
    svc = service_mod.Service(workers=2)
    assert svc.submit({"payload": "x", "delay": 0.01})
    svc.draining = True
    try:
        svc.submit({"payload": "y"})
        raise AssertionError("draining 中应拒绝新任务")
    except RuntimeError:
        pass
    # 就绪探针在 draining 时必须 503
    code, body = svc.readyz()
    assert code == 503 and body["status"] == "draining"


def test_retry_uses_original_payload():
    """retry 必须用原始 payload 重跑，而不是空参数（静默错误结果回归）。"""
    import importlib
    service_mod = importlib.import_module("service")

    class FlakyProcessor(service_mod.Processor):
        """第一次按 payload 抛错，第二次成功——用来验证重试真的带上了原文。"""
        name = "flaky"

        def __init__(self):
            self.calls = []

        def process(self, item):
            self.calls.append(dict(item))
            if len(self.calls) == 1:
                raise RuntimeError("boom")
            return {"echo": item.get("payload")}

    svc = service_mod.Service(processor=FlakyProcessor(), workers=2)
    job_id = svc.submit({"payload": "important-data"})
    # 等第一次失败
    for _ in range(50):
        if svc.get_job(job_id)["status"] in ("failed", "failed"):
            break
        time.sleep(0.05)
    assert svc.get_job(job_id)["status"] in ("failed",), svc.get_job(job_id)

    svc.retry(job_id)
    for _ in range(50):
        job = svc.get_job(job_id)
        if job["status"] in ("succeeded", "verified"):
            break
        time.sleep(0.05)

    job = svc.get_job(job_id)
    assert job["status"] in ("succeeded", "verified"), job
    # 关键断言：两次调用都带上了原始 payload，且结果正确
    assert svc.processor.calls[0] == {"payload": "important-data"}, svc.processor.calls
    assert svc.processor.calls[1] == {"payload": "important-data"}, svc.processor.calls
    assert job["result"] == {"echo": "important-data"}, job


def test_objstore_local_mode_roundtrip():
    """objstore CLI 本地模式：put/get/exists/list（S3 未覆盖时的最小保障）。"""
    import importlib
    import os

    objstore = importlib.import_module("objstore")
    with tempfile.TemporaryDirectory() as d:
        os.environ["OBJSTORE_KIND"] = "local"
        os.environ["OBJSTORE_ROOT"] = d
        src = Path(d) / "raw.jsonl"
        src.write_text("1\n2\n3\n", encoding="utf-8")

        key = objstore.contract_key("run-1", "101", "raw")
        assert key == "runs/run-1/101/raw.jsonl", key
        objstore.put(key, src)
        assert objstore.exists(key)

        dest = Path(d) / "back.jsonl"
        objstore.get(key, dest)
        assert dest.read_text(encoding="utf-8") == "1\n2\n3\n"
        assert key in objstore.list_keys("runs/")

        # 不存在的 key：exists False，get 抛错（CLI 里转为退出码 1）
        assert not objstore.exists("runs/run-1/999/raw.jsonl")
        try:
            objstore.get("runs/run-1/999/raw.jsonl", Path(d) / "nope.jsonl")
            raise AssertionError("应抛 FileNotFoundError")
        except FileNotFoundError:
            pass


def test_objstore_cli_exit_codes():
    """CLI 的退出码语义：exists 用退出码表达（shell 里好判断）。"""
    import os
    import subprocess
    import sys

    app_dir = Path(__file__).resolve().parents[1] / "app"
    with tempfile.TemporaryDirectory() as d:
        env = dict(os.environ)
        env["OBJSTORE_KIND"] = "local"
        env["OBJSTORE_ROOT"] = d
        src = Path(d) / "a.jsonl"
        src.write_text("x\n", encoding="utf-8")

        p = subprocess.run([sys.executable, "objstore.py", "put",
                            "--key", "runs/r/1/raw.jsonl", "--file", str(src)],
                           cwd=app_dir, env=env, capture_output=True, text=True)
        assert p.returncode == 0, p.stderr

        p = subprocess.run([sys.executable, "objstore.py", "exists",
                            "--key", "runs/r/1/raw.jsonl"],
                           cwd=app_dir, env=env, capture_output=True, text=True)
        assert p.returncode == 0 and p.stdout.strip() == "true", (p.returncode, p.stdout)

        p = subprocess.run([sys.executable, "objstore.py", "exists",
                            "--key", "runs/r/2/raw.jsonl"],
                           cwd=app_dir, env=env, capture_output=True, text=True)
        assert p.returncode == 1 and p.stdout.strip() == "false", (p.returncode, p.stdout)


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"  PASS {name}")
            except AssertionError as exc:
                failures += 1; print(f"  FAIL {name}: {exc}")
    print(f"\n{'服务/存储/指标测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
