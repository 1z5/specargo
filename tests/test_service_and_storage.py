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
