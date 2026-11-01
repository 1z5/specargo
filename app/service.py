"""Python 常驻处理服务（JD 职责 3 的完整对应）。

JD 原文："维护现有 Python 常驻处理服务，完成接口适配、处理模块接入及运行问题修复，
支持同步调用或异步任务处理，配合完成部署和版本升级。"
要求 5："能够检查任务状态、日志、健康探针和资源配置，配合完成滚动升级及优雅停机。"

本模块用**标准库**实现一个生产形态的常驻服务，覆盖上述全部要点：

    GET  /healthz            存活探针：只探进程本身（事件循环是否卡死）
    GET  /readyz             就绪探针：探依赖（状态库/存储可达），失败即摘流量
    POST /tasks              提交异步任务 -> 返回 job_id（异步任务处理）
    POST /process            同步处理 -> 直接返回结果（同步调用）
    GET  /tasks/{job_id}     任务状态 + 进度（current/total）
    POST /tasks/{job_id}/retry  失败任务重处理（JD 职责 2）
    GET  /tasks              任务列表 + 积压量
    GET  /metrics            Prometheus 指标
    GET  /version            版本信息（配合滚动升级/回滚）

优雅停机（K8s 删 Pod 时的时序）：
    收到 SIGTERM -> /readyz 立即返回 503（停止接新流量）
    -> 等在途请求与在途任务完成（有上限）-> 保存 checkpoint -> 退出
    preStop 里 sleep 的目的不是拖延，是等 Endpoints 传播到所有客户端。

为什么用标准库而不是 FastAPI：零依赖、可离线跑、可单测；
生产换 FastAPI/uvicorn 时这些端点和停机语义一一对应（README 有对照表）。
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socketserver
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

# ---- 可选依赖：没有也能起服务（降级为"不登记状态"）----
try:
    import metrics as _metrics
except Exception:                                     # pragma: no cover
    _metrics = None
try:
    from state import FAILED, GENERATED, VERIFIED, connect, upsert_generic_item
except Exception:                                     # pragma: no cover
    connect = None

VERSION = os.getenv("SERVICE_VERSION", "dev")


# ---------------------------------------------------------------------------
# 任务执行器：业务处理模块的接入点（JD："完成接口适配、处理模块接入"）
# ---------------------------------------------------------------------------

class Processor:
    """处理模块基类。新模块（切图、转写、推理）继承并实现 process()。"""

    name = "base"

    def process(self, item: dict) -> dict:
        raise NotImplementedError


class EchoProcessor(Processor):
    """演示用处理模块：回显输入并模拟耗时。"""

    name = "echo"

    def process(self, item: dict) -> dict:
        time.sleep(float(item.get("delay", 0.05)))
        return {"echo": item.get("payload"), "processed_at": time.time()}


# ---------------------------------------------------------------------------
# 服务本体
# ---------------------------------------------------------------------------

class Service:
    def __init__(self, *, processor: Processor | None = None, state_db: str | None = None,
                 workers: int = 4, graceful_timeout: float = 30.0):
        self.processor = processor or EchoProcessor()
        self.workers = workers
        self.graceful_timeout = graceful_timeout
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="task")
        self.state = connect(state_db) if (state_db and connect) else None

        self.jobs: dict[str, dict] = {}          # job_id -> 任务记录（生产应落库）
        self._jobs_lock = threading.Lock()
        self.inflight = 0
        self._inflight_lock = threading.Lock()

        # ---- 停机状态机 ----
        self.draining = False                    # True 后 /readyz 返回 503
        self.stopping = False
        self._drain_event = threading.Event()

    # ---------------- 健康检查 ----------------
    def healthz(self) -> tuple[int, dict]:
        """存活：只探进程本身。探依赖会被依赖抖动拖成重启风暴。"""
        return 200, {"status": "ok", "version": VERSION}

    def readyz(self) -> tuple[int, dict]:
        """就绪：探依赖 + 停机状态。draining 中必须摘流量。"""
        if self.draining:
            return 503, {"status": "draining"}
        checks = {"processor": self.processor.name}
        if self.state is not None:
            try:
                self.state.execute("SELECT 1").fetchone()
                checks["state_db"] = "ok"
            except Exception as exc:
                checks["state_db"] = f"error: {exc}"
                return 503, {"status": "not-ready", "checks": checks}
        return 200, {"status": "ready", "checks": checks}

    # ---------------- 任务提交与查询 ----------------
    def submit(self, payload: dict) -> str:
        if self.draining:
            raise RuntimeError("服务正在停机，拒绝新任务")
        job_id = uuid.uuid4().hex[:12]
        with self._jobs_lock:
            self.jobs[job_id] = {
                "job_id": job_id, "status": "queued", "progress_current": 0,
                "progress_total": 1, "error": None, "result": None,
                "created_at": time.time(), "updated_at": time.time(),
            }
        self.pool.submit(self._run_job, job_id, payload)
        return job_id

    def _run_job(self, job_id: str, payload: dict) -> None:
        with self._inflight_lock:
            self.inflight += 1
            if _metrics:
                _metrics.METRICS.inflight.inc(1, stage=self.processor.name)
        started = time.perf_counter()
        try:
            self._update_job(job_id, status=GENERATED if connect else "running",
                             progress_current=0)
            result = self.processor.process(payload)
            self._update_job(job_id, status=VERIFIED if connect else "succeeded",
                             progress_current=1, result=result)
            if _metrics:
                _metrics.METRICS.stage_runs.inc(1, stage=self.processor.name, status="succeeded")
                _metrics.METRICS.check_results.inc(1, check=self.processor.name, result="pass")
        except Exception as exc:
            self._update_job(job_id, status=FAILED if connect else "failed", error=repr(exc))
            if _metrics:
                _metrics.METRICS.stage_runs.inc(1, stage=self.processor.name, status="failed")
                _metrics.METRICS.check_results.inc(1, check=self.processor.name, result="fail")
        finally:
            if _metrics:
                _metrics.METRICS.stage_duration.observe(time.perf_counter() - started,
                                                stage=self.processor.name)
                _metrics.METRICS.inflight.inc(-1, stage=self.processor.name)
            with self._inflight_lock:
                self.inflight -= 1
            if self.draining and self.inflight == 0:
                self._drain_event.set()

    def _update_job(self, job_id: str, **fields) -> None:
        with self._jobs_lock:
            job = self.jobs.get(job_id)
            if job is None:
                return
            job.update(fields)
            job["updated_at"] = time.time()
            # 状态登记到 item 表（任务记录，对应 JD 要求 4）
            if self.state is not None and connect:
                try:
                    upsert_generic_item(
                        self.state, dataset="service", partition=job_id, variant="",
                        status=job["status"], run_id=job_id, stage=self.processor.name)
                except Exception:
                    pass

    def get_job(self, job_id: str) -> dict | None:
        with self._jobs_lock:
            job = self.jobs.get(job_id)
            return dict(job) if job else None

    def list_jobs(self) -> dict:
        with self._jobs_lock:
            jobs = list(self.jobs.values())
        counts: dict[str, int] = {}
        for j in jobs:
            counts[j["status"]] = counts.get(j["status"], 0) + 1
        return {"total": len(jobs), "by_status": counts,
                "inflight": self.inflight,
                "backlog": counts.get("queued", 0) + counts.get("running", 0)
                + counts.get("generated", 0)}

    def retry(self, job_id: str) -> str:
        """失败任务重处理：重置状态并重新入队（JD 职责 2）。"""
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job["status"] not in ("failed", FAILED):
            raise RuntimeError(f"仅失败任务可重试，当前 {job['status']}")
        self._update_job(job_id, status="queued", error=None)
        self.pool.submit(self._run_job, job_id, job.get("payload") or {})
        return job_id

    # ---------------- 优雅停机 ----------------
    def begin_drain(self) -> None:
        """第一步：摘流量。/readyz 立刻返回 503，但**端口还开着**。

        必须与"停止 accept"分开：K8s 删 Pod 时会同时发生
        "从 Service Endpoints 摘除"和"发 SIGTERM"，而 Endpoints 传播到所有
       客户端有延迟。若摘除和关端口同时做，传播期内到达的请求会被 connection
        refused——这就是 preStop hook 里 sleep 的真正用途（等传播，不是拖延）。
        """
        if not self.draining:
            self.draining = True
            print("[drain] /readyz 已置 503，停止接入新任务（端口仍开放）", flush=True)

    def shutdown(self, endpoint_delay: float = 0.0) -> None:
        """SIGTERM 处理：摘流量 -> 等 Endpoints 传播 -> 等在途 -> checkpoint -> 退出。"""
        if self.stopping:
            return
        self.stopping = True
        self.begin_drain()

        if endpoint_delay > 0:
            print(f"[drain] 等待 Endpoints 传播 {endpoint_delay}s（等价 preStop sleep）",
                  flush=True)
            time.sleep(endpoint_delay)

        deadline = time.time() + self.graceful_timeout
        while time.time() < deadline:
            with self._inflight_lock:
                if self.inflight == 0:
                    break
            time.sleep(0.2)
        else:
            print(f"[shutdown] 宽限 {self.graceful_timeout}s 内未完成，强制退出", flush=True)

        # checkpoint：生产这里要落 offset/进度（批处理被抢占时靠它续跑）
        print(f"[shutdown] checkpoint: inflight={self.inflight} "
              f"jobs={len(self.jobs)}", flush=True)
        self.pool.shutdown(wait=False, cancel_futures=True)
        print("[shutdown] 已退出", flush=True)


# ---------------------------------------------------------------------------
# HTTP 层
# ---------------------------------------------------------------------------

def make_handler(service: Service):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"PipelineService/{VERSION}"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):        # 结构化日志到 stdout
            print(json.dumps({"ts": time.time(), "level": "info", "msg": fmt % args},
                             ensure_ascii=False), flush=True)

        def _send(self, code: int, body, content_type="application/json") -> None:
            data = body if isinstance(body, bytes) else json.dumps(
                body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception:
                return {}

        # ---------------- 路由 ----------------
        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/healthz":
                code, body = service.healthz()
                return self._send(code, body)
            if path == "/readyz":
                code, body = service.readyz()
                return self._send(code, body)
            if path == "/metrics":
                text = _metrics.METRICS.render() if _metrics else "# metrics unavailable\n"
                return self._send(200, text.encode("utf-8"), "text/plain; version=0.0.4")
            if path == "/version":
                return self._send(200, {"version": VERSION, "processor": service.processor.name})
            if path == "/tasks":
                return self._send(200, service.list_jobs())
            if path.startswith("/tasks/"):
                job = service.get_job(path.rsplit("/", 1)[-1])
                if job is None:
                    return self._send(404, {"error": "job not found"})
                return self._send(200, job)
            return self._send(404, {"error": f"unknown path: {path}"})

        def do_POST(self):
            path = urlparse(self.path).path
            body = self._read_json()
            try:
                if path == "/tasks":
                    job_id = service.submit(body)
                    return self._send(202, {"job_id": job_id, "status": "queued"})
                if path == "/process":                     # 同步调用
                    result = service.processor.process(body)
                    return self._send(200, {"result": result})
                if path.startswith("/tasks/") and path.endswith("/retry"):
                    job_id = path[len("/tasks/"):-len("/retry")]
                    service.retry(job_id)
                    return self._send(202, {"job_id": job_id, "status": "requeued"})
            except RuntimeError as exc:
                return self._send(503, {"error": str(exc)})
            except KeyError:
                return self._send(404, {"error": "job not found"})
            except Exception as exc:
                return self._send(500, {"error": repr(exc)})
            return self._send(404, {"error": f"unknown path: {path}"})

    return Handler


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main() -> int:
    parser = argparse.ArgumentParser(description="Python 常驻处理服务")
    parser.add_argument("--host", default=os.getenv("SERVICE_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SERVICE_PORT", "8080")))
    parser.add_argument("--workers", type=int, default=int(os.getenv("SERVICE_WORKERS", "4")))
    parser.add_argument("--graceful-timeout", type=float,
                        default=float(os.getenv("SERVICE_GRACEFUL_TIMEOUT", "30")))
    parser.add_argument("--endpoint-delay", type=float,
                        default=float(os.getenv("SERVICE_ENDPOINT_DELAY", "2")),
                        help="摘流量后等待 Endpoints 传播的秒数（等价 K8s preStop sleep）")
    parser.add_argument("--state-db", default=os.getenv("TTS_STATE_DB", ""))
    parser.add_argument("--processor", default="echo", choices=["echo"])
    args = parser.parse_args()

    service = Service(state_db=args.state_db or None, workers=args.workers,
                      graceful_timeout=args.graceful_timeout)
    httpd = Server((args.host, args.port), make_handler(service))

    def _term(signum, _frame):
        print(f"[signal] 收到 {signal.Signals(signum).name}，开始优雅停机", flush=True)

        def _graceful():
            # 顺序至关重要：先摘流量 -> 等传播 -> 等在途 -> 最后才停止 accept
            service.begin_drain()
            service.shutdown(endpoint_delay=args.endpoint_delay)
            httpd.shutdown()

        threading.Thread(target=_graceful, daemon=True).start()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    print(f"[boot] processor={service.processor.name} workers={args.workers} "
          f"version={VERSION} listen={args.host}:{args.port}", flush=True)
    try:
        httpd.serve_forever()
    finally:
        service.shutdown()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
