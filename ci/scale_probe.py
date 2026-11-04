"""规模探针：状态日志在真实 S3 HTTP 端点上的成本曲线（非 CI，手工跑）。

用法::

    python3 ci/scale_probe.py                 # 默认 200/500/1000 分区
    python3 ci/scale_probe.py 50 200          # 自定义分区数

测的是"真实部署的并发/规模验证"这条 ROADMAP 遗留项里**能在本机证明的部分**：
对象数增长公式、写吞吐（并发，模拟多 Pod 同时写各自 key）、末 stage 直读成本、
onExit 对账成本，以及与"全量读 + 过滤"的倍数关系。

不测（也无法在本机测）：Argo controller 的队列深度 / API server QPS / Pod 调度延迟、
真实 MinIO/OSS/S3 的网络往返量级——本机没有 docker/kind/argo，那部分只能留给
有集群的环境，ROADMAP 里如实标注。
"""
from __future__ import annotations

import importlib.util
import logging
import os
import socket
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

logging.getLogger("werkzeug").setLevel(logging.ERROR)

import specargo.journal as journal                                # noqa: E402
from specargo.state import evaluate_items                         # noqa: E402

STAGES = ("extract", "transform", "load")
FINAL = STAGES[-1]
DATASET = "dwd_order"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Probe:
    def __init__(self):
        from moto.server import ThreadedMotoServer
        import boto3
        self.port = _free_port()
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self.bucket = "specargo-scale-probe"
        self._server = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port)
        self._server.start()
        self._boto3 = boto3
        self._client = None
        self._wait_ready()

    def _wait_ready(self):
        for _ in range(100):
            try:
                self.client.list_buckets()
                return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("moto server 未就绪")

    @property
    def client(self):
        if self._client is None:
            self._client = self._boto3.client(
                "s3", endpoint_url=self.endpoint,
                aws_access_key_id="test-access-key",
                aws_secret_access_key="test-secret-key",
                region_name="us-east-1")
            self._client.create_bucket(Bucket=self.bucket)
        return self._client

    def stop(self):
        self._server.stop()

    def count(self, prefix: str) -> int:
        total, token = 0, None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kw)
            total += len(resp.get("Contents", []))
            if resp.get("IsTruncated"):
                token = resp["NextContinuationToken"]
            else:
                return total


def keys(n: int) -> list[str]:
    return [f"2026-{i // 28 + 1:02d}-{i % 28 + 1:02d}" for i in range(n)]


def write_concurrent(run_id: str, n: int, workers: int) -> float:
    """并发写 n 分区 × 3 stage（模拟多 Pod 各写自己的 key），返回墙钟秒数。"""
    all_keys = keys(n)
    chunks = [all_keys[i::workers] for i in range(workers)]

    def work(chunk: list[str]) -> None:
        for key in chunk:
            for stage in STAGES:
                journal.put_status(run_id=run_id, dataset=DATASET, partition=key,
                                   variant="", stage=stage, status="verified")

    threads = [threading.Thread(target=work, args=(c,)) for c in chunks]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - start


def measure(probe: Probe, n: int) -> dict:
    run_id = f"scale-{n}"
    journal.put_partitions(run_id, [{"dataset": DATASET, "key": k, "variant": ""}
                                    for k in keys(n)])
    write_s = write_concurrent(run_id, n, workers=8)
    status_objects = probe.count(f"runs/{run_id}/{journal.STATUS_ROOT}/")

    items = [(DATASET, k, "") for k in keys(n)]
    t0 = time.perf_counter()
    rows = journal.get_statuses_for(run_id, FINAL, items)     # 末 stage 直读
    read_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    full = journal.load_statuses(run_id)                      # 全量读 + 过滤（旧路径）
    full_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    report = evaluate_items(DATASET, items, rows, final_stage=FINAL)  # onExit 对账判定
    reconcile_s = time.perf_counter() - t0

    return {
        "n": n, "objects": status_objects, "write_s": write_s,
        "read_s": read_s, "full_s": full_s, "reconcile_s": reconcile_s,
        "rows": len(rows), "full_rows": len(full), "report": report,
    }


def main(argv: list[str]) -> int:
    sizes = [int(a) for a in argv[1:]] or [200, 500, 1000]
    probe = Probe()
    os.environ.update({
        "OBJSTORE_KIND": "s3", "OBJSTORE_ENDPOINT": probe.endpoint,
        "OBJSTORE_BUCKET": probe.bucket, "OBJSTORE_ACCESS_KEY": "test-access-key",
        "OBJSTORE_SECRET_KEY": "test-secret-key", "OBJSTORE_REGION": "us-east-1",
    })
    print(f"# 状态日志规模探针（真实 S3 HTTP 端点：moto server @ {probe.endpoint}，"
          f"8 并发写）\n")
    print("| 分区数 N | 状态对象数 | 写入(8并发) | 末stage直读 | 全量读(旧路径) | 直读/全量 | 对账判定 |")
    print("|---|---|---|---|---|---|---|")
    try:
        for n in sizes:
            r = measure(probe, n)
            ratio = f"{r['full_s'] / r['read_s']:.1f}×" if r["read_s"] else "-"
            print(f"| {r['n']} | {r['objects']} | {r['write_s']:.2f}s | "
                  f"{r['read_s']:.2f}s | {r['full_s']:.2f}s | {ratio} | "
                  f"expected={r['report'].get('expected')} "
                  f"missing={len(r['report'].get('missing', []))} "
                  f"ok={r['report'].get('ok')} |")
    finally:
        probe.stop()
    print("\n说明：对账判定耗时与 N 无关（纯内存 evaluate_items），见 evaluate_items 实现；"
          "\n表中时间为墙钟秒，含 localhost HTTP + boto3 序列化开销。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
