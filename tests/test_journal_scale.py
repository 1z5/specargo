"""状态日志的规模与并发：走真实 S3 HTTP 路径，而不是内存假客户端。

为什么值得单独一套：journal（状态日志）是集群侧"跨 Pod 无共享盘"的唯一真相源，
它的两个属性必须被**真实对象存储路径**证明，而不是被一个内存 dict 证明：

1. **规模**：状态对象数 = stage × partition（+ 1 个分区清单），且对账只读末 stage
   → GET 次数 == N、LIST 0 次；对照的"全量读"是 LIST 1 次 + 3N 次 GET。
2. **并发**：多个 Pod 同时写各自的 key 不丢不覆（key 独立 → 天然并发安全），
   且并发写完之后对象数仍是 3N、内容可完整读回。

实现：`moto.server.ThreadedMotoServer` 起一个**真的 HTTP S3 端点**，
`S3ArtifactStore` 通过 boto3 走完整网络栈（localhost TCP + HTTP + 序列化），
与生产换 endpoint 的差别只是延迟量级。API 调用次数用 botocore 的
`_make_api_call` 钩子精确计数（GetObject / ListObjectsV2 / PutObject 分开）。

依赖：moto[server] + boto3（unit-tests CI 已装；缺了本套件 skip 并说明原因，
不让环境差异变成"假红"）。
"""
from __future__ import annotations

import importlib.util
import os
import socket
import sys
import threading
import time
from pathlib import Path

import logging

ROOT = Path(__file__).resolve().parents[1]
# moto server（werkzeug）默认逐请求打印访问日志，会把测试输出淹掉；
# 断言失败时自己会带计数明细，这里只需要错误级日志。
logging.getLogger("werkzeug").setLevel(logging.ERROR)
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

import specargo.journal as journal                                # noqa: E402

_MOTO_AVAILABLE = importlib.util.find_spec("moto") is not None \
    and importlib.util.find_spec("boto3") is not None

_STAGES = ("extract", "transform", "load")
_DATASET = "dwd_order"


class _ApiCounter:
    """按操作名统计 botocore 的 API 调用次数（等价于对象存储往返次数）。"""

    def __init__(self):
        self.counts: dict[str, int] = {}
        self._lock = threading.Lock()
        self._orig = None
        self._botocore = None

    def __enter__(self):
        import botocore.client
        self._botocore = botocore
        self._orig = botocore.client.BaseClient._make_api_call

        def counted(self_client, operation_name, api_params):
            with self._lock:
                self.counts[operation_name] = self.counts.get(operation_name, 0) + 1
            return self._orig(self_client, operation_name, api_params)

        botocore.client.BaseClient._make_api_call = counted
        return self

    def __exit__(self, *exc):
        self._botocore.client.BaseClient._make_api_call = self._orig
        return False

    def get(self, operation: str) -> int:
        return self.counts.get(operation, 0)


class _Server:
    """起一个本地 HTTP S3 兼容端点（moto server），退出时关掉。"""

    def __init__(self):
        from moto.server import ThreadedMotoServer
        with socket.socket() as s:                       # 找个空闲端口，避免撞车
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self._server = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port)
        self.endpoint = f"http://127.0.0.1:{self.port}"

    def __enter__():
        raise TypeError

    def start(self):
        self._server.start()
        import boto3
        for _ in range(50):                              # 等服务就绪（本地毫秒级）
            try:
                boto3.client("s3", endpoint_url=self.endpoint,
                             aws_access_key_id="t", aws_secret_access_key="t",
                             region_name="us-east-1").list_buckets()
                return self
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("moto server 未就绪")

    def stop(self):
        self._server.stop()


def _point_env_at(server: _Server, bucket: str) -> dict:
    return {
        "OBJSTORE_KIND": "s3",
        "OBJSTORE_ENDPOINT": server.endpoint,
        "OBJSTORE_BUCKET": bucket,
        "OBJSTORE_ACCESS_KEY": "test-access-key",
        "OBJSTORE_SECRET_KEY": "test-secret-key",
        "OBJSTORE_REGION": "us-east-1",
    }


def _make_bucket(endpoint: str, bucket: str) -> None:
    import boto3
    boto3.client("s3", endpoint_url=endpoint,
                 aws_access_key_id="test-access-key",
                 aws_secret_access_key="test-secret-key",
                 region_name="us-east-1").create_bucket(Bucket=bucket)


def _keys(n: int) -> list[str]:
    """n 个互不相同的分区键（YYYY-MM-DD 形态，与真实日期分区一致）。"""
    return [f"2026-{i // 28 + 1:02d}-{i % 28 + 1:02d}" for i in range(n)]


def _items(n: int) -> list[tuple[str, str, str]]:
    return [(_DATASET, key, "") for key in _keys(n)]


def _seed(server: _Server, bucket: str, run_id: str, n: int,
          stages=_STAGES, dataset: str = _DATASET) -> None:
    """写 n 个分区 × 每个 stage 的状态 + 分区清单（串行，供规模断言用）。"""
    journal.put_partitions(run_id, [
        {"dataset": dataset, "key": key, "variant": ""} for key in _keys(n)
    ])
    for key in _keys(n):
        for stage in stages:
            journal.put_status(run_id=run_id, dataset=dataset, partition=key,
                               variant="", stage=stage, status="verified")


def _count_objects(server: _Server, bucket: str, prefix: str) -> int:
    import boto3
    client = boto3.client("s3", endpoint_url=server.endpoint,
                          aws_access_key_id="test-access-key",
                          aws_secret_access_key="test-secret-key",
                          region_name="us-east-1")
    total = 0
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        total += len(resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            return total


def _run_case(fn):
    """起服务 → 建 bucket → 跑用例 → 关服务。每个用例独立 run_id。"""
    if not _MOTO_AVAILABLE:
        print(f"  SKIP {fn.__name__}（未安装 moto[server]/boto3，无法起真实 HTTP 端点）")
        return None
    server = _Server().start()
    bucket = "specargo-journal-scale"
    _make_bucket(server.endpoint, bucket)
    run_id = f"r-{fn.__name__}"
    env_backup = dict(os.environ)
    os.environ.update(_point_env_at(server, bucket))
    try:
        fn(server, bucket, run_id)
        print(f"  PASS {fn.__name__}")
        return True
    except AssertionError as exc:
        print(f"  FAIL {fn.__name__}: {exc}")
        return False
    finally:
        os.environ.clear()
        os.environ.update(env_backup)
        server.stop()


# ---------------------------------------------------------------------------
# 用例
# ---------------------------------------------------------------------------

def test_status_object_count_equals_stage_times_partition(server, bucket, run_id):
    """对象数必须严格 = stage 数 × 分区数（+1 分区清单）——规模上限由此公式决定。"""
    n = 40
    _seed(server, bucket, run_id, n)
    status_objects = _count_objects(
        server, bucket, f"runs/{run_id}/{journal.STATUS_ROOT}/")
    assert status_objects == n * len(_STAGES), (
        f"状态对象数 {status_objects} != {n}×{len(_STAGES)}；key 若重复会互相覆盖，"
        f"并发写就会丢状态")
    assert _count_objects(server, bucket, f"runs/{run_id}/{journal.RUN_DIR}/") == 1


def test_final_stage_read_costs_exactly_n_gets(server, bucket, run_id):
    """对账只读末 stage：GET 次数 == N、LIST 0 次（这是 3N→N 优化的硬证据）。"""
    n = 40
    _seed(server, bucket, run_id, n)
    items = _items(n)
    with _ApiCounter() as counter:
        rows = journal.get_statuses_for(run_id, _STAGES[-1], items)
    assert len(rows) == n, len(rows)
    assert counter.get("GetObject") == n, (
        f"末 stage 直读用了 {counter.get('GetObject')} 次 GetObject，预期 {n}"
        f"（多出来的说明在列举或重复读）")
    assert counter.get("ListObjectsV2") == 0, (
        "直读路径不应 LIST：key 能从分区清单直接算出")


def test_full_read_costs_one_list_plus_three_n_gets(server, bucket, run_id):
    """对照：全量读 = LIST 1 次 + 3N 次 GET——证明直读确实省掉 2/3 往返。"""
    n = 30
    _seed(server, bucket, run_id, n)
    with _ApiCounter() as counter:
        rows = journal.load_statuses(run_id)
    assert len(rows) == n * len(_STAGES), len(rows)
    assert counter.get("ListObjectsV2") == 1, counter.counts
    assert counter.get("GetObject") == n * len(_STAGES), counter.counts


def test_concurrent_writes_lose_nothing(server, bucket, run_id):
    """并发写：4 线程各写一批分区，对象数仍 3N、内容可完整读回（key 独立才敢并发）。"""
    n, workers = 40, 4
    items = [f"p{i:04d}" for i in range(n)]
    chunks = [items[i::workers] for i in range(workers)]
    errors: list[Exception] = []

    def write(chunk: list[str]) -> None:
        try:
            for key in chunk:
                for stage in _STAGES:
                    journal.put_status(run_id=run_id, dataset="ds", partition=key,
                                       variant="", stage=stage, status="verified")
        except Exception as exc:                          # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(c,)) for c in chunks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert _count_objects(server, bucket,
                          f"runs/{run_id}/{journal.STATUS_ROOT}/") == n * len(_STAGES)
    # 内容可完整读回：每个分区的末 stage 状态都在且为 verified
    rows = journal.get_statuses_for(run_id, _STAGES[-1], [("ds", k, "") for k in items])
    assert len(rows) == n and all(r["status"] == "verified" for r in rows)


def test_missing_final_status_counts_as_missing(server, bucket, run_id):
    """末 stage 缺状态 → 直读不产出行 → 对账算"漏数"（与全量读路径结论一致）。"""
    n = 20
    _seed(server, bucket, run_id, n)
    items = _items(n)
    rows = journal.get_statuses_for(run_id, _STAGES[-1], items)
    assert len(rows) == n
    # 删掉一个末 stage 状态对象，模拟"跑了但没登记上"
    victim = rows[0]
    store = journal._store()
    key = journal.status_key(run_id, _STAGES[-1], victim["dataset"],
                             victim["partition"], victim["variant"])
    store._s3.delete_object(Bucket=store.bucket, Key=key)
    after = journal.get_statuses_for(run_id, _STAGES[-1], items)
    assert len(after) == n - 1, "缺失的状态对象不应产出行（由对账算漏数）"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and fn.__code__.co_argcount == 3:
            if _run_case(fn) is False:
                failures += 1
    print(f"\n{'状态日志规模/并发测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
