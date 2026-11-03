"""集群侧状态日志（对象存储）：跨 Pod 的无共享盘状态登记与对账。

为什么不用 SQLite：Argo 每个 task 是独立 Pod，多 Pod 写同一 SQLite（RWX/NFS）
锁不可靠；对象存储是集群里唯一天然共享、按 key 原子写的存储。

每个 (dataset, partition, variant, stage) 写一个 JSON（key 含内容哈希，重复写幂等）::

    runs/<run_id>/_status/<stage>/<sha256(dataset|partition|variant)[:16]>.json

分区清单（discover 类型管线在运行期才知道）::

    runs/<run_id>/_run/partitions.json

onExit 时列举状态前缀、读取全部状态，再用 ``state.evaluate_items`` 做与本地
runner 完全一致的对账——判定逻辑只有一份。
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .objstore import _store

STATUS_ROOT = "_status"
RUN_DIR = "_run"
PARTITIONS_NAME = "partitions.json"


def _check_component(name: str, value: str) -> None:
    """journal 层校验 key 组件：不依赖 store 兜底，且避免 CLI 直调时写出怪 key。"""
    if not isinstance(value, str) or value in ("", ".", "..") or "/" in value:
        raise ValueError(f"非法 journal {name}: {value!r}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def status_key(run_id: str, stage: str, dataset: str, partition: str,
               variant: str = "") -> str:
    _check_component("run_id", run_id)
    _check_component("stage", stage)
    _check_component("dataset", dataset)
    _check_component("partition", partition)
    if variant:
        _check_component("variant", variant)
    digest = hashlib.sha256(
        f"{dataset}\x00{partition}\x00{variant}".encode("utf-8")).hexdigest()[:16]
    return f"runs/{run_id}/{STATUS_ROOT}/{stage}/{digest}.json"


def partitions_key(run_id: str) -> str:
    _check_component("run_id", run_id)
    return f"runs/{run_id}/{RUN_DIR}/{PARTITIONS_NAME}"


def _write_temp(payload) -> Path:
    fd, name = tempfile.mkstemp(suffix=".json")
    os.close(fd)            # mkstemp 返回的 fd 必须显式关闭，否则大批量状态会泄漏 fd
    path = Path(name)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _read_temp(key: str) -> dict:
    fd, name = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    path = Path(name)
    try:
        _store().get(key, path)
        return json.loads(path.read_text(encoding="utf-8"))
    finally:
        path.unlink(missing_ok=True)


def put_status(*, run_id: str, dataset: str, partition: str, variant: str,
               stage: str, status: str, attempt: int | None = None) -> dict:
    """登记一个 item 的 stage 终态（每个 key 独立，天然并发安全）。"""
    payload = {
        "run_id": run_id,
        "dataset": dataset,
        "partition": partition,
        "variant": variant,
        "stage": stage,
        "status": status,
        "attempt": attempt,
        "updated_at": _now(),
    }
    path = _write_temp(payload)
    try:
        _store().put(path, status_key(run_id, stage, dataset, partition, variant))
    finally:
        path.unlink(missing_ok=True)
    return payload


def get_item_status(run_id: str, *, dataset: str, partition: str, variant: str,
                    stage: str) -> dict | None:
    """读单个 (dataset, partition, variant, stage) 的最新状态；不存在返回 None。

    集群 resume 用：stage 启动时只查自己这一个 key，不必列举整个 run 的
    状态前缀（千级分区时列举是 O(N) 读，单点读是 O(1)）。
    """
    key = status_key(run_id, stage, dataset, partition, variant)
    store = _store()
    if not store.exists(key):
        return None
    return _read_temp(key)


def load_statuses(run_id: str) -> list[dict]:
    """读取某次 run 的全部状态记录（onExit 对账用）。"""
    store = _store()
    prefix = f"runs/{run_id}/{STATUS_ROOT}/"
    rows: list[dict] = []
    for key in store.list(prefix):
        try:
            rows.append(_read_temp(key))
        except Exception as exc:                     # 单条坏记录不应让整批对账崩掉
            print(f"[journal] 状态记录损坏（按缺失处理）: {key}: {exc!r}",
                  file=sys.stderr)
            rows.append({"dataset": "", "partition": "", "variant": "",
                         "stage": "", "status": "corrupt", "detail": repr(exc)})
    return rows


def get_statuses_for(run_id: str, stage: str,
                     items) -> list[dict]:
    """按分区清单直接算 key，只读某个 stage 的状态（onExit 对账专用）。

    为什么不复用 ``load_statuses``：对账只关心**末 stage**的状态，而
    ``load_statuses`` 会把 3×N 个状态全读一遍。S3 上每次 GET 都是网络往返，
    N=1000 时 O(3N) vs O(N) 差 3 倍；而且 key 能从分区清单直接算出，
    连 LIST 操作都可以省掉。

    语义与 load_statuses + 按 final_stage 过滤**等价**（evaluate_items 只看
    expected 列表里的条目）。关键细节：读不到/读坏的 key **不产生行**——
    这样 evaluate_items 才会把它算进"漏数"而不是"未过检"（两者是不同故障：
    前者是没跑，后者是跑了没过检，告警文案与处理方式都不同）。
    """
    rows: list[dict] = []
    for dataset, partition, variant in items:
        key = status_key(run_id, stage, dataset, partition, variant)
        try:
            rows.append(_read_temp(key))
        except FileNotFoundError:
            continue                            # 无状态对象 → 让对账算"漏数"
        except Exception as exc:               # 单条坏记录不应让整批对账崩掉
            print(f"[journal] 状态记录损坏（按缺失处理）: {key}: {exc!r}",
                  file=sys.stderr)
            continue
    return rows


def put_partitions(run_id: str, partitions: list[dict]) -> None:
    path = _write_temp(partitions)
    try:
        _store().put(path, partitions_key(run_id))
    finally:
        path.unlink(missing_ok=True)


def get_partitions(run_id: str) -> list[dict]:
    return _read_temp(partitions_key(run_id))
