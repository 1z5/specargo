"""Prometheus 指标：进度跟踪、积压、失败率的数据源。

要回答的是运维时的三个问题：
    - 任务走到哪了        -> ``stage_progress`` / ``backlog`` gauge
    - 有没有失败/超时/积压 -> ``stage_runs_total{status}`` / ``stage_duration_seconds``
    - 数据有没有少        -> ``reconcile_missing`` / ``reconcile_not_verified``

输出标准 Prometheus text 格式，可直接被 scrape，也可供 node_exporter textfile 采集。

零依赖实现（不引 prometheus_client）：输出 OpenMetrics/Prometheus text 格式，
既能挂到常驻服务的 ``/metrics``，也能在批处理结束时写成文件供 node_exporter
textfile collector 采集。真实项目可平替为 prometheus_client——语义一致。

指标命名遵循 Prometheus 惯例：``<namespace>_<subsystem>_<name>_<unit>``，
label 只放低基数字段（dataset/stage/status），**绝不放 partition/item_id**（基数爆炸）。
"""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from pathlib import Path


class Counter:
    """只增计数器（失败数、处理数）。"""

    def __init__(self, name: str, doc: str, labels: tuple[str, ...] = ()):
        self.name, self.doc, self.label_names = name, doc, labels
        self._values: dict[tuple, float] = {}
        self._lock = threading.Lock()

    def inc(self, value: float = 1.0, **labels) -> None:
        key = tuple(labels.get(k, "") for k in self.label_names)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.doc}", f"# TYPE {self.name} counter"]
        with self._lock:
            for key, value in sorted(self._values.items()):
                lines.append(f"{self.name}{self._fmt_labels(key)} {value}")
        return lines

    def _fmt_labels(self, key: tuple) -> str:
        if not self.label_names:
            return ""
        inner = ",".join(f'{k}="{v}"' for k, v in zip(self.label_names, key))
        return "{" + inner + "}"


class Gauge:
    """当前水位（队列深度、在途数、进度）。"""

    def __init__(self, name: str, doc: str, labels: tuple[str, ...] = ()):
        self.name, self.doc, self.label_names = name, doc, labels
        self._values: dict[tuple, float] = {}
        self._lock = threading.Lock()

    def set(self, value: float, **labels) -> None:
        key = tuple(labels.get(k, "") for k in self.label_names)
        with self._lock:
            self._values[key] = float(value)

    def inc(self, value: float = 1.0, **labels) -> None:
        key = tuple(labels.get(k, "") for k in self.label_names)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.doc}", f"# TYPE {self.name} gauge"]
        with self._lock:
            for key, value in sorted(self._values.items()):
                lines.append(f"{self.name}{self._fmt_labels(key)} {value}")
        return lines

    _fmt_labels = Counter._fmt_labels


class Histogram:
    """分布观测（耗时）——看 P95/P99，不看均值。"""

    DEFAULT_BUCKETS = (0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600)

    def __init__(self, name: str, doc: str, labels: tuple[str, ...] = (),
                 buckets=None):
        self.name, self.doc, self.label_names = name, doc, labels
        self.buckets = tuple(buckets or self.DEFAULT_BUCKETS)
        self._counts: dict[tuple, dict[float, int]] = {}
        self._sums: dict[tuple, float] = {}
        self._totals: dict[tuple, int] = {}
        self._lock = threading.Lock()

    def observe(self, value: float, **labels) -> None:
        key = tuple(labels.get(k, "") for k in self.label_names)
        with self._lock:
            counts = self._counts.setdefault(key, {b: 0 for b in self.buckets})
            for b in self.buckets:
                if value <= b:
                    counts[b] += 1
            self._sums[key] = self._sums.get(key, 0.0) + value
            self._totals[key] = self._totals.get(key, 0) + 1

    @contextmanager
    def time(self, **labels):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.observe(time.perf_counter() - start, **labels)

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.doc}", f"# TYPE {self.name} histogram"]
        with self._lock:
            for key in sorted(self._counts):
                label_str = self._fmt_labels(key)
                cumulative = 0
                for b in self.buckets:
                    cumulative += self._counts[key][b]
                    lines.append(f'{self.name}_bucket{{le="{b}"{self._label_inner(key)}}} {cumulative}')
                lines.append(f'{self.name}_bucket{{le="+Inf"{self._label_inner(key)}}} {self._totals[key]}')
                lines.append(f"{self.name}_sum{label_str} {self._sums[key]}")
                lines.append(f"{self.name}_count{label_str} {self._totals[key]}")
        return lines

    def _label_inner(self, key: tuple) -> str:
        if not self.label_names:
            return ""
        return "," + ",".join(f'{k}="{v}"' for k, v in zip(self.label_names, key))

    _fmt_labels = Counter._fmt_labels


# ---------------------------------------------------------------------------
# 指标注册表（单例）：全项目共用一套指标名
# ---------------------------------------------------------------------------

class Metrics:
    def __init__(self, namespace: str = "pipeline"):
        self.ns = namespace
        self.stage_runs = Counter(f"{namespace}_stage_runs_total",
                                  "stage 执行次数（按状态）", ("stage", "status"))
        self.stage_duration = Histogram(f"{namespace}_stage_duration_seconds",
                                        "stage 执行耗时分布", ("stage",))
        self.stage_progress = Gauge(f"{namespace}_stage_progress",
                                    "stage 进度 current/total", ("stage", "dataset"))
        self.backlog = Gauge(f"{namespace}_backlog",
                             "待处理积压量（未成功的 partition 数）", ("stage",))
        self.check_results = Counter(f"{namespace}_check_results_total",
                                     "质量检查结果", ("check", "result"))
        self.reconcile_missing = Gauge(f"{namespace}_reconcile_missing",
                                       "对账发现的缺失 partition 数", ("dataset",))
        self.reconcile_not_verified = Gauge(f"{namespace}_reconcile_not_verified",
                                            "对账发现的未过检 partition 数", ("dataset",))
        self.inflight = Gauge(f"{namespace}_inflight", "在途任务数", ("stage",))
        self._all = [self.stage_runs, self.stage_duration, self.stage_progress,
                     self.backlog, self.check_results, self.reconcile_missing,
                     self.reconcile_not_verified, self.inflight]

    def render(self) -> str:
        lines: list[str] = []
        for m in self._all:
            lines.extend(m.render())
        return "\n".join(lines) + "\n"

    def write_file(self, path: str | Path) -> Path:
        """写成 Prometheus textfile 格式（供 node_exporter textfile collector 采集）。"""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Prometheus textfile 要求先写临时文件再原子改名，避免采集到半截
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(self.render(), encoding="utf-8")
        tmp.replace(p)
        return p


# 全局默认实例。
# ⚠️ 不能叫 ``metrics``：模块名也是 metrics，``import metrics as m; m.inflight``
# 会解析到模块而不是实例（这个坑在 service.py 联调里真实踩到过）。
METRICS = Metrics()


def render() -> str:
    return METRICS.render()
