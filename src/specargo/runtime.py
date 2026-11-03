"""执行目标运行时配置：编译器不再持有任何"业务默认值"。

配置只描述**执行环境**（镜像 / 挂载 / 对象存储 / deadline / podGC / 插件路径），
不包含业务参数（那是 ``spec.params`` 的事）。

来源优先级（后者按 key 覆盖前者）::

    1. 内置默认值（全部中性、明确、可在文档里逐条列出）
    2. spec 顶层 ``runtime:`` 节（旧名 ``k8s:`` 仍接受，作为别名）
    3. CLI ``--runtime runtime.yaml``（按 key 覆盖，便于同一份 spec 多环境部署）

固定契约（不可配置，属于框架语义而不是环境差异）：
    - run_id 环境变量名 ``PIPELINE_RUN_ID``
    - 对象存储 key 根 ``runs``
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

from .spec import SpecError

# 固定契约：编译产物里注入/引用的 env 名与 key 根（不允许配置覆盖）
RUN_ID_ENV = "PIPELINE_RUN_ID"
OBJSTORE_KEY_ROOT = "runs"

RUNTIME_KEYS = {
    "image", "image_pull_policy", "service_account", "namespace",
    "objstore_endpoint", "objstore_env", "volumes", "mounts",
    "active_deadline_seconds", "pod_gc_strategy", "journal",
    "notify_webhook", "plugin_path", "resume", "parallelism",
}
_STRING_KEYS = ("image", "image_pull_policy", "service_account", "namespace",
                "objstore_endpoint", "pod_gc_strategy", "notify_webhook",
                "plugin_path")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# 预留 env：框架自己注入，不允许 objstore_env 覆盖
RESERVED_ENV = {"PIPELINE_RUN_ID", "OBJSTORE_ENDPOINT", "PIPELINE_PLUGINS",
                "PIPELINE_PLUGIN_PATH", "PIPELINE_WORKFLOW_STATUS",
                "NOTIFY_WEBHOOK"}


@dataclass
class RuntimeConfig:
    """编译期使用的执行环境配置（默认值 = 当前 CI/文档口径）。"""

    image: str = "pipeline-image:latest"
    image_pull_policy: str = "IfNotPresent"
    service_account: str = "default"
    namespace: str = "argo"
    objstore_endpoint: str = "http://object-store.argo.svc.cluster.local:5000"
    objstore_env: dict[str, str] = field(default_factory=dict)
    volumes: dict[str, Any] = field(default_factory=dict)
    mounts: dict[str, str] = field(default_factory=dict)
    active_deadline_seconds: int = 1800
    #: workflow 级并发上限（Argo ``spec.parallelism``）：0 = 不写该字段（Argo 默认不限）。
    #: 扇出管线（N 分区 × M 个 stage/gate）没有它时，controller 会一次性收到 N×M 个 Pod 请求。
    parallelism: int = 0
    pod_gc_strategy: str = "OnWorkflowSuccess"
    journal: bool = False
    #: 集群 resume：stage 启动时查状态日志，已到终态则跳过（不重跑、不重复写产物）
    resume: bool = False
    notify_webhook: str = ""
    plugin_path: str = ""

    # ------------------------------------------------------------------
    # 构造与校验
    # ------------------------------------------------------------------
    @classmethod
    def from_mapping(cls, data: dict | None, *, where: str = "runtime") -> "RuntimeConfig":
        """从映射构造（全量校验）；``data`` 为空时返回默认配置。"""
        cfg = cls()
        cfg.apply_mapping(data or {}, where=where)
        return cfg

    def apply_mapping(self, data: dict, *, where: str = "runtime") -> "RuntimeConfig":
        """按 key 覆盖（只设置出现的键），其余保持现值。"""
        if not isinstance(data, dict):
            raise SpecError(f"{where}: 必须是映射，实际 {type(data).__name__}")
        unknown = sorted(set(data) - RUNTIME_KEYS)
        if unknown:
            raise SpecError(f"{where}: 未知字段 {unknown}（支持: {sorted(RUNTIME_KEYS)}）")

        for key in _STRING_KEYS:
            if key in data and not isinstance(data[key], str):
                raise SpecError(f"{where}.{key} 必须是字符串，实际 {data[key]!r}")
        for bool_key in ("journal", "resume"):
            if bool_key in data and not isinstance(data[bool_key], bool):
                raise SpecError(
                    f"{where}.{bool_key} 必须是布尔值，实际 {data[bool_key]!r}")
        if "active_deadline_seconds" in data and (
                isinstance(data["active_deadline_seconds"], bool)
                or not isinstance(data["active_deadline_seconds"], int)):
            raise SpecError(
                f"{where}.active_deadline_seconds 必须是整数，"
                f"实际 {data['active_deadline_seconds']!r}")
        if "parallelism" in data:
            value = data["parallelism"]
            # bool 是 int 的子类（True 会被当成 1），必须显式拒绝
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise SpecError(
                    f"{where}.parallelism 必须是正整数（并发 Pod 上限；不写 = 不限），"
                    f"实际 {value!r}")
        if "objstore_env" in data:
            obj_env = data["objstore_env"]
            if not isinstance(obj_env, dict):
                raise SpecError(f"{where}.objstore_env 必须是映射（环境变量名 -> 值）")
            for name, value in obj_env.items():
                if not isinstance(name, str) or not _ENV_NAME_RE.match(name):
                    raise SpecError(f"{where}.objstore_env 变量名非法: {name!r}")
                if name in RESERVED_ENV:
                    raise SpecError(
                        f"{where}.objstore_env 不允许覆盖框架保留变量 {name}")
                if not isinstance(value, str):
                    raise SpecError(f"{where}.objstore_env[{name!r}] 必须是字符串")
        if "volumes" in data:
            vols = data["volumes"]
            if not isinstance(vols, dict):
                raise SpecError(f"{where}.volumes 必须是映射（卷名 -> 卷定义）")
            for vol_name, vol_def in vols.items():
                if not isinstance(vol_name, str) or not vol_name.strip():
                    raise SpecError(f"{where}.volumes 卷名非法: {vol_name!r}")
                if not isinstance(vol_def, dict) or not vol_def:
                    raise SpecError(
                        f"{where}.volumes[{vol_name!r}] 必须是**非空**映射"
                        f"（如 {{hostPath: {{path: /data}}}} / {{configMap: {{name: x}}}}）")
                # 编译器会给每个卷注入 name 字段；用户声明里不允许覆盖它
                if "name" in vol_def:
                    raise SpecError(
                        f"{where}.volumes[{vol_name!r}] 不需要（也不允许）写 name，"
                        f"编译器会按卷名注入")
        if "mounts" in data:
            mounts = data["mounts"]
            if not isinstance(mounts, dict) or any(
                    not isinstance(k, str) or not isinstance(v, str)
                    for k, v in mounts.items()):
                raise SpecError(f"{where}.mounts 必须是 字符串 -> 字符串 的映射")

        for key, value in data.items():
            setattr(self, key, value)
        return self

    # ------------------------------------------------------------------
    # 来源：spec / 文件
    # ------------------------------------------------------------------
    @classmethod
    def from_spec(cls, spec) -> "RuntimeConfig":
        """从 spec 读取：``runtime:`` 优先，旧名 ``k8s:`` 作为别名仍可用。"""
        runtime_block = getattr(spec, "runtime", None)
        legacy_block = getattr(spec, "k8s", None)
        if runtime_block and legacy_block:
            raise SpecError(
                "spec 同时包含 runtime 与 k8s 两个执行目标节；k8s 是旧名，"
                "请只保留 runtime（或把 k8s 改名 runtime）")
        for name, block in (("runtime", runtime_block), ("k8s", legacy_block)):
            if block is not None and not isinstance(block, dict):
                raise SpecError(
                    f"spec.{name} 必须是映射，实际 {type(block).__name__}")
        block = runtime_block if runtime_block else (legacy_block or {})
        where = "runtime" if runtime_block else "k8s(旧名)"
        return cls.from_mapping(block, where=where)

    @classmethod
    def load(cls, path: str | Path) -> "RuntimeConfig":
        """从 YAML 文件加载（用于 ``--runtime`` 多环境部署）。"""
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(data, where=str(path))

    # ------------------------------------------------------------------
    # 派生量
    # ------------------------------------------------------------------
    @property
    def volume_mounts(self) -> list[dict]:
        return [{"name": name, "mountPath": path} for name, path in self.mounts.items()]

    def as_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}
