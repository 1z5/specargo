"""声明式管线定义：把"编排"从代码里搬进 YAML。

设计目标：一次运行 = 若干 **stage** × 若干 **partition**，全部由 YAML 声明，
同一个执行引擎既能本地跑、也能编译成 Argo Workflows CRD 上集群。

    - stage 之间用输入输出契约连接（对应 Argo 的 parameters/artifacts）
    - 质量检查是插件（``checks.py`` 注册表 + spec ``plugins`` 显式加载）
    - 领域知识不进框架：领域示例见 ``examples/``

YAML 形态::

    name: etl-demo
    description: 抽取 → 转换 → 装载
    params:
      python: python3
      state_db: ""
    partitions:
      from: discover
      dataset: dwd_order
      command: "printf '%s\\n' 1 2 3"
    stages:
      - name: extract
        command: "{{params.python}} {{params.app_dir}}/etl_steps.py extract --partition {{partition.key}} --out {{params.work_dir}}/extract/{{partition.key}}.jsonl"
        outputs: [raw]
        artifacts:
          raw: "{{params.work_dir}}/extract/{{partition.key}}.jsonl"
        checks: [file_nonempty]
        retry: {policy: OnTransientError, limit: 2}
        timeout: 10m
      - name: load
        dependencies: [extract]
        consumes: [raw]
        command: "..."
        timeout: 10m
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

@dataclass
class RetryPolicy:
    limit: int = 0
    policy: str = "OnFailure"          # Always / OnFailure / OnError / OnTransientError
    duration: str = "1m"
    factor: int = 2
    max_duration: str = "10m"

    @classmethod
    def from_dict(cls, d: dict | None) -> "RetryPolicy":
        d = d or {}
        backoff = d.get("backoff", {}) or {}
        if not isinstance(backoff, dict):
            raise ValueError("retry.backoff 必须是映射")

        def _as_int(value, name: str) -> int:
            # 2.0 这类"整数值的浮点"可以接受；2.5 必须拒绝——int() 静默截断会
            # 让用户以为配了 2.5 次重试，实际只重试 2 次。
            if isinstance(value, float) and value.is_integer():
                value = int(value)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} 必须是整数，实际 {value!r}")
            return value

        def _as_str(value, name: str) -> str:
            if not isinstance(value, str):
                raise ValueError(f"{name} 必须是字符串，实际 {value!r}")
            return value

        return cls(
            limit=_as_int(d.get("limit", 0), "retry.limit"),
            policy=_as_str(d.get("policy", "OnFailure"), "retry.policy"),
            duration=_as_str(backoff.get("duration", "1m"), "retry.backoff.duration"),
            factor=_as_int(backoff.get("factor", 2), "retry.backoff.factor"),
            max_duration=_as_str(backoff.get("max_duration", "10m"),
                                 "retry.backoff.max_duration"),
        )


@dataclass
class Stage:
    name: str
    command: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: str = "30m"
    checks: list[str] = field(default_factory=list)
    when: str = ""                    # 条件表达式（对应 Argo 的 when）
    artifacts: dict[str, str] = field(default_factory=dict)   # name -> 路径模板（本 stage 产出）
    consumes: list[str] = field(default_factory=list)         # 消费的上游 artifact 名
    check_config: dict = field(default_factory=dict)          # 检查名 -> 配置
    resources: dict = field(default_factory=dict)             # K8s requests/limits


@dataclass
class Partition:
    dataset: str
    key: str                          # 分区键：日期 / 分片号 / 文件名
    variant: str = ""                 # 变体：版本 / 环境 / 模型配置

    @property
    def item_key(self) -> tuple[str, str, str]:
        return (self.dataset, self.key, self.variant)


@dataclass
class PipelineSpec:
    name: str
    stages: list[Stage]
    partitions: list[Partition] = field(default_factory=list)
    description: str = ""
    # 批次级参数（可被 CLI / Argo arguments 覆盖）
    params: dict = field(default_factory=dict)
    # 执行目标运行时配置（spec2argo 读取；本地 runner 忽略）。
    # ``k8s`` 是旧名，仍作为别名接受；两者同时非空会报错（见 runtime.py）。
    runtime: dict = field(default_factory=dict)
    k8s: dict = field(default_factory=dict)
    # 分区来源：list / range / discover（discover 时由 discover_command 产出）
    partitions_source: str = "list"
    discover_command: str = ""
    # 显式插件模块（领域能力通过装饰器注册；框架不反向 import 领域代码）
    plugins: list[str] = field(default_factory=list)
    # spec 文件路径（插件加载时用于定位同目录/父目录模块）
    source_path: str = ""


# ---------------------------------------------------------------------------
# 模板渲染：{{name}} / {{partition.key}} / {{params.xxx}}
# ---------------------------------------------------------------------------

_VAR = re.compile(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}")

# 允许的字段集合：拼错/写错键必须报错，不能静默丢弃（静默丢弃会让整条管线"假绿"）。
_ALLOWED_TOP = {"name", "description", "params", "partitions", "variants",
                "stages", "k8s", "runtime", "plugins"}
_ALLOWED_STAGE = {
    "name", "command", "inputs", "outputs", "dependencies", "retry", "timeout",
    "checks", "when", "artifacts", "consumes", "check_config", "resources",
}
_ALLOWED_PARTITIONS = {"from", "items", "start", "end", "dataset", "command"}
_RETRY_POLICIES = {"Always", "OnFailure", "OnError", "OnTransientError"}
# 时长必须是"数字+单位"的组合（30m / 1h / 45s / 2h30m）；'30x' 这类拼错不能被
# parse_duration 宽松放过（它会把无法识别的字符直接忽略，返回 30）。
_DURATION_RE = re.compile(r"^(?:\d+(?:\.\d+)?[smhd])+$")
# Argo/K8s 的资源名约束（workflow 名直接用 spec.name；63 是 Pod 名派生上限的安全值）
_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_MAX_NAME_LEN = 63
# 参数名：YAML 模板里用 {{params.x}} 引用，必须是标识符（否则编译期无法翻译）。
_PARAM_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# 检查名 / artifact 名会进入编译产物里的 shell 命令与对象存储 key：限制安全字符集
_CHECK_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")
_ARTIFACT_NAME_RE = re.compile(r"^(?!\.+$)[A-Za-z0-9_.-]+$")
# 分区键/数据集/变体会进入文件路径与对象存储 key：只允许 Unicode 字母/数字/_.-
# （中文分区键可用；引号/空格/$ 等 shell 元字符会破坏脚本与 key，拒绝）。
# 额外排除纯 "." / ".."（作为路径段可穿越；本地对象存储会拒绝而 S3 不会，必须统一）。
_SAFE_KEY_RE = re.compile(r"^(?!\.+$)[\w.-]+$")
# discover 命令在加载期执行：必须有超时，否则一个挂住的命令会卡死 load_spec。
_DISCOVER_TIMEOUT_SECONDS = 60


class SpecError(ValueError):
    """管线定义或渲染错误（未知变量、缺参数、结构非法等）。"""


def render(template: str, context: dict, *, strict: bool = True) -> str:
    """把 ``{{a.b}}`` 替换为 context 里的值（支持点号路径）。

    strict=True 时，未定义变量抛 SpecError 而不是静默替换成空串——
    静默空串会让 ``--run-id `` 这种残缺命令照常执行，是最难查的一类错。
    """
    def lookup(path: str):
        cur = context
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    missing: list[str] = []

    def repl(m: re.Match) -> str:
        value = lookup(m.group(1))
        if value is None:
            if strict:
                missing.append(m.group(1))
                return m.group(0)
            return ""
        return str(value)

    out = _VAR.sub(repl, template)
    if missing:
        raise SpecError(f"模板引用了未定义变量: {sorted(set(missing))} | 模板: {template[:120]}")
    return out


def build_context(spec: PipelineSpec, partition: Partition, params: dict) -> dict:
    return {
        "pipeline": spec.name,
        "partition": {"dataset": partition.dataset, "key": partition.key,
                      "variant": partition.variant},
        "params": params,
    }


# ---------------------------------------------------------------------------
# YAML 加载
# ---------------------------------------------------------------------------

def parse_duration(text: str) -> float:
    """'30m' / '1h' / '45s' / '2h30m' -> 秒。"""
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    total, num = 0.0, ""
    for ch in str(text):
        if ch.isdigit() or ch == ".":
            num += ch
        elif ch in units and num:
            total += float(num) * units[ch]
            num = ""
    return total or float(num or 0)


def _reject_unknown_keys(d: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(set(d) - allowed)
    if unknown:
        raise SpecError(
            f"{where}: 未知字段 {unknown}（支持: {sorted(allowed)}）。"
            f"拼写错误不会被静默忽略。"
        )


def validate_spec(spec: PipelineSpec, *, path: str | Path = "") -> None:
    """加载后的结构校验：把"配置写错"变成加载期错误，而不是静默跑出假绿。

    覆盖：必填/命名、stage 唯一性、依赖存在性与无环、artifact 闭合、
    时长/重试枚举、checks 目标、未实现的 when 特性显式拒绝。
    这是框架"跑完≠跑对"承诺的前提——框架连"配置错了"都发现不了，
    就谈不上保证结果正确。
    """
    where = str(path) or spec.name
    if not spec.name:
        raise SpecError(f"{where}: name 不能为空")
    if not _NAME_RE.match(spec.name):
        raise SpecError(
            f"{where}: name={spec.name!r} 不是合法的 K8s 资源名"
            f"（只允许小写字母/数字/连字符）"
        )
    if len(spec.name) > _MAX_NAME_LEN:
        raise SpecError(f"{where}: name 长度 {len(spec.name)} 超过 {_MAX_NAME_LEN}")
    for key in spec.params:
        if not isinstance(key, str) or not _PARAM_NAME_RE.match(key):
            raise SpecError(f"{where}: 参数名 {key!r} 非法（只允许字母/数字/下划线）")
    # 执行目标配置校验（runtime/k8s 的字段与类型由 RuntimeConfig 统一校验）
    from runtime import RuntimeConfig                     # 函数内 import：避免循环依赖
    RuntimeConfig.from_spec(spec)

    for module in spec.plugins:
        if not re.match(r"^[A-Za-z_][\w.]*$", module):
            raise SpecError(
                f"{where}: plugins 模块名 {module!r} 非法"
                f"（只允许字母/数字/下划线/点，且不以数字开头）")
    if (spec.runtime or spec.k8s).get("journal") and any(
            not part.dataset for part in spec.partitions):
        raise SpecError(
            f"{where}: journal 要求每个分区的 dataset 非空"
            f"（状态日志 key 需要 dataset 维度）")

    if not spec.stages:
        raise SpecError(f"{where}: stages 不能为空（没有任何 stage 的管线会静默成功）")
    if not spec.partitions:
        raise SpecError(
            f"{where}: partitions 为空——list 模式检查 items/range，"
            f"discover 模式检查命令输出是否有分区"
        )
    seen_partitions: set[tuple[str, str, str]] = set()
    for part in spec.partitions:
        if not part.key or not _SAFE_KEY_RE.match(part.key):
            raise SpecError(
                f"{where}: 分区 key={part.key!r} 含不安全字符"
                f"（只允许 Unicode 字母/数字/下划线/点/连字符；"
                f"key 会进入文件路径与对象存储 key）"
            )
        if part.dataset and not _SAFE_KEY_RE.match(part.dataset):
            raise SpecError(f"{where}: 分区 dataset={part.dataset!r} 含不安全字符")
        if part.variant and not _SAFE_KEY_RE.match(part.variant):
            raise SpecError(f"{where}: 分区 variant={part.variant!r} 含不安全字符")
        item = (part.dataset, part.key, part.variant)
        if item in seen_partitions:
            raise SpecError(f"{where}: 分区重复 {item}（重复扇出会让对账 expected 虚高）")
        seen_partitions.add(item)

    # ---- stage 级校验 ----
    seen: set[str] = set()
    artifact_owner: dict[str, str] = {}
    for idx, stage in enumerate(spec.stages):
        loc = f"{where}: stages[{idx}]({stage.name})"
        if not _NAME_RE.match(stage.name or ""):
            raise SpecError(f"{loc}: stage name={stage.name!r} 不是合法 K8s 名")
        if stage.name in seen:
            raise SpecError(f"{loc}: stage 名重复")
        seen.add(stage.name)
        if not (stage.command or "").strip():
            raise SpecError(f"{loc}: command 不能为空")
        if stage.when.strip():
            raise SpecError(
                f"{loc}: when 条件分支尚未实现（本地 runner 与 Argo 编译器都不会执行它）；"
                f"请改用 dependencies + checks，避免写了不生效"
            )
        for dur_name, dur_value in (("timeout", stage.timeout),
                                    ("retry.backoff.duration", stage.retry.duration),
                                    ("retry.backoff.max_duration", stage.retry.max_duration)):
            if not _DURATION_RE.match(str(dur_value)) or parse_duration(dur_value) <= 0:
                raise SpecError(
                    f"{loc}: {dur_name}={dur_value!r} 不是有效时长"
                    f"（示例: 45s / 30m / 2h30m）"
                )
        if stage.retry.limit < 0:
            raise SpecError(f"{loc}: retry.limit 不能为负")
        if stage.retry.policy not in _RETRY_POLICIES:
            raise SpecError(
                f"{loc}: retry.policy={stage.retry.policy!r} 非法"
                f"（可选: {sorted(_RETRY_POLICIES)}）"
            )
        for name in stage.checks:
            if not _CHECK_NAME_RE.match(name):
                raise SpecError(
                    f"{loc}: 检查名 {name!r} 非法（只允许字母/数字/下划线/连字符，"
                    f"且不以数字开头；名字会进入编译产物的 shell 命令）")
        for name in stage.outputs + stage.consumes + list(stage.artifacts):
            if not _ARTIFACT_NAME_RE.match(str(name)):
                raise SpecError(
                    f"{loc}: artifact 名 {name!r} 非法（只允许字母/数字/_.-；"
                    f"名字会进入对象存储 key 与 shell 命令）")
        missing_artifacts = [o for o in stage.outputs if o not in stage.artifacts]
        if missing_artifacts:
            raise SpecError(
                f"{loc}: outputs {missing_artifacts} 没有对应的 artifacts 路径声明"
            )
        for art, art_path in stage.artifacts.items():
            if not art_path.strip():
                raise SpecError(f"{loc}: artifact {art!r} 的路径不能为空")
            if art in artifact_owner and artifact_owner[art] != stage.name:
                raise SpecError(
                    f"{loc}: artifact 名 {art!r} 与 stage {artifact_owner[art]!r} 重复"
                    f"（artifact 名必须全局唯一，否则集群侧无法接线）"
                )
            artifact_owner[art] = stage.name
        if stage.checks:
            if not stage.outputs:
                raise SpecError(f"{loc}: 配了 checks 但没有 outputs，质量闸门没有可检对象")
            target = stage.outputs[0]
            if target not in stage.artifacts:
                raise SpecError(f"{loc}: checks 的目标 {target!r} 缺少 artifacts 路径")
        unknown_cfg = sorted(set(stage.check_config) - set(stage.checks))
        if unknown_cfg:
            raise SpecError(f"{loc}: check_config 配了未在 checks 里的检查 {unknown_cfg}")

    # ---- 依赖存在性 / 自依赖 / 成环 ----
    stage_names = [s.name for s in spec.stages]
    for stage in spec.stages:
        for dep in stage.dependencies:
            if dep == stage.name:
                raise SpecError(f"{where}: stage {stage.name!r} 依赖了自己")
            if dep not in stage_names:
                raise SpecError(
                    f"{where}: stage {stage.name!r} 依赖不存在的 stage {dep!r}"
                    f"（已定义: {stage_names}）"
                )
    graph = {s.name: list(s.dependencies) for s in spec.stages}
    visiting: set[str] = set()
    done: set[str] = set()

    def _visit(node: str, trail: tuple[str, ...] = ()) -> None:
        if node in done:
            return
        if node in visiting:
            raise SpecError(f"{where}: stage 依赖成环: {' -> '.join(trail + (node,))}")
        visiting.add(node)
        for dep in graph.get(node, []):
            _visit(dep, trail + (node,))
        visiting.discard(node)
        done.add(node)

    for s in spec.stages:
        _visit(s.name)

    # ---- consumes 必须指向别的 stage 产出的 artifact，且生产者必须是上游依赖 ----
    def _ancestors(name: str) -> set[str]:
        out: set[str] = set()
        stack = list(graph.get(name, []))
        while stack:
            dep = stack.pop()
            if dep in out:
                continue
            out.add(dep)
            stack.extend(graph.get(dep, []))
        return out

    for stage in spec.stages:
        ancestors = _ancestors(stage.name)
        for art in stage.consumes:
            owner = artifact_owner.get(art)
            if owner is None:
                raise SpecError(
                    f"{where}: stage {stage.name!r} consumes 了不存在的 artifact {art!r}"
                )
            if owner == stage.name:
                raise SpecError(
                    f"{where}: stage {stage.name!r} consumes 了自己产出的 artifact {art!r}"
                )
            if owner not in ancestors:
                raise SpecError(
                    f"{where}: stage {stage.name!r} consumes 了 {owner!r} 的 artifact {art!r}，"
                    f"但 {owner!r} 不是它的上游依赖（集群侧会在产物生成前就执行）"
                )


def _str_list(value, where: str) -> list[str]:
    """YAML 列表字段必须是字符串列表。

    写错类型（如 ``checks: file_nonempty``）被 ``list()`` 静默拆成字符列表，
    运行期会变成 13 个"未注册检查"——必须在校验期拒绝。
    """
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        raise SpecError(f"{where}: 必须是字符串列表")
    return list(value)


def _str_map(value, where: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or any(
            not isinstance(k, str) or not isinstance(v, str)
            for k, v in value.items()):
        raise SpecError(f"{where}: 必须是 字符串 -> 字符串 的映射")
    return dict(value)


def load_spec(path: str | Path, overrides: dict | None = None) -> PipelineSpec:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw is None:
        raise SpecError(f"{path}: 空文件（没有任何管线定义）")
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: 顶层必须是 YAML 映射，实际是 {type(raw).__name__}")
    _reject_unknown_keys(raw, _ALLOWED_TOP, str(path))

    raw_partitions = raw.get("partitions") or {}
    if not isinstance(raw_partitions, dict):
        raise SpecError(f"{path}: partitions 必须是映射（from/items/range/command）")
    _reject_unknown_keys(raw_partitions, _ALLOWED_PARTITIONS, f"{path}: partitions")
    raw_stages = raw.get("stages") or []
    if not isinstance(raw_stages, list) or not raw_stages:
        raise SpecError(f"{path}: stages 必须是非空列表")
    for i, item in enumerate(raw_stages):
        if not isinstance(item, dict):
            raise SpecError(f"{path}: stages[{i}] 必须是映射")
        _reject_unknown_keys(item, _ALLOWED_STAGE, f"{path}: stages[{i}]")

    raw_params = raw.get("params")
    if raw_params is not None and not isinstance(raw_params, dict):
        raise SpecError(f"{path}: params 必须是映射")
    raw_k8s = raw.get("k8s")
    if raw_k8s is not None and not isinstance(raw_k8s, dict):
        raise SpecError(f"{path}: k8s 必须是映射（image/volumes/mounts 等）")
    raw_plugins = raw.get("plugins")
    if raw_plugins is None:
        plugins: list[str] = []
    else:
        if not isinstance(raw_plugins, list) or any(
                not isinstance(item, str) or not item for item in raw_plugins):
            raise SpecError(f"{path}: plugins 必须是非空字符串列表（模块名，如 my_domain_checks）")
        plugins = list(raw_plugins)
    raw_variants = raw.get("variants")
    if raw_variants is None:
        variants = [""]
    else:
        if not isinstance(raw_variants, list) or not raw_variants:
            raise SpecError(f"{path}: variants 必须是非空列表（单变体写 [\"\"]）")
        if any(not isinstance(v, str) for v in raw_variants):
            raise SpecError(f"{path}: variants 元素必须是字符串")
        variants = list(raw_variants)
    params = {**(raw_params or {}), **(overrides or {})}
    for key in params:
        if not isinstance(key, str) or not _PARAM_NAME_RE.match(key):
            raise SpecError(
                f"{path}: 参数名 {key!r} 非法（只允许字母/数字/下划线，且不以数字开头）"
            )

    # 分区来源：list（显式） / range（1..N，适合日期/序号）/ discover（由命令产出）
    part_cfg = raw_partitions
    partitions: list[Partition] = []
    source = part_cfg.get("from", "list")
    dataset = part_cfg.get("dataset", "")
    if not isinstance(dataset, str):
        raise SpecError(f"{path}: partitions.dataset 必须是字符串")

    if source == "list":
        items = part_cfg.get("items")
        if items is None:
            items = []
        if not isinstance(items, list):
            raise SpecError(f"{path}: partitions.items 必须是列表")
        for idx, item in enumerate(items):
            if isinstance(item, str):
                key, item_ds = item, item
            elif isinstance(item, dict):
                _reject_unknown_keys(item, {"dataset", "key"},
                                     f"{path}: partitions.items[{idx}]")
                key = item.get("key", "")
                item_ds = item.get("dataset", "")
                if not isinstance(key, str) or not isinstance(item_ds, str):
                    raise SpecError(
                        f"{path}: partitions.items[{idx}] 的 key/dataset 必须是字符串")
            else:
                raise SpecError(
                    f"{path}: partitions.items[{idx}] 必须是字符串或 {{dataset, key}} 映射")
            if not key:
                raise SpecError(f"{path}: partitions.items[{idx}] 的 key 不能为空")
            for variant in variants:
                partitions.append(Partition(item_ds, key, variant))
    elif source == "range":
        try:
            start = int(part_cfg.get("start", 1))
            end = int(part_cfg.get("end", 1))
        except (TypeError, ValueError) as exc:
            raise SpecError(f"{path}: range 分区 start/end 必须是整数: {exc}") from exc
        for key in range(start, end + 1):
            for variant in variants:
                partitions.append(Partition(dataset, str(key), variant))
    elif source == "discover":
        # 由 discover 命令产出分区清单（对应 Argo 的动态工作流生成）
        import subprocess
        cmd = part_cfg.get("command", "")
        if not cmd:
            raise SpecError(f"{path}: partitions.from=discover 但缺少 command")
        if not isinstance(cmd, str):
            raise SpecError(f"{path}: partitions.command 必须是字符串（shell 命令）")
        try:
            proc = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                                  timeout=_DISCOVER_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            raise SpecError(
                f"{path}: discover 命令超过 {_DISCOVER_TIMEOUT_SECONDS}s 未返回") from exc
        except Exception as exc:                       # pragma: no cover - 环境异常
            raise SpecError(f"{path}: discover 命令无法执行: {exc!r}") from exc
        if proc.returncode != 0:
            raise SpecError(
                f"{path}: discover 命令失败（exit={proc.returncode}）: "
                f"{(proc.stderr or proc.stdout).strip()[:300]}"
            )
        # 按行解析（与集群 discover 脚本一致）：含空格的 key 会作为一整行被
        # charset 校验拒绝，而不是在加载期被 split() 拆成两个"合法"分区。
        lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
        if not lines:
            raise SpecError(f"{path}: discover 命令没有输出任何分区")
        for line in lines:
            for variant in variants:
                partitions.append(Partition(dataset, line, variant))
    else:
        raise SpecError(f"{path}: 未知 partitions.from={source!r}（可选: list / range / discover）")

    stages: list[Stage] = []
    for i, s in enumerate(raw_stages):
        loc = f"{path}: stages[{i}]"
        for required in ("name", "command"):
            if not s.get(required):
                raise SpecError(f"{loc} 缺少必填字段 {required!r}")
            if not isinstance(s[required], str):
                raise SpecError(f"{loc}.{required} 必须是字符串")
        retry_raw = s.get("retry")
        if retry_raw is not None and not isinstance(retry_raw, dict):
            raise SpecError(f"{loc}.retry 必须是映射")
        try:
            retry = RetryPolicy.from_dict(retry_raw)
        except (TypeError, ValueError) as exc:
            raise SpecError(f"{loc}.retry 非法: {exc}") from exc
        timeout_raw = s.get("timeout", "30m")
        if not isinstance(timeout_raw, str):
            raise SpecError(f"{loc}.timeout 必须是字符串（如 30m）")
        when_raw = s.get("when", "")
        if not isinstance(when_raw, str):
            raise SpecError(f"{loc}.when 必须是字符串")
        check_config = s.get("check_config") or {}
        if not isinstance(check_config, dict):
            raise SpecError(f"{loc}.check_config 必须是映射")
        resources = s.get("resources") or {}
        if not isinstance(resources, dict):
            raise SpecError(f"{loc}.resources 必须是映射")
        stages.append(Stage(
            name=s["name"],
            command=s["command"],
            inputs=_str_list(s.get("inputs"), f"{loc}.inputs"),
            outputs=_str_list(s.get("outputs"), f"{loc}.outputs"),
            dependencies=_str_list(s.get("dependencies"), f"{loc}.dependencies"),
            retry=retry,
            timeout=timeout_raw,
            checks=_str_list(s.get("checks"), f"{loc}.checks"),
            when=when_raw,
            artifacts=_str_map(s.get("artifacts"), f"{loc}.artifacts"),
            consumes=_str_list(s.get("consumes"), f"{loc}.consumes"),
            check_config=dict(check_config),
            resources=dict(resources),
        ))

    name = raw.get("name", Path(path).stem)
    if not isinstance(name, str):
        raise SpecError(f"{path}: name 必须是字符串")
    description = raw.get("description", "")
    if not isinstance(description, str):
        raise SpecError(f"{path}: description 必须是字符串")
    spec = PipelineSpec(
        name=name,
        stages=stages,
        partitions=partitions,
        description=description,
        params=params,
        k8s=dict(raw_k8s or {}),
        runtime=(dict(raw["runtime"]) if isinstance(raw.get("runtime"), dict)
                 else ({} if raw.get("runtime") in (None, "") else raw["runtime"])),
        partitions_source=source,
        discover_command=str(part_cfg.get("command", "")),
        plugins=plugins,
        source_path=str(Path(path).resolve()),
    )

    # 变量校验：YAML 里用到的 {{params.*}} 必须在 params 里显式声明。
    # Runner 注入的 app_dir / run_id 也要求 YAML 声明（值为空即可）——
    # 不用白名单放行，否则"忘声明"会被静默容忍，严格模式就失去意义。
    declared = set(params)
    used: set[str] = set()
    for stage in stages:
        for field in (stage.command, stage.when):
            used |= set(_VAR.findall(field or ""))
        for tmpl in stage.artifacts.values():
            used |= set(_VAR.findall(tmpl))
    param_used = {u.split(".", 1)[1] for u in used if u.startswith("params.")}
    unknown = param_used - declared
    if unknown:
        raise SpecError(
            f"{path}: stages 使用了未声明的参数 {sorted(unknown)}；"
            f"已声明: {sorted(declared)}。请在 params 里补上或由运行方注入。"
        )

    validate_spec(spec, path=path)
    return spec
