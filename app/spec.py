"""声明式管线定义：把"编排"从代码里搬进 YAML。

原实现的三个领域耦合点（普适性障碍）：
    1. ``state.py`` 的 item key 硬编码为 (book_slug, chapter_num, voice)
    2. ``pipeline.py`` 的编排写死成 opening/各章/closing 四段
    3. ``verify_audio.py`` 的质量判定是 ASR 专用

框架化之后：
    - 一次运行 = 若干 **stage** × 若干 **partition**，全部由 YAML 声明
    - stage 之间用输入输出契约连接（对应 Argo 的 parameters/artifacts）
    - 质量检查是**插件**（``checks.py`` 注册表），TTS 的 ASR 三态判定只是其中一个
    - 同一个引擎可以跑 TTS 生成、ETL、抽帧质检——见 ``pipelines/`` 下两个实例

YAML 形态::

    name: tts
    partitions: {from: list, items: ["book_a", "book_b"]}
    variants: ["cedar"]
    stages:
      - name: generate
        command: "python main.py production {{dataset}}/{{partition}}.json --voice {{variant}}"
        inputs: [dataset, partition, variant]
        outputs: [audio]
        retry: {policy: OnTransientError, limit: 3, backoff: {duration: 1m, factor: 2}}
        timeout: 30m
      - name: verify
        command: "python verify_audio.py output/{{partition}} --state-db {{state_db}}"
        dependencies: [generate]
        checks: [asr_three_state]
        retry: {limit: 0}          # 数据问题不重试
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
        return cls(
            limit=int(d.get("limit", 0)),
            policy=d.get("policy", "OnFailure"),
            duration=str(backoff.get("duration", "1m")),
            factor=int(backoff.get("factor", 2)),
            max_duration=str(backoff.get("max_duration", "10m")),
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
    artifacts: dict[str, str] = field(default_factory=dict)   # name -> 路径模板
    check_config: dict = field(default_factory=dict)          # 检查名 -> 配置


@dataclass
class Partition:
    dataset: str
    key: str                          # 分区键：章节号 / 日期 / 文件名
    variant: str = ""                 # 变体：音色 / 版本 / 环境

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


# ---------------------------------------------------------------------------
# 模板渲染：{{name}} / {{partition.key}} / {{params.xxx}}
# ---------------------------------------------------------------------------

_VAR = re.compile(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}")


class SpecError(ValueError):
    """管线定义或渲染错误（未知变量、缺参数等）。"""


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


def load_spec(path: str | Path, overrides: dict | None = None) -> PipelineSpec:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    params = {**(raw.get("params") or {}), **(overrides or {})}

    # 分区来源：list（显式） / range（1..N，适合章节）/ discover（由命令产出）
    part_cfg = raw.get("partitions", {}) or {}
    partitions: list[Partition] = []
    source = part_cfg.get("from", "list")
    variants = [str(v) for v in (raw.get("variants") or [""])]

    if source == "list":
        for item in part_cfg.get("items", []):
            if isinstance(item, dict):
                for variant in variants:
                    partitions.append(Partition(item.get("dataset", ""),
                                                str(item.get("key", "")), variant))
            else:
                for variant in variants:
                    partitions.append(Partition(str(item), str(item), variant))
    elif source == "range":
        for key in range(int(part_cfg.get("start", 1)), int(part_cfg.get("end", 1)) + 1):
            for variant in variants:
                partitions.append(Partition(str(part_cfg.get("dataset", "")), str(key), variant))
    elif source == "discover":
        # 由 discover 命令产出分区清单（对应 Argo 的动态工作流生成）
        import subprocess
        cmd = part_cfg.get("command", "")
        if cmd:
            out = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True).stdout
            for line in out.split():
                for variant in variants:
                    partitions.append(Partition(str(part_cfg.get("dataset", "")), line, variant))
    else:
        raise ValueError(f"未知 partitions.from: {source}")

    stages = [
        Stage(
            name=s["name"],
            command=s["command"],
            inputs=list(s.get("inputs", [])),
            outputs=list(s.get("outputs", [])),
            dependencies=list(s.get("dependencies", [])),
            retry=RetryPolicy.from_dict(s.get("retry")),
            timeout=str(s.get("timeout", "30m")),
            checks=list(s.get("checks", [])),
            when=str(s.get("when", "")),
            artifacts=dict(s.get("artifacts", {}) or {}),
            check_config=dict(s.get("check_config", {}) or {}),
        )
        for s in raw.get("stages", [])
    ]

    spec = PipelineSpec(
        name=raw.get("name", Path(path).stem),
        stages=stages,
        partitions=partitions,
        description=raw.get("description", ""),
        params=params,
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

    return spec
