"""spec2argo：把声明式管线定义编译成 Argo Workflows CRD。

解决的问题：此前本地 runner 与集群 workflow 是**两份手写定义**
（``pipelines/etl-demo.yaml`` 与 ``k8s/workflow-etl-smoke.yaml``），
分区数、重试配置、质量闸门写法各自维护，必然漂移。

本模块让同一份 spec 有两个执行目标：

    本地：Runner 按 stage 顺序执行，产物落相对路径
    集群：本编译器生成 Argo CRD，并自动做两件事
          1. 把 ``{{params.work_dir}}`` 换成集群挂载路径（/output）
          2. 按 **artifact 名** 在 stage 之间接线对象存储路径契约——
             上游产出 put，下游消费前 get（替代脆弱的扇出 artifact 引用）

映射关系：

    ==================  ===============================
    PipelineSpec        Argo Workflow
    ==================  ===============================
    stages              dag.tasks（按 dependencies 连边）
    partitions          withItems / withParam 扇出
    partitions=discover discover 任务 + withParam 引用其输出
    retry               retryStrategy（limit/policy/backoff）
    timeout             template timeout
    artifacts           路径契约 put/get（objstore.py）
    checks              独立 gate-<stage> 任务（跑 checks.py 插件）
    ==================  ===============================

用法::

    specargo compile pipelines/etl-demo.yaml -o /tmp/wf.yaml
    argo lint --offline /tmp/wf.yaml

调用形态（包化后）：编译产物用 ``python3 -m specargo.<module>`` 调用框架能力，
不再依赖镜像里存在 ``/app/*.py`` 源文件——镜像只要 ``pip install specargo``。
spec 里的业务命令同样推荐 ``-m your_package.your_module`` 形式。
"""
from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
from pathlib import Path

import yaml

from .resume_policy import stage_terminal_statuses
from .runtime import OBJSTORE_KEY_ROOT, RUN_ID_ENV, RuntimeConfig
from .spec import PipelineSpec, SpecError, Stage, load_spec

# ---------------------------------------------------------------------------
# 表达式翻译：spec 模板 → Argo 模板
# ---------------------------------------------------------------------------

# 集群执行时的"有效参数值"：执行环境约定不交给 spec.params 的字面值。
#   python   -> 容器里的解释器
#   app_dir  -> 容器内代码目录
#   work_dir -> 集群统一挂载点（由 k8s.mounts 决定）
# run_id 由编排器注入（{{workflow.name}}）。
_EFFECTIVE_PARAMS = {
    "python": "python3",
    "app_dir": "/app",
    "work_dir": "/output",
}
_PARAMS_RE = re.compile(r"\{\{\s*params\.([a-zA-Z_]\w*)\s*\}\}")
_PARTITION_RE = re.compile(r"\{\{\s*partition\.(dataset|key|variant)\s*\}\}")
# spec 的 partition.key 对应 Argo 模板输入参数名 partition（dataset/variant 同名）
_PARTITION_PARAM = {"key": "partition", "dataset": "dataset", "variant": "variant"}


def _param_str(spec: PipelineSpec, name: str) -> str:
    """spec 参数 -> 集群侧字符串值（None 归一为空串，与 workflow arguments 一致）。"""
    if name in _EFFECTIVE_PARAMS:
        return _EFFECTIVE_PARAMS[name]
    value = spec.params.get(name, "")
    return "" if value is None else str(value)


def translate(expr: str, *, in_fanout: bool = True) -> str:
    """把 spec 的 ``{{...}}`` 表达式翻成 Argo 的模板语法。

    ``{{partition.key}}``      -> ``{{inputs.parameters.partition}}``
    ``{{partition.variant}}``  -> ``{{inputs.parameters.variant}}``
    ``{{partition.dataset}}``  -> ``{{inputs.parameters.dataset}}``
    ``{{params.run_id}}``      -> ``{{workflow.name}}``
    ``{{params.app_dir}}``     -> ``/app``（``python`` -> ``python3``，``work_dir`` -> ``/output``）
    其余 ``{{params.x}}``      -> ``{{workflow.parameters.x}}``（编译进 workflow arguments）
    """
    def _params(m: re.Match) -> str:
        name = m.group(1)
        if name == "run_id":
            return "{{workflow.name}}"
        if name in _EFFECTIVE_PARAMS:
            return _EFFECTIVE_PARAMS[name]
        return "{{workflow.parameters.%s}}" % name

    out = _PARAMS_RE.sub(_params, expr)
    out = _PARTITION_RE.sub(
        lambda m: "{{inputs.parameters.%s}}" % _PARTITION_PARAM[m.group(1)], out)
    return out


def _workflow_arguments(spec: PipelineSpec) -> list[dict]:
    """所有 spec.params 编译成 workflow arguments（可在 submit 时用 -p 覆盖）。"""
    args: list[dict] = []
    for name in spec.params:
        name = str(name)
        if name == "run_id":            # 由编排器注入 workflow.name，不做参数
            continue
        args.append({"name": name, "value": _param_str(spec, name)})
    return args


def _partition_items(spec: PipelineSpec) -> list[dict]:
    return [
        {"key": p.key, "dataset": p.dataset, "variant": p.variant}
        for p in spec.partitions
    ]


# ---------------------------------------------------------------------------
# 代码片段构造
# ---------------------------------------------------------------------------

def _artifact_key(artifact_name: str) -> str:
    """对象存储路径契约：``runs/$RUN_ID/<partition>/<artifact>.<ext>``。

    上下游只约定 key 怎么算，不依赖编排器的 artifact 配对行为；
    run_id 用容器环境变量展开（由编译产物注入，不硬编码值）。
    """
    return '%s/$%s/{{inputs.parameters.partition}}/%s' % (
        OBJSTORE_KEY_ROOT, RUN_ID_ENV, _ext(artifact_name))


def _objstore_put_snippet(artifacts: dict[str, str]) -> str:
    """上游 stage：把声明产物按路径契约 put 到对象存储。"""
    if not artifacts:
        return ""
    lines = ["# 路径契约：上游只负责把产物写到约定 key，不依赖编排器的 artifact 配对"]
    for name, path in artifacts.items():
        lines.append(
            'python3 -m specargo.objstore put --key "%s" --file %s'
            % (_artifact_key(name), shlex.quote(translate(path)))
        )
    return "\n".join(lines)


def _objstore_get_snippet(artifacts: dict[str, str]) -> str:
    """下游 stage：消费前先从对象存储 get（跨 Pod 取数）。"""
    if not artifacts:
        return ""
    lines = ["# 路径契约：从上游产物取数（经对象存储，与节点无关）"]
    for name, path in artifacts.items():
        lines.append(
            'python3 -m specargo.objstore get --key "%s" --file %s'
            % (_artifact_key(name), shlex.quote(translate(path)))
        )
    return "\n".join(lines)


def _discover_env(spec: PipelineSpec, runtime: RuntimeConfig,
                  dataset: str, variants: list[str],
                  *, journal: bool = False) -> list[dict]:
    env = [
        {"name": "SPEC_DATASET", "value": dataset},
        {"name": "SPEC_VARIANTS",
         "value": json.dumps(variants or [""], ensure_ascii=False)},
    ]
    if journal:
        env = _base_env(spec, runtime) + env
    return env


def _discover_script(cmd: str, dataset: str, variants: list[str],
                     *, journal: bool = False) -> str:
    """把用户的发现命令包一层，产出 Argo withParam 需要的 JSON 数组。

    输出元素是 ``{key, dataset, variant}`` 对象，而不是裸字符串——
    否则集群侧 ``{{partition.dataset}}`` / ``{{partition.variant}}`` 都渲染成空串，
    "同一 spec 本地跑 4 个 (dataset×variant) 分区、集群只跑 4 个没有维度的 key"
    这种漂移就发生了（本地 runner 与集群语义必须一致）。

    dataset / variants 由编译期注入环境变量，避免把引号拼进 shell。
    """
    # discover 的输出是 withParam 的元素来源，必须同时满足 Argo 的 JSON 契约与
    # 本框架的安全字符集（key 会进入路径/对象 key）：非法值直接失败，不要带病扇出。
    return (
        "set -e\n"
        "mkdir -p /tmp/out\n"
        + cmd.strip() + " > /tmp/out/found.txt\n"
        "python3 - <<'PY' > /tmp/out/items.json\n"
        "import json, os, re, sys\n"
        "lines = [l.strip() for l in open('/tmp/out/found.txt', encoding='utf-8') "
        "if l.strip()]\n"
        "dataset = os.environ.get('SPEC_DATASET', '')\n"
        "variants = json.loads(os.environ.get('SPEC_VARIANTS', '[\"\"]')) or ['']\n"
        "bad = [k for k in lines if not re.fullmatch(r'[\\w.-]+', k)]\n"
        "if bad:\n"
        "    print(f'discover 输出含非法分区 key（只允许字母/数字/_.-）: {bad[:5]}', "
        "file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "items = [{'key': k, 'dataset': dataset, 'variant': v}\n"
        "         for k in lines for v in variants]\n"
        "print(json.dumps(items, ensure_ascii=False))\n"
        "PY\n"
        + ('python3 -m specargo.objstore put --key '
           '"%s/$%s/_run/partitions.json" --file /tmp/out/items.json\n'
           % (OBJSTORE_KEY_ROOT, RUN_ID_ENV)
           if journal else '')
    )


def _ext(artifact_name: str) -> str:
    """artifact 名 -> 文件名（raw -> raw.jsonl）。"""
    return artifact_name if "." in artifact_name else artifact_name + ".jsonl"


def _resolve_for_gate(value, *, spec: PipelineSpec, artifact_path: str):
    """把 gate 配置里的 spec 模板解析成集群侧可直接使用的值。

    gate 是独立 Pod，没有 runner 的 context。策略：
    - ``{{params.x}}``  用编译期有效值替换成字面量（配置是静态的，无需运行时覆盖）
    - ``{{artifact}}``  替换成本 Pod 里要检查的本地路径
    - ``{{partition.*}}`` 保留并翻译成 Argo 模板（由 withParam 展开）
    """
    if isinstance(value, dict):
        return {k: _resolve_for_gate(v, spec=spec, artifact_path=artifact_path)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_for_gate(v, spec=spec, artifact_path=artifact_path)
                for v in value]
    if isinstance(value, str):
        def _param(m: re.Match) -> str:
            name = m.group(1)
            if name == "run_id":
                return "{{workflow.name}}"
            return _param_str(spec, name)

        text = _PARAMS_RE.sub(_param, value)
        text = text.replace("{{artifact}}", artifact_path)
        return translate(text)
    return value


def _base_env(spec: PipelineSpec, runtime: RuntimeConfig) -> list[dict]:
    """所有集群容器共用：run_id / 对象存储端点 / 用户自定义对象存储 env。"""
    env = [
        {"name": RUN_ID_ENV, "value": "{{workflow.name}}"},
        {"name": "OBJSTORE_ENDPOINT", "value": runtime.objstore_endpoint},
    ]
    for name, value in runtime.objstore_env.items():
        env.append({"name": str(name), "value": str(value)})
    return env


def _plugin_env(spec: PipelineSpec, runtime: RuntimeConfig) -> list[dict]:
    """领域插件声明注入容器 env（本地 runner 与集群 gate 同源加载）。"""
    if not spec.plugins:
        return []
    env = [{"name": "PIPELINE_PLUGINS",
            "value": json.dumps(spec.plugins, ensure_ascii=False)}]
    plugin_path = runtime.plugin_path
    if plugin_path:
        env.append({"name": "PIPELINE_PLUGIN_PATH", "value": str(plugin_path)})
    return env


def _resume_skip_snippet(stage: Stage) -> str:
    """集群 resume：stage 启动前查状态日志，已到成功终态则整体跳过。

    为什么在 stage 命令里而不是 DAG ``when``：Argo 的 ``when`` 只能引用参数，
    拿不到"上一轮这个分区跑到哪"——状态在对象存储里，只能由 task 自己查。
    退出码语义（见 pipeline_cli.state check-skip）：0=已终态应跳过，1=需执行。
    """
    return (
        'python3 -m specargo.pipeline_cli state check-skip '
        '--run-id "$%s" --dataset "{{inputs.parameters.dataset}}" '
        '--partition "{{inputs.parameters.partition}}" '
        '--variant "{{inputs.parameters.variant}}" --stage %s%s '
        '&& { echo "[resume] %s 已完成，跳过"; exit 0; }'
        % (RUN_ID_ENV, shlex.quote(stage.name),
           " --has-checks" if stage.checks else "",
           stage.name)
    )


def _state_put_snippet(stage_name: str, status: str) -> str:
    """stage/gate 结束后把 item 终态写入对象存储状态日志（集群跨 Pod）。"""
    return (
        'python3 -m specargo.pipeline_cli state put-remote --run-id "$%s" ' % RUN_ID_ENV +
        '--dataset "{{inputs.parameters.dataset}}" '
        '--partition "{{inputs.parameters.partition}}" '
        '--variant "{{inputs.parameters.variant}}" '
        '--stage %s --status %s' % (shlex.quote(stage_name), status)
    )


def _gate_snippet(stage: Stage, spec: PipelineSpec, *, journal: bool = False) -> str:
    """质量闸门：独立 Pod 先按路径契约把产物取回来，再调用框架 CLI 跑 checks。

    为什么必须先 get：gate 与 producer 是两个 Pod，不能假设同节点共享 hostPath。
    为什么改成 CLI：旧实现把配置/检查名内联进 ``python3 -c`` / heredoc，
    引号与运行期值容易破坏脚本；CLI 让同一套逻辑本地可单测、集群只负责编排。
    """
    if not stage.checks:
        return ""
    if not stage.outputs or stage.outputs[0] not in stage.artifacts:
        raise SpecError(
            f"stage {stage.name!r}: 配了 checks 时 outputs[0] 必须有对应的 artifacts 路径"
        )
    artifact_name = stage.outputs[0]
    local_path = translate(stage.artifacts[artifact_name])
    cfg = _resolve_for_gate(stage.check_config, spec=spec, artifact_path=local_path)
    lines = [
        "set -e",
        'python3 -m specargo.objstore get --key "%s" --file %s'
        % (_artifact_key(artifact_name), shlex.quote(local_path)),
        "cat > /tmp/gate_config.json <<'JSON'",
        json.dumps(cfg, ensure_ascii=False, indent=0),
        "JSON",
        ("python3 -m specargo.pipeline_cli gate --stage %s --artifact %s --checks '%s' "
         "--check-config-file /tmp/gate_config.json --run-id \"$%s\""
         % (shlex.quote(stage.name), shlex.quote(local_path),
            json.dumps(list(stage.checks), ensure_ascii=False), RUN_ID_ENV)),
    ]
    if journal:
        lines[-1] += (
            ' --journal --dataset "{{inputs.parameters.dataset}}" '
            '--partition "{{inputs.parameters.partition}}" '
            '--variant "{{inputs.parameters.variant}}"'
        )
    return "\n".join(lines) + "\n"


def _topo_order(stages: list[Stage]) -> list[Stage]:
    """按依赖拓扑排序（取"最后一个 stage"不能假设 YAML 声明顺序是拓扑序）。"""
    remaining = list(stages)
    installed: list[str] = []
    order: list[Stage] = []
    while remaining:
        progressed = False
        for stage in list(remaining):
            if all(dep in installed for dep in stage.dependencies):
                installed.append(stage.name)
                order.append(stage)
                remaining.remove(stage)
                progressed = True
        if not progressed:
            raise SpecError("stage 依赖无法拓扑排序（成环或引用不存在）")
    return order


def _on_exit_template(spec: PipelineSpec, *, image: str, pull_policy: str,
                      runtime: RuntimeConfig, use_discover: bool,
                      partition_items: list[dict], final_stage: str,
                      terminal_statuses: list[str]) -> dict:
    """onExit：从对象存储状态日志对账，差异非空则让工作流失败（+ 可选告警）。"""
    lines = ["set -e"]
    if use_discover:
        lines += [
            ("if ! python3 -m specargo.objstore get "
             '--key "%s/$%s/_run/partitions.json" '
             "--file /tmp/partitions.json; then"
             % (OBJSTORE_KEY_ROOT, RUN_ID_ENV)),
            '  if [ "$PIPELINE_WORKFLOW_STATUS" = "Failed" ] || '
            '[ "$PIPELINE_WORKFLOW_STATUS" = "Error" ]; then',
            '    echo "[onExit] 工作流已失败且没有分区清单，跳过对账"',
            "    exit 0",
            "  fi",
            # 成功路径拿不到分区清单 = 无法证明数据完整，必须失败（否则假绿）
            '  echo "[onExit] 工作流成功但分区清单缺失，无法对账"',
            "  exit 1",
            "fi",
        ]
    else:
        lines += ["cat > /tmp/partitions.json <<'JSON'",
                  json.dumps(partition_items, ensure_ascii=False), "JSON"]
    lines.append(
        ('python3 -m specargo.pipeline_cli reconcile-journal --run-id "$%s" '
         "--partitions-file /tmp/partitions.json --final-stage %s "
         "--terminal-statuses '%s' --report-json /tmp/reconcile-report.json"
         % (RUN_ID_ENV, shlex.quote(final_stage), json.dumps(terminal_statuses))))
    env = _base_env(spec, runtime)
    env.append({"name": "PIPELINE_WORKFLOW_STATUS", "value": "{{workflow.status}}"})
    webhook = runtime.notify_webhook
    if webhook:
        env.append({"name": "NOTIFY_WEBHOOK", "value": str(webhook)})
    return {
        "name": "on-exit",
        "timeout": "10m",
        "container": {
            "image": image,
            "imagePullPolicy": pull_policy,
            "command": ["sh", "-c"],
            "args": ["\n".join(lines) + "\n"],
            "env": env,
        },
    }


def _assert_no_spec_templates(wf: dict, source: str = "") -> None:
    """编译后自检：产物里不能再有 spec 专有的模板变量。

    CI 的 argo lint 只做 schema 校验、不解析模板变量，所以"编译产物里残留
    ``{{params.x}}``"这类问题一直能骗过 CI，直到真正 submit。这条断言把
    "一份 spec 两个目标"变成可验证条件。
    """
    text = yaml.safe_dump(wf, allow_unicode=True, sort_keys=False)
    # 宽松匹配变量名（含 - 等字符），防止 {{params.foo-bar}} 这类漏网；
    # 同时覆盖带空格的 {{ params.x }} / {{ artifact }} 写法。
    leftover = sorted(set(re.findall(
        r"\{\{\s*(?:params\.[^{}\s]*|partition\.[^{}\s]*|artifact)\s*\}\}", text)))
    if leftover:
        raise SpecError(f"{source}: 编译产物里还有未解析的 spec 模板变量: {leftover}")


# ---------------------------------------------------------------------------
# 编译器
# ---------------------------------------------------------------------------

def compile_workflow(spec: PipelineSpec, *, name: str | None = None,
                     namespace: str | None = None,
                     runtime: RuntimeConfig | None = None) -> dict:
    """编译 spec 为 Argo Workflow。

    ``runtime``：执行环境配置；缺省从 spec 的 ``runtime:`` 节（旧名 ``k8s:``）
    读取。namespace 优先级：显式参数 > runtime.namespace > "argo"。
    """
    runtime = runtime or RuntimeConfig.from_spec(spec)
    namespace = namespace or runtime.namespace
    image = runtime.image
    pull_policy = runtime.image_pull_policy
    volumes = runtime.volumes
    mounts = runtime.mounts
    objstore_endpoint = runtime.objstore_endpoint
    service_account = runtime.service_account
    active_deadline = runtime.active_deadline_seconds
    journal = runtime.journal
    resume = runtime.resume
    # 默认成功后清理 Pod（默认行为不变）；CI 需要保留 Pod 做跨节点断言时可置空
    pod_gc_strategy = runtime.pod_gc_strategy

    # artifact 名 -> 产出它的 stage（用于给下游接线 get）
    produced_by: dict[str, str] = {}
    artifact_paths: dict[str, str] = {}
    for stage in spec.stages:
        for art_name, art_path in stage.artifacts.items():
            produced_by[art_name] = stage.name
            artifact_paths[art_name] = art_path

    volume_mounts = [{"name": n, "mountPath": p} for n, p in mounts.items()]

    # 扇出元素统一为 (key, dataset, variant) 对象：
    # 只传 key 会让集群侧 {{partition.dataset}}/{{partition.variant}} 渲染成空串，
    # 与本地 runner 的 (dataset × variant) 分区语义漂移。
    partition_items = _partition_items(spec)
    variants: list[str] = []
    datasets: list[str] = []
    for p in spec.partitions:
        if p.variant not in variants:
            variants.append(p.variant)
        if p.dataset not in datasets:
            datasets.append(p.dataset)
    discover_dataset = datasets[0] if datasets else ""

    templates: list[dict] = []

    # ---- discover 任务（动态分区）----
    use_discover = getattr(spec, "partitions_source", "list") == "discover"
    discover_cmd = getattr(spec, "discover_command", "")
    if use_discover:
        templates.append({
            "name": "discover-scan",
            "script": {
                "image": image,
                "imagePullPolicy": pull_policy,
                "command": ["sh", "-c"],
                "args": [_discover_script(discover_cmd, discover_dataset,
                                          variants or [""], journal=journal)],
                "env": _discover_env(spec, runtime, discover_dataset,
                                     variants or [""], journal=journal),
                "volumeMounts": volume_mounts,
            },
            "outputs": {
                "parameters": [
                    # valueFrom.path 指向脚本写出的文件；JSON 数组才能被 withParam 展开
                    {"name": "items", "valueFrom": {"path": "/tmp/out/items.json"}},
                ],
            },
        })

    # ---- 每个 stage 一个 template ----
    for stage in spec.stages:
        # 本 stage 显式声明消费的上游 artifact（路径用上游的声明，保证 key 一致）
        consumed = []
        # 注意：循环变量不能叫 name，否则会遮蔽本函数的 name 参数，
        # 导致 workflow metadata.name 取到最后一个 consumed artifact 名。
        for art_name in stage.consumes:
            if art_name in produced_by and produced_by[art_name] != stage.name:
                consumed.append((art_name, artifact_paths[art_name]))

        parts = ["set -e"]          # get/put 失败必须中断，不能继续"成功"
        if resume:
            parts.append(_resume_skip_snippet(stage))
        get_snip = _objstore_get_snippet(dict(consumed))
        if get_snip:
            parts.append(get_snip)
        parts.append(translate(stage.command))
        put_snip = _objstore_put_snippet(stage.artifacts)
        if put_snip:
            parts.append(put_snip)
        if journal:
            # 产物落对象存储后登记 generated；gate 通过后会覆盖为 verified
            parts.append(_state_put_snippet(stage.name, "generated"))
        script = "\n".join(p for p in parts if p)

        container = {
            "image": image,
            "imagePullPolicy": pull_policy,
            "command": ["sh", "-c"],
            "args": [script],
            "env": _base_env(spec, runtime),
        }
        if volume_mounts:
            container["volumeMounts"] = volume_mounts
        if stage.resources:
            container["resources"] = stage.resources

        tmpl: dict = {
            "name": stage.name,
            "inputs": {"parameters": [
                {"name": "partition"}, {"name": "dataset"}, {"name": "variant"},
            ]},
            "timeout": stage.timeout,
            "container": container,
        }
        # 总是输出 retryStrategy：limit=0 是显式的"不重试"设计意图，
        # 不能因为等于默认值就省略（防止上游默认值变化时静默改变行为）
        if stage.retry:
            rs: dict = {"limit": str(stage.retry.limit)}
            if stage.retry.policy:
                rs["retryPolicy"] = stage.retry.policy
            if stage.retry.duration:
                rs["backoff"] = {
                    "duration": stage.retry.duration,
                    "factor": stage.retry.factor,
                    "maxDuration": stage.retry.max_duration,
                }
            tmpl["retryStrategy"] = rs
        templates.append(tmpl)

        # ---- 质量闸门：独立 gate 任务 ----
        if stage.checks:
            gate_container = {
                "image": image,
                "imagePullPolicy": pull_policy,
                "command": ["sh", "-c"],
                "args": [_gate_snippet(stage, spec, journal=journal)],
                "env": _base_env(spec, runtime) + _plugin_env(spec, runtime),
            }
            if volume_mounts:
                gate_container["volumeMounts"] = volume_mounts
            templates.append({
                "name": "gate-" + stage.name,
                # gate 可能引用 {{partition.dataset}}/{{partition.variant}}（例如按变体
                # 选阈值），所以三个维度都要声明，否则 Argo 运行期解析未声明输入直接失败。
                "inputs": {"parameters": [
                    {"name": "partition"}, {"name": "dataset"}, {"name": "variant"},
                ]},
                "timeout": "5m",
                "retryStrategy": {"limit": "0"},   # 检查失败是数据问题，重试无意义
                "container": gate_container,
            })

    # ---- 主 DAG ----
    tasks: list[dict] = []
    if use_discover:
        tasks.append({"name": "discover", "template": "discover-scan"})

    for stage in spec.stages:
        deps = list(stage.dependencies)
        if use_discover:
            # 扇出依赖 discover 的输出
            with_param = "{{tasks.discover.outputs.parameters.items}}"
            if "discover" not in deps:
                deps = ["discover"] + deps
        else:
            with_param = json.dumps(partition_items, ensure_ascii=False)

        tasks.append({
            "name": stage.name,
            "template": stage.name,
            "dependencies": deps,
            "withParam": with_param,
            "arguments": {"parameters": [
                {"name": "partition", "value": "{{item.key}}"},
                {"name": "dataset", "value": "{{item.dataset}}"},
                {"name": "variant", "value": "{{item.variant}}"},
            ]},
        })

        if stage.checks:
            gate_deps = [stage.name] + (["discover"] if use_discover else [])
            tasks.append({
                "name": "gate-" + stage.name,
                "template": "gate-" + stage.name,
                "dependencies": gate_deps,
                "withParam": with_param,
                "arguments": {"parameters": [
                    {"name": "partition", "value": "{{item.key}}"},
                    {"name": "dataset", "value": "{{item.dataset}}"},
                    {"name": "variant", "value": "{{item.variant}}"},
                ]},
            })

    templates.insert(0, {"name": "main", "dag": {"tasks": tasks}})

    if journal:
        ordered = _topo_order(spec.stages)
        final_stage = ordered[-1]
        terminal = (["verified"] if final_stage.checks
                    else ["generated", "verified"])
        templates.append(_on_exit_template(
            spec, image=image, pull_policy=pull_policy,
            runtime=runtime, use_discover=use_discover,
            partition_items=partition_items, final_stage=final_stage.name,
            terminal_statuses=terminal))

    wf_spec: dict = {
        "entrypoint": "main",
        "activeDeadlineSeconds": active_deadline,
        "ttlStrategy": {"secondsAfterCompletion": 3600},
        "serviceAccountName": runtime.service_account,
        "volumes": [{"name": n, **v} for n, v in volumes.items()],
        "templates": templates,
    }
    if pod_gc_strategy:
        wf_spec["podGC"] = {"strategy": pod_gc_strategy}
    if journal:
        wf_spec["onExit"] = "on-exit"
    arguments = _workflow_arguments(spec)
    if arguments:
        wf_spec["arguments"] = {"parameters": arguments}

    wf = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "name": name or spec.name,
            "namespace": namespace,
            # 版本跟着产物走：线上跑着的工作流能回答"它按哪版 spec 契约编译"。
            # 排查"本地能跑、集群行为不同"时，第一手信息就是这里。
            "annotations": {"specargo.dev/spec-version": str(spec.version)},
        },
        "spec": wf_spec,
    }
    _assert_no_spec_templates(wf, source=spec.name)
    return wf


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def compile_template(spec: PipelineSpec, *, name: str | None = None,
                     namespace: str | None = None,
                     runtime: RuntimeConfig | None = None) -> dict:
    """编译成 ``WorkflowTemplate``（可注册到集群、被多个 Workflow 引用）。

    与 ``compile_workflow`` 的唯一差别是 CRD 类型与 metadata：
    ``spec.templates`` / ``spec.entrypoint`` / ``spec.arguments`` 完全相同——
    同一份 spec 既能直接提交 Workflow，也能发布成版本化模板（灰度/回滚）。
    """
    wf = compile_workflow(spec, name=name, namespace=namespace, runtime=runtime)
    return {
        "apiVersion": wf["apiVersion"],
        "kind": "WorkflowTemplate",
        "metadata": dict(wf["metadata"]),
        "spec": wf["spec"],
    }


def render_reference(template: dict, *, name: str, arguments: dict | None = None,
                     namespace: str | None = None) -> dict:
    """生成一个引用该 WorkflowTemplate 的最小 Workflow（用于"发布模板后怎么跑"）。"""
    spec: dict = {"workflowTemplateRef": {"name": template["metadata"]["name"]}}
    if arguments:
        spec["arguments"] = {"parameters": [
            {"name": k, "value": str(v)} for k, v in arguments.items()]}
    return {
        "apiVersion": template["apiVersion"],
        "kind": "Workflow",
        "metadata": {"name": name,
                     "namespace": namespace or template["metadata"].get("namespace", "argo")},
        "spec": spec,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="把管线定义编译成 Argo Workflow")
    parser.add_argument("spec", help="pipelines/*.yaml")
    parser.add_argument("-o", "--output", help="输出路径（默认 stdout）")
    parser.add_argument("--name", help="workflow 名（默认用 spec.name）")
    parser.add_argument("--namespace", default=None,
                        help="命名空间（缺省用 runtime.namespace）")
    parser.add_argument("--runtime", help="运行时配置 YAML（按 key 覆盖 spec 的 runtime/k8s 节）")
    parser.add_argument("--as-template", action="store_true",
                        help="编译成 WorkflowTemplate（可注册到集群、被多个 Workflow 引用）")
    parser.add_argument("--reference", metavar="NAME",
                        help="生成引用该模板的最小 Workflow（与 --as-template 配合使用）")
    args = parser.parse_args()

    spec = load_spec(args.spec)
    runtime = RuntimeConfig.from_spec(spec)
    if args.runtime:
        runtime.apply_mapping(
            yaml.safe_load(Path(args.runtime).read_text(encoding="utf-8")) or {},
            where=args.runtime)
    if args.reference:
        template = compile_template(spec, name=args.name, namespace=args.namespace,
                                    runtime=runtime)
        wf = render_reference(template, name=args.reference,
                              namespace=args.namespace)
    elif args.as_template:
        wf = compile_template(spec, name=args.name, namespace=args.namespace,
                             runtime=runtime)
    else:
        wf = compile_workflow(spec, name=args.name, namespace=args.namespace,
                              runtime=runtime)
    text = yaml.safe_dump(wf, allow_unicode=True, sort_keys=False, width=120)

    header = ("# 由 spec2argo 从 %s 自动生成，请勿手改\n"
              "# 重新生成：specargo compile %s -o %s\n"
              % (args.spec, args.spec, args.output or "<stdout>"))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(header + text, encoding="utf-8")
        print(f"✅ 已生成 {args.output}（{wf['kind']}，"
              f"{len(wf['spec'].get('templates', []))} 个 template）",
              file=sys.stderr)
    else:
        print(header + text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
