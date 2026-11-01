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

    python3 spec2argo.py pipelines/etl-demo.yaml -o /tmp/wf.yaml
    argo lint --offline /tmp/wf.yaml
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from spec import PipelineSpec, Stage, load_spec

# ---------------------------------------------------------------------------
# 表达式翻译：spec 模板 → Argo 模板
# ---------------------------------------------------------------------------

_PARAM_MAP = {
    "params.python": "python3",
    "params.app_dir": "/app",
}


def translate(expr: str, *, in_fanout: bool = True) -> str:
    """把 spec 的 ``{{...}}`` 表达式翻成 Argo 的模板语法。

    ``{{partition.key}}``      -> ``{{inputs.parameters.partition}}``
    ``{{partition.variant}}``  -> ``{{inputs.parameters.variant}}``
    ``{{params.run_id}}``      -> ``{{workflow.name}}``
    ``{{params.work_dir}}``    -> ``/output``（集群统一挂载点，由 k8s.mounts 决定）
    其余 ``{{params.x}}``      -> 字面量（见 _PARAM_MAP）
    """
    import re

    out = expr
    for spec_var, argo_var in _PARAM_MAP.items():
        out = out.replace("{{%s}}" % spec_var, argo_var)

    out = out.replace("{{params.run_id}}", "{{workflow.name}}")
    out = out.replace("{{params.work_dir}}", "/output")
    out = out.replace("{{partition.key}}", "{{inputs.parameters.partition}}")
    out = out.replace("{{partition.variant}}", "{{inputs.parameters.variant}}")
    out = out.replace("{{partition.dataset}}", "{{inputs.parameters.dataset}}")
    return out


# ---------------------------------------------------------------------------
# 代码片段构造
# ---------------------------------------------------------------------------

def _objstore_put_snippet(artifacts: dict[str, str]) -> str:
    """上游 stage：把声明产物按路径契约 put 到对象存储。"""
    if not artifacts:
        return ""
    lines = ["# 路径契约：上游只负责把产物写到约定 key，不依赖编排器的 artifact 配对"]
    for name, path in artifacts.items():
        key = 'runs/$TTS_RUN_ID/{{inputs.parameters.partition}}/%s' % _ext(name)
        lines.append(
            'python3 /app/objstore.py put --key "%s" --file %s'
            % (key, translate(path))
        )
    return "\n".join(lines)


def _objstore_get_snippet(artifacts: dict[str, str]) -> str:
    """下游 stage：消费前先从对象存储 get（跨 Pod 取数）。"""
    if not artifacts:
        return ""
    lines = ["# 路径契约：从上游产物取数（经对象存储，与节点无关）"]
    for name, path in artifacts.items():
        key = 'runs/$TTS_RUN_ID/{{inputs.parameters.partition}}/%s' % _ext(name)
        lines.append(
            'python3 /app/objstore.py get --key "%s" --file %s'
            % (key, translate(path))
        )
    return "\n".join(lines)


def _discover_script(cmd: str) -> str:
    """把用户的发现命令包一层：结果同时打印并写成 JSON 数组文件。

    Argo 的 withParam 需要 JSON 数组字符串，而用户的命令输出的是每行一个分区，
    所以在脚本里做一次转换。
    """
    # 用户的命令可能带 if/fi 等多行结构，不能直接接管道；
    # 先把输出落临时文件，再统一转 JSON 数组并 tee 到 Argo 输出参数路径
    return (
        "set -e\n"
        "mkdir -p /tmp/out\n"
        + cmd.strip() + " > /tmp/out/found.txt\n"
        "python3 -c 'import sys, json\n"
        "lines = [l.strip() for l in open(\"/tmp/out/found.txt\") if l.strip()]\n"
        "print(json.dumps(lines))' | tee /tmp/out/items.json\n"
    )


def _ext(artifact_name: str) -> str:
    """artifact 名 -> 文件名（raw -> raw.jsonl）。"""
    return artifact_name if "." in artifact_name else artifact_name + ".jsonl"


def _gate_snippet(stage: Stage) -> str:
    """质量闸门：独立跑 checks.py 插件，不过检则非零退出。"""
    if not stage.checks:
        return ""
    cfg = json.dumps(stage.check_config, ensure_ascii=False)
    checks = ", ".join('"%s"' % c for c in stage.checks)
    return (
        "python3 -c '\n"
        "import sys\n"
        'sys.path.insert(0, "/app")\n'
        "from pathlib import Path\n"
        "from checks import run_check\n"
        "\n"
        "failed = []\n"
        "cfg = %s\n"
        "for name in [%s]:\n"
        "    r = run_check(name, Path(\"%s\"), cfg.get(name, {}), {})\n"
        "    print(f\"[gate] {name}: passed={r.passed} metric={r.metric} {r.detail}\")\n"
        "    if not r.passed:\n"
        "        failed.append(name)\n"
        "sys.exit(1 if failed else 0)\n"
        "'" % (cfg, checks, translate(stage.artifacts[stage.outputs[0]]))
    )


# ---------------------------------------------------------------------------
# 编译器
# ---------------------------------------------------------------------------

def compile_workflow(spec: PipelineSpec, *, name: str | None = None,
                     namespace: str = "argo") -> dict:
    k8s = getattr(spec, "k8s", None) or {}
    image = k8s.get("image", "tts-argo:ci")
    pull_policy = k8s.get("image_pull_policy", "Never")
    volumes = k8s.get("volumes", {}) or {}
    mounts = k8s.get("mounts", {}) or {}

    # artifact 名 -> 产出它的 stage（用于给下游接线 get）
    produced_by: dict[str, str] = {}
    artifact_paths: dict[str, str] = {}
    for stage in spec.stages:
        for art_name, art_path in stage.artifacts.items():
            produced_by[art_name] = stage.name
            artifact_paths[art_name] = art_path

    volume_mounts = [{"name": n, "mountPath": p} for n, p in mounts.items()]

    templates: list[dict] = []

    # ---- discover 任务（动态分区）----
    use_discover = getattr(spec, "partitions_source", "list") == "discover"
    discover_cmd = getattr(spec, "discover_command", "")
    if use_discover:
        # discover 的输出必须是 JSON 数组字符串，Argo 才能当 withParam 的列表
        templates.append({
            "name": "discover-scan",
            "script": {
                "image": image,
                "imagePullPolicy": pull_policy,
                "command": ["sh", "-c"],
                "args": [_discover_script(discover_cmd)],
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
        for name in stage.consumes:
            if name in produced_by and produced_by[name] != stage.name:
                consumed.append((name, artifact_paths[name]))

        parts = []
        get_snip = _objstore_get_snippet(dict(consumed))
        if get_snip:
            parts.append(get_snip)
        parts.append(translate(stage.command))
        put_snip = _objstore_put_snippet(stage.artifacts)
        if put_snip:
            parts.append(put_snip)
        script = "\n".join(p for p in parts if p)

        container = {
            "image": image,
            "imagePullPolicy": pull_policy,
            "command": ["sh", "-c"],
            "args": [script],
            "env": [
                {"name": "TTS_RUN_ID", "value": "{{workflow.name}}"},
                {"name": "OBJSTORE_ENDPOINT",
                 "value": "http://object-store.argo.svc.cluster.local:5000"},
            ],
        }
        if volume_mounts:
            container["volumeMounts"] = volume_mounts
        if stage.resources:
            container["resources"] = stage.resources

        tmpl: dict = {
            "name": stage.name,
            "inputs": {"parameters": [
                {"name": "partition"}, {"name": "variant"}, {"name": "dataset"},
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
                "args": [_gate_snippet(stage)],
                "env": [{"name": "TTS_RUN_ID", "value": "{{workflow.name}}"}],
            }
            if volume_mounts:
                gate_container["volumeMounts"] = volume_mounts
            templates.append({
                "name": "gate-" + stage.name,
                "inputs": {"parameters": [{"name": "partition"}]},
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
            with_param = json.dumps([p.key for p in spec.partitions])

        tasks.append({
            "name": stage.name,
            "template": stage.name,
            "dependencies": deps,
            "withParam": with_param,
            "arguments": {"parameters": [
                {"name": "partition", "value": "{{item}}"},
                {"name": "variant", "value": ""},
                {"name": "dataset", "value": ""},
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
                    {"name": "partition", "value": "{{item}}"},
                ]},
            })

    templates.insert(0, {"name": "main", "dag": {"tasks": tasks}})

    wf = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {"name": name or spec.name, "namespace": namespace},
        "spec": {
            "entrypoint": "main",
            "activeDeadlineSeconds": 1800,
            "podGC": {"strategy": "OnWorkflowSuccess"},
            "ttlStrategy": {"secondsAfterCompletion": 3600},
            "serviceAccountName": "default",
            "volumes": [{"name": n, **v} for n, v in volumes.items()],
            "templates": templates,
        },
    }
    return wf


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="把管线定义编译成 Argo Workflow")
    parser.add_argument("spec", help="pipelines/*.yaml")
    parser.add_argument("-o", "--output", help="输出路径（默认 stdout）")
    parser.add_argument("--name", help="workflow 名（默认用 spec.name）")
    parser.add_argument("--namespace", default="argo")
    args = parser.parse_args()

    spec = load_spec(args.spec)
    wf = compile_workflow(spec, name=args.name, namespace=args.namespace)
    text = yaml.safe_dump(wf, allow_unicode=True, sort_keys=False, width=120)

    header = ("# 由 spec2argo 从 %s 自动生成，请勿手改\n"
              "# 重新生成：python3 app/spec2argo.py %s -o %s\n"
              % (args.spec, args.spec, args.output or "<stdout>"))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(header + text, encoding="utf-8")
        print(f"✅ 已生成 {args.output}（{len(wf['spec']['templates'])} 个 template）",
              file=sys.stderr)
    else:
        print(header + text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
