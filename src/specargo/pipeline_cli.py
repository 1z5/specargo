"""框架 CLI：让集群侧也能调用同一套质量闸门 / 状态 / 对账语义。

为什么需要 CLI：编译产物里的 gate/state/onExit 以前是内联 Python 片段，
引号、转义、参数透传全靠拼字符串（换引号就炸）。改成 CLI 后：
  - 逻辑可在本地单测，集群只负责编排；
  - 本地 runner 与集群执行共用同一套判定实现。

子命令::

    gate               跑某个 stage 的质量检查（checks 注册表 + spec.plugins 加载）
    state put          写本地 SQLite 状态（单机 / 调试）
    state put-remote   写对象存储状态日志（集群跨 Pod，无共享盘）
    reconcile-journal  从状态日志对账（onExit 用），可选 webhook 告警

示例（集群模板里实际调用的形态）::

    python3 /app/pipeline_cli.py gate --stage verify --artifact /output/x/report.json \
      --checks '["my_domain_check"]' --check-config-file /tmp/gate_config.json
    python3 /app/pipeline_cli.py state put-remote --run-id "$PIPELINE_RUN_ID" \
      --dataset dwd_order --partition 101 --stage extract --status generated
    python3 /app/pipeline_cli.py reconcile-journal --run-id "$PIPELINE_RUN_ID" \
      --partitions-file /tmp/partitions.json --final-stage load
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _load_plugins(cli_plugins: list[str] | None, cli_paths: list[str] | None) -> None:
    """按 CLI 参数 + env 加载领域插件（与本地 runner 的 spec.plugins 同源）。"""
    from .plugins import load

    modules = list(cli_plugins or [])
    env_modules = os.getenv("PIPELINE_PLUGINS", "").strip()
    if env_modules:
        modules.extend(json.loads(env_modules))
    dirs = [Path(p) for p in (cli_paths or [])]
    env_path = os.getenv("PIPELINE_PLUGIN_PATH", "").strip()
    if env_path:
        dirs.append(Path(env_path))
    if modules:
        load(modules, search_dirs=dirs)


def _read_config(args) -> dict:
    """读检查配置；文件缺失/JSON 非法时给出可读错误（不抛 traceback）。"""
    try:
        if args.check_config_file:
            return json.loads(Path(args.check_config_file).read_text(encoding="utf-8"))
        if args.check_config:
            return json.loads(args.check_config)
    except FileNotFoundError:
        raise SystemExit(f"[gate] 检查配置文件不存在: {args.check_config_file}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"[gate] 检查配置不是合法 JSON: {exc}")
    return {}


def cmd_gate(args) -> int:
    """跑一个 stage 的质量闸门；任一检查失败 → 非零退出（集群 task 失败）。"""
    from .checks import run_check

    _load_plugins(args.plugin, args.plugin_path)
    checks = json.loads(args.checks) if args.checks else []
    cfg = _read_config(args)
    if args.journal and not (args.dataset and args.partition):
        print("[gate] --journal 需要 --dataset 与 --partition", file=sys.stderr)
        return 2
    if not checks:
        print("[gate] checks 为空：质量闸门没有可执行的检查", file=sys.stderr)
        return 2
    artifact = Path(args.artifact)
    failed: list[str] = []
    for name in checks:
        result = run_check(name, artifact, cfg.get(name, {}), {})
        print(f"[gate] {name}: passed={result.passed} metric={result.metric} "
              f"{result.detail}")
        if not result.passed:
            failed.append(name)

    status = "failed" if failed else "verified"
    if args.journal:
        _record_journal(args, status)
    if failed:
        print(f"[gate] 未通过检查: {failed}", file=sys.stderr)
        return 1
    return 0


def _record_journal(args, status: str) -> None:
    from .journal import put_status

    put_status(run_id=args.run_id, dataset=args.dataset, partition=args.partition,
               variant=args.variant or "", stage=args.stage, status=status,
               attempt=args.attempt)


def cmd_state_put(args) -> int:
    from .state import connect, upsert_item

    conn = connect(args.state_db)
    try:
        upsert_item(conn, dataset=args.dataset, partition=args.partition,
                    variant=args.variant or "", stage=args.stage, status=args.status,
                    run_id=args.run_id, attempt=args.attempt)
    finally:
        conn.close()
    print(f"[state] {args.dataset}/{args.partition}/{args.variant or '-'}/"
          f"{args.stage} = {args.status}")
    return 0


def cmd_state_put_remote(args) -> int:
    _record_journal(args, args.status)
    print(f"[state] remote {args.dataset}/{args.partition}/{args.variant or '-'}/"
          f"{args.stage} = {args.status}")
    return 0


def cmd_state_check_skip(args) -> int:
    """集群 resume 判定：该 stage 对这个分区是否已到成功终态。

    退出码即判定结果（给 shell 的 ``&&`` 用）::

        0 = 已终态，调用方应跳过（不执行业务命令）
        1 = 未终态 / 无记录，调用方应正常执行
        2 = 用法或环境错误

    终态规则来自 ``resume_policy.stage_terminal_statuses``——与本地 runner
    同一份定义，避免"本地跳过、集群重跑"的漂移。
    """
    from .journal import get_item_status
    from .resume_policy import is_terminal

    run_id = args.run_id or os.environ.get("PIPELINE_RUN_ID", "")
    if not run_id:
        print("[resume] 缺少 run_id（--run-id 或 PIPELINE_RUN_ID）", file=sys.stderr)
        return 2
    if not (args.dataset and args.partition and args.stage):
        print("[resume] 需要 --dataset / --partition / --stage", file=sys.stderr)
        return 2
    try:
        row = get_item_status(run_id, dataset=args.dataset,
                              partition=args.partition,
                              variant=args.variant or "", stage=args.stage)
    except Exception as exc:                       # 读不到状态不能当成"已完成"
        print(f"[resume] 读取状态失败，按未完成处理: {exc!r}", file=sys.stderr)
        return 1
    if row is not None and is_terminal(row.get("status", ""), args.has_checks):
        print(f"[resume] {args.dataset}/{args.partition}/{args.variant or '-'}/"
              f"{args.stage} 已是 {row.get('status')}，跳过")
        return 0
    status = row.get("status") if row else "无记录"
    print(f"[resume] {args.dataset}/{args.partition}/{args.variant or '-'}/"
          f"{args.stage} 未完成（{status}），继续执行")
    return 1


def cmd_reconcile_journal(args) -> int:
    from .notify import render_summary, send_webhook
    from .state import reconcile_datasets

    try:
        partitions = json.loads(Path(args.partitions_file).read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"[reconcile] 分区清单不存在: {args.partitions_file}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        print(f"[reconcile] 分区清单不是合法 JSON: {exc}", file=sys.stderr)
        return 2
    if not partitions:
        print("[reconcile] 分区清单为空：无法对账（expected=0 不应视为通过）",
              file=sys.stderr)
        return 1
    expected = [(p.get("dataset", ""), p.get("key", ""), p.get("variant", ""))
                for p in partitions]
    datasets: list[str] = []
    for ds, _key, _variant in expected:
        if ds not in datasets:
            datasets.append(ds)

    from .journal import load_statuses
    rows = load_statuses(args.run_id)
    terminal = json.loads(args.terminal_statuses) if args.terminal_statuses else ["verified"]
    recons = reconcile_datasets(datasets, expected, rows,
                                final_stage=args.final_stage,
                                terminal_statuses=terminal)

    text = render_summary(args.run_id, recons)
    print(text)
    if args.report_json:
        Path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_json).write_text(
            json.dumps({"run_id": args.run_id, "terminal_statuses": terminal,
                        "reconcile": recons}, ensure_ascii=False, indent=2),
            encoding="utf-8")
    webhook = args.notify_webhook or os.getenv("NOTIFY_WEBHOOK", "")
    if webhook and send_webhook(text, webhook):
        print("[notify] webhook 已发送")
    return 0 if all(r["ok"] for r in recons) else 1


# ---------------------------------------------------------------------------
# CLI 装配
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="specargo 框架 CLI（gate/state/reconcile）")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_journal_fields(p, *, required=False):
        p.add_argument("--run-id", default=os.getenv("PIPELINE_RUN_ID", ""),
                       required=required)
        p.add_argument("--dataset", required=required)
        p.add_argument("--partition", required=required)
        p.add_argument("--variant", default="")
        p.add_argument("--attempt", type=int, default=None)

    g = sub.add_parser("gate", help="跑 stage 质量闸门")
    g.add_argument("--stage", required=True)
    g.add_argument("--artifact", required=True)
    g.add_argument("--checks", default="[]", help='JSON 列表，如 \'["file_nonempty"]\'')
    g.add_argument("--check-config", default="", help="JSON 字符串")
    g.add_argument("--check-config-file", default="", help="JSON 文件（推荐，避免转义）")
    g.add_argument("--plugin", action="append", default=[], help="插件模块名（可多次）")
    g.add_argument("--plugin-path", action="append", default=[], help="插件搜索目录（可多次）")
    g.add_argument("--journal", action="store_true", help="把检查结果写入对象存储状态日志")
    add_journal_fields(g)
    g.set_defaults(func=cmd_gate)

    sp = sub.add_parser("state", help="状态登记")
    sp_sub = sp.add_subparsers(dest="state_command", required=True)

    p_put = sp_sub.add_parser("put", help="写本地 SQLite 状态")
    p_put.add_argument("--state-db", required=True)
    p_put.add_argument("--stage", required=True)
    p_put.add_argument("--status", required=True,
                       choices=["pending", "generated", "verified", "failed"])
    add_journal_fields(p_put)
    p_put.set_defaults(func=cmd_state_put)

    p_remote = sp_sub.add_parser("put-remote", help="写对象存储状态日志")
    p_remote.add_argument("--stage", required=True)
    p_remote.add_argument("--status", required=True,
                          choices=["pending", "generated", "verified", "failed"])
    add_journal_fields(p_remote)
    p_remote.set_defaults(func=cmd_state_put_remote)

    p_skip = sp_sub.add_parser(
        "check-skip", help="集群 resume 判定：已到终态则退出码 0（应跳过）")
    p_skip.add_argument("--stage", required=True)
    p_skip.add_argument("--has-checks", action="store_true",
                        help="该 stage 配了质量闸门（终态只认 verified）")
    add_journal_fields(p_skip)
    p_skip.set_defaults(func=cmd_state_check_skip)

    rec = sub.add_parser("reconcile-journal", help="从状态日志对账（onExit 用）")
    rec.add_argument("--run-id", default=os.getenv("PIPELINE_RUN_ID", ""), required=True)
    rec.add_argument("--partitions-file", required=True)
    rec.add_argument("--final-stage", default=None)
    rec.add_argument("--terminal-statuses", default='["verified"]',
                     help='JSON 列表，如 \'["generated","verified"]\'')
    rec.add_argument("--report-json", default="")
    rec.add_argument("--notify-webhook", default="")
    rec.set_defaults(func=cmd_reconcile_journal)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
