"""统一 CLI：把各模块的命令行入口收到一个 ``specargo`` 命令下。

子命令只是转发到既有模块的 ``main()``（行为完全一致），
让「安装后只有一个入口」成立::

    specargo run       pipelines/etl-demo.yaml --state-db s.db --workdir out
    specargo compile   pipelines/etl-demo.yaml -o wf.yaml
    specargo gate      --stage verify --artifact out/x.json --checks '["file_nonempty"]'
    specargo state     put --state-db s.db --dataset ds --partition 1 --stage load --status verified
    specargo reconcile --run-id r --partitions-file parts.json --final-stage load
    specargo service   --port 8080
"""
from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="specargo", description="声明式批量数据管线框架（本地执行 / 编译 Argo CRD）")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="本地执行一条管线（runner.py）")
    sub.add_parser("compile", help="编译 spec 成 Argo Workflow CRD（spec2argo.py）")
    sub.add_parser("gate", help="跑质量闸门（pipeline_cli.py gate）")
    sub.add_parser("state", help="登记 item 状态（pipeline_cli.py state）")
    sub.add_parser("reconcile", help="对账（pipeline_cli.py reconcile-journal）")
    sub.add_parser("service", help="常驻处理服务（service.py）")

    args, rest = parser.parse_known_args(argv)
    command = args.command

    if command == "run":
        from .runner import main as target
    elif command == "compile":
        from .spec2argo import main as target
    elif command == "gate":
        from .pipeline_cli import main as target
        rest = ["gate"] + rest
    elif command == "state":
        from .pipeline_cli import main as target
        rest = ["state"] + rest
    elif command == "reconcile":
        from .pipeline_cli import main as target
        rest = ["reconcile-journal"] + rest
    elif command == "service":
        from .service import main as target
    else:                                       # pragma: no cover - argparse 已拦截
        parser.error(f"未知命令: {command}")
        return 2

    return target(rest)


if __name__ == "__main__":
    sys.exit(main())
