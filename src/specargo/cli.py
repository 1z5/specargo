"""统一 CLI：把各模块的命令行入口收到一个 ``specargo`` 命令下。

子命令只是转发到既有模块的 ``main()``（行为完全一致），
让「安装后只有一个入口」成立::

    specargo run       pipelines/etl-demo.yaml --state-db s.db --workdir out
    specargo compile   pipelines/etl-demo.yaml -o wf.yaml
    specargo gate      --stage verify --artifact out/x.json --checks '["file_nonempty"]'
    specargo state     put --state-db s.db --dataset ds --partition 1 --stage load --status verified
    specargo reconcile --run-id r --partitions-file parts.json --final-stage load
    specargo service   --port 8080

错误呈现（``friendly_errors``）：``SpecError`` 是"你写的东西不对"，不是"跑挂了"——
两者必须能从退出码和输出上分开，否则 CI 里"spec 拼错"和"数据没过检"会长得一样。
"""
from __future__ import annotations

import argparse
import functools
import sys
from typing import Callable

# 退出码约定（与 pipeline_cli 的 gate/state 子命令一致）：
#   0 = 成功；1 = 执行失败（管线失败 / 对账不过 / 检查不过）；2 = 输入错误（spec / 用法）
EXIT_USAGE = 2


def friendly_errors(main: Callable[..., int]) -> Callable[..., int]:
    """把 ``SpecError`` / 文件读不到变成一行可行动的 stderr 报错 + 退出码 2。

    不捕获其它异常：真正的 bug 应该带完整 traceback 暴露出来，别被藏着。
    """

    @functools.wraps(main)
    def wrapper(*args, **kwargs) -> int:
        from .spec import SpecError

        try:
            return main(*args, **kwargs)
        except SpecError as exc:
            print(f"spec 错误: {exc}", file=sys.stderr)
            return EXIT_USAGE
        except FileNotFoundError as exc:
            print(f"文件不存在: {exc.filename or exc}", file=sys.stderr)
            return EXIT_USAGE
        except IsADirectoryError as exc:
            print(f"这是一个目录，不是文件: {exc.filename or exc}", file=sys.stderr)
            return EXIT_USAGE

    return wrapper


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

    raw = list(sys.argv[1:] if argv is None else argv)
    if not raw:
        parser.print_help()
        return EXIT_USAGE
    # 只让 argparse 解析"命令名"这一个 token，其余参数原样透传给子命令自己的解析器——
    # 否则 ``specargo gate --help`` 会被这一层的 subparser 吞掉，用户看不到 gate 的选项。
    args, _ = parser.parse_known_args(raw[:1])
    command, rest = args.command, raw[1:]

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
