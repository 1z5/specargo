"""文档与清单的一致性守卫：照文档做必须真的能跑。

为什么值得一套测试：部署文档里写错的命令/键名不会让 CI 变红——它会安静地等到
第一个照着文档操作的人踩坑（典型的两个已发生案例）：

1. `kubectl create secret ... --from-literal=accessKey=...`，而
   `k8s/artifact-repository.yaml` 引用的是 `key: accesskey`（小写）——键名对不上，
   controller 读不到凭据，报错指向"取数失败"，排查成本极高。
2. 文档里写 `pipeline_cli reconcile`，而真实子命令叫 `reconcile-journal`——直接
   `invalid choice`。

本套件把"文档里出现的子命令 / Secret 键 / 清单路径"与真实实现对齐。
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
_SRC = ROOT / "src"
if str(_SRC) not in sys.path and not importlib.util.find_spec("specargo"):
    sys.path.insert(0, str(_SRC))

from specargo.pipeline_cli import build_parser                # noqa: E402


def _text(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _subcommands() -> set[str]:
    """从真实 parser 里取子命令集合（文档校验的"事实来源"）。"""
    parser = build_parser()
    actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    assert actions, "pipeline_cli 没有子命令"
    return set(actions[0].choices)


def test_documented_cli_subcommands_exist():
    """文档里出现的 `pipeline_cli <sub>` 必须是真实子命令。"""
    real = _subcommands()
    pattern = re.compile(r"pipeline_cli\s+([a-z][a-z0-9-]*)")
    for doc in ("docs/DEPLOYMENT.md", "docs/RUNBOOK.md", "README.md", "CHANGELOG.md"):
        path = ROOT / doc
        if not path.exists():
            continue
        for match in pattern.finditer(_text(doc)):
            sub = match.group(1)
            assert sub in real, (
                f"{doc} 里写了 `pipeline_cli {sub}`，但真实子命令是 {sorted(real)}"
                f"（照文档做会 invalid choice）")


def test_documented_secret_keys_match_artifact_repository():
    """部署文档让我们建的 Secret 键名，必须与 artifact repository 引用的键名一致。"""
    manifest = yaml.safe_load_all(_text("k8s/artifact-repository.yaml"))
    repo = next(d for d in manifest if d and d.get("kind") == "ConfigMap")
    config = yaml.safe_load(repo["data"]["default"])["s3"]
    referenced = {config["accessKeySecret"]["key"], config["secretKeySecret"]["key"]}

    doc = _text("docs/DEPLOYMENT.md")
    created = set(re.findall(r"--from-literal=([A-Za-z_][A-Za-z0-9_-]*)=", doc))
    assert created, "DEPLOYMENT.md 里没找到 create secret 的 --from-literal"
    assert created == referenced, (
        f"DEPLOYMENT.md 建的 Secret 键 {sorted(created)} 与 artifact-repository.yaml "
        f"引用的键 {sorted(referenced)} 不一致——照文档做 controller 读不到凭据")


def test_documented_manifest_paths_exist():
    """文档里引用的 k8s/*.yaml 与 docs/*.md 路径必须真实存在。"""
    for doc in ("docs/DEPLOYMENT.md", "docs/RUNBOOK.md", "README.md"):
        text = _text(doc)
        for rel in re.findall(r"`((?:k8s|docs)/[A-Za-z0-9_.-]+\.(?:yaml|md))`", text):
            assert (ROOT / rel).exists(), f"{doc} 引用了不存在的文件: {rel}"


def test_documented_objstore_env_semantics():
    """`objstore_env` 的值是**字面量**（编译进容器 env），文档不能写成像 Secret 引用。"""
    from specargo.runtime import RuntimeConfig
    cfg = RuntimeConfig.from_mapping({"objstore_env": {"OBJSTORE_ACCESS_KEY": "literal-value"}})
    assert cfg.objstore_env == {"OBJSTORE_ACCESS_KEY": "literal-value"}
    # 文档必须明确"值是字面量"，而不是"环境变量名到 Secret 的映射"
    doc = _text("docs/DEPLOYMENT.md")
    assert "字面量" in doc or "直接注入" in doc, (
        "DEPLOYMENT.md 的 objstore_env 示例必须说明值会被字面量注入容器"
        "（不是从 Secret 取值），否则照做会把凭据名当成凭据值")


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"  FAIL {name}: {exc}")
            except Exception as exc:                     # noqa: BLE001
                failures += 1
                print(f"  ERROR {name}: {exc!r}")
    print(f"\n{'文档/清单一致性测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
