"""文档数字必须与真实用例数一致——本套件就是这条规则的守卫。

为什么值得一个测试：README / ROADMAP / STATUS 里有 8 处手写的"用例数"，
每加一套用例要改 8 个地方，漏改一次就变成"文档在撒谎"（而这个仓库最核心的自我要求
就是"已验证必须能指出证据"）。已经因为漏改发生过两次手工返工，所以让它自动化：

- 每个 ``tests/test_*.py`` 的用例数（AST 数 ``def test_``）必须与 README、STATUS 表格一致；
- 总例数必须与 README 的 4 处、ROADMAP 的 1 处、STATUS 的 2 处一致。

任何一处不匹配 → 本套件失败，提示"文档数字过期"，而不是把错数字发出去。
（格式如果变了，测试会以"找不到预期表述"失败，同样需要显式更新，不会静默放过。）
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_FRAMEWORK_TESTS = sorted((ROOT / "tests").glob("test_*.py"))
_EXAMPLE_TESTS = sorted((ROOT / "examples" / "tts-asr" / "tests").glob("test_*.py"))


def _count_cases(path: Path) -> int:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return sum(1 for node in tree.body
               if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"))


def _per_suite() -> dict[str, int]:
    counts = {str(path.relative_to(ROOT)): _count_cases(path)
              for path in _FRAMEWORK_TESTS + _EXAMPLE_TESTS}
    assert counts, "没有找到任何测试文件"
    return counts


def _totals(per_suite: dict[str, int]) -> tuple[int, int]:
    framework = sum(n for name, n in per_suite.items()
                    if not name.startswith("examples/"))
    example = sum(n for name, n in per_suite.items()
                  if name.startswith("examples/"))
    return framework, example


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_every_suite_listed_with_correct_count_in_readme():
    """README「已验证」必须逐套件列出，且例数与真实一致。

    匹配"提到套件的那一行里有 `<N> 例`"：允许括号写法（``（11 例）``）与
    反引号后直写（`` `tests/x.py` 11 例``）两种，别因为排版变化就误报。
    """
    readme = _text("README.md")
    for name, count in _per_suite().items():
        assert name in readme, f"{name} 未在 README 出现（已验证清单应逐套件列出）"
        pattern = re.compile(rf"{re.escape(name)}[^\n]*?\b{count}\s*例")
        assert pattern.search(readme), (
            f"{name} 的实际例数是 {count}，但 README 里该套件附近没有「{count} 例」"
            f"——请同步更新已验证清单（本套件就是防止漏改）")


def test_every_suite_listed_with_correct_count_in_status_table():
    """STATUS §2.1 表格逐行列出每个套件，且例数一致。"""
    status = _text("docs/STATUS.md")
    for name, count in _per_suite().items():
        row = re.search(rf"^\| `{re.escape(name)}` \| (\d+) \|", status, re.M)
        assert row, f"STATUS §2.1 表格缺少 {name} 这一行"
        assert int(row.group(1)) == count, (
            f"STATUS 表格里 {name} 是 {row.group(1)} 例，实际 {count} 例")


def test_total_counts_match_every_documented_place():
    """README 4 处 + ROADMAP 1 处 + STATUS 2 处的总例数必须全部一致。"""
    framework, example = _totals(_per_suite())
    total = framework + example
    doc = _text("README.md")
    roadmap = _text("docs/ROADMAP.md")
    status = _text("docs/STATUS.md")

    expected_total = f"{total} 个离线用例"
    expected_framework = f"框架 {framework}"
    places = [
        ("README badge 注释", doc, f"{expected_total}（{expected_framework} + 领域示例 {example}）"),
        ("README 快速开始", doc, f"离线测试：{total} 例（{expected_framework} + 领域示例 {example}）"),
        ("README 已验证标题", doc, f"**已验证（{expected_total} = {expected_framework} + 领域示例 {example}，+ 集群 E2E）**"),
        ("README 目录说明", doc, f"离线测试（{expected_framework} 例）；领域示例测试在 examples/tts-asr/tests/（{example} 例）"),
        ("ROADMAP 套件合计", roadmap, f"离线套件合计 **{total} 例**（{expected_framework} + 领域示例 {example}）"),
        ("STATUS 一句话结论", status, f"**{expected_total} + 本地 E2E + kind 真实集群 E2E"),
        ("STATUS 2.1 标题", status, f"### 2.1 离线套件：{total} 例全绿（{expected_framework} + 领域示例 {example}）"),
    ]
    for label, text, expected in places:
        assert expected in text, (
            f"{label} 与实际用例数不符：应为「{expected}」，实际框架 {framework} + 示例 "
            f"{example} = {total} 例。请把该处数字改过来（本套件就是防止漏改）。")


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
    print(f"\n{'文档数字一致性测试全部通过' if not failures else str(failures) + ' 个失败'}")
    sys.exit(1 if failures else 0)
