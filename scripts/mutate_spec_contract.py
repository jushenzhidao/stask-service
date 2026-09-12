"""变异自证：逐条破坏被测契约，确认对应守卫**真的会红**。

守卫写完就绿只说明「当前一致」，不说明「将来不一致时会拦」。这里对每条守卫
注入一个必然违规的变异，断言它确实失败，然后还原。任一条变异下守卫仍是绿的，
就说明该守卫是空跑的假绿灯。

**节点路径失效必须当成「变异无效」，不能当成「如期变红」。** pytest 的退出码
4（用法错）与 5（没收集到用例）都不是「测试失败」：节点名写错时它只会说
"no tests ran" 并以 4 退出。若把「退出码 != 0」一律读成「守卫拦住了」，
节点一改名就会得到一次**假绿**——本脚本的早期版本正是这样，写在这里免得再犯。
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "SPEC.md"
PYPROJECT = ROOT / "pyproject.toml"

PY = str(ROOT / ".venv" / "bin" / "python")

#: pytest 退出码：0 通过 / 1 失败 / 2 中断 / 3 内部错 / 4 用法错 / 5 没收集到用例
_INVALID_CODES = frozenset({4, 5})


def run_test(node: str) -> tuple[bool, int]:
    """跑单个用例，返回 ``(是否通过, 退出码)``。"""
    proc = subprocess.run(
        [PY, "-m", "pytest", node, "-q", "--no-header", "-p", "no:cacheprovider"],
        cwd=ROOT, capture_output=True, text=True,
    )
    return proc.returncode == 0, proc.returncode


def mutate(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text, f"变异锚点在 {path.name} 里找不到（锚点已失效，请重新指向）：{old!r}"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


CASES = [
    (
        "幽灵端点（SPEC 登记了不存在的端点）",
        SPEC,
        "| POST | `/ops/sweep/stale` |",
        "| POST | `/ops/ghost/endpoint` |",
        "tests/test_spec_contract.py::test_spec_endpoint_table_matches_routes",
    ),
    (
        "契约缺口（新增端点没登记进 SPEC）",
        SPEC,
        "### 用户面",
        "### 用户面\n\n| GET | `/admin/api/secret` | 新端点 | - | `200` |",
        "tests/test_spec_contract.py::test_spec_endpoint_table_matches_routes",
    ),
    (
        "方法漂移（SPEC 声明的方法真实不存在）",
        SPEC,
        "| GET | `/healthz/live` |",
        "| PATCH | `/healthz/live` |",
        "tests/test_spec_contract.py::test_spec_endpoint_table_matches_routes",
    ),
    (
        "已废概念回流（正文出现 SUBMITTED）",
        SPEC,
        "| AC-12 | 执行 |",
        "| AC-12 | 执行 | 已废状态 SUBMITTED 的残留说明 |",
        "tests/test_spec_contract.py::test_spec_body_has_no_retired_concepts",
    ),
    (
        "已废概念回流（正文出现 reconcile_pending）",
        SPEC,
        "| AC-01 | 提交 |",
        "| AC-01 | 提交 | 对账扫描 reconcile_pending 说明 |",
        "tests/test_spec_contract.py::test_spec_body_has_no_retired_concepts",
    ),
    (
        "死路径引用（引用了不存在的文件）",
        SPEC,
        "> 状态：已确认",
        "> 状态：已确认\n> 详见 `docs/nonexistent-design.md`",
        "tests/test_spec_contract.py::test_docs_do_not_reference_missing_repo_files",
    ),
    (
        "版本漂移（§4 声明 pyproject 里没有的版本）",
        SPEC,
        "| Redis | redis (asyncio) | 8.1.0 |",
        "| Redis | redis (asyncio) | 5.2.1 |",
        "tests/test_spec_contract.py::test_spec_dependency_versions_come_from_pyproject",
    ),
    (
        "版本三头马车（pyproject 又写死 version）",
        PYPROJECT,
        'dynamic = ["version"]',
        'version = "0.1.0"',
        "tests/test_spec_contract.py::test_service_version_has_single_source",
    ),
]


def main() -> int:
    failures: list[str] = []
    print(f"变异测试：{len(CASES)} 条\n")
    for label, path, old, new, node in CASES:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            backup = Path(tmp.name)
        shutil.copy2(path, backup)
        try:
            mutate(path, old, new)
            passed, code = run_test(node)
        finally:
            shutil.copy2(backup, path)
            backup.unlink()

        if code in _INVALID_CODES:
            failures.append(label)
            print(f"  无效 {label}")
            print(f"        → {node.split('::')[1]}: 节点跑不起来（退出码 {code}），"
                  f"这条变异没有验证任何东西")
            continue

        ok = not passed
        mark = "OK  " if ok else "漏  "
        verdict = "仍绿（守卫失效）" if passed else "如期变红"
        if not ok:
            failures.append(label)
        print(f"  {mark} {label}")
        print(f"        → {node.split('::')[1]}: {verdict}")

    print()
    if failures:
        print(f"变异未通过 {len(failures)} 条：")
        for item in failures:
            print("  -", item)
        return 1
    print(f"全部 {len(CASES)} 条变异都被拦住，且文件已还原。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
