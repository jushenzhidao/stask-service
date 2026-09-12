"""SPEC 与代码事实的契约守卫（P1）。

**为什么需要这个文件**：代码侧已有 6 道机械守卫（ruff/mypy 自检、emoji、孤儿函数、
孤儿类、``SELECT *`` 白名单），但**没有一条**校验 ``docs/SPEC.md`` 的声明与代码一致。
2026-09-13 实测的后果：SPEC 的端点清单里写着早就不存在的 ``POST /ops/reconcile/run``、
验收标准仍在描述已删除的计费与对账子系统、字段表含已废字段、并引用了三个不存在的
文件（``docs/stask-service-design.md`` / ``app/services/providers/`` / ADR-003）——
而三道门禁全绿，漂移静默累积了 6 天，没有任何信号。

本文件把那句「SPEC 是契约」的声明变成可执行断言：

1. §5 端点清单 ⇄ ``app.routes`` **双向**一致（含方法归属）；
2. SPEC 正文不得残留已废弃概念；
3. 文档（``docs/*.md`` + README）引用的仓库内路径与 ``.py`` 文件必须真实存在；
4. §4 声明的依赖版本必须来自 ``pyproject.toml``；
5. 服务版本号只有一处事实源。

**口径**：``§13 变更记录`` 是**追加式历史**，一律豁免——它按设计就要保留
``freeze_amount`` / ``SUBMITTED`` / 已删除文件等字样作为审计追溯。

每个守卫都带一条「解析出的条目数下限」断言：若 SPEC 结构被改动导致解析失效，
守卫会**大声失败**而不是静默变成空跑（后者是这类结构扫描最常见的假绿灯）。
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "docs" / "SPEC.md"
PYPROJECT_PATH = ROOT / "pyproject.toml"
ENV_EXAMPLE_PATH = ROOT / ".env.example"

#: 变更记录标题——用它把「正文」（受契约约束）与「历史」（豁免）切开
_CHANGELOG_HEADING = "## 13. 变更记录"

#: FastAPI/Starlette 自带路由，不是本服务的对外契约，SPEC 不登记
_FRAMEWORK_ROUTES = frozenset(
    {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}
)

#: 已随「去计费化 / 状态机收敛 / 对账删除」废弃的概念。
#: 它们再次出现在 SPEC 正文里几乎只有一个原因：有人把旧稿粘了回来。
_RETIRED_TOKENS = (
    "SUBMITTED",              # 已废状态：落库即 QUEUED，无二段提交
    "NOT_START",              # 已废状态
    "reconcile",              # 主动对账子系统（v0.2 起整体删除）
    "inflight_slot",          # 占槽标记已改为 slot_flags 掩码
    "freeze_amount",          # 计费时代列
    "ref_price",              # 计费时代配置
    "billing_http",           # 已删除的服务分层
    "billing_newapi",         # 同上
    "stask-service-design.md",   # 不存在（已被 PRD/ARCH 取代）
    "app/services/providers",    # 不存在（已删除的目录）
)

#: 视为「仓库内路径引用」的前缀（只看反引号包裹的 token）
_REF_PREFIXES = (
    "docs/", "app/", "deploy/", "tests/", "scripts/",
    "gunicorn.conf.py", "pyproject.toml", "docker-compose.yml", "Dockerfile",
)

_VERSION_TOKEN_RE = re.compile(r"\b\d+\.\d+(?:\.\d+)?\b")


def _spec_text() -> str:
    return SPEC_PATH.read_text(encoding="utf-8")


def _spec_body() -> str:
    """§1~§12：排除追加式变更记录。"""
    body, sep, _ = _spec_text().partition(_CHANGELOG_HEADING)
    assert sep, f"SPEC 缺少 `{_CHANGELOG_HEADING}` 章节——本守卫依赖它划分正文与历史"
    return body


def _section(markdown: str, start: str, end: str) -> str:
    """取 ``start`` 到 ``end`` 之间的片段（两个标记都必须存在）。"""
    _, sep, rest = markdown.partition(start)
    assert sep, f"SPEC 缺少章节标记 `{start}`——守卫定位失败，请勿随意改标题"
    part, sep, _ = rest.partition(end)
    assert sep, f"SPEC 在 `{start}` 之后找不到 `{end}`——守卫定位失败"
    return part


def _table_rows(section: str) -> list[list[str]]:
    """把 markdown 表格行切成单元格。

    只认以 ``|`` 开头的行——**正文段落里的路径与版本号不是契约条目**
    （正文有 ``/ops/*`` 这类泛指、也有「三层」「4KB」这类描述性数字）。
    """
    rows: list[list[str]] = []
    for line in section.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        rows.append(cells)
    return rows


def _spec_endpoints() -> dict[str, set[str]]:
    """从 §5 解析 ``{path: {METHOD, ...}}``。

    路径取反引号包裹、以 ``/`` 开头的 token；方法取首列里的全大写词
    （``POST/PUT`` → ``{POST, PUT}``；``其他方法`` 这种行文没有全大写词，自然跳过）。
    """
    section = _section(_spec_body(), "## 5. API 端点清单", "## 6. 数据模型")
    table: dict[str, set[str]] = {}
    for cells in _table_rows(section):
        if len(cells) < 2:
            continue
        methods = set(re.findall(r"\b[A-Z]{3,7}\b", cells[0]))
        for path in re.findall(r"`(/[^`]+)`", "|".join(cells)):
            table.setdefault(path, set()).update(methods)
    return table


def _actual_routes() -> dict[str, set[str]]:
    """真实注册的 ``{path: {METHOD, ...}}``（框架自带路由除外）。"""
    from app.main import app

    routes: dict[str, set[str]] = {}
    for route in app.routes:
        path = getattr(route, "path", "")
        if not path or path in _FRAMEWORK_ROUTES:
            continue
        methods = getattr(route, "methods", None) or set()
        routes.setdefault(path, set()).update(methods)
    return routes


# ---------------------------------------------------------------------------
# 1. 端点清单 ⇄ 真实路由
# ---------------------------------------------------------------------------


def test_spec_endpoint_table_matches_routes() -> None:
    """§5 端点清单必须与 ``app.routes`` 双向一致。

    单向检查会漏掉一半：只查「SPEC 写的都存在」漏掉**契约缺口**（新增了端点没登记），
    只查「存在的都写了」漏掉**幽灵端点**（删了端点没清文档）。两者都出现过。
    """
    declared = _spec_endpoints()
    actual = _actual_routes()

    # 下限只用来抓「解析器失效」——正常是 18 个左右。取 10 而不是贴着实际值，
    # 是为了让**合法的端点删减**不至于被误报成「守卫定位失效」。
    assert len(declared) >= 10, (
        f"只从 §5 解析出 {len(declared)} 个端点，守卫定位可能已失效（表格结构变了？）"
    )

    ghost = sorted(set(declared) - set(actual))
    missing = sorted(set(actual) - set(declared))

    assert not ghost, (
        "SPEC §5 登记了实际不存在的端点（幽灵端点，照它开发会 404）: " + ", ".join(ghost)
    )
    assert not missing, (
        "实际注册了但 SPEC §5 未登记的端点（契约缺口）: " + ", ".join(missing)
    )

    wrong_method: dict[str, tuple[list[str], list[str]]] = {}
    for path, methods in declared.items():
        if methods and not methods <= actual[path]:
            wrong_method[path] = (sorted(methods), sorted(actual[path]))
    assert not wrong_method, (
        f"SPEC §5 的方法列与真实路由不符（key=path, 值=(SPEC声明, 真实)）: {wrong_method}"
    )


# ---------------------------------------------------------------------------
# 2. 正文不得残留已废概念
# ---------------------------------------------------------------------------


def test_spec_body_has_no_retired_concepts() -> None:
    """已删除的机制不得回到 SPEC 正文（§13 历史记录豁免）。

    这类残留最难发现：它读起来完全自洽（一个完整的计费公式、一条完整的对账链路），
    只有跟代码对照才知道标的物早就不存在了。
    """
    body = _spec_body()
    lines = body.splitlines()

    offenders = {
        token: [i for i, line in enumerate(lines, 1) if token in line]
        for token in _RETIRED_TOKENS
    }
    offenders = {token: hits for token, hits in offenders.items() if hits}

    assert not offenders, (
        "SPEC 正文出现已废弃概念（行号）——它们是已删除机制的残影，"
        f"照它们实现会写到不存在的接口上: {offenders}"
    )


# ---------------------------------------------------------------------------
# 3. 文档引用的仓库内文件必须存在
# ---------------------------------------------------------------------------

#: 文档里**按设计**会提到不存在文件名的地方：历史快照类文档。
#: 它们不是「该修的死引用」，而是「记录当时状态」；正文里已就地标注标的文件不存在。
_HISTORICAL_DOCS = frozenset({
    "OPTIMIZATION.md",   # 2026-09-03 快照，按设计保留了 submit_v2.py 这个不存在的文件名
})

#: 文档引用的**第三方库内部文件**（不在本仓，也不该在）。每项都要写明是哪个库的哪一层。
_EXTERNAL_PY_REFS = frozenset({
    "list_schedule_source.py",   # taskiq_redis 内部实现
    "run.py",                    # taskiq 内部实现
})


def _repo_py_basenames() -> set[str]:
    """本仓自有 Python 文件的 basename 集合（不递归 .venv 等目录）。"""
    names = {p.name for p in ROOT.glob("*.py")}
    for sub in ("app", "tests", "scripts"):
        names |= {p.name for p in (ROOT / sub).rglob("*.py")}
    return names


def _doc_files() -> list[Path]:
    """受本守卫约束的文档：``docs/*.md``（顶层，不含 ``decisions/`` 这类历史裁决）+ README。"""
    files = sorted(SPEC_PATH.parent.glob("*.md"))
    readme = ROOT / "README.md"
    if readme.exists():
        files.append(readme)
    return [p for p in files if p.exists()]


def test_docs_do_not_reference_missing_repo_files() -> None:
    """文档引用的仓库内路径 / ``.py`` 文件必须真实存在。

    2026-09-13 复核实测：``docs/OPTIMIZATION.md`` 让读者去找 ``submit_v2.py``——该文件
    在本仓**不存在**（没有实现、也没有任何引用）。死引用比没有文档更糟：读者会以为有
    更详细的源头可查，实际找到的是 404。同日 SPEC 头部也曾引用
    ``docs/stask-service-design.md`` 与 ``app/services/providers/``，是同一类问题。

    两类豁免**都必须写明理由**，否则下一个人分不清「该修的漏网」与「故意保留」：

    - ``_HISTORICAL_DOCS``：历史快照类文档，按设计保留当时（现已失效）的文件名；
    - ``_EXTERNAL_PY_REFS``：第三方库内部文件，本来就不在本仓。

    **行号与章节后缀不参与判定**（``app/redis.py:39``、``docs/SPEC.md §10.1``）：
    它们描述的是「当时读到的那一行/那一节」，会随代码演进而失效，守卫不了也不该守。
    """
    repo_py = _repo_py_basenames()
    checked_paths = 0
    checked_files = 0
    offenders: list[str] = []

    for path in _doc_files():
        if path.name in _HISTORICAL_DOCS:
            continue
        text = path.read_text(encoding="utf-8")
        if path.name == SPEC_PATH.name:
            text = text.partition(_CHANGELOG_HEADING)[0]   # 追加式历史豁免

        # (a) 带目录前缀的仓库内路径
        for token in sorted(set(re.findall(r"`([^`\n]+)`", text))):
            token = token.strip()
            if not token.startswith(_REF_PREFIXES):
                continue
            if any(ch in token for ch in "*{}<>|"):
                continue
            token = re.split(r"[:\s]", token, maxsplit=1)[0]   # 去掉 `:行号` / ` §章节` / `::用例名`
            if not token:
                continue
            checked_paths += 1
            if not (ROOT / token).exists():
                offenders.append(f"{path.name} -> `{token}`")

        # (b) 裸 .py 文件名——`submit_v2.py` 正是这种没有目录前缀的形态，
        #     只查 (a) 会整类漏掉
        for token in sorted(set(re.findall(r"`([^`\n]*\.py)(?::[\d\-, ]+)?`", text))):
            base = token.strip().rsplit("/", 1)[-1]
            if base in _EXTERNAL_PY_REFS:
                continue
            checked_files += 1
            if base not in repo_py:
                offenders.append(f"{path.name} -> `{base}`")

    # 下限只抓「解析器失效」，取低于实际值一个量级，避免合法的删改被误报成定位失效
    assert checked_paths >= 5, (
        f"只解析出 {checked_paths} 个路径引用，守卫定位可能已失效（引用写法变了？）"
    )
    assert checked_files >= 10, (
        f"只解析出 {checked_files} 个 .py 引用，守卫定位可能已失效（引用写法变了？）"
    )
    assert not offenders, (
        "以下文档引用了本仓不存在的文件（死引用会让读者去找 404）。"
        "订正引用，或加进 _HISTORICAL_DOCS / _EXTERNAL_PY_REFS 并写明理由：\n  "
        + "\n  ".join(sorted(set(offenders)))
    )


# ---------------------------------------------------------------------------
# 4. §4 声明的依赖版本必须来自 pyproject.toml
# ---------------------------------------------------------------------------


def test_spec_dependency_versions_come_from_pyproject() -> None:
    """§4 技术架构表里每个版本号都必须能在 ``pyproject.toml`` 找到。

    不做「库名 → 依赖名」映射（那张别名表本身就会漂移），只要求**版本号原样出现**。
    这样既抓住了真实漂移（SPEC 曾长期写着 redis 5.2.1，而 pyproject 是 8.1.0），
    又不会因为表格换行/措辞调整而误报。
    """
    section = _section(_spec_body(), "## 4. 技术架构", "## 5. API 端点清单")
    pyproject_text = PYPROJECT_PATH.read_text(encoding="utf-8")

    declared: set[str] = set()
    orphan: set[str] = set()
    for cells in _table_rows(section):
        if len(cells) < 3:
            continue
        for token in _VERSION_TOKEN_RE.findall(cells[2]):
            declared.add(token)
            if token not in pyproject_text:
                orphan.add(token)

    assert len(declared) >= 10, (
        f"只从 §4 解析出 {len(declared)} 个版本号，守卫定位可能已失效（列序变了？）"
    )
    assert not orphan, (
        "SPEC §4 声明的版本在 pyproject.toml 里找不到（表已漂移）: "
        + ", ".join(sorted(orphan))
    )


# ---------------------------------------------------------------------------
# 5. 服务版本号单一事实源
# ---------------------------------------------------------------------------


def test_service_version_has_single_source() -> None:
    """服务版本号只允许有一处手写：``app/__init__.py`` 的 ``__version__``。

    修复前是**三头马车**：``pyproject.toml`` 写 0.1.0、``config.py`` 写 0.2.0、
    ``.env.example`` 写 0.2.0，且 Dockerfile/compose 都不注入 ``APP_VERSION``——
    于是 ``/healthz``、OpenAPI ``version``、logfire ``service_version`` 报的是哪个
    全凭运气，而没有任何东西会为此报错。
    """
    import app
    from app.config import Settings

    single = app.__version__
    problems: list[str] = []

    project = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))["project"]
    version_attr = (
        tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
        .get("tool", {})
        .get("setuptools", {})
        .get("dynamic", {})
        .get("version", {})
        .get("attr")
    )

    if "version" in project:
        problems.append(
            "pyproject.toml 的 [project] 又写死了 version——应交给 dynamic 从 app.__version__ 派生"
        )
    if "version" not in project.get("dynamic", []):
        problems.append('pyproject.toml 的 [project] 缺少 dynamic = ["version"]')
    if version_attr != "app.__version__":
        problems.append(
            f"[tool.setuptools.dynamic].version.attr 应为 'app.__version__'，实为 {version_attr!r}"
        )

    default = Settings.model_fields["app_version"].default
    if default != single:
        problems.append(f"Settings.app_version 默认值 {default!r} != app.__version__ {single!r}")

    hit = re.search(r"^APP_VERSION=(.+?)\s*(?:#.*)?$", ENV_EXAMPLE_PATH.read_text(encoding="utf-8"), re.M)
    if hit is None:
        problems.append(".env.example 缺少 APP_VERSION= 行")
    elif hit.group(1).strip() != single:
        problems.append(
            f".env.example 的 APP_VERSION={hit.group(1).strip()} != app.__version__ {single!r}"
        )

    assert not problems, "服务版本号不是单一事实源：\n  - " + "\n  - ".join(problems)
