"""代码结构与房规门禁（`make check` 之外那一道）。

这一族用例都不测行为，只测「代码/文档/配置有没有违反某条硬规则」。放在
一起是因为它们的失败含义相同：**不是功能坏了，是有东西偷偷漂移了**。

从 `tests/test_misc.py` 切出（2026-09-13）——那里原本混着行为测试与门禁，
加了十几条门禁之后变成了杂物间。
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_mypy_clean():
    """把类型检查做成一个测试用例——CI 里跑 pytest 就等于跑了 mypy。"""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "app/"],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_ruff_clean():
    proc = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "app", "tests"],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_no_emoji_in_source():
    """团队 P0 规则：源码、文档与交付配置中不得出现 emoji 作为功能标识。

    扫描面刻意覆盖**所有随仓库交付、且有人读的文本**：``app/**/*.py``、
    ``tests/**/*.py``、``docs/**/*.md``、``app/static/**/*``（单文件看板）、
    ``deploy/**/*``（nginx 样例）、``.env.example``。

    2026-09-13 补上后两个：此前 ``deploy/`` 与 ``app/static/`` 不在面内，
    于是「同一份规则」在这两处无人执行（``deploy/nginx.conf`` 里就留了一个警告符号）。

    **不扫 ``.workbuddy/``**：那是 agent 的工作记忆（含 `.env` 式的本地状态），
    不是交付物，且它自身允许用符号做视觉标记。
    """

    pattern = re.compile(
        "[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF"
        "\U0001FA00-\U0001FAFF\U0001F000-\U0001F0FF]"
    )
    offenders = []
    for path in list(ROOT.glob("app/**/*.py")) + list(ROOT.glob("tests/**/*.py")) \
            + list(ROOT.glob("docs/**/*.md")) + list(ROOT.glob("app/static/**/*")) \
            + list(ROOT.glob("deploy/**/*")) + [ROOT / ".env.example"]:
        text = path.read_text(encoding="utf-8")
        if pattern.search(text):
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"emoji found in: {offenders}"


#: 允许调用 ``taskstore.get``（= ``SELECT *``，含 request_body / upstream_response
#: 大字段）的模块白名单。**只有真正需要原始体的路径可以在这里**：
#: - ``execute.py``：worker 要把 request_body 发给上游；
#: - ``flow.py``：终态回放要把 upstream_response 原文还给客户端。
#:
#: 其余读取一律走 ``get_meta`` 投影。这条不变式（设计 §6 第 6 条）用测试
#: 钉住而不是靠人记得——热路径误用 get 会静默拉回每条最多 12MB 的列，
#: 功能完全正常、只是把 DB 带宽烧穿，评审很难看出来。
_GET_ALLOWLIST = {"services/execute.py", "services/flow.py"}


def test_select_star_only_in_allowlisted_modules():
    """``taskstore.get``（SELECT *）只允许出现在白名单模块里。

    背景（实测）：到期通道与批次放行曾是每任务一次 ``SELECT *``，而它们只用
    到几个标量字段。一轮 200 条到期放行的额外 DB 流量，按提交体中位数计约
    数 MB、按 2MB 上限计约 800MB——纯属浪费，且**没有任何行为测试会发现**
    （返回值完全正确）。故用结构断言把读取入口锁死。
    """

    call = re.compile(r"taskstore\.get\(")
    offenders = []
    for path in ROOT.glob("app/**/*.py"):
        rel = str(path.relative_to(ROOT / "app"))
        if rel in _GET_ALLOWLIST:
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if call.search(line):
                offenders.append(f"{rel}:{lineno}")
    assert not offenders, (
        "这些位置用了 taskstore.get（SELECT *），应改为 get_meta 投影；"
        "若确实需要原始体，请把它加进 _GET_ALLOWLIST 并写明理由：\n  "
        + "\n  ".join(offenders)
    )


#: 允许「生产代码无调用方」的 services 公开函数，**每条都必须写明理由**。
#: 白名单是豁免清单，不是垃圾桶——不写理由就等于默许死代码堆积。
_ORPHAN_ALLOWLIST = {
    # 测试专用的缓存清理钩子。Redis 层靠 TTL 自然过期，进程内层只有测试需要
    # 在用例之间重置。docstring 已声明「测试用」。
    "clear_cache",
    # 为未实现的看板需求（PRD R-21/R-22）预留的读取入口，口径已对齐
    # batch_waiting()。见其 docstring。
    "batch_counts_by_model",
}


def test_no_orphan_service_functions():
    """``app/services`` 里不得存在「没人调用」的公开函数。

    这一类缺陷（实现了但没有任何调用方）**测试发现不了**——被测函数本身是
    绿的，缺的只是调用点。实测过一次：``artifacts.parse_for_store`` 是文档里
    写明的「唯一入口」，但 ``execute.py`` 内联了同样的四个字段，于是
    ``parse_for_store`` 成了只被测试调用的死代码。危害不是「多几行」，
    而是**同一个契约有了两份实现**：改了一份、另一份静默漂移，
    而测试盯着的恰好是不跑的那份。

    判据：模块外无引用，且模块内除定义行外也无引用（路由与 cron 任务用
    装饰器注册，故有装饰器的一律跳过）。测试文件的引用**不算数**——
    「只被测试调用」正是本测试要抓的信号。

    **匹配口径要按「名字是否有歧义」分两档**（这里踩过两次坑）：

    - 裸名匹配（默认）：函数名在全仓唯一时，`from ...idem import new_task_id`
      这类导入后直呼的名字也算调用。早期的「只认模块限定」版本会因为
      漏掉这种形式而**大量误报**。
    - 限定匹配：名字在多个模块里都有同名定义时（如 `stats` 同时存在于
      `batching` / `dispatch` / `ops`），裸名无法判断指向谁，必须写成
      `batching.stats`。早期版本一律按裸名匹配，于是 `ops.py` 的路由处理器
      `stats` 把 `batching.stats` 的孤儿身份盖住了——**同名不同物**是这类
      扫描最常见的假阴性来源。
    """
    import re
    from collections import defaultdict

    app_src = {p: p.read_text(encoding="utf-8")
               for p in (ROOT / "app").rglob("*.py")}

    def top_level_defs(source: str) -> list:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        return [n for n in tree.body
                if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)]

    # 名字 → 定义了它的模块集合（用于判断名字是否有歧义）
    owners: dict[str, set] = defaultdict(set)
    for path, source in app_src.items():
        for node in top_level_defs(source):
            owners[node.name].add(path)

    def import_qualifier(path) -> str:
        """消费者**实际会写**的限定前缀。

        扁平模块 → 文件 stem（``dynconf.get``）；包内子模块 → **包名**
        （``taskstore.get``）。2026-09-13 包化后踩到过：没人写 ``_read.get``，
        那只是包内私有路径——按 stem 限定会把 ``get`` / ``now`` 这类同名函数
        全判成孤儿（假阳性）。
        """
        rel = path.relative_to(ROOT / "app" / "services")
        return rel.parts[0] if len(rel.parts) > 1 else path.stem

    offenders = []
    for path, source in sorted(app_src.items()):
        if "services" not in path.parts:
            continue
        own = re.escape(import_qualifier(path))
        for node in top_level_defs(source):
            if node.name.startswith("_") or node.decorator_list:
                continue
            if node.name in _ORPHAN_ALLOWLIST:
                continue
            bare = r"\b" + re.escape(node.name) + r"\b"
            if len(owners[node.name]) > 1:
                # 有歧义：只认 "<模块名>.<函数名>" 形式的限定引用
                outside = sum(
                    len(re.findall(rf"\b{own}\.{bare}", s))
                    for q, s in app_src.items() if q != path)
            else:
                outside = sum(
                    len(re.findall(bare, s))
                    for q, s in app_src.items() if q != path)
            inside = len(re.findall(bare, source)) - 1     # 减去定义行
            if outside == 0 and inside == 0:
                offenders.append(
                    f"{node.name} ({path.relative_to(ROOT)})")
    assert not offenders, (
        "以下 services 公开函数没有任何调用方。要么接上调用点、要么删除；"
        "确实要保留（如为未实现需求预留）请加进 _ORPHAN_ALLOWLIST 并写明理由：\n  "
        + "\n  ".join(offenders)
    )


#: 允许「生产代码无调用方」的公开类，**每条都必须写明理由**。
#: 2026-09-12 清理后为空，留空是常态：往里加一条，等于承认一份没人用的契约。
_ORPHAN_CLASS_ALLOWLIST: set[str] = set()


def test_no_orphan_public_classes():
    """``app`` 里不得存在「没人调用」的公开类。

    上一个测试只扫模块顶层的**函数**，扫不到类。2026-09-12 实测：全仓有且
    仅有两个零引用的公开类，都是完整契约——

    - ``modelpolicy.ModelPolicy``：字段集与 ``ResolvedPolicy`` 完全重复，
      即「同一份策略契约的第二份实现」；
    - ``schemas.TaskView``：202 响应体的第二份实现，且已漂移（``docs/SPEC.md``
      与 ``flow._view`` 都有 ``replayed``，它没有）。

    危害与孤儿函数同源（见上一个测试），但类更隐蔽：它自带 docstring 与字段
    声明，**长得就像「对外契约」**，后来者照着它改，而真正生效的是别处
    （``flow._view`` 里的 dict 字面量），改一份漂一份。

    判据与函数版同构，只有一处刻意差异：**类自身源码区间内的引用不算数**。
    ``def from_declared(cls, node) -> ModelPolicy`` 这种「在自己的方法签名里
    写自己的名字」不是使用证据；而函数版的 ``-1``（减去定义行）对跨多行的类
    不成立——按 ``-1`` 算，``ModelPolicy`` 会因这行注解被判成「模块内有引用」
    而漏报。故这里改成「把整个类体抠掉，再看模块剩余部分还有没有它」。

    类方法/类属性同理被覆盖：类整体无人引用时一并报出，不单列。
    """
    import re
    from collections import defaultdict

    app_src = {p: p.read_text(encoding="utf-8")
               for p in (ROOT / "app").rglob("*.py")}

    def top_level_classes(source: str) -> list:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        return [n for n in tree.body
                if isinstance(n, ast.ClassDef) and not n.name.startswith("_")]

    # 名字 → 定义了它的模块集合（用于判断名字是否有歧义）
    owners: dict[str, set] = defaultdict(set)
    for path, source in app_src.items():
        for node in top_level_classes(source):
            owners[node.name].add(path)

    offenders = []
    for path, source in sorted(app_src.items()):
        own = re.escape(path.stem)
        for node in top_level_classes(source):
            if node.name in _ORPHAN_CLASS_ALLOWLIST:
                continue
            bare = r"\b" + re.escape(node.name) + r"\b"
            # 有歧义（同名类存在于多个模块）时只认 "<模块名>.<类名>" 限定引用
            pattern = (rf"\b{own}\.{bare}"
                       if len(owners[node.name]) > 1 else bare)
            outside = sum(len(re.findall(pattern, s))
                          for q, s in app_src.items() if q != path)
            lines = source.splitlines()
            end = node.end_lineno or node.lineno
            rest = "\n".join(lines[:node.lineno - 1] + lines[end:])
            if outside == 0 and not re.findall(pattern, rest):
                offenders.append(f"{node.name} ({path.relative_to(ROOT)})")
    assert not offenders, (
        "以下公开类没有任何调用方。要么接上调用点、要么删除；"
        "确实要保留（如为未实现需求预留）请加进 _ORPHAN_CLASS_ALLOWLIST "
        "并写明理由：\n  " + "\n  ".join(offenders)
    )
