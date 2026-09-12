"""测试替身与注册表完整性门禁。

这些用例守的是**替身与真实实现的同步**：手工维护的登记表（`_TASKSTORE_FUNCS` /
`_REDIS_CONSUMERS`）漏一项，对应路径在测试里就会落到真 MySQL / 真 Redis，
而且**失败形态取决于被替身那一侧的异常策略**——会抛错的是响亮的，
`except Exception: pass` 那种是静默的空操作（套件全绿）。

从 `tests/test_misc.py` 切出（2026-09-13）。
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from tests import conftest
# `_ORPHAN_ALLOWLIST`（「生产代码允许无调用方」的豁免表）定义在 test_guards.py 里、
# 紧挨着孤儿守卫那族用例；本文件的「再导出但包外无人调用」检查复用同一张表，
# **必须复用而不是复制**——两份豁免表就是两个漂移点。
from tests.test_guards import _ORPHAN_ALLOWLIST

ROOT = Path(__file__).resolve().parents[1]


def _taskstore_public_functions() -> dict[str, str]:
    """``{公开函数名: 定义它的子模块}``——扫 ``taskstore`` 包的**全部**子模块。

    顺带守住「一个函数只能定义一次」：包化后最大的退化风险是把同一个函数在两个
    子模块里各写一份，而 ``__init__`` 的再导出会**静默**选中其中一个（后导入的
    覆盖先导入的），谁生效全看 import 顺序。
    """

    package = ROOT / "app" / "services" / "taskstore"
    found: dict[str, str] = {}
    for path in sorted(package.glob("*.py")):
        if path.name == "__init__.py":
            continue
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if node.name.startswith("_"):
                continue
            previous = found.get(node.name)
            assert previous is None, (
                f"`{node.name}` 在 {previous} 与 {path.name} 里重复定义——包化后这是"
                "静默缺陷：__init__ 的再导出只会选中其中一个"
            )
            found[node.name] = path.name
    return found


def test_taskstore_test_double_covers_every_public_function():
    """``conftest._TASKSTORE_FUNCS`` 必须覆盖 taskstore 的每个公开函数。

    ``taskstore`` 是「一个契约只能有一份实现」最容易被打破的地方：真实实现连
    MySQL，测试替身（``InMemoryTaskStore``）连内存，conftest 靠**逐名替换模块
    属性**把两者对上。于是**新增一个函数却忘了登记**，那条路径在测试里就会落到
    真 MySQL 上——而本套件对外声称「不依赖 MySQL/Redis」。失败形态还很误导：
    报出来是连接错误，看着像环境问题，不像「你漏登记了一个名字」。

    判据分六档，任何一档不过都说明这份登记表与事实脱节：

    1. 真实存在但两边都没登记 —— 漏登记（或它其实该进纯函数白名单）
    2. 登记了但 ``taskstore`` 里不存在 —— 改名/删除后的残影
    3. 登记了但替身没实现 —— fixture 会在 ``getattr`` 时 AttributeError
    4. 同一个名字两边都登记 —— 口径自相矛盾（到底要不要替换？）
    5. **再导出缺失**（包化后新增）—— 消费者一律写 ``taskstore.<name>(...)``，
       漏再导出等于把公开 API 悄悄删掉
    6. **再导出但包外无人调用**（包化后新增）—— 见下方长注释

    第 6 条是**必要的补偿**：`taskstore/__init__.py` 的再导出本身就是一次
    「跨模块引用」，会让 `test_no_orphan_service_functions` 对这一整个包失效
    （死代码只要被再导出就看着像有人用）。这里把那条性质按包的口径补回来。

    ``_TASKSTORE_PURE_FUNCS`` 里的名字是**唯一**允许不登记的类别，且这个白名单
    本身也被本用例钉住：它不能包含不存在的名字，也不能与登记表重叠。
    """


    public = _taskstore_public_functions()
    assert len(public) >= 20, (
        f"只解析出 {len(public)} 个公开函数，守卫定位可能已失效（taskstore 结构变了？）"
    )

    registered = set(conftest._TASKSTORE_FUNCS)
    pure = set(conftest._TASKSTORE_PURE_FUNCS)

    overlap = sorted(registered & pure)
    assert not overlap, f"同一个名字既登记为需替换、又登记为纯函数（口径矛盾）: {overlap}"

    unregistered = sorted(set(public) - registered - pure)
    assert not unregistered, (
        "taskstore 的公开函数既没进 _TASKSTORE_FUNCS 也没进 _TASKSTORE_PURE_FUNCS。"
        "漏登记会让它在测试里**落到真 MySQL**：\n  "
        + "\n  ".join(unregistered)
        + "\n  有状态 → 加进 _TASKSTORE_FUNCS 并在 InMemoryTaskStore 里实现；"
        "纯函数 → 加进 _TASKSTORE_PURE_FUNCS。"
    )

    ghost = sorted(registered - set(public))
    assert not ghost, (
        f"_TASKSTORE_FUNCS 登记了 taskstore 里不存在的名字（改名/删除后的残影）: {ghost}"
    )

    stale_pure = sorted(pure - set(public))
    assert not stale_pure, (
        f"_TASKSTORE_PURE_FUNCS 里的名字在 taskstore 里不存在: {stale_pure}"
    )

    store = conftest.InMemoryTaskStore()
    unimplemented = sorted(
        name for name in registered if not callable(getattr(store, name, None))
    )
    assert not unimplemented, (
        f"测试替身没有实现这些已登记的函数（fixture 会在 getattr 时崩）: {unimplemented}"
    )

    # ---- 5. 再导出完整性 --------------------------------------------------
    import app.services.taskstore as package

    not_exported = sorted(name for name in public if not hasattr(package, name))
    assert not not_exported, (
        "这些公开函数没有被 `app/services/taskstore/__init__.py` 再导出。消费者一律写 "
        "`taskstore.<name>(...)`，漏再导出 = 静默删掉公开 API：\n  "
        + "\n  ".join(not_exported)
    )

    # ---- 6. 再导出但包外无人调用 = 死代码 --------------------------------
    outside = "\n".join(
        p.read_text(encoding="utf-8")
        for p in (ROOT / "app").rglob("*.py")
        if "services/taskstore" not in p.as_posix()
    )
    orphan = sorted(
        name for name in public
        if name not in pure
        and name not in _ORPHAN_ALLOWLIST
        and not re.search(rf"\b{re.escape(name)}\s*\(", outside)
    )
    assert not orphan, (
        "这些 taskstore 公开函数在包外的 app 代码里没有任何调用方（死代码）。"
        "删掉它，或加进 _ORPHAN_ALLOWLIST 并写明理由：\n  " + "\n  ".join(orphan)
    )


def test_no_direct_name_import_of_stateful_taskstore_functions():
    """不得 ``from app.services.taskstore import <有状态函数>``。

    这不是风格洁癖，而是**测试替身能不能生效**的硬前提：conftest 的猴子补丁打在
    ``taskstore`` 模块的**属性**上（以及消费者模块的 ``taskstore`` 属性上）。
    而 `from ...taskstore import get_meta` 会在导入期把函数对象绑进消费者模块的
    命名空间——补丁够不着它，那条调用在测试里直接打到真 MySQL。

    纯函数（``_TASKSTORE_PURE_FUNCS``）不受此限：它们不碰 session，绑到哪里都一样。
    私有名也放行——补丁只覆盖公开契约，私有名不在替身职责内。
    """


    pure = set(conftest._TASKSTORE_PURE_FUNCS)

    offenders = []
    for root in (ROOT / "app", ROOT / "tests"):
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if not (node.module or "").endswith("taskstore"):
                    continue
                for alias in node.names:
                    name = alias.name
                    if name.startswith("_") or name in pure or name == "*":
                        continue
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno} {name}")

    assert not offenders, (
        "以下位置用了「直接名字导入」taskstore 的有状态函数，测试替身会失效"
        "（调用会落到真 MySQL）。改成 `from app.services import taskstore` 后写 "
        "`taskstore.<name>(...)`，或（仅当它确实不碰 DB 时）加进 "
        "_TASKSTORE_PURE_FUNCS：\n  " + "\n  ".join(offenders)
    )


def test_redis_consumer_list_is_complete():
    """``conftest._REDIS_CONSUMERS`` 必须覆盖每个 ``from app.redis import r`` 的模块。

    漏登记的后果**取决于消费方的异常策略**，两种都很难看：

    - 会抛错的那种 → 无 Redis 的测试环境里**挂起**（比失败更难查）；
    - ``except Exception: pass`` 那种（缓存类）→ **静默降级成空操作**：套件全绿，
      而那条路径在测试里从未真正执行过。

    2026-09-13 实测：``app.services.statuscache`` 就漏在表外，而它同时还是
    「测试里零引用」——两层叠加 = 整套 522 个用例全绿而它一次没跑过。
    """


    listed = set(conftest._REDIS_CONSUMERS)
    actual: set[str] = set()
    for path in (ROOT / "app").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if not (node.module or "").endswith("app.redis"):
                continue
            if any(alias.name == "r" for alias in node.names):
                actual.add(".".join(path.relative_to(ROOT).with_suffix("").parts))

    assert len(actual) >= 10, f"只扫到 {len(actual)} 个 r 消费方，守卫定位可能已失效"
    missing = sorted(actual - listed)
    assert not missing, (
        "这些模块从 `app.redis` 导入了 `r` 却不在 _REDIS_CONSUMERS 里——"
        "测试不会替换它们的 r，该路径会连真 Redis：\n  " + "\n  ".join(missing)
    )
    stale = sorted(listed - actual)
    assert not stale, (
        f"_REDIS_CONSUMERS 登记了不再消费 r 的模块（改名/删除后的残影）: {stale}"
    )


def test_fake_redis_covers_every_command_used():
    """``FakeRedis``（含它的最小 pipeline）必须实现 app 用到的每个 Redis 命令。

    失败形态是 **AttributeError**——响亮、不是假绿，但报出来像是"替身有 bug"，
    不像"你加了个命令却没实现替身"。用结构断言把它变成一句话能懂的失败。

    扫描口径：对 ``r`` 发起的属性调用，**加上**由 ``r.pipeline()`` 绑定出来的管道
    对象的属性调用（``batching`` 用 pipeline 批量 ``zadd``/``expire``）。
    刻意**不扫字符串字面量**——Lua 脚本里的 ``redis.call(...)`` 是服务端 API，
    不是客户端命令，扫进来会得到一个永远对不上的假缺口。
    """


    implemented = {n for n in dir(conftest.FakeRedis) if not n.startswith("_")}
    implemented |= {n for n in dir(conftest._FakePipeline) if not n.startswith("_")}

    used: set[str] = set()
    for path in (ROOT / "app").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        # 由 `X = r.pipeline()` 绑定出来的名字也算 Redis 门面（管道命令同样要替身支持）
        faces = {"r"}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == "pipeline"):
                faces |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in faces):
                used.add(node.func.attr)

    assert len(used) >= 8, f"只扫到 {len(used)} 个 Redis 命令，守卫定位可能已失效"
    missing = sorted(used - implemented)
    assert not missing, (
        f"app 里用到了 FakeRedis/_FakePipeline 没实现的 Redis 命令: {missing}\n"
        "  请在 tests/conftest.py 里补实现——否则相关用例会以 AttributeError 失败，"
        "看起来像替身的 bug，而不是「漏实现」。"
    )
