"""入口覆盖门禁：本会话内每个「被路由 / 被调度」的入口都必须真的执行过。

**为什么是插件而不是普通用例。** 「某入口没被任何用例调用」是一个**跨用例**的
性质——它只在整个会话结束后才可判定。普通测试只能看到自己那一格，所以这条
不变式（被调度/被路由的入口必须有「真的调用一遍」的用例）过去完全没有机械守护：
单个入口零覆盖，在每一条用例里都是绿的。

**怎么用。**
    make entries                                   # = ENTRY_COVERAGE_STRICT=1 + 全量套件
    .venv/bin/python -m pytest tests/ -q -p tests.entry_coverage_plugin
非严格模式下只打印审计结果、不改退出码；``ENTRY_COVERAGE_STRICT=1`` 时未覆盖即失败。
**必须跑全量套件**：只跑一个文件时绝大多数入口必然「未覆盖」，那不是缺陷。

**自证（抽掉入口用例后必须变红，否则本门禁是空跑）：**
    ENTRY_COVERAGE_STRICT=1 .venv/bin/python -m pytest tests/ -q \
        -p tests.entry_coverage_plugin --ignore=tests/test_entrypoints.py
    预期：退出码非 0，并列出 10 个未执行入口（8 个 taskiq 任务体 + 2 个 /ops 端点）。

**判定口径：code object 是否被执行过**，而不是「名字有没有在测试里出现过」。
后者是这类门禁最经典的假绿来源——本文件首次落地时用名字粗筛，把
``/healthz/live``（明明有专门用例，只是走 HTTP 调用、不出现函数名）也判成了未覆盖。
如实测数据：名字粗筛能报出约 20 条「未覆盖」，其中绝大多数是假阳性。

**实现**：``sys.settrace`` 只吃 ``call`` 事件并返回 ``None``（不做行级跟踪），
所以只记录调用图、开销可控（全量套件约 +8s）。
"""

from __future__ import annotations

import os
import sys
import threading

#: FastAPI/Starlette 自带路由，不是本服务的对外契约
_FRAMEWORK_PATHS = frozenset(
    {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}
)
#: 只有显式要求时才让未覆盖影响退出码
_STRICT_ENV = "ENTRY_COVERAGE_STRICT"

_targets: dict = {}      # code object -> 可读标签
_executed: set = set()


def _tracer(frame, event, arg):
    if event == "call":
        _executed.add(frame.f_code)
    return None


def _collect_targets() -> None:
    from app.main import app

    for route in app.routes:
        if getattr(route, "path", "") in _FRAMEWORK_PATHS:
            continue
        endpoint = getattr(route, "endpoint", None)
        if callable(endpoint):
            _targets.setdefault(endpoint.__code__, f"route {route.path}")

    import app.queue as queue

    tasks = queue.broker.get_all_tasks()
    for name, task in tasks.items():
        func = (
            getattr(task, "original_func", None)
            or getattr(task, "func", None)
            or getattr(task, "_func", None)
        )
        if callable(func):
            _targets.setdefault(func.__code__, f"taskiq {name}")
        else:
            # taskiq 改名会让我们静默漏掉整个任务集合——必须吵
            print(f"[entry-coverage] 无法从 {name} 取到原函数"
                  f"（类型 {type(task).__name__}）——taskiq 版本变了？")


def pytest_configure(config) -> None:
    _collect_targets()
    sys.settrace(_tracer)
    threading.settrace(_tracer)


def pytest_sessionfinish(session, exitstatus) -> None:
    sys.settrace(None)
    threading.settrace(None)

    if not _targets:
        # 采集失败比「全部覆盖」更危险：它会伪装成通过
        print("\n[entry-coverage] 未采集到任何入口——插件失效，请检查 app.main / app.queue")
        session.exitstatus = 1
        return

    missed = sorted(label for code, label in _targets.items() if code not in _executed)
    total = len(_targets)
    print(f"\n[entry-coverage] 入口 {total} 个，已执行 {total - len(missed)} 个，未执行 {len(missed)} 个")

    if not missed:
        return

    print("  未执行的入口：")
    for label in missed:
        print(f"    - {label}")

    if os.environ.get(_STRICT_ENV) == "1":
        print(
            "\n  这些入口在本会话里一次都没跑过。请补用例**真的调用**它们"
            "（见 tests/test_entrypoints.py），或确认它们已被删除。"
        )
        session.exitstatus = 1
    else:
        print(f"  （仅审计；设 {_STRICT_ENV}=1 或跑 `make entries` 让未覆盖导致失败）")
