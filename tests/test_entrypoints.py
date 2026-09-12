"""入口冒烟：每个「被路由 / 被调度」的入口都真的调用一遍。

**为什么单独一个文件。** `tests/entry_coverage_plugin.py` 按下述判据把关：
「本 pytest 会话内，该入口的 code object 是否被执行过」。它只能看见**别的测试真的
跑过**——所以进入入口的用例必须存在，否则守卫会（正确地）报红。这条防线针对的是
**入口零覆盖**：单个入口的「没被调用」在任何一条用例里都看不出来，CI 全绿。

**这些用例刻意保持薄。** 只钉入口层能独立成立的事实：恒返回结构化摘要 dict、
未知 task_id 不炸、跳过原因明确。业务语义由各模块自己的测试覆盖；这里不可
替代的价值只有一个——**入口本身被执行过**。

2026-09-13 实测（首次接入该守卫时）：29 个入口里 **10 个零覆盖**——全部 8 个
taskiq 任务体（`execute_task`/`notify_task`/`release_batch`/`tick_batches`/
4 个 sweep）加 2 个 `/ops` 手工触发端点。测试此前一律调**服务层**
（`sweeper.sweep_stale()`），而调度器真正投递的 taskiq 任务体一行没跑过。
"""

from __future__ import annotations

import pytest

from app import queue
from app.services import batching
from tests.conftest import AUTH

#: 形态合法但一定不存在的 task_id（`{slug}_{uuid4hex}`），用来走「任务行不存在」分支
_MISSING = "nosuch_00000000000000000000000000000000"


# ---------------------------------------------------------------------------
# taskiq 任务体（调度器真正投递的入口）
# ---------------------------------------------------------------------------


async def test_entry_execute_task_missing_row(task_store) -> None:
    """出队执行：任务行不存在时必须返回摘要 dict 而不是抛异常。

    抛出会触发 taskiq 重投，而重投正是本服务要防的（上游会被调两次）。
    """
    out = await queue.execute_task(_MISSING)
    assert isinstance(out, dict)
    assert out["ok"] is False
    assert out["stage"] == "not_found"


async def test_entry_notify_task_missing_row(task_store) -> None:
    """回调推送：任务行已消失时按 skipped 收敛，不抛异常。"""
    out = await queue.notify_task(_MISSING)
    assert out["result"] == "skipped"
    assert out["reason"] == "task_row_missing"


async def test_entry_release_batch_empty(task_store, patch_redis) -> None:
    """整批放行：空批次返回同形的计数 dict（不是 None、不是空 dict）。"""
    out = await queue.release_batch("m-empty")
    assert out["claimed"] == 0
    assert set(out) >= {"claimed", "released", "requeued", "skipped"}


async def test_entry_tick_batches_runs_all_rounds(
    monkeypatch: pytest.MonkeyPatch, task_store, patch_redis
) -> None:
    """cron 入口：必须把 `_TICK_ROUNDS` 轮全跑完并汇总。

    自旋间隔补成 0——真实值是 15s × 4 轮 = 45s，直接在用例里跑会把套件拖垮。
    补的是**间隔**而不是轮数：轮数是本用例要钉的东西（每轮一次 `tick_once`）。
    """
    monkeypatch.setattr(batching, "_TICK_INTERVAL", 0)
    out = await queue.tick_batches()
    assert out["rounds"] == batching._TICK_ROUNDS

    # 空库下每轮都是 0：键集必须与 `_TICK_STAT_KEYS` 同形（历史上这里漂过 KeyError）
    assert set(out) >= {"rounds", *batching._TICK_STAT_KEYS}


async def test_entry_sweep_stale_task(task_store, patch_redis) -> None:
    out = await queue.sweep_stale()
    assert out["scanned"] == 0
    assert out["killed"] == 0


async def test_entry_sweep_overdue_task(task_store, patch_redis) -> None:
    out = await queue.sweep_overdue()
    assert out["scanned"] == 0
    assert out["killed"] == 0


async def test_entry_sweep_slots_task(task_store, patch_redis) -> None:
    out = await queue.sweep_slots()
    assert "tokens" in out


async def test_entry_sweep_results_task(task_store, patch_redis) -> None:
    out = await queue.sweep_results()
    assert out["purged"] == 0


async def test_entry_sweep_tasks_respect_global_off_switch(
    monkeypatch: pytest.MonkeyPatch, task_store, patch_redis
) -> None:
    """`SWEEP_ENABLED=false` 时四个 sweep 入口都必须短路，且**仍返回 dict**。

    短路路径同样算「入口被执行过」——但它必须自己能被跑到，否则将来有人只测
    开着的分支，关掉的那条就永远没人验证。
    """
    from app.config import settings

    monkeypatch.setattr(settings, "sweep_enabled", False)
    for entry in (queue.sweep_stale, queue.sweep_overdue,
                  queue.sweep_slots, queue.sweep_results):
        out = await entry()
        assert out == {"skipped": "sweep_disabled"}, entry


# ---------------------------------------------------------------------------
# /ops 手工触发端点（走真实路由，不是直接调服务层）
# ---------------------------------------------------------------------------


def test_entry_ops_sweep_stale_route(client) -> None:
    """POST /ops/sweep/stale 走通整条路由（依赖注入 + 序列化）。"""
    resp = client.post("/ops/sweep/stale", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["scanned"] == 0


def test_entry_ops_recalibrate_route(client) -> None:
    resp = client.post("/ops/slots/recalibrate", headers=AUTH)
    assert resp.status_code == 200
    assert "tokens" in resp.json()
