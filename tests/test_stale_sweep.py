"""兜底 sweeper：卡死重投 / 派发后失联判死 / 超龄判死。

守的是「入队消息丢失」与「worker 失联」两条路径。本服务零资金动作，
规则简单直接：锁过期 + 未派发过 → 重投；锁过期 + 已派发过 → 判死；
超龄 → 无条件判死（必须先于 new-api 的 24h 清理线收敛）。
"""

from __future__ import annotations

from app.redis import K_DISPATCH
from app.services import slots, sweeper

TH = "tokenhash0000000000000000000000"


async def _seed(task_store, task_id: str, *, status: str = "NOT_START",
                age: int = 0, epoch: int = 0, created_age: int | None = None) -> None:
    await task_store.create(task_id, "/v1/images/generations", {
        "source": "stask", "token_hash": TH, "dispatch_epoch": epoch,
    })
    row = task_store.rows[task_id]
    row["status"] = status
    row["updated_at"] = task_store.now() - age
    if created_age is not None:
        row["created_at"] = task_store.now() - created_age


# ---------------------------------------------------------------------------
# 卡死收敛
# ---------------------------------------------------------------------------


async def test_lost_enqueue_is_requeued(task_store, patch_redis, test_settings,
                                        queue_events):
    """核心场景：NOT_START、从未派发（epoch=0）、锁不在 → 重投。"""
    task_id = "img_" + "a" * 32
    await _seed(task_store, task_id, age=9999)

    result = await sweeper.sweep_stale()

    assert result["requeued"] == 1 and result["killed"] == 0
    assert queue_events.execute == [task_id]
    assert task_store.rows[task_id]["status"] == "NOT_START"  # 状态不变，等 worker


async def test_dispatched_then_lost_is_killed(task_store, patch_redis, test_settings,
                                              queue_events):
    """已派发过（epoch>0）且锁已过期 → 结果不可得，判死并释放槽。"""
    task_id = "img_" + "b" * 32
    await _seed(task_store, task_id, status="IN_PROGRESS", age=9999, epoch=1)
    await slots.acquire(TH, 10)

    result = await sweeper.sweep_stale()

    assert result["killed"] == 1 and result["requeued"] == 0
    assert task_store.rows[task_id]["status"] == "FAILURE"
    assert await slots.current(TH) == 0
    assert queue_events.execute == []


async def test_lock_alive_is_skipped(task_store, patch_redis, test_settings,
                                     queue_events):
    """锁还在 = 一次调用可能在飞 → 本轮跳过，绝不重投。"""
    task_id = "img_" + "c" * 32
    await _seed(task_store, task_id, status="IN_PROGRESS", age=9999, epoch=1)
    patch_redis._data[K_DISPATCH.format(task_id=task_id)] = "1"

    result = await sweeper.sweep_stale()

    assert result["requeued"] == 0 and result["killed"] == 0
    assert task_store.rows[task_id]["status"] == "IN_PROGRESS"


async def test_fresh_task_not_touched(task_store, patch_redis, test_settings,
                                      queue_events):
    """未超龄的任务绝不误捞。"""
    task_id = "img_" + "d" * 32
    await _seed(task_store, task_id, age=0)

    result = await sweeper.sweep_stale()

    assert result["scanned"] == 0
    assert queue_events.execute == []


async def test_terminal_task_not_touched(task_store, patch_redis, test_settings,
                                         queue_events):
    task_id = "img_" + "e" * 32
    await _seed(task_store, task_id, status="SUCCESS", age=9999)

    result = await sweeper.sweep_stale()

    assert result["scanned"] == 0


async def test_sweep_lock_prevents_double_run(task_store, patch_redis, test_settings):
    """重入锁：同一轮只有一个 sweeper 在跑。"""
    first = await sweeper.sweep_stale()
    second = await sweeper.sweep_stale()
    assert first is not None
    assert second == {"skipped": "locked"}


# ---------------------------------------------------------------------------
# 超龄判死
# ---------------------------------------------------------------------------


async def test_overdue_task_is_killed(task_store, patch_redis, test_settings,
                                      monkeypatch, queue_events):
    """超过最大生命期 → 无条件 FAILURE（先于 new-api 24h 清理线收敛）。"""
    monkeypatch.setattr(test_settings, "task_max_lifetime_seconds", 300)
    task_id = "img_" + "f" * 32
    await _seed(task_store, task_id, status="IN_PROGRESS", created_age=9999)
    await slots.acquire(TH, 10)

    result = await sweeper.sweep_overdue()

    assert result["killed"] == 1
    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "lifetime" in row["fail_reason"]
    assert await slots.current(TH) == 0


async def test_overdue_triggers_callback(task_store, patch_redis, test_settings,
                                         monkeypatch, queue_events):
    monkeypatch.setattr(test_settings, "task_max_lifetime_seconds", 300)
    task_id = "img_" + "9" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": TH, "callback_url": "http://cb.example/hook",
    })
    task_store.rows[task_id]["created_at"] = task_store.now() - 9999

    await sweeper.sweep_overdue()

    assert queue_events.notify == [(task_id, 1, 0)]


# ---------------------------------------------------------------------------
# 槽位校准
# ---------------------------------------------------------------------------


async def test_recalibrate_fixes_drift_both_ways(task_store, patch_redis,
                                                 test_settings):
    """Redis 计数按 tasks 表事实双向修正。"""
    task_id = "img_" + "1" * 32
    await task_store.create(task_id, "/x", {"token_hash": TH})

    # 虚高：Redis 说 5，事实是 1
    await slots.reset(TH, 5)
    await sweeper.recalibrate_slots()
    assert await slots.current(TH) == 1

    # 虚低：Redis 说 0，事实是 1
    patch_redis._data.pop("st:sweep:slots", None)   # 清重入锁
    await slots.reset(TH, 0)
    await sweeper.recalibrate_slots()
    assert await slots.current(TH) == 1
