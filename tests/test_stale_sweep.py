"""卡死任务兜底扫描（AC-33）。

守的是「入队消息丢失」这条路径：任务落库了但队列消息没了，worker 永不执行。
没有这只 sweeper 时那行会永久停在 SUBMITTED 并永久吃掉一个并发额度——
对账扫不到它（它没被标 reconcile_pending），槽位校准还会把它当成合法占用
反复「修正」回去。
"""

from __future__ import annotations

import pytest

from app.services import reconcile, slots

TH = "tokenhash0000000000000000000000"


async def _seed(task_store, task_id: str, *, status: str = "SUBMITTED",
                age: int = 0, pending: bool = False) -> None:
    await task_store.create(task_id, 42, "/v1/images/generations", {
        "source": "stask", "token_hash": TH, "inflight_slot": True,
        "reconcile_pending": pending, "reconcile_checked_at": 0,
    })
    row = task_store.rows[task_id]
    row["status"] = status
    row["updated_at"] = task_store.now() - age


async def test_lost_enqueue_is_handed_to_reconciliation(task_store, patch_redis,
                                                        test_settings):
    """核心场景：SUBMITTED 且长时间无进展 → 标 reconcile_pending。

    **不直接判死**——万一它其实已被派发（只是 updated_at 没刷新），
    判 FAILURE 会把已扣费的成功任务打掉。交给对账查消费日志定性。
    """
    task_id = "img_" + "a" * 32
    await _seed(task_store, task_id, age=9999)

    result = await reconcile.sweep_stale()

    assert result == {"scanned": 1, "marked": 1}
    data = task_store.rows[task_id]["data"]
    assert data["reconcile_pending"] is True
    assert data["reconcile_reason"] == "stale_no_progress"
    # 状态**不变**：不判死是这条设计的关键
    assert task_store.rows[task_id]["status"] == "SUBMITTED"


async def test_fresh_task_untouched(task_store, patch_redis, test_settings):
    """正常执行中的任务绝不能被误捞。

    阈值 = worker_timeout × 3 + margin，一次调用最长 worker_timeout，
    所以执行中的任务永远够不到这个线。
    """
    task_id = "img_" + "b" * 32
    await _seed(task_store, task_id, status="IN_PROGRESS", age=5)

    assert await reconcile.sweep_stale() == {"scanned": 0, "marked": 0}
    assert task_store.rows[task_id]["data"]["reconcile_pending"] is False


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
async def test_terminal_untouched(task_store, patch_redis, test_settings, status):
    task_id = "img_" + "c" * 32
    await _seed(task_store, task_id, status=status, age=9999)
    assert await reconcile.sweep_stale() == {"scanned": 0, "marked": 0}


async def test_already_pending_not_remarked(task_store, patch_redis, test_settings):
    """已在对账队列里的任务不重复标记——重置 checked_at 会打乱退避节奏，
    让同一批任务每 2 分钟就被重新查一遍 billing 日志。"""
    task_id = "img_" + "d" * 32
    await _seed(task_store, task_id, age=9999, pending=True)
    task_store.rows[task_id]["data"]["reconcile_checked_at"] = task_store.now()

    result = await reconcile.sweep_stale()

    assert result == {"scanned": 1, "marked": 0}
    assert task_store.rows[task_id]["data"]["reconcile_checked_at"] > 0


async def test_stale_then_reconciled_end_to_end(task_store, patch_redis,
                                                fake_billing, test_settings):
    """端到端：卡死任务被扫出 → 对账确认无扣费 → 判 FAILURE → 槽位归还。

    这是修复的完整闭环，也是没有 sweep_stale 时永远不会发生的事。
    """
    task_id = "img_" + "e" * 32
    await _seed(task_store, task_id, age=9999)
    await slots.acquire(TH, 10)
    assert await slots.current(TH) == 1

    await reconcile.sweep_stale()

    # 对账需要令牌会话才能查日志；这里模拟会话仍在
    from app.services import tokensession

    await tokensession.store(task_id, "sk-test-token")
    task_store.rows[task_id]["data"]["reconcile_checked_at"] = 0
    task_store.rows[task_id]["created_at"] = task_store.now() - 9999

    await reconcile.run_reconcile()

    assert task_store.rows[task_id]["status"] == "FAILURE"
    assert await slots.current(TH) == 0          # 槽位终于还回来了


async def test_stale_sweep_lock(task_store, patch_redis, test_settings):
    task_id = "img_" + "f" * 32
    await _seed(task_store, task_id, age=9999)

    assert "scanned" in await reconcile.sweep_stale()
    assert await reconcile.sweep_stale() == {"skipped": "locked"}
