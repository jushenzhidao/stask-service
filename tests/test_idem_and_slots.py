"""显式幂等（Idempotency-Key → 确定性 task_id + 创建窗口占位）+ 并发槽。

这两块是本服务并发正确性的核心：幂等失守 = 双建任务 = 上游被重复调用；
槽位失守 = 并发保护形同虚设。
"""

from __future__ import annotations

import asyncio

from app.services import idem, slots
from app.services.submit import model_slug

TH = "tokenhash0000000000000000000000"
TID = "img_" + "0" * 32


# ---------------------------------------------------------------------------
# task_id 生成
# ---------------------------------------------------------------------------


def test_task_id_with_key_is_deterministic():
    """带 Idempotency-Key：同 token 同 key 恒同 task_id——显式幂等的根基。"""
    a = idem.new_task_id("dall_e_3", TH, "order-1")
    b = idem.new_task_id("dall_e_3", TH, "order-1")
    assert a == b
    assert a.startswith("dall_e_3_")
    assert len(a) <= 53                                 # varchar(64) 留余量


def test_task_id_varies_by_token_and_key():
    """token 或 key 任一变化都必须换 id。"""
    base = idem.new_task_id("m", TH, "k1")
    assert idem.new_task_id("m", "other", "k1") != base
    assert idem.new_task_id("m", TH, "k2") != base


def test_task_id_without_key_is_random():
    """不带 Idempotency-Key：每次都是新任务（默认不幂等）。"""
    a = idem.new_task_id("m", TH)
    b = idem.new_task_id("m", TH)
    assert a != b
    assert a.startswith("m_") and len(a) <= 53


def test_model_slug_shape():
    """task_id 前缀：小写、非字母数字替 _、截断 16。"""
    assert model_slug("dall-e-3") == "dall_e_3"
    assert model_slug("Qwen/Image-Edit") == "qwen_image_edit"
    assert model_slug("") == "task"
    assert len(model_slug("a" * 50)) == 16


# ---------------------------------------------------------------------------
# 创建窗口占位
# ---------------------------------------------------------------------------


async def test_acquire_single_owner(patch_redis, test_settings):
    assert await idem.acquire(TID) is True
    assert await idem.acquire(TID) is False             # 占位在，第二个抢不到


async def test_true_concurrency_single_winner(patch_redis, test_settings):
    """真并发：N 个协程抢同一个 task_id，只能有一个拿到占位。"""
    results = await asyncio.gather(*[idem.acquire(TID) for _ in range(20)])
    assert sum(1 for ok in results if ok) == 1


async def test_release_clears_pending(patch_redis, test_settings):
    await idem.acquire(TID)
    await idem.release(TID)
    assert await idem.acquire(TID) is True              # 已归还，可重新占位


async def test_settle_removes_placeholder(patch_redis, test_settings):
    await idem.acquire(TID)
    await idem.settle(TID)
    assert f"st:idem:{TID}" not in patch_redis.dump()


async def test_wait_row_returns_when_row_appears(patch_redis, test_settings):
    """等待方：占位者落库后返回 True（调用方回放）。"""
    await idem.acquire(TID)
    appeared = {"flag": False}

    async def exists(_tid: str) -> bool:
        return appeared["flag"]

    async def create_later():
        await asyncio.sleep(0.05)
        appeared["flag"] = True

    waiter = asyncio.create_task(idem.wait_row(TID, exists))
    await create_later()
    assert await waiter is True


async def test_wait_row_times_out_when_never_created(patch_redis, test_settings):
    """占位方一直不落库 → 等待方超时返 False → 路由层 409。

    绝不能因为等超时就放行重建：重建 = 上游被重复调用。
    """
    await idem.acquire(TID)

    async def exists(_tid: str) -> bool:
        return False

    assert await idem.wait_row(TID, exists) is False


# ---------------------------------------------------------------------------
# 槽位占用
# ---------------------------------------------------------------------------


async def test_slot_acquire_until_limit(patch_redis, test_settings):
    """占满即拒。"""
    for _ in range(3):
        assert await slots.acquire(TH, 3) is True
    assert await slots.acquire(TH, 3) is False
    assert await slots.current(TH) == 3        # 超限的那次必须回滚，不能留 4


async def test_slot_release_and_underflow_guard(patch_redis, test_settings):
    await slots.acquire(TH, 3)
    await slots.release(TH)
    assert await slots.current(TH) == 0
    await slots.release(TH)                     # 多释放一次
    assert await slots.current(TH) == 0         # Lua 内置下溢保护拉回 0


async def test_slot_concurrent_acquire_respects_limit(patch_redis, test_settings):
    """真并发占槽：Lua 原子性保证总数不超限（check-then-act 会超）。"""
    results = await asyncio.gather(*[slots.acquire(TH, 5) for _ in range(20)])
    assert sum(1 for ok in results if ok) == 5
    assert await slots.current(TH) == 5


async def test_slot_reset(patch_redis, test_settings):
    await slots.acquire(TH, 10)
    await slots.acquire(TH, 10)
    await slots.reset(TH, 7)
    assert await slots.current(TH) == 7
    await slots.reset(TH, 0)
    assert await slots.current(TH) == 0
