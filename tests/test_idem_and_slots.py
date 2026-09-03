"""幂等原子占位 + 并发槽额度。

对应 AC-05 / AC-06 / AC-07 / AC-08。这两块是本服务并发正确性的核心：
幂等失守 = 双建任务 = 双扣费；槽位失守 = 额度形同虚设。
"""

from __future__ import annotations

import asyncio

import pytest

from app.services import idem, pricing, slots

TH = "tokenhash0000000000000000000000"


# ---------------------------------------------------------------------------
# 幂等
# ---------------------------------------------------------------------------


async def test_acquire_then_replay(patch_redis, test_settings):
    owned, replay = await idem.acquire(TH, "k1")
    assert owned is True and replay is None

    # 回填前，第二个请求既拿不到占位也拿不到 task_id（真并发场景）
    owned2, replay2 = await idem.acquire(TH, "k1")
    assert owned2 is False and replay2 is None

    await idem.set_task_id(TH, "k1", "img_abc")

    # 回填后，AC-05：同键返回同一个 task_id，不重建
    owned3, replay3 = await idem.acquire(TH, "k1")
    assert owned3 is False and replay3 == "img_abc"


async def test_wait_returns_none_on_timeout(patch_redis, test_settings):
    """AC-06：占位方一直不回填 → 等待方超时返 None → 路由层 409。

    绝不能因为等超时就放行重建：重建 = 双建 = 双扣。
    """
    await idem.acquire(TH, "k2")
    assert await idem.wait_task_id(TH, "k2") is None


async def test_wait_picks_up_backfill(patch_redis, test_settings):
    await idem.acquire(TH, "k3")

    async def backfill():
        await asyncio.sleep(0.05)
        await idem.set_task_id(TH, "k3", "img_late")

    waiter = asyncio.create_task(idem.wait_task_id(TH, "k3"))
    await backfill()
    assert await waiter == "img_late"


async def test_release_is_cas_protected(patch_redis, test_settings):
    """归还占位必须是 CAS：已回填的真实 task_id 绝不能被误删。

    直接 DEL 的写法在「本请求失败 + 另一路径已回填」时会抹掉有效幂等
    记录，后续重试就会重建任务。
    """
    await idem.acquire(TH, "k4")
    await idem.set_task_id(TH, "k4", "img_real")
    await idem.release(TH, "k4")                      # 值不是 pending → 不删
    assert await idem.get_task_id(TH, "k4") == "img_real"


async def test_release_clears_pending(patch_redis, test_settings):
    await idem.acquire(TH, "k5")
    await idem.release(TH, "k5")
    owned, _ = await idem.acquire(TH, "k5")
    assert owned is True                               # 已归还，可重新占位


async def test_true_concurrency_single_winner(patch_redis, test_settings):
    """真并发：N 个协程抢同一个 key，只能有一个拿到占位。"""
    results = await asyncio.gather(*[idem.acquire(TH, "race") for _ in range(20)])
    winners = [owned for owned, _ in results if owned]
    assert len(winners) == 1


# ---------------------------------------------------------------------------
# 槽位公式
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("balance,price,expected", [
    (100.0, 100.0, 1),      # 设计 §6 举例：100 / 单价100 → 1 路
    (1000.0, 100.0, 10),    # 1000 → 10 路
    (0.0, 100.0, 1),        # 下限恒为 1（余额判定归 relay，不在这里拒）
    (5.5, 1.0, 5),          # floor
    (99999.0, 1.0, 10),     # 上限 ST_MAX_SLOTS
    (None, 1.0, 1),         # 余额未知（billing 抖动）→ 回落 1，不硬拒
])
def test_slots_formula(monkeypatch, test_settings, balance, price, expected):
    """AC-07：clamp(floor(balance / ref_price), 1, ST_MAX_SLOTS)。"""
    monkeypatch.setattr(test_settings, "ref_price_default", price)
    assert pricing.slots_for(balance, "any-model") == expected


def test_ref_price_env_override(monkeypatch, test_settings):
    """ST_REF_PRICE_{MODEL} 覆盖默认值（ADR-003）。"""
    pricing._lookup_env.cache_clear()
    monkeypatch.setenv("ST_REF_PRICE_DALL_E_3", "0.08")
    assert pricing.ref_price("dall-e-3") == 0.08
    assert pricing.ref_price("unknown-model") == test_settings.ref_price_default
    pricing._lookup_env.cache_clear()


def test_model_slug_shape():
    """task_id 前缀：小写、非字母数字替 _、截断 16。"""
    assert pricing.model_slug("dall-e-3") == "dall_e_3"
    assert pricing.model_slug("Qwen/Image-Edit") == "qwen_image_edit"
    assert pricing.model_slug("") == "task"
    assert len(pricing.model_slug("a" * 50)) == 16


# ---------------------------------------------------------------------------
# 槽位占用
# ---------------------------------------------------------------------------


async def test_slot_acquire_until_limit(patch_redis, test_settings):
    """AC-08：占满即拒。"""
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
