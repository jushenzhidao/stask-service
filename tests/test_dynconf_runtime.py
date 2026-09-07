from __future__ import annotations

import pytest

from app.services import dynconf


@pytest.mark.asyncio
async def test_runtime_config_snapshot_contains_operational_fields(patch_redis, test_settings):
    config = await dynconf.get_runtime_config()

    assert config.max_slots == test_settings.max_slots
    assert config.rate_limit == test_settings.rate_limit
    assert config.worker_timeout == test_settings.worker_timeout
    assert config.sweep_enabled == test_settings.sweep_enabled
    with pytest.raises(AttributeError):
        config.max_slots = 1


@pytest.mark.asyncio
async def test_runtime_config_reuses_one_cached_lookup(patch_redis, monkeypatch):
    calls = 0
    original = patch_redis.hgetall

    async def counting_hgetall(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    monkeypatch.setattr(patch_redis, "hgetall", counting_hgetall)
    config = await dynconf.get_runtime_config()
    assert config.rate_limit_window_seconds >= 1
    assert config.max_slots >= 1
    assert calls == 1


@pytest.mark.asyncio
async def test_runtime_config_hot_update_takes_effect(patch_redis):
    before = await dynconf.get_runtime_config()
    await dynconf.set_many({"max_slots": 42, "rate_limit": 7})
    after = await dynconf.get_runtime_config()

    assert before.max_slots != after.max_slots
    assert after.max_slots == 42
    assert after.rate_limit == 7


@pytest.mark.asyncio
async def test_runtime_config_redis_outage_falls_back_to_settings(
    patch_redis, monkeypatch, test_settings
):
    async def boom(*_args, **_kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(patch_redis, "hgetall", boom)
    config = await dynconf.get_runtime_config()

    assert config.max_slots == test_settings.max_slots
