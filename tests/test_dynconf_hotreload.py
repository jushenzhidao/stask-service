"""动态配置热改生效性测试：证明白名单里的项改了**确实**走新值。

修复前：proxy.py、flow.py、slots.py、execute.py、identity.py 等处直接读
settings（进程启动时固化），改 Redis 不生效。

修复后：这些路径通过 dynconf.get_runtime_config() 取快照，热改立即生效。
"""
import pytest

from app.services import dynconf

pytestmark = pytest.mark.anyio


async def test_body_max_bytes_hotreload_blocks_oversized_submit(
    client, monkeypatch, patch_redis, test_settings
):
    """body_max_bytes 热改后，proxy 提交路径按新值判定 413（而非 env 值）。"""
    # env 设的是 10MB，热改为 1500 字节（spec minimum=1024，小于 2KB 请求体）
    monkeypatch.setattr(test_settings, "body_max_bytes", 10_000_000)
    await patch_redis.hset("st:dynconf", mapping={"body_max_bytes": "1500"})
    dynconf._invalidate()  # 清本地缓存

    # 构造约 2KB 的请求体（超过热改的 1500 字节上限）
    large_prompt = "x" * 2000
    resp = client.post(
        "/async/v1/images/generations",
        json={"model": "test-model", "prompt": large_prompt},
        headers={"Authorization": "Bearer sk-test-token"},
    )
    assert resp.status_code == 413
    assert "1500 bytes" in resp.text


async def test_poll_wait_max_seconds_hotreload_422(
    client, task_store, monkeypatch, patch_redis, test_settings
):
    """poll_wait_max_seconds 热改后，超限 wait 返回 422（而非进程启动时上限）。

    修复前：Query(le=settings.poll_wait_max_seconds) 在模块 import 时烧死；
    修复后：handler 显式取快照并校验。
    """
    # 先提交一个任务（task_id 格式：前缀_32位hex）
    task_id = "test_model_abcdef01234567890123456789abcdef"
    task_store.rows[task_id] = {
        "task_id": task_id,
        "user_id": "user_test",
        "status": "NOT_START",
        "data": {"request_path": "/v1/images/generations"},
    }

    # env 是 60s，热改为 5s
    monkeypatch.setattr(test_settings, "poll_wait_max_seconds", 60)
    await patch_redis.hset("st:dynconf", mapping={"poll_wait_max_seconds": "5"})
    dynconf._invalidate()

    resp = client.get(
        f"/async/v1/images/generations/{task_id}?wait=10",
        headers={"Authorization": "Bearer sk-test-token"},
    )
    assert resp.status_code == 422
    data = resp.json()
    assert "5" in data["error"]["message"]
    assert data["error"]["param"] == "wait"

    # 在上限内的值能通过
    resp2 = client.get(
        f"/async/v1/images/generations/{task_id}?wait=3",
        headers={"Authorization": "Bearer sk-test-token"},
    )
    assert resp2.status_code == 202


async def test_slot_ttl_seconds_hotreload_redis_expiry(
    client, monkeypatch, patch_redis, test_settings
):
    """slot_ttl_seconds 热改后，Redis 槽键 TTL 用新值（而非 env 值）。"""
    from app.deps.auth import token_hash

    # env 是 300s，热改为 65s（spec minimum=60）
    monkeypatch.setattr(test_settings, "slot_ttl_seconds", 300)
    await patch_redis.hset("st:dynconf", mapping={"slot_ttl_seconds": "65"})
    dynconf._invalidate()

    resp = client.post(
        "/async/v1/images/generations",
        json={"model": "test-model", "prompt": "hi"},
        headers={"Authorization": "Bearer sk-test-token"},
    )
    assert resp.status_code == 202

    th = token_hash("sk-test-token")
    key = f"st:slot:{th}"
    ttl = await patch_redis.ttl(key)
    # 允许 ±3s 误差（异步执行可能有微小延迟）
    assert 62 <= ttl <= 68, f"expected TTL ~65s, got {ttl}s"


async def test_result_ttl_seconds_in_410_message(
    client, task_store, monkeypatch, patch_redis, test_settings
):
    """result_ttl_seconds 热改后，410 错误文案里的保留时长反映新值。"""
    task_id = "test_model_fedcba09876543210987654321fedcba"
    task_store.rows[task_id] = {
        "task_id": task_id,
        "user_id": "user_test",
        "status": "SUCCESS",
        "data": {"result_purged": True, "request_path": "/v1/images/generations"},
    }

    # env 是 3600s，热改为 120s
    monkeypatch.setattr(test_settings, "result_ttl_seconds", 3600)
    await patch_redis.hset("st:dynconf", mapping={"result_ttl_seconds": "120"})
    dynconf._invalidate()

    resp = client.get(
        f"/async/v1/images/generations/{task_id}",
        headers={"Authorization": "Bearer sk-test-token"},
    )
    assert resp.status_code == 410
    assert "120s" in resp.text

