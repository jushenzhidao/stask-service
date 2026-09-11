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
        "status": "QUEUED",
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



async def test_model_policies_write_replaces_whole_table(
    patch_redis, test_settings, monkeypatch
):
    """``model_policies`` 是**整表替换**，不是按模型合并。

    这条是给运维的警告做成断言：配置存在 Redis 的一个 hash 字段里，值是一整个
    JSON 串，``hset(mapping={key: json})`` 直接覆盖该字段。于是「给第二个模型
    开启攒批」时如果只发自己那条，**第一个模型的策略会被静默清掉**——它不再
    攒批、也不再受并发上限约束，而请求侧一切正常，没人会注意到。

    所以开启第二个模型时必须把已有条目一起带上（先 GET 现表 → 改 → 整表 PUT）。
    """
    from app.services import modelpolicy

    await dynconf.set_many({"model_policies": {"model-a": {
        "batch": 10, "batch_wait": 60}}})
    await dynconf.set_many({"model_policies": {"model-b": {
        "batch": 5, "batch_wait": 30}}})

    table = await dynconf.get_model_policies()
    assert "model-b" in table, "新写入的必须生效"
    assert "model-a" not in table, (
        "整表替换语义：只发自己的那条会把别人的策略清掉（这正是要警告的坑）"
    )
    # 顺手确认「batch 就是开关」：0 = 不攒批，>=2 = 攒批
    off = modelpolicy.resolve(model="model-c", policies=table)
    assert off.batch == 0 and off.queues is False, "未配置的模型默认不攒批"


# ---------------------------------------------------------------------------
# 写入语义：replace（默认，整表替换） vs merge（顶层键合并）
# ---------------------------------------------------------------------------

ADMIN = {"X-Admin-Key": "admin-secret"}


@pytest.fixture
def admin_client(client, monkeypatch, test_settings):
    """启用管理面的客户端（与 test_admin.py 同款）。"""
    monkeypatch.setattr(test_settings, "admin_key", "admin-secret")
    return client


async def test_model_policies_merge_keeps_untouched_entries(patch_redis):
    """``mode="merge"``：只覆盖写到的模型条目，未写到的保持原值。

    与上面那条整表替换用例互为对偶——那条把「漏抄即静默清空」的坑钉成断言，
    这条把「不必再抄」的能力钉成断言。
    """
    await dynconf.set_many({"model_policies": {
        "model-a": {"batch": 10, "batch_wait": 60}}})

    await dynconf.set_many(
        {"model_policies": {"model-b": {"batch": 5, "batch_wait": 30}}},
        mode="merge",
    )
    table = await dynconf.get_model_policies()
    assert set(table) == {"model-a", "model-b"}, "合并必须保留未写到的条目"
    assert table["model-a"]["batch"] == 10
    assert table["model-b"]["batch"] == 5

    # 同名条目是**整条替换**，不做字段级深合并：字段级合并无法区分
    # 「改一个字段」与「删一个字段」，排障时也说不清生效了哪套参数。
    await dynconf.set_many(
        {"model_policies": {"model-a": {"batch_wait": 15}}}, mode="merge")
    table = await dynconf.get_model_policies()
    assert table["model-a"] == {"batch_wait": 15}
    assert table["model-b"] == {"batch": 5, "batch_wait": 30}


async def test_config_api_merge_mode(admin_client):
    """路由层真的把 ``?mode=merge`` 传下去（入口零覆盖会骗过 CI）。"""
    resp = admin_client.put(
        "/admin/api/config", headers=ADMIN,
        json={"model_policies": {"model-a": {"batch": 10, "batch_wait": 60}}})
    assert resp.status_code == 200

    resp = admin_client.put(
        "/admin/api/config?mode=merge", headers=ADMIN,
        json={"model_policies": {"model-b": {"batch": 5, "batch_wait": 30}}})
    assert resp.status_code == 200

    table = await dynconf.get_model_policies()
    assert set(table) == {"model-a", "model-b"}, "merge 不得清掉已有条目"

    # 未知 mode 被 Query 的 pattern 拦下（422），不会静默退化成 replace
    resp = admin_client.put(
        "/admin/api/config?mode=whatever", headers=ADMIN, json={"max_slots": 5})
    assert resp.status_code == 422


async def test_config_api_default_is_still_replace(admin_client):
    """不带 mode 时仍是整表替换——不能因为新增 merge 就把默认语义改掉。"""
    resp = admin_client.put(
        "/admin/api/config", headers=ADMIN,
        json={"model_policies": {"model-a": {"batch": 10, "batch_wait": 60}}})
    assert resp.status_code == 200

    resp = admin_client.put(
        "/admin/api/config", headers=ADMIN,
        json={"model_policies": {"model-b": {"batch": 5, "batch_wait": 30}}})
    assert resp.status_code == 200

    table = await dynconf.get_model_policies()
    assert set(table) == {"model-b"}, "默认必须保持整表替换（向后兼容）"
