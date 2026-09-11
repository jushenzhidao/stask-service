"""客户端分批参数（R-14~R-20 / AC-55~AC-58）与归组维度。

覆盖三件事：

1. **头解析与校验**：非法 N/T 必须 400 且带专用错误码；``X-Batch-Key``
   超长只截断不报错（与 ``Idempotency-Key`` 一致）；
2. **归组维度**：``X-Batch-Key`` 显式优先于配置维度（AC-58），配置可在
   ``model`` 与 ``token_model`` 之间选择；
3. **AC-57**：客户端只声明 N 而不声明 T，**不得**变成无限等待。

最后一类是重点：``batch_wait`` 只在「客户端给了 T」时才由头决定，
其余情况必须有兜底，否则 LUA 里 ``due_at = now`` 会让整批立刻到期——
攒批形同虚设，而任务还停在 ``waiting`` 状态上。
"""

from __future__ import annotations

import pytest

from app.redis import K_BATCH
from app.services import batching, dispatch, dynconf, modelpolicy, taskstore
from tests.conftest import AUTH

PATH = "/async/v1/images/generations"
BODY = {"model": "dall-e-3", "prompt": "a red cube", "n": 1}
MODEL = "dall-e-3"


# ---------------------------------------------------------------------------
# 头解析（纯函数）
# ---------------------------------------------------------------------------


def test_absent_headers_declare_nothing():
    """不带任何分批头 = 完全交服务端策略，行为与改造前一致。"""
    ov = batching.parse_overrides({}, max_wait=300)
    assert (ov.size, ov.wait, ov.key) == (None, None, None)


def test_valid_headers_parsed():
    ov = batching.parse_overrides(
        {"x-batch-size": "8", "x-batch-wait": "45", "x-batch-key": "nightly-01"},
        max_wait=300,
    )
    assert (ov.size, ov.wait, ov.key) == (8, 45, "nightly-01")


@pytest.mark.parametrize("value", ["0", "-3", "abc", "", "1.5", "1e3"])
def test_invalid_batch_size_rejected(value):
    """N 必须是正整数：0 与负数在语义上自相矛盾，静默当成「不设」会掩盖 bug。"""
    with pytest.raises(batching.BatchParamError) as ei:
        batching.parse_overrides({"x-batch-size": value}, max_wait=300)
    assert ei.value.code == "invalid_batch_size"
    assert ei.value.status == 400


def test_batch_size_over_max_rejected():
    with pytest.raises(batching.BatchParamError) as ei:
        batching.parse_overrides(
            {"x-batch-size": str(batching.MAX_BATCH + 1)}, max_wait=300)
    assert ei.value.code == "invalid_batch_size"


def test_batch_wait_over_max_rejected():
    """R-17：超上限即 400 且错误码独立，便于客户端区分是 N 错还是 T 错。"""
    with pytest.raises(batching.BatchParamError) as ei:
        batching.parse_overrides({"x-batch-wait": "3601"}, max_wait=3600)
    assert ei.value.code == "batch_wait_too_long"
    assert ei.value.param == "x-batch-wait"

    # 边界值必须可用
    assert batching.parse_overrides(
        {"x-batch-wait": "3600"}, max_wait=3600).wait == 3600


def test_over_long_key_is_truncated_not_rejected():
    """R-15/§4.1：``X-Batch-Key`` 超长截断至 64，不报错。"""
    ov = batching.parse_overrides({"x-batch-key": "k" * 200}, max_wait=300)
    assert ov.key is not None and len(ov.key) == batching.MAX_KEY_LEN


def test_key_is_sanitized_before_entering_redis_keys():
    """归组键会直接拼进 Redis 键名，必须过白名单。

    客户端可控字符串不经约束地进键名，会带来键空间污染（注入 ``:`` 或
    空格与其他键碰撞）与不可读的键。
    """
    ov = batching.parse_overrides(
        {"x-batch-key": "  bad key/with*wild  "}, max_wait=300)
    assert ov.key == "bad_key_with_wild"

    # 纯空白 → strip 后为空 → 等同未声明（而不是拿一个空键去当维度）
    ov2 = batching.parse_overrides({"x-batch-key": "   "}, max_wait=300)
    assert ov2.key is None


# ---------------------------------------------------------------------------
# 归组维度（AC-58）
# ---------------------------------------------------------------------------


def test_explicit_key_wins_over_config_dimension():
    """AC-58：提供了 X-Batch-Key 就必须以它为准，两种维度配置下都成立。"""
    for group_by in ("model", "token_model"):
        assert batching.group_key("mine", model="m", token_hash="t" * 32,
                                  group_by=group_by) == "mine"


def test_default_dimension_is_model():
    """默认取 ``model``（产品 2026-09-11 决定：简单 + 批次大）。

    这与 ARCH Q5 / AC-58 的原始裁决不同，属**有意偏离**（ARCH §6 Q5 注已记录）。
    这条用例直接断言配置默认值——改它属于行为变更，必须连同 ARCH 注一起改，
    所以让测试在这里逼一下。
    """
    from app.config import settings

    assert settings.batch_group_by == "model"
    assert batching.group_key("", model="Dall-E-3", token_hash="t" * 32,
                              group_by="model") == "dall-e-3"


def test_token_model_dimension_prefixes_token_hash():
    key = batching.group_key("", model="Dall-E-3", token_hash="t" * 32,
                             group_by="token_model")
    assert key.startswith("t" * 12) and key.endswith(":dall-e-3")


def test_token_model_dimension_isolates_tokens():
    """``token_model`` 维度下，不同 token 即使同一模型也不混批。"""
    a = batching.group_key("", model="m", token_hash="a" * 32,
                           group_by="token_model")
    b = batching.group_key("", model="m", token_hash="b" * 32,
                           group_by="token_model")
    assert a != b
    assert "m" in a


def test_unknown_dimension_falls_back_to_model():
    """配错了维度不该让提交链路挂掉——回落默认并继续。"""
    assert batching.group_key("", model="m", token_hash="t" * 32,
                              group_by="nonsense") == "m"


# ---------------------------------------------------------------------------
# 端到端：头真的改变了入批行为
# ---------------------------------------------------------------------------


def test_client_size_alone_enables_batching_with_bounded_wait(
    client, task_store, patch_redis, queue_events
):
    """AC-57 核心：只给 N 不给 T，必须有兜底等待，不得无限等。

    该模型在策略表里**没有**任何声明，所以能入批完全来自客户端头——
    同时验证「客户端可以主动要求攒批」这条设计。
    """
    resp = client.post(PATH, json=BODY, headers={**AUTH, "X-Batch-Size": "5"})
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]

    data = task_store.rows[task_id]["data"]
    assert data["batch_state"] == "waiting", "客户端声明了 N 就该入批"
    assert data["batch_size"] == 5
    assert data["batch_wait"] > 0, "只给 N 时必须有兜底 T，否则整批立刻到期"
    assert data["batch_due_at"] > 0
    assert queue_events.execute == [], "入批≠执行，必须等 N/T 触发"

    # 响应与查询视图都要看得到批次信息（R-20），且键与落库值一致
    assert resp.json()["batch_key"] == data["batch_key"]
    assert resp.json()["batch_state"] == "waiting"
    assert MODEL in resp.json()["batch_key"]


def test_client_wait_does_not_override_server_size(
    client, task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """只给 T 不给 N：N 回落策略值（逐字段覆盖，不是整块替换）。"""
    monkeypatch.setattr(test_settings, "model_policies",
                        {MODEL: {"batch": 4, "batch_wait": 60}})

    task_id = client.post(
        PATH, json=BODY, headers={**AUTH, "X-Batch-Wait": "30"},
    ).json()["task_id"]

    data = task_store.rows[task_id]["data"]
    assert data["batch_size"] == 4, "未声明的维度必须回落策略值"
    assert data["batch_wait"] == 30, "已声明的维度以客户端为准"


def test_client_size_overrides_server_size(
    client, task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """客户端把 N 调小（10 → 3）：落库的必须是生效值 3。"""
    monkeypatch.setattr(test_settings, "model_policies",
                        {MODEL: {"batch": 10, "batch_wait": 60}})

    task_id = client.post(
        PATH, json=BODY, headers={**AUTH, "X-Batch-Size": "3"},
    ).json()["task_id"]

    assert task_store.rows[task_id]["data"]["batch_size"] == 3


async def test_explicit_batch_key_groups_different_models_together(
    client, task_store, patch_redis, queue_events
):
    """R-15：同 batch key 可强制跨模型混批（显式优先于默认维度）。"""
    h = {**AUTH, "X-Batch-Size": "2", "X-Batch-Key": "mixed-01"}
    first = client.post(PATH, json=BODY, headers=h).json()["task_id"]
    second = client.post(
        "/async/v1/audio/speech", json={"model": "sora", "prompt": "x"},
        headers=h,
    ).json()["task_id"]

    from app.redis import K_BATCH

    members = [_norm(m) for m in
               await patch_redis.zrange(K_BATCH.format(key="mixed-01"), 0, -1)]
    assert first in members and second in members, (
        "两个不同模型声明了同一个 X-Batch-Key，必须进同一个批次"
    )
    # 它们不该各自跑到按模型归组的批次里去
    assert int(await patch_redis.zcard(K_BATCH.format(key="dall-e-3")) or 0) == 0


async def test_default_dimension_shares_batch_across_tokens(
    client, task_store, patch_redis, queue_events
):
    """默认（``model``）下，同一模型的不同 token 合并成一批。

    这是「简单 + 批次大」的直接体现：批次越大 N 越容易触发。
    代价是放行后成员分散到各自的并发窗口，与 ARCH Q5 的论证不完全吻合。
    """
    other = dict(AUTH)
    other["Authorization"] = "Bearer sk-other-token-0000000000000000000000"

    t1 = client.post(PATH, json=BODY,
                     headers={**AUTH, "X-Batch-Size": "2"}).json()["task_id"]
    t2 = client.post(PATH, json=BODY,
                     headers={**other, "X-Batch-Size": "2"}).json()["task_id"]

    assert task_store.rows[t1]["data"]["batch_key"] == MODEL
    assert task_store.rows[t2]["data"]["batch_key"] == MODEL
    assert int(await patch_redis.zcard(K_BATCH.format(key=MODEL)) or 0) == 2


async def test_token_model_dimension_isolates_tokens_when_configured(
    client, task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """显式配成 ``token_model`` 时，两个 token 各自成批（AC-58 的原始口径）。"""
    monkeypatch.setattr(test_settings, "batch_group_by", "token_model")

    other = dict(AUTH)
    other["Authorization"] = "Bearer sk-other-token-0000000000000000000000"

    t1 = client.post(PATH, json=BODY,
                     headers={**AUTH, "X-Batch-Size": "2"}).json()["task_id"]
    t2 = client.post(PATH, json=BODY,
                     headers={**other, "X-Batch-Size": "2"}).json()["task_id"]

    key1 = task_store.rows[t1]["data"]["batch_key"]
    key2 = task_store.rows[t2]["data"]["batch_key"]
    assert ":" in key1 and ":" in key2
    assert key1 != key2
    assert int(await patch_redis.zcard(K_BATCH.format(key=key1)) or 0) == 1
    assert int(await patch_redis.zcard(K_BATCH.format(key=key2)) or 0) == 1
    assert int(await patch_redis.zcard(K_BATCH.format(key=MODEL)) or 0) == 0


def test_invalid_batch_header_rejected_before_creating_row(client, task_store):
    """非法分批头必须在落库前拒掉，不留半成品行。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Batch-Size": "0"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_batch_size"
    assert task_store.rows == {}


def _norm(member) -> str:
    return member.decode() if isinstance(member, bytes) else str(member)


async def test_cancel_leaves_the_explicit_batch_not_the_model_batch(
    client, task_store, patch_redis, queue_events
):
    """取消必须从**它自己那个批次**退批，而不是按模型名去退另一个。

    这是 ``flow.cancel`` 里改动的那一处：退批键取 ``data.batch_key``。
    若错用 ``slot_model``，客户端自定义键的批次计数永远不减（该批永远差
    N-1 条到不了 N），同时另一个按模型归组的批次被误删成员。
    """
    h = {**AUTH, "X-Batch-Size": "3", "X-Batch-Key": "mine-01"}
    task_id = client.post(PATH, json=BODY, headers=h).json()["task_id"]

    # 另一个走默认维度的批次，作为「不该被动到」的对照组。
    # 它的键形态取决于默认维度配置，所以从落库值取而不是硬编码。
    other = client.post(PATH, json=BODY,
                        headers={**AUTH, "X-Batch-Size": "3"}).json()["task_id"]
    other_key = task_store.rows[other]["data"]["batch_key"]
    assert other_key != "mine-01"
    assert int(await patch_redis.zcard(K_BATCH.format(key="mine-01")) or 0) == 1
    assert int(await patch_redis.zcard(K_BATCH.format(key=other_key)) or 0) == 1

    resp = client.delete(f"{PATH}/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELED"

    assert int(await patch_redis.zcard(K_BATCH.format(key="mine-01")) or 0) == 0, (
        "取消的成员必须从自己那个批次里摘掉"
    )
    assert int(await patch_redis.zcard(K_BATCH.format(key=other_key)) or 0) == 1, (
        "不得误删另一个批次的成员"
    )
    assert other  # 对照组仍在等待


async def test_kill_switch_stops_batching_but_not_delay_or_requeue(
    task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """``batch_enabled=false`` 必须**只**停掉攒批，不得连坐到期通道。

    这两件事共用同一个 ticker 纯属实现复用，语义上无关：到期通道扫的是
    「计划任务到点」与「占槽失败重排」。早前 ``queue.tick_batches`` 拿
    ``batch_enabled`` 把整个 tick 掐掉，于是「关掉攒批止血」会把延迟任务
    一起停掉——而延迟任务在等待期**被 sweeper 豁免**（不能重投也不能判死），
    没有第二条恢复路径，只能等超龄被判 FAILURE。

    另一处更隐蔽的不一致：``queue.py`` 读的是 ``settings.batch_enabled``
    （env 值），而 ``batching`` 读的是热改快照。于是「用管理面止血」与
    「改 env 止血」效果不同——同一个开关两套语义。
    """
    import dataclasses

    now = taskstore.now()
    # 一个等待中的批次成员（默认维度下 key = 模型名）
    await task_store.create("img_" + "9" * 32, "/v1/images/generations", {
        "token_hash": "th", "model": MODEL, "slot_model": MODEL,
        "slot_flags": 0, "batch_state": "waiting",
        "batch_size": 5, "batch_wait": 60, "batch_due_at": now - 5,
    })
    await batching.join("img_" + "9" * 32, MODEL, batch_size=5, batch_wait=60)
    # 一个已到点的计划任务
    await task_store.create("dl_" + "9" * 32, "/v1/images/generations", {
        "token_hash": "th", "model": MODEL, "slot_model": MODEL,
        "slot_flags": 0, "batch_state": "scheduled", "scheduled_at": now - 1,
    })
    await dispatch.schedule("dl_" + "9" * 32, now - 1)

    # 止血：热改快照置为关闭（管理面走的就是这条路径）
    real = await dynconf.get_runtime_config()
    off = dataclasses.replace(real, batch_enabled=False)

    async def fake_cfg():
        return off

    monkeypatch.setattr(dynconf, "get_runtime_config", fake_cfg)

    stat = await batching.tick_once(now=now)

    assert stat["batches"] == 0, "关闭后不得再放行批次"
    assert stat["released"] + stat["batched"] + stat["requeued"] >= 1, (
        "计划任务到点必须照常准入——止血开关不得连坐到期通道"
    )
    assert int(await patch_redis.zcard(K_BATCH.format(key=MODEL)) or 0) == 1, (
        "批次成员应仍留在批次里等"
    )


async def test_kill_switch_blocks_client_batch_header_too(
    client, task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """总开关关闭时，**客户端头也不能开启攒批**（闸门要有第二道）。

    ``queued = config.batch_enabled and policy.queues`` 只保证「调用方记得拦」。
    ``apply_batch_overrides`` 曾把 ``batch_enabled`` 声明成参数却从不读它——
    参数表本身是契约，写着不生效的东西会让后来人以为闸门在这里。
    现在函数内也拦一道：新加调用方忘了拦也不会绕过总开关。
    """
    import dataclasses

    from app.services import dynconf

    real = await dynconf.get_runtime_config()
    off = dataclasses.replace(real, batch_enabled=False)

    async def fake_cfg():
        return off

    monkeypatch.setattr(dynconf, "get_runtime_config", fake_cfg)

    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Batch-Size": "3"})
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]

    data = task_store.rows[task_id]["data"]
    assert data["batch_state"] != "waiting", (
        "总开关关闭时客户端不得靠 X-Batch-Size 自己开攒批"
    )
    assert resp.json()["batch_key"] == ""
    assert resp.json()["batch_state"] == ""


def test_apply_batch_overrides_refuses_when_switch_off():
    """函数级的第二道闸：``batch_enabled=False`` 时必须原样返回策略。

    为什么要单独测这一层：经公开路径（提交接口）**测不到它**——上层的
    ``queued = config.batch_enabled and policy.queues`` 已经先拦了一次，
    所以即使这个函数完全不读 ``batch_enabled``，端到端用例照样全绿
    （实测确认过：删掉这道闸，集成用例仍然通过）。

    而它存在的意义恰恰是「上层的拦有可能被漏掉」——新加一个调用方忘了判
    开关时，这一层就是最后一道。所以必须**直接调函数**来钉住它，
    否则它就是一段没人验证的代码。
    """
    from app.schemas import SubmitPlan
    from app.services import submit as submit_mod

    plan = SubmitPlan(
        task_id="t", token_hash="th", model="m", method="POST", path="/p",
        query="", headers={}, body="{}", body_encoding="plain",
        body_truncated=False, upstream_base_url="http://u", user_id=0,
        batch_size=8, batch_wait=30,
    )
    base = modelpolicy.ResolvedPolicy(
        batch=0, batch_wait=0, limit_per_token=10,
        limit_model_token=0, limit_global=0, source="settings",
    )

    kept = submit_mod.apply_batch_overrides(
        base, plan, default_wait=300, batch_enabled=False)
    assert kept is base, "开关关闭时必须原样返回，不得让客户端头把 N 抬起来"
    assert kept.batch == 0

    # 开关打开时头照常生效（否则这道闸就是「一律拒绝」而不是闸门）
    on = submit_mod.apply_batch_overrides(
        base, plan, default_wait=300, batch_enabled=True)
    assert on.batch == 8 and on.batch_wait == 30
