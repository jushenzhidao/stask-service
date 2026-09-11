"""攒批与放行：N/T 双触发、放行幂等、三层槽的占与还、策略解析。

这一层守的是**上游不被超发**与**槽计数不漂移**两件事，两者的失败模式都是
静默的（闸门形同虚设 / 若干次任务后永久 429），所以断言必须落在 Redis 与
DB 的真实状态上，而不是只看返回值。

重点覆盖两个曾真实存在的缺陷：

1. 立即路径占的是第一层，但落库 ``slot_flags=0`` → 终态释放一层都不还，
   该 token 在 ``max_slots`` 条任务后永久 429；
2. 终态释放只还第一层（``st:slot``），``st:mslot``/``st:gslot`` 单调累积，
   模型全局闸门卡死。
"""

from __future__ import annotations

import pytest

from app.redis import K_BATCH, K_BATCH_DUE, K_DUE
from app.schemas import SubmitPlan
from app.services import (
    batching,
    dispatch,
    dynconf,
    modelpolicy,
    slots,
    submit,
    taskstore,
)

MODEL = "doubao-seedream"
TH = "tokenhash0000000000000000000000"
TH2 = "tokenhash1111111111111111111111"
TASK_A = "doubao_" + "a" * 32
TASK_B = "doubao_" + "b" * 32
TASK_C = "doubao_" + "c" * 32


def _plan(task_id: str, model: str = MODEL, token_hash: str = TH) -> SubmitPlan:
    return SubmitPlan(
        task_id=task_id, token_hash=token_hash, model=model,
        method="POST", path="/v1/images/generations", query="",
        headers={"Content-Type": "application/json"},
        body='{"model":"' + model + '"}', body_encoding="plain",
        body_truncated=False, upstream_base_url="http://newapi:3000",
        user_id=42,
    )


async def _seed_waiting(store, task_id: str, *, model: str = MODEL,
                        token_hash: str = TH, due_at: int = 0) -> None:
    """落一条「攒批等待期」的行——与 submit 的排队路径同形。"""
    await store.create(task_id, "/v1/images/generations", {
        "source": "stask", "model": model, "token_hash": token_hash,
        "slot_model": model, "slot_flags": 0, "batch_state": "waiting",
        "batch_size": 2, "batch_wait": 60, "requeue_attempts": 0,
        "batch_due_at": due_at,
    })


async def _set_policies(monkeypatch, policies: dict) -> None:
    """热改 model_policies（dynconf 读侧 5s 缓存由 autouse fixture 清）。"""
    from app.config import settings

    monkeypatch.setattr(settings, "model_policies", policies)


# ---------------------------------------------------------------------------
# 策略解析：命中层级与 queues 判定
# ---------------------------------------------------------------------------


def test_exact_model_beats_prefix_and_default():
    """命中哪层整块用哪层，不做字段级深合并。"""
    p = modelpolicy.resolve(
        model=MODEL, path="/v1/images/generations",
        policies={
            MODEL: {"batch": 5, "batch_wait": 30},
            "/v1/images": {"batch": 99, "batch_wait": 99},
            "__default__": {"batch": 7, "batch_wait": 7},
        },
        default_limit_per_token=10,
    )
    assert p.source == f"model:{MODEL}"
    assert (p.batch, p.batch_wait) == (5, 30)
    # 未声明的字段回落下一层（这里是全局标量），不是继承被跳过的那层
    assert p.limit_per_token == 10


def test_path_prefix_longest_match_wins():
    p = modelpolicy.resolve(
        model="unknown-model", path="/v1/images/generations",
        policies={"/v1": {"batch": 2, "batch_wait": 5},
                  "/v1/images": {"batch": 9, "batch_wait": 9}},
    )
    assert p.source == "path:/v1/images" and p.batch == 9


def test_default_key_used_when_no_model_or_path():
    # batch>=2 必须带 batch_wait，否则写侧就会整批拒绝（见 validate）
    p = modelpolicy.resolve(model="whatever",
                            policies={"__default__": {"batch": 3, "batch_wait": 30}})
    assert p.source == "default" and p.batch == 3


@pytest.mark.parametrize("policies,queued", [
    ({}, False),
    ({"m": {"batch": 1, "batch_wait": 10}}, False),
    ({"m": {"batch": 2, "batch_wait": 60}}, True),
    ({"m": {"batch": 1, "batch_wait": 10, "limit_global": 3}}, True),
    # 第二层也必须强制排队：立即路径走单层 acquire，不认这一层，
    # 不排队就等于该层声明了却不生效
    ({"m": {"batch": 1, "batch_wait": 10, "limit_model_token": 3}}, True),
])
def test_queues_predicate(policies, queued):
    """batch>=2 或任一分层上限>0 才排队；其余保持「收到即发」老路径。"""
    p = modelpolicy.resolve(model="m", policies=policies)
    assert p.queues is queued


async def test_layer2_limit_parsed_and_enforced_per_model_token(patch_redis):
    """第二层是 (模型, token) 维度的独立上限：同 key 在 A 模型打满不影响 B。

    这条覆盖本轮接线前的缺口——那时 dispatch 硬编码 limit_model_token=0，
    第二层声明得再多也不生效。
    """
    mask_a = await slots.acquire_layered(
        TH, "model-a", limit_per_token=10, limit_model_token=1, limit_global=0)
    assert mask_a == slots.FLAG_TOKEN | slots.FLAG_MODEL_TOKEN

    # 同 key 同模型的第二条 → 第二层拒绝
    assert await slots.acquire_layered(
        TH, "model-a", limit_per_token=10, limit_model_token=1, limit_global=0) == 0
    # 同 key 换模型 → 另一组独立额度，放行
    assert await slots.acquire_layered(
        TH, "model-b", limit_per_token=10, limit_model_token=1, limit_global=0) != 0


def test_unknown_model_normalized_consistently():
    """占与释放必须用同一个归一化字符串，否则「占 A 释放 B」。"""
    assert modelpolicy.normalize_model("  Doubao-2.0  ") == "doubao-2.0"
    assert modelpolicy.normalize_model("") == modelpolicy.UNKNOWN_MODEL


def test_validate_rejects_broken_policy_tables():
    with pytest.raises(ValueError):
        modelpolicy.validate({"m": {"batch": 2}})            # 攒批却没有 T 上限
    with pytest.raises(ValueError):
        modelpolicy.validate({"m": {"bathc": 1}})            # 字段拼错
    with pytest.raises(ValueError):
        modelpolicy.validate({"m": {"batch": 0, "batch_wait": 0}})  # wait 低于下限
    assert modelpolicy.validate("") == {}                     # 空串 = 空表
    assert modelpolicy.validate(None) == {}


def test_validate_enforces_token_ttl_ceiling(monkeypatch):
    """等待 + 执行 + 余量 必须 ≤ 令牌会话 TTL，否则到点 100% token_missing。

    把 TTL 压到与 ``batch_wait`` 上界同量级，才能在不越 ``MAX_BATCH_WAIT``
    的前提下触到这条天花板——默认 TTL(7h) 远大于 3600s，天然碰不到。
    """
    from app.config import settings

    monkeypatch.setattr(settings, "sk_session_ttl_seconds", 1000)
    with pytest.raises(ValueError, match="token session TTL"):
        modelpolicy.validate({"m": {"batch": 2, "batch_wait": 900}})

    # 留够余量时放行
    assert modelpolicy.validate({"m": {"batch": 2, "batch_wait": 300}})


# ---------------------------------------------------------------------------
# 攒批入批：N 计数、deadline 写定与落库权威值
# ---------------------------------------------------------------------------


async def test_join_counts_and_fixes_deadline_on_first_member(patch_redis, task_store):
    """deadline 由**首个成员**写定，后续成员不得延长（ZADD NX）。"""
    store = patch_redis
    n1, due1 = await batching.join(TASK_A, MODEL, batch_size=2, batch_wait=60, now=1000)
    n2, due2 = await batching.join(TASK_B, MODEL, batch_size=2, batch_wait=60, now=1100)

    assert (n1, n2) == (1, 2)
    assert due1 == 1060
    # 第二个成员的 due2 若生效就会把批次窗口一路后推 —— 必须仍是首个值
    assert due2 == 1060
    assert float(await store.zscore(K_BATCH_DUE, MODEL)) == 1060.0


async def test_join_returns_authoritative_deadline_not_local_one(patch_redis):
    """返回的必须是 Redis 里真实生效的 ZSCORE，不是自己算的那个。

    落了偏晚的值，Redis 丢数据后 rebuild_from_db 会把整批放行时刻集体后移。
    """
    await batching.join(TASK_A, MODEL, batch_size=5, batch_wait=60, now=1000)
    _, due = await batching.join(TASK_B, MODEL, batch_size=5, batch_wait=60, now=5000)
    assert due == 1060, "NX 命中时应回权威值，而不是本调用者算出的 5060"


async def test_deadline_lands_in_db_for_rebuild(task_store, patch_redis, queue_events):
    """入批后 deadline 必须落库——Redis 丢了靠它重建 T 触发时刻。"""
    await _seed_waiting(task_store, TASK_A)
    await submit.join_batch(
        TASK_A, MODEL,
        modelpolicy.ResolvedPolicy(batch=2, batch_wait=60, limit_per_token=5, limit_model_token=0,
                                   limit_global=0, source="model:x"),
    )
    assert task_store.rows[TASK_A]["data"]["batch_due_at"] > 0


async def test_join_below_n_does_not_publish_release(task_store, patch_redis,
                                                     queue_events):
    """没攒够 N 就不该触发整批放行——否则攒批形同虚设。"""
    await _seed_waiting(task_store, TASK_A)
    policy = modelpolicy.ResolvedPolicy(batch=3, batch_wait=60, limit_per_token=5, limit_model_token=0,
                                        limit_global=0, source="model:x")
    await submit.join_batch(TASK_A, MODEL, policy)
    assert queue_events.release_batch == []


async def test_join_reaching_n_publishes_release_once(task_store, patch_redis,
                                                      queue_events):
    """计数推过阈值的那一次提交负责投递放行（且只投一次）。"""
    policy = modelpolicy.ResolvedPolicy(batch=2, batch_wait=60, limit_per_token=5, limit_model_token=0,
                                       limit_global=0, source="model:x")
    for tid in (TASK_A, TASK_B):
        await _seed_waiting(task_store, tid)
        await submit.join_batch(tid, MODEL, policy)

    assert queue_events.release_batch == [(MODEL, "size")]


async def test_limit_global_only_model_skips_batch_and_queues(
    task_store, queue_events, patch_redis
):
    """batch<2 但 limit_global>0：不攒批，只挂重排通道等槽。"""
    await _seed_waiting(task_store, TASK_A)
    policy = modelpolicy.ResolvedPolicy(batch=0, batch_wait=0, limit_per_token=5, limit_model_token=0,
                                       limit_global=3, source="model:x")
    joined = await submit.join_batch(TASK_A, MODEL, policy)

    assert joined is False
    assert queue_events.release_batch == []
    assert await patch_redis.zcard(K_DUE) == 1


# ---------------------------------------------------------------------------
# 放行：原子摘取 + 逐条 release 的幂等
# ---------------------------------------------------------------------------


async def test_claim_is_atomic_single_winner(patch_redis):
    """摘取即互斥：N 触发与 T 触发并发时只有一方拿到成员。

    这是「不需要额外放行锁」的根据，必须如实成立。
    """
    for tid in (TASK_A, TASK_B):
        await batching.join(tid, MODEL, batch_size=9, batch_wait=60)

    first = await batching.claim(MODEL)
    second = await batching.claim(MODEL)

    assert sorted(first) == sorted([TASK_A, TASK_B])
    assert second == []
    assert await patch_redis.zcard(K_BATCH.format(key=MODEL)) == 0
    assert await patch_redis.zcard(K_BATCH_DUE) == 0


async def test_release_model_dispatches_whole_batch(task_store, patch_redis,
                                                    queue_events, test_settings):
    for tid in (TASK_A, TASK_B):
        await _seed_waiting(task_store, tid)
        await batching.join(tid, MODEL, batch_size=9, batch_wait=60)

    tally = await batching.release_model(MODEL)

    assert tally["released"] == 2 and tally["claimed"] == 2
    assert sorted(queue_events.execute) == sorted([TASK_A, TASK_B])
    for tid in (TASK_A, TASK_B):
        row = task_store.rows[tid]
        assert row["data"]["batch_state"] == "released"
        assert row["data"]["slot_flags"] > 0        # 掩码随行落库


async def test_release_is_idempotent_second_call_skips(task_store, patch_redis,
                                                       queue_events, test_settings):
    """重复放行只有第一次生效——「绝不重复调上游」在放行侧的落点。"""
    await _seed_waiting(task_store, TASK_A)
    await batching.join(TASK_A, MODEL, batch_size=9, batch_wait=60)

    first = await dispatch.release(TASK_A)
    second = await dispatch.release(TASK_A)

    assert first is dispatch.Released.OK
    assert second is dispatch.Released.SKIPPED
    assert queue_events.execute == [TASK_A], "第二次绝不能再次入队"


async def test_release_skips_canceled_task(task_store, patch_redis, queue_events,
                                           test_settings):
    """已取消（非 QUEUED）的任务静默丢弃——钱不能花在用户已经不要的结果上。"""
    await _seed_waiting(task_store, TASK_A)
    task_store.rows[TASK_A]["status"] = "CANCELED"

    assert await dispatch.release(TASK_A) is dispatch.Released.SKIPPED
    assert queue_events.execute == []


async def test_release_no_slot_requeues_and_rolls_back(task_store, patch_redis,
                                                       queue_events, test_settings,
                                                       monkeypatch):
    """占不到槽 → NO_SLOT，且必须把放行权**退回** waiting 以便重试。"""
    await _set_policies(monkeypatch, {MODEL: {"batch": 2, "batch_wait": 60,
                                              "limit_global": 1}})
    await _seed_waiting(task_store, TASK_A)
    await _seed_waiting(task_store, TASK_B)

    await dispatch.release(TASK_A)                 # 占满模型全局的唯一名额
    assert await slots.current_global(MODEL) == 1

    assert await dispatch.release(TASK_B) is dispatch.Released.NO_SLOT
    assert task_store.rows[TASK_B]["data"]["batch_state"] == "waiting"
    assert queue_events.execute == [TASK_A]


async def test_requeue_sets_backoff_and_is_visible_to_ticker(task_store, patch_redis,
                                                             test_settings):
    """退避重排落库次数 + 挂进重排通道，ticker 才能捞到它。"""
    await _seed_waiting(task_store, TASK_A)
    due_at = await dispatch.requeue(TASK_A, backoff_ceiling=300)

    data = task_store.rows[TASK_A]["data"]
    assert data["requeue_attempts"] == 1
    assert data["requeue_due_at"] == due_at
    assert await patch_redis.zcard(K_DUE) == 1
    assert TASK_A in await dispatch.due(now=due_at + 1)


# ---------------------------------------------------------------------------
# T 触发
# ---------------------------------------------------------------------------


async def test_due_keys_then_tick_releases_expired_batch(task_store, patch_redis,
                                                           queue_events, test_settings):
    """到期的批次由 ticker 整批放行（不必等下一笔提交把它带出来）。"""
    for tid in (TASK_A, TASK_B):
        await _seed_waiting(task_store, tid)
        await batching.join(tid, MODEL, batch_size=99, batch_wait=60, now=1000)

    assert await batching.due_keys(now=1000) == []      # 还没到点
    assert await batching.due_keys(now=1061) == [MODEL]

    stat = await batching.tick_once(now=1061)
    assert stat["released"] == 2
    assert sorted(queue_events.execute) == sorted([TASK_A, TASK_B])


async def test_tick_drains_requeue_channel(task_store, patch_redis, queue_events,
                                           test_settings):
    """重排任务到期后由同一轮 ticker 放行（两条通道必须一起扫）。"""
    await _seed_waiting(task_store, TASK_A)
    await dispatch.requeue(TASK_A, backoff_ceiling=300)
    due_at = task_store.rows[TASK_A]["data"]["requeue_due_at"]

    stat = await batching.tick_once(now=due_at + 1)

    assert stat["released"] == 1
    assert queue_events.execute == [TASK_A]
    assert await patch_redis.zcard(K_DUE) == 0


# ---------------------------------------------------------------------------
# 退批与重建
# ---------------------------------------------------------------------------


async def test_leave_removes_member_and_clears_empty_batch(patch_redis):
    """退批必须真的减计数：否则整批永远差几条到不了 N，只能干等 T。"""
    store = patch_redis
    for tid in (TASK_A, TASK_B):
        await batching.join(tid, MODEL, batch_size=2, batch_wait=60)

    await batching.leave(TASK_A, MODEL)
    assert await store.zcard(K_BATCH.format(key=MODEL)) == 1
    assert await store.zcard(K_BATCH_DUE) == 1

    await batching.leave(TASK_B, MODEL)             # 批空 → 到期索引一并清掉
    assert await store.zcard(K_BATCH.format(key=MODEL)) == 0
    assert await store.zcard(K_BATCH_DUE) == 0


async def test_rebuild_from_db_restores_overdue_batch_and_releases(
    task_store, patch_redis, queue_events, test_settings
):
    """Redis 索引整个丢失后，按 DB 事实重建并立即放行已超期的批。"""
    for tid in (TASK_A, TASK_B):
        await _seed_waiting(task_store, tid, due_at=1)   # 早已过期

    stat = await batching.rebuild_from_db(now=10_000)

    assert stat == {"batches": 1, "members": 2, "overdue": 1}
    assert sorted(queue_events.execute) == sorted([TASK_A, TASK_B])


async def test_rebuild_keeps_existing_deadline(patch_redis, task_store, queue_events):
    """只补不删：已在 Redis 的 deadline 不得被重置成「现在 + T」。

    deadline 取**未来**时刻，否则批次会被判成超期而就地放行（那是另一条
    路径，测不到「保留」这个语义）。
    """
    store = patch_redis
    await _seed_waiting(task_store, TASK_A, due_at=5000)
    await store.zadd(K_BATCH_DUE, {MODEL: 5000.0})
    await store.zadd(K_BATCH.format(key=MODEL), {TASK_A: 1.0})

    await batching.rebuild_from_db(now=1000)

    assert float(await store.zscore(K_BATCH_DUE, MODEL)) == 5000.0, "不得覆盖既有 deadline"


async def test_batch_waiting_projection_carries_due_at(task_store):
    """重建的事实源：只在 waiting 且带 due_at 的行上工作。"""
    await _seed_waiting(task_store, TASK_A, due_at=1234)
    await task_store.patch_data(TASK_A, {"batch_state": "released"})

    assert await task_store.batch_waiting() == []


# ---------------------------------------------------------------------------
# 路径前缀策略必须在放行路径上生效（曾因读错键名而静默失效）
# ---------------------------------------------------------------------------


async def test_path_prefix_policy_applies_on_release_path(
    task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """按端点前缀配的策略，在**放行路径**上必须同样命中。

    这里覆盖一个只有端到端用例能发现的缺陷：``dispatch.release`` 解析策略时
    读的是 ``data["path"]``，而落库的键名是 ``request_path``。``dict.get``
    对缺失键静默返回 None，于是 ``path=""`` → 前缀策略**永远匹配不上**，
    一律回落到 ``__default__``/settings。单测 ``modelpolicy`` 本身是绿的，
    坏的只是「没人把正确的 path 传进去」。

    判据：按前缀配 ``limit_global`` 后，放行必须真的占第三层（掩码含 4）。
    模型名本身**不在**策略表里，所以只有前缀命中才可能带上第三层。
    """
    monkeypatch.setattr(test_settings, "model_policies",
                        {"/v1/videos": {"limit_global": 2, "batch_wait": 60}})
    task_id = "vid_" + "e" * 32
    await _seed_waiting(task_store, task_id, model="some-unknown-model")
    await task_store.patch_data(task_id, {
        "request_path": "/v1/videos/generations",
    })

    await dispatch.release(task_id)

    data = task_store.rows[task_id]["data"]
    assert data["slot_flags"] & slots.FLAG_GLOBAL, (
        "端点前缀策略没生效——放行路径解析策略时没有把真实 request_path 传进去"
    )


# ---------------------------------------------------------------------------
# 热路径不得 SELECT *（不变式 6）
# ---------------------------------------------------------------------------


async def test_release_and_requeue_use_projection_not_full_row(
    task_store, patch_redis, queue_events, test_settings
):
    """放行/重排只读元数据投影，绝不拉整行。

    这两个函数是到期通道与批次放行里的**每任务一次**的热路径，而整行含
    ``request_body``（≤2MB）与 ``upstream_response``（≤10MB）——它们一个字节
    都用不到。一次 200 条的放行批次会因此多搬数百 MB 的 DB 流量。

    用替身记录的读取入口断言（真实实现里对应 ``SELECT *`` 与逐字段投影）。
    """
    task_id = "img_" + "f" * 32
    await _seed_waiting(task_store, task_id)
    task_store.reads.clear()

    await dispatch.release(task_id)
    await dispatch.requeue(task_id, backoff_ceiling=60)

    assert "get" not in task_store.reads, (
        f"热路径走了整行读取（{task_store.reads}）——应改用 get_meta 投影"
    )
    assert "get_meta" in task_store.reads


async def test_admit_due_uses_projection_not_full_row(
    task_store, patch_redis, queue_events, test_settings
):
    """到期准入决策同样只读投影（它每轮对每个到期任务跑一次）。"""
    now = taskstore.now()
    task_id = "dl_" + "a" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": MODEL, "slot_model": MODEL, "slot_flags": 0,
        "batch_state": "scheduled", "scheduled_at": now + 10,
    })
    task_store.reads.clear()

    await batching.admit_due(task_id, now=now + 11)

    assert "get" not in task_store.reads, (
        f"到期准入走了整行读取（{task_store.reads}）——应改用 get_meta 投影"
    )


# ---------------------------------------------------------------------------
# 三层槽：占与还必须严格对称（曾在此泄漏）
# ---------------------------------------------------------------------------


async def test_layered_acquire_release_symmetry(patch_redis):
    """占三层 → 按掩码还三层，计数必须回到 0。"""
    mask = await slots.acquire_layered(TH, MODEL, limit_per_token=5,
                                       limit_model_token=4, limit_global=3)
    assert mask == slots.FLAG_ALL
    assert await slots.current(TH) == 1
    assert await slots.current_model_token(TH, MODEL) == 1
    assert await slots.current_global(MODEL) == 1

    await slots.release_layered(TH, MODEL, mask)

    assert await slots.current(TH) == 0
    assert await slots.current_model_token(TH, MODEL) == 0
    assert await slots.current_global(MODEL) == 0


async def test_layered_global_only_release_does_not_touch_other_layers(patch_redis):
    """掩码只含全局层时，绝不能顺手 DECR 另外两层（会还掉别人的槽）。"""
    mask = await slots.acquire_layered(TH, MODEL, limit_per_token=0,
                                       limit_model_token=0, limit_global=2)
    assert mask == slots.FLAG_GLOBAL
    await slots.acquire(TH, 5)                       # 另一条任务占了第一层
    assert await slots.current(TH) == 1

    await slots.release_layered(TH, MODEL, mask)

    assert await slots.current_global(MODEL) == 0
    assert await slots.current(TH) == 1, "别人的第一层槽绝不能被还掉"


async def test_acquire_layered_rolls_back_when_global_full(patch_redis):
    """三层任一超限 → 本次已占的层必须在同块内回滚（无跨层泄漏窗口）。"""
    await slots.acquire_layered(TH, MODEL, limit_per_token=9,
                                limit_model_token=0, limit_global=1)
    assert await slots.current_global(MODEL) == 1

    mask = await slots.acquire_layered(TH2, MODEL, limit_per_token=9,
                                       limit_model_token=0, limit_global=1)

    assert mask == 0
    assert await slots.current(TH2) == 0, "全局层失败时第一层必须回滚"


async def test_release_for_task_honors_mask(patch_redis):
    """终态释放按落库掩码逐层回退——只还第一层是泄漏的根源。"""
    mask = await slots.acquire_layered(TH, MODEL, limit_per_token=5,
                                       limit_model_token=4, limit_global=3)
    await slots.release_for_task({"token_hash": TH, "slot_model": MODEL,
                                  "slot_flags": mask})

    assert await slots.current(TH) == 0
    assert await slots.current_model_token(TH, MODEL) == 0
    assert await slots.current_global(MODEL) == 0


async def test_release_for_task_without_mask_is_noop(patch_redis):
    """掩码缺失 = 从未占槽，一层都不许还（否则误扣同 token 其他任务）。"""
    await slots.acquire(TH, 5)                       # 别人的槽
    await slots.release_for_task({"token_hash": TH, "slot_model": MODEL})

    assert await slots.current(TH) == 1, "掩码缺失时绝不能凭猜测还第一层"


async def test_invariant_release_after_full_cycle_is_balanced(
    task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """端到端不变式：一批任务走完「放行 → 终态」后三层计数归零。

    这条是整个三层闸门的核心承诺——不成立就意味着模型全局闸门会在若干次
    任务后永久卡死（在途明明是 0 却判定为满）。

    三层必须全部配置，否则对应层不会被占（掩码缺位），也就测不到那一层的
    泄漏——第二层的泄漏正是本轮接线前无法暴露的那个缺陷。
    """
    await _set_policies(monkeypatch, {MODEL: {"batch": 9, "batch_wait": 60,
                                              "limit_model_token": 3,
                                              "limit_global": 2}})
    for tid in (TASK_A, TASK_B):
        await _seed_waiting(task_store, tid)
        await batching.join(tid, MODEL, batch_size=9, batch_wait=60)

    await batching.release_model(MODEL)

    assert task_store.rows[TASK_A]["data"]["slot_flags"] == slots.FLAG_ALL
    assert await slots.current_global(MODEL) == 2
    assert await slots.current(TH) == 2
    assert await slots.current_model_token(TH, MODEL) == 2

    for tid in (TASK_A, TASK_B):
        await slots.release_for_task(task_store.rows[tid]["data"])

    assert await slots.current_global(MODEL) == 0, "全局层泄漏 = 该模型永久卡死"
    assert await slots.current(TH) == 0
    assert await slots.current_model_token(TH, MODEL) == 0, "第二层泄漏 = 该 key 在此模型上永久受限"


# ---------------------------------------------------------------------------
# 立即路径（非攒批）：曾因 slot_flags 写死 0 而导致槽永久泄漏
# ---------------------------------------------------------------------------


async def test_immediate_path_records_slot_flags_and_releases(
    task_store, patch_redis, queue_events, test_settings
):
    """收到即发：提交时占第一层，``slot_flags`` 必须随行落库。

    写死 0 会让终态释放认定「从未占槽」，该 token 在 max_slots 条任务之后
    永久 429——这是本轮实测到的真实缺陷。
    """
    async def _enqueue(task_id: str) -> None:
        queue_events.execute.append(task_id)

    config = await dynconf.get_runtime_config()
    await submit.submit("sk-test-token", _plan(TASK_A), enqueue=_enqueue,
                        config=config)

    data = task_store.rows[TASK_A]["data"]
    assert data["batch_state"] == "immediate"
    assert data["slot_flags"] == slots.FLAG_TOKEN
    assert await slots.current(TH) == 1

    await slots.release_for_task(data)
    assert await slots.current(TH) == 0, "终态必须把第一层还回来"


async def test_immediate_path_loop_does_not_leak_slots(
    task_store, patch_redis, queue_events, test_settings
):
    """连做 N 轮「提交 → 终态」，第一层计数必须归零（不能单调累积）。"""
    async def _enqueue(task_id: str) -> None:
        queue_events.execute.append(task_id)

    config = await dynconf.get_runtime_config()
    for i in range(6):
        tid = f"task_{i:02d}_" + "d" * 28
        await submit.submit("sk-test-token", _plan(tid), enqueue=_enqueue,
                            config=config)
        await slots.release_for_task(task_store.rows[tid]["data"])

    assert await slots.current(TH) == 0, "重复提交-释放循环不得累积槽"


async def test_queued_path_does_not_occupy_slot_at_submit(
    task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """排队路径提交时**不占槽**：等待期不该计入并发额度。

    否则攒批越久闸门越紧，一批还没放行就先把自己的槽耗光了。
    """
    await _set_policies(monkeypatch, {MODEL: {"batch": 5, "batch_wait": 60}})

    async def _enqueue(task_id: str) -> None:                # pragma: no cover
        queue_events.execute.append(task_id)

    config = await dynconf.get_runtime_config()
    await submit.submit("sk-test-token", _plan(TASK_A), enqueue=_enqueue,
                        config=config)

    data = task_store.rows[TASK_A]["data"]
    assert data["batch_state"] == "waiting"
    assert data["slot_flags"] == 0
    assert await slots.current(TH) == 0
    assert queue_events.execute == [], "排队路径不得直接入队"


async def test_batch_enabled_switch_disables_queueing(
    task_store, patch_redis, queue_events, test_settings, monkeypatch
):
    """batch_enabled=False 是线上止血开关：一切回到「收到即发」。"""
    await _set_policies(monkeypatch, {MODEL: {"batch": 5, "batch_wait": 60}})
    monkeypatch.setattr(test_settings, "batch_enabled", False)

    async def _enqueue(task_id: str) -> None:
        queue_events.execute.append(task_id)

    config = await dynconf.get_runtime_config()
    await submit.submit("sk-test-token", _plan(TASK_A), enqueue=_enqueue,
                        config=config)

    assert task_store.rows[TASK_A]["data"]["batch_state"] == "immediate"
    assert queue_events.execute == [TASK_A]
    assert await slots.current(TH) == 1


async def test_slot_exhausted_on_immediate_path(task_store, patch_redis,
                                                queue_events, test_settings,
                                                monkeypatch):
    """立即路径占满 → 429（SlotExhausted），且不落库任何行。"""
    monkeypatch.setattr(test_settings, "max_slots", 1)

    async def _enqueue(task_id: str) -> None:                # pragma: no cover
        queue_events.execute.append(task_id)

    config = await dynconf.get_runtime_config()
    await submit.submit("sk-test-token", _plan(TASK_A), enqueue=_enqueue,
                        config=config)

    with pytest.raises(submit.SlotExhausted):
        await submit.submit("sk-test-token", _plan(TASK_B), enqueue=_enqueue,
                            config=config)

    assert TASK_B not in task_store.rows, "占槽失败不得留下僵尸行"
    assert await slots.current(TH) == 1
