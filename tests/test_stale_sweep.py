"""兜底 sweeper：卡死重投 / 派发后失联判死 / 超龄判死。

守的是「入队消息丢失」与「worker 失联」两条路径。本服务零资金动作，
规则简单直接：锁过期 + 未派发过 → 重投；锁过期 + 已派发过 → 判死；
超龄 → 无条件判死（必须先于 new-api 的 24h 清理线收敛）。
"""

from __future__ import annotations

from app.redis import K_DISPATCH, K_DUE
from app.services import slots, sweeper, taskstore

TH = "tokenhash0000000000000000000000"


async def _seed(task_store, task_id: str, *, status: str = "QUEUED",
                age: int = 0, epoch: int = 0, created_age: int | None = None) -> None:
    # slot_flags 必须落库：终态释放按掩码按位回退，缺了它一层都不还
    # （立即路径提交时占的就是第一层 = FLAG_TOKEN）。
    await task_store.create(task_id, "/v1/images/generations", {
        "source": "stask", "token_hash": TH, "dispatch_epoch": epoch,
        "slot_flags": slots.FLAG_TOKEN, "slot_model": "",
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
    """核心场景：QUEUED、从未派发（epoch=0）、锁不在 → 重投。"""
    task_id = "img_" + "a" * 32
    await _seed(task_store, task_id, age=9999)

    result = await sweeper.sweep_stale()

    assert result["requeued"] == 1 and result["killed"] == 0
    assert queue_events.execute == [task_id]
    assert task_store.rows[task_id]["status"] == "QUEUED"  # 状态不变，等 worker


async def test_waiting_batch_member_is_not_executed_without_slot(
    task_store, patch_redis, test_settings, queue_events
):
    """批次成员被误判 stale 时，**必须**经放行单点占槽，绝不能直接执行。

    真实触发条件很容易满足：stale 阈值约 210s（worker_timeout+margin+60），
    而 ``batch_wait`` 可配到 3600s——「正在正常凑批」的成员会先被判为卡死。
    若这条路径直接 ``publish_execute``，任务就在**没有占用任何并发槽**的情况下
    被送进上游：三层闸门形同虚设，攒批也被击穿。

    判据：走放行单点后 ``slot_flags`` 必 > 0（占了槽），且未占槽前不得入队。
    """
    task_id = "img_" + "c" * 32
    # 批次成员：掩码 0（放行前从未占槽）、batch_state=waiting、已「卡死」
    await task_store.create(task_id, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "slot_model": "dall-e-3", "slot_flags": 0,
        "batch_state": "waiting", "batch_size": 100, "batch_wait": 3600,
        "dispatch_epoch": 0,
    })
    task_store.rows[task_id]["updated_at"] = task_store.now() - 9999

    await sweeper.sweep_stale()

    row = task_store.rows[task_id]
    assert row["data"]["slot_flags"] > 0, (
        "被误判 stale 的批次成员必须经 dispatch.release 占槽后才能入队——"
        "直接 publish 会让它在零占槽状态下打到上游"
    )
    assert await slots.current(TH) == 1
    assert queue_events.execute == [task_id]


async def test_immediate_task_requeue_does_not_double_occupy_slot(
    task_store, patch_redis, test_settings, queue_events
):
    """立即任务的槽在提交时就占了：重投消息**不能**再占一次。

    再占一次等于同一任务吃两份额度，该 token 会被自己挤到 429。
    """
    task_id = "img_" + "d" * 32
    await _seed(task_store, task_id, age=9999)      # _seed 落 slot_flags=FLAG_TOKEN
    await slots.acquire(TH, 10)                     # 提交时占的那一份
    assert await slots.current(TH) == 1

    result = await sweeper.sweep_stale()

    assert result["requeued"] == 1
    assert await slots.current(TH) == 1, "重投不得重复占槽"
    assert queue_events.execute == [task_id]


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
    """Redis 计数按 tasks 表事实双向修正。

    注意事实源必须只数**真正占了槽**的任务：本用例落的行没有 slot_flags
    （= 从未占槽），但按旧口径它会被算成「占用 1」——这里显式给它第一层，
    保持用例语义为「占了一个槽且计数漂移」。
    """
    task_id = "img_" + "1" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": TH, "slot_flags": slots.FLAG_TOKEN, "slot_model": "m",
    })

    # 虚高：Redis 说 5，事实是 1
    await slots.reset(TH, 5)
    await sweeper.recalibrate_slots()
    assert await slots.current(TH) == 1

    # 虚低：Redis 说 0，事实是 1
    patch_redis._data.pop("st:sweep:slots", None)   # 清重入锁
    await slots.reset(TH, 0)
    await sweeper.recalibrate_slots()
    assert await slots.current(TH) == 1


async def test_recalibrate_ignores_tasks_that_hold_no_slot(
    task_store, patch_redis, test_settings
):
    """**等待期任务不占槽，校准时绝不能被算进活跃数**（R-13/AC-42）。

    这是新等待态引入的真实风险：批次成员与计划任务在放行前 ``slot_flags=0``，
    但它们同样是 ``status=QUEUED`` 的活跃行。若事实源按「活跃行数」统计，
    一次校准就会把闸门计数**拉到远超真实占用**——用户提交 20 条延迟任务后
    槽计数变成 20，正常请求全部 429。校准本意是修漂移，口径错了反而自造漂移。
    """
    # 一个真正持槽的立即任务
    await task_store.create("img_" + "2" * 32, "/x", {
        "token_hash": TH, "slot_flags": slots.FLAG_TOKEN, "slot_model": "m",
    })
    # 一个批次成员（等待期，未占槽）
    await task_store.create("img_" + "3" * 32, "/x", {
        "token_hash": TH, "slot_flags": 0, "slot_model": "m",
        "batch_state": "waiting", "batch_size": 100, "batch_wait": 60,
    })
    # 一个延迟任务（等待期，未占槽）
    await task_store.create("img_" + "4" * 32, "/x", {
        "token_hash": TH, "slot_flags": 0, "slot_model": "m",
        "batch_state": "scheduled", "scheduled_at": task_store.now() + 3600,
    })

    await sweeper.recalibrate_slots()

    assert await slots.current(TH) == 1, (
        "事实源只能数真正持槽的任务（3 行活跃里只有 1 行占了槽）"
    )


async def test_recalibrate_repairs_layer2_and_layer3_leaks(
    task_store, patch_redis, test_settings
):
    """第二/三层必须也有校准口径——它们的泄漏没有别的自愈路径。

    第三层泄漏的后果最重：模型全局闸门在在途为 0 时仍判定为满，**该模型
    永久卡死**。此前校准只管第一层，第二三层一旦泄漏只能等
    ``slot_ttl_seconds``（6h）过期。
    """
    # 一条任务占满三层
    await task_store.create("img_" + "5" * 32, "/x", {
        "token_hash": TH, "slot_model": "dall-e-3",
        "slot_flags": slots.FLAG_ALL,
    })
    # 人为制造三层泄漏（模拟「占槽后落库前崩溃」）
    await slots.reset_model_token(TH, "dall-e-3", 7)
    await slots.reset_global("dall-e-3", 7)
    assert await slots.current_model_token(TH, "dall-e-3") == 7
    assert await slots.current_global("dall-e-3") == 7

    result = await sweeper.recalibrate_slots()

    assert await slots.current_model_token(TH, "dall-e-3") == 1
    assert await slots.current_global("dall-e-3") == 1
    assert result["fixed_model_token"] == 1
    assert result["fixed_global"] == 1


# ---------------------------------------------------------------------------
# 计划任务的到期索引回补
# ---------------------------------------------------------------------------


async def test_stale_sweep_rearms_lost_due_index(task_store, patch_redis,
                                                test_settings, queue_events):
    """Redis 丢掉 st:due 后，等待期的计划任务必须被自动补回索引。

    不对称缺陷：普通任务「入队消息丢了」有 sweep_stale 重投兜底，而计划任务
    在等待期被豁免（不能重投、不能判死，这是对的），于是索引一丢就再也
    没有东西把它们放回去——只能等超龄被判 FAILURE。执行有恢复路径、延迟没有。

    判据：sweep 一轮后索引里重新出现该任务，且**绝不能**因此被执行。
    """
    now = taskstore.now()
    task_id = "dl_" + "7" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": TH, "model": "m",
        "batch_state": "scheduled", "scheduled_at": now + 3600,
    })
    # 模拟 Redis 丢索引：st:due 里没有它
    assert float(await patch_redis.zscore(K_DUE, task_id) or 0) == 0

    result = await sweeper.sweep_stale()

    assert result["due_rearmed"] == 1
    assert float(await patch_redis.zscore(K_DUE, task_id)) == float(now + 3600)
    assert queue_events.execute == [], "回补索引 ≠ 执行，未到点绝不能入队"


async def test_rearm_is_idempotent_and_keeps_original_score(
    task_store, patch_redis, test_settings
):
    """回补只补不覆盖：已在索引里的保持原 score，不得被重置。"""
    now = taskstore.now()
    task_id = "dl_" + "8" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": TH, "model": "m",
        "batch_state": "scheduled", "scheduled_at": now + 3600,
    })
    from app.services import dispatch

    await dispatch.schedule(task_id, now + 3600)

    result = await sweeper.sweep_stale()

    assert result["due_rearmed"] == 0, "已在索引里的不该被算作回补"
    assert float(await patch_redis.zscore(K_DUE, task_id)) == float(now + 3600)


async def test_stale_sweep_rebuilds_lost_batch_index(
    task_store, patch_redis, test_settings, queue_events
):
    """Redis 丢掉批次索引后，等待中的成员必须被补回 ``st:batch:{model}``。

    与延迟侧对称：两侧都得有回补路径。不补的后果比「任务卡住」更隐蔽——
    批次索引一丢，N 触发永远不命中，成员会被兜底扫描逐条单独放行，
    **任务照跑、只是攒批静默失效**（削峰没了，看板却一切正常）。
    """
    now = taskstore.now()
    task_id = "img_" + "b" * 32
    await task_store.create(task_id, "/v1/images/generations", {
        "token_hash": TH, "model": "dall-e-3", "slot_model": "dall-e-3",
        "slot_flags": 0, "batch_state": "waiting",
        "batch_size": 10, "batch_wait": 600, "batch_due_at": now + 600,
        "dispatch_epoch": 0,
    })
    # 模拟 Redis 丢索引：批次成员集合里没有它
    from app.redis import K_BATCH

    assert int(await patch_redis.zcard(K_BATCH.format(key="dall-e-3")) or 0) == 0

    result = await sweeper.sweep_stale()

    assert result["batch_members_rebuilt"] == 1
    members = await patch_redis.zrange(K_BATCH.format(key="dall-e-3"), 0, -1)
    assert any(task_id in str(m) for m in members), (
        "成员没有被补回批次索引——该批永远不会因 N 触发而放行"
    )
