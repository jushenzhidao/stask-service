"""延迟 / 定时下发：调度头解析、等待期不占槽、到期放行、兜底豁免。

对应 PRD R-01~R-13 / AC-37~AC-45。

三条最容易写错、也最容易静默失效的约束，各有专门用例：

1. **等待期不占槽**（R-04/AC-42）——占槽点必须在放行那一刻，否则用户提交
   几条长延迟任务就把自己的并发配额锁死整个等待窗口；
2. **未到点绝不放行**（AC-44）——兜底扫描必须豁免未到点的任务，否则延迟
   语义被 `sweep_stale` 当场摧毁；
3. **延迟上限由令牌 TTL 反推**（B1）——超出容量的延迟是必然失败的任务，
   必须在提交时就拒掉，而不是放进来看着它 token_missing。
"""

from __future__ import annotations

import pytest

from app.redis import K_DUE
from app.services import batching, dispatch, schedule, slots, sweeper, taskstore
from tests.conftest import AUTH

PATH = "/async/v1/images/generations"
BODY = {"model": "dall-e-3", "prompt": "a red cube", "n": 1}
MODEL = "dall-e-3"


# ---------------------------------------------------------------------------
# 调度头解析（纯函数）
# ---------------------------------------------------------------------------


def test_absent_headers_mean_no_delay():
    """不带调度头 = 立即执行，行为与改造前逐字节一致。"""
    assert schedule.parse({}, now=1000, max_delay=3600) == 0


def test_delay_zero_is_immediate():
    assert schedule.parse({"x-delay-seconds": "0"}, now=1000, max_delay=3600) == 0


def test_relative_delay_adds_to_now():
    assert schedule.parse(
        {"x-delay-seconds": "300"}, now=1000, max_delay=3600) == 1300


def test_absolute_execute_after_unix_and_rfc3339():
    """两种写法指向同一时刻，且过去时刻视为立即（不报错）。"""
    from_unix = schedule.parse(
        {"x-execute-after": "1300"}, now=1000, max_delay=3600)
    from_rfc = schedule.parse(
        {"x-execute-after": "1970-01-01T00:21:40+00:00"}, now=1000, max_delay=3600)
    assert from_unix == from_rfc == 1300

    # 过去 → 立即，不是错误
    assert schedule.parse({"x-execute-after": "10"}, now=1000, max_delay=3600) == 0


def test_rfc3339_z_suffix_is_utc_not_local():
    """'Z' 必须按 UTC 解释：本地时区会让同一串在不同机器上算出不同时刻。"""
    assert schedule.parse(
        {"x-execute-after": "1970-01-01T00:21:40Z"}, now=1000, max_delay=3600) == 1300


@pytest.mark.parametrize("headers,code", [
    ({"x-delay-seconds": "-1"}, "invalid_delay"),
    ({"x-delay-seconds": "abc"}, "invalid_delay"),
    ({"x-delay-seconds": ""}, "invalid_delay"),
    ({"x-execute-after": "not-a-time"}, "invalid_execute_after"),
    ({"x-delay-seconds": "10", "x-execute-after": "2000"},
     "conflicting_schedule_headers"),
])
def test_invalid_headers_rejected(headers, code):
    with pytest.raises(schedule.ScheduleError) as ei:
        schedule.parse(headers, now=1000, max_delay=3600)
    assert ei.value.code == code
    assert ei.value.status == 400


def test_delay_over_max_rejected():
    with pytest.raises(schedule.ScheduleError) as ei:
        schedule.parse({"x-delay-seconds": "3601"}, now=1000, max_delay=3600)
    assert ei.value.code == "delay_too_long"

    # 边界值必须可提交（AC-40）
    assert schedule.parse(
        {"x-delay-seconds": "3600"}, now=1000, max_delay=3600) == 4600


def test_max_delay_is_clamped_by_token_ttl_capacity():
    """运营配置再大也不能超过 TTL 容量——那是必然 token_missing 的区间。"""
    from app.config import settings

    capacity = schedule.capacity_seconds()
    assert capacity == (settings.sk_session_ttl_seconds
                        - settings.worker_timeout - 600)

    # 想要 12h（PRD 硬上限）也只能拿到容量那么多
    assert schedule.resolve_max_delay(12 * 3600) == capacity
    # 运营调小则尊重运营
    assert schedule.resolve_max_delay(60) == 60
    # 容量为 0 时不得返回负数
    assert schedule.resolve_max_delay(0) == 0


def test_capacity_covers_prd_default_six_hours():
    """PRD 默认 6h 延迟必须在当前 TTL 下真的可行（否则默认值就是坏的）。"""
    assert schedule.capacity_seconds() >= 6 * 3600
    assert schedule.resolve_max_delay(21600) == 21600


# ---------------------------------------------------------------------------
# 提交：等待期不占槽、挂到期索引
# ---------------------------------------------------------------------------


async def test_delayed_submit_occupies_no_slot(client, task_store, queue_events,
                                               patch_redis):
    """R-04/AC-42：计划任务提交时不占任何槽，也不入队。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Delay-Seconds": "3600"})
    assert resp.status_code == 202

    task_id = resp.json()["task_id"]
    row = task_store.rows[task_id]
    data = row["data"]

    assert data["batch_state"] == "scheduled"
    assert data["slot_flags"] == 0
    assert data["scheduled_at"] > taskstore.now()
    assert queue_events.execute == [], "计划任务提交时绝不能入队"

    # 槽计数必须仍是 0——提交计划任务不该锁死自己的配额
    assert await slots.current(data["token_hash"]) == 0


async def test_delayed_submit_appears_in_due_index(client, patch_redis):
    """到期索引里能查到它，ticker 才可能放行。"""
    task_id = client.post(PATH, json=BODY,
                          headers={**AUTH, "X-Delay-Seconds": "600"}).json()["task_id"]
    assert float(await patch_redis.zscore(K_DUE, task_id) or 0) > 0


def test_submit_response_exposes_scheduled_at(client):
    """R-12：202 响应暴露 scheduled_at，客户端可区分「排队中」与「计划中」。"""
    payload = client.post(PATH, json=BODY,
                          headers={**AUTH, "X-Delay-Seconds": "600"}).json()
    assert payload["scheduled_at"] > 0

    immediate = client.post(PATH, json=BODY, headers=AUTH).json()
    assert immediate["scheduled_at"] == 0


def test_delay_too_long_rejected_before_creating_row(client, task_store):
    """超限延迟必须在落库前拒掉，不留一条注定判死的行。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Delay-Seconds": str(999999)})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "delay_too_long"
    assert task_store.rows == {}, "被拒的提交不得留下任务行"


def test_conflicting_headers_rejected(client):
    resp = client.post(PATH, json=BODY, headers={
        **AUTH, "X-Delay-Seconds": "60", "X-Execute-After": "9999999999"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "conflicting_schedule_headers"


# ---------------------------------------------------------------------------
# 到期放行：未到点绝不放行，到点才占槽入队
# ---------------------------------------------------------------------------


async def test_release_refuses_to_run_before_due(task_store, patch_redis,
                                                 queue_events, test_settings):
    """AC-44 的最后一道闸：任何路径试图提前放行都会被挡回并重挂索引。"""
    now = taskstore.now()
    await task_store.create("dl_" + "a" * 32, "/x", {
        "token_hash": "th", "slot_model": MODEL, "slot_flags": 0,
        "batch_state": "scheduled", "scheduled_at": now + 3600,
    })

    outcome = await dispatch.release("dl_" + "a" * 32)

    assert outcome is dispatch.Released.SKIPPED
    assert queue_events.execute == [], "未到点绝不允许调上游"
    # 必须重挂回索引，否则这条任务永远不会再被扫到
    assert float(await patch_redis.zscore(K_DUE, "dl_" + "a" * 32)) == float(now + 3600)


async def test_ticker_releases_when_due(task_store, patch_redis, queue_events,
                                        test_settings):
    """到点后由 ticker 放行：此时才占槽并入队。"""
    now = taskstore.now()
    task_id = "dl_" + "b" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": MODEL, "slot_model": MODEL, "slot_flags": 0,
        "batch_state": "scheduled", "scheduled_at": now + 60,
    })
    await dispatch.schedule(task_id, now + 60)

    assert (await batching.tick_once(now=now + 30))["released"] == 0   # 未到点
    assert queue_events.execute == []

    stat = await batching.tick_once(now=now + 61)
    assert stat["released"] == 1
    assert queue_events.execute == [task_id]

    row = task_store.rows[task_id]
    assert row["data"]["batch_state"] == "released"
    assert row["data"]["slot_flags"] > 0, "放行时必须占槽并落掩码"


# ---------------------------------------------------------------------------
# 兜底扫描豁免（AC-44）
# ---------------------------------------------------------------------------


async def test_stale_sweep_exempts_not_yet_due(task_store, patch_redis, test_settings,
                                               queue_events):
    """延迟任务「长时间无进展」是正常的，绝不能被当僵尸重投执行。"""
    now = taskstore.now()
    task_id = "dl_" + "c" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "scheduled_at": now + 3 * 3600,
    })
    task_store.rows[task_id]["updated_at"] = now - 9999     # 早已「卡死」

    result = await sweeper.sweep_stale()

    assert result["requeued"] == 0 and result["killed"] == 0
    assert queue_events.execute == [], "计划任务不得被兜底扫描提前执行"


async def test_overdue_sweep_exempts_not_yet_due(task_store, patch_redis,
                                                 test_settings, monkeypatch,
                                                 queue_events):
    """生命期起点取 max(created_at, scheduled_at)：延迟 5h 的任务不该在执行前被判死。"""
    monkeypatch.setattr(test_settings, "task_max_lifetime_seconds", 300)
    now = taskstore.now()
    task_id = "dl_" + "d" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "scheduled_at": now + 3600,
    })
    task_store.rows[task_id]["created_at"] = now - 9999     # created_at 早已超龄

    result = await sweeper.sweep_overdue()

    assert result["killed"] == 0, "未到计划时刻的任务不得因 created_at 超龄被判死"


async def test_overdue_kills_after_scheduled_time_passes(task_store, patch_redis,
                                                         test_settings, monkeypatch,
                                                         queue_events):
    """但计划时刻也过去很久之后，仍要被判死——豁免不是免死金牌。"""
    monkeypatch.setattr(test_settings, "task_max_lifetime_seconds", 300)
    now = taskstore.now()
    task_id = "dl_" + "e" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "scheduled_at": now - 9999,
    })
    task_store.rows[task_id]["created_at"] = now - 99999

    result = await sweeper.sweep_overdue()

    assert result["killed"] == 1
    assert task_store.rows[task_id]["status"] == "FAILURE"


def test_sweep_exemption_predicates_still_referenced_in_sql():
    """结构断言：两个 sweeper 的豁免谓词必须仍然接在真实 SQL 上。

    为什么需要这条「看源码」的断言：豁免只存在于 SQL 里，而单测跑的是手写
    ``InMemoryTaskStore``（它有一份独立的等价实现）。如果有人把 SQL 里的
    谓词删掉，内存替身不受影响、**所有行为用例照样全绿**，但线上 `sweep_stale`
    会把延迟 3h 的任务当僵尸立刻重投执行，延迟语义当场失效。
    这是实测过的：注入「删除该谓词」的变异时，行为用例一个都没红。
    """
    import inspect

    for fn in (taskstore.stale_active, taskstore.overdue_active):
        src = inspect.getsource(fn)
        assert "SQL_SCHEDULED" in src, (
            f"{fn.__name__} 不再引用计划任务豁免谓词——延迟任务会被兜底扫描"
            "提前执行/判死"
        )

    # 两条谓词的边界必须不同（stale 是「不晚于」，overdue 是「早于」），
    # 写反了会让边界那一秒的任务行为错位
    assert "<=" in taskstore.SQL_SCHEDULED_NOT_FUTURE
    assert "<=" not in taskstore.SQL_SCHEDULED_DUE_BEFORE


# ---------------------------------------------------------------------------
# 等待期取消（R-07）
# ---------------------------------------------------------------------------


async def test_cancel_delayed_task_unqueues(client, task_store, patch_redis):
    """R-07：取消计划任务必须摘掉到期索引（否则 ticker 每轮白扫一次）。"""
    task_id = client.post(PATH, json=BODY,
                          headers={**AUTH, "X-Delay-Seconds": "600"}).json()["task_id"]
    assert float(await patch_redis.zscore(K_DUE, task_id) or 0) > 0

    resp = client.delete(f"{PATH}/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELED"
    assert float(await patch_redis.zscore(K_DUE, task_id) or 0) == 0


async def test_cancel_delayed_task_does_not_touch_other_tasks_slots(
    task_store, patch_redis, test_settings
):
    """取消计划任务绝不能还掉同 token 其他在途任务的槽。"""
    now = taskstore.now()
    # 另一条在途任务占了第一层
    await slots.acquire("th", 10)
    assert await slots.current("th") == 1

    await task_store.create("dl_" + "f" * 32, "/x", {
        "token_hash": "th", "slot_model": MODEL, "slot_flags": 0,
        "batch_state": "scheduled", "scheduled_at": now + 600,
    })

    from app.services import flow

    await flow.cancel("dl_" + "f" * 32)

    assert await slots.current("th") == 1, "计划任务从未占槽，取消不得还别人的槽"
    assert task_store.rows["dl_" + "f" * 32]["status"] == "CANCELED"


# ---------------------------------------------------------------------------
# 与攒批叠加：先等到计划时刻，再参与批次聚合
# ---------------------------------------------------------------------------


async def test_delayed_task_joins_batch_at_due_time(task_store, patch_redis,
                                                    queue_events, test_settings,
                                                    monkeypatch):
    """PRD §4.1：调度与聚合可叠加——到期时不直接放行，而是先入批。"""
    monkeypatch.setattr(test_settings, "model_policies",
                        {MODEL: {"batch": 5, "batch_wait": 60}})
    now = taskstore.now()
    task_id = "dl_" + "9" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": MODEL, "slot_model": MODEL, "slot_flags": 0,
        "batch_state": "scheduled", "scheduled_at": now + 30,
    })
    await dispatch.schedule(task_id, now + 30)

    stat = await batching.tick_once(now=now + 31)

    assert stat["batched"] == 1
    assert queue_events.execute == [], "入批≠放行，必须等 N/T 触发"
    row = task_store.rows[task_id]
    assert row["data"]["batch_state"] == "waiting"
    assert row["data"]["batch_due_at"] > 0, "入批后必须落 deadline 供重建"


# ---------------------------------------------------------------------------
# AC-60 / AC-63：幂等不重排 + 向后兼容
# ---------------------------------------------------------------------------


def test_idempotent_replay_keeps_original_schedule_and_batch(client, task_store):
    """AC-60：回放必须保持原有 ``scheduled_at`` 与批次归属不变。

    这是一个很容易被「顺手实现」破坏的契约：回放路径在 ``submit`` 里提前
    return，若有人把调度头的解析挪到 return 之前，回放就会用**本次请求**的
    头把已排期的任务重新排期（甚至把等待中的批次成员挪走），而客户端拿到的
    仍是原 task_id——**响应看起来完全正常**。

    所以断言必须落在「库里那一行没被动过」，而不是只看响应体。
    """
    # 延迟 + 分批同时声明（PRD §4.1 允许叠加）：先等到点、再参与聚合。
    # 只给 X-Batch-Key 而不给 N 是凑不出批的——N 才是「攒多少条」的定义，
    # 所以这里三个头都给，才能验证「批次归属不被本次请求改写」。
    head = {**AUTH, "Idempotency-Key": "ac60-1", "X-Delay-Seconds": "7200",
            "X-Batch-Size": "4", "X-Batch-Key": "orig-batch"}
    first = client.post("/async/v1/images/generations",
                        json={"model": "dall-e-3", "prompt": "x"}, headers=head)
    assert first.status_code == 202
    body1 = first.json()
    row_before = dict(task_store.rows[body1["task_id"]]["data"])

    # 同一幂等键再次提交，但**换一套**调度头与分批头
    replay = client.post("/async/v1/images/generations",
                         json={"model": "dall-e-3", "prompt": "x"},
                         headers={**AUTH, "Idempotency-Key": "ac60-1",
                                  "X-Delay-Seconds": "60", "X-Batch-Size": "99",
                                  "X-Batch-Key": "hijacked-batch"})
    assert replay.status_code == 202
    body2 = replay.json()

    assert body2["task_id"] == body1["task_id"]
    assert body2["replayed"] is True
    # 回报的是**原始**调度与批次，不是本次请求的
    assert body2["scheduled_at"] == body1["scheduled_at"]
    assert body2["batch_key"] == "orig-batch"

    row_after = task_store.rows[body1["task_id"]]["data"]
    assert row_after == row_before, "回放不得改动库里那一行的任何字段"


def test_response_unchanged_when_no_new_headers(client, task_store):
    """AC-63：不带任何新增头时，增量字段为空、既有字段语义不变。

    新增的调度/批次能力必须**纯增量**：老客户端不感知它们也能照常工作。
    这里钉住的是「增量字段的默认值」——一旦有人让 ``batch_state`` 在普通
    提交里非空，老客户端就可能把「立即执行」误读成「在排队」。
    """
    resp = client.post("/async/v1/images/generations",
                       json={"model": "dall-e-3", "prompt": "x"}, headers=AUTH)
    assert resp.status_code == 202
    body = resp.json()

    assert body["scheduled_at"] == 0, "无延迟头 → 不得排期"
    assert body["batch_key"] == "", "无批次 → 空串（PRD R-20）"
    assert body["batch_state"] == "", "无批次 → 空串（PRD R-20）"
    assert body["replayed"] is False
    # 既有字段语义不变
    assert body["status"] == "QUEUED"
    assert set(body) >= {"task_id", "status", "created_at", "replayed"}
    assert resp.headers["Location"].endswith(body["task_id"])
