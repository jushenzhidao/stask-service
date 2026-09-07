"""超时对账三态 + 槽位校准 + 结果清理。

对应 AC-26 / AC-27 / AC-28 / AC-32。

三态的区分是这个模块存在的全部意义：
- 查到扣费 → 补记 SUCCESS（钱扣了，任务标成功，同时打「已付费未交付」告警）
- 确认无记录 → FAILURE（没扣钱，安全）
- 查询失败 → **保持挂起**（既不能判死也不能判活）

把第三态混进第二态会把成功任务误判为失败——用户付了钱，系统告诉他失败了。
"""

from __future__ import annotations

from app.services import reconcile, slots, tokensession

TASK = "dall_e_3_" + "c" * 32
TH = "tokenhash0000000000000000000000"


async def _seed_pending(task_store, patch_redis, *, age: int = 300,
                        with_session: bool = True, callback_url: str = "") -> str:
    await task_store.create(TASK, 42, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "callback_url": callback_url,
        "inflight_slot": True, "reconcile_pending": True,
        "reconcile_checked_at": 0, "reconcile_reason": "timeout:ReadTimeout",
        "upstream_response": "", "upstream_status": 0,
    })
    row = task_store.rows[TASK]
    row["status"] = "IN_PROGRESS"
    row["created_at"] = task_store.now() - age
    if with_session:
        await tokensession.store(TASK, "sk-test-token")
    await slots.acquire(TH, 10)
    return TASK


# ---------------------------------------------------------------------------
# 三态
# ---------------------------------------------------------------------------


async def test_charge_found_recovers_success(task_store, patch_redis, fake_billing,
                                             test_settings, queue_events):
    """AC-26：查到成功扣费 → 补记 SUCCESS（结果体缺失，打告警）。"""
    await _seed_pending(task_store, patch_redis)
    fake_billing.charges[TASK] = {"request_id": "req-1", "amount": 40000,
                                  "direction": "settle"}

    result = await reconcile.run_reconcile()

    assert result["stats"] == {"success_recovered": 1}
    row = task_store.rows[TASK]
    assert row["status"] == "SUCCESS"
    assert row["data"]["reconciled"] is True
    assert row["data"]["reconcile_pending"] is False
    assert row["data"]["reconcile_charge_id"] == "req-1"
    assert row["data"]["upstream_response"] == ""       # 结果体确实拿不到
    assert await slots.current(TH) == 0                 # 终态释放槽


async def test_no_charge_confirms_failure(task_store, patch_redis, fake_billing,
                                          test_settings):
    """AC-27：窗口内确认无扣费记录 → FAILURE（没花钱，安全）。"""
    await _seed_pending(task_store, patch_redis)

    result = await reconcile.run_reconcile()

    assert result["stats"] == {"failure_confirmed": 1}
    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert "no charge record" in row["fail_reason"]
    assert await slots.current(TH) == 0


async def test_query_failure_keeps_task_hanging(task_store, patch_redis,
                                                fake_billing, test_settings):
    """AC-28：查询本身失败 → 保持非终态挂起，绝不判死。

    这里是最容易写错的地方：把「查不到」当成「没有」会把成功任务误判
    为失败，用户付了钱却被告知失败。
    """
    await _seed_pending(task_store, patch_redis)
    fake_billing.fail_find = True

    result = await reconcile.run_reconcile()

    assert result["stats"] == {"query_failed": 1}
    row = task_store.rows[TASK]
    assert row["status"] == "IN_PROGRESS"               # 仍非终态
    assert row["data"]["reconcile_pending"] is True     # 下轮继续查
    assert await slots.current(TH) == 1                 # 槽仍占着（任务在途）


async def test_freeze_direction_is_not_a_charge(task_store, patch_redis,
                                                fake_billing, test_settings):
    """只有 settle/charge 才算真扣费；freeze 是预冻结，relay 失败会回滚。

    把 freeze 当扣费会把「预扣后失败并回滚」的任务误判成成功。
    """
    await _seed_pending(task_store, patch_redis)
    # FakeBilling 直接返回 charges 里的字典，这里改走真 provider 的过滤逻辑
    from app.services.providers.billing_http import _SETTLED_DIRECTIONS

    assert "freeze" not in _SETTLED_DIRECTIONS
    assert "cancel" not in _SETTLED_DIRECTIONS
    assert {"settle", "charge"} == set(_SETTLED_DIRECTIONS)


async def test_expired_session_within_ttl_keeps_waiting(task_store, patch_redis,
                                                        fake_billing, test_settings):
    """会话过期但任务未超龄 → 继续等（不能无凭证去查日志）。"""
    await _seed_pending(task_store, patch_redis, with_session=False, age=100)

    result = await reconcile.run_reconcile()

    assert result["stats"] == {"no_session": 1}
    assert task_store.rows[TASK]["status"] == "IN_PROGRESS"


async def test_expired_session_beyond_ttl_abandons(task_store, patch_redis,
                                                   fake_billing, monkeypatch,
                                                   test_settings):
    """会话过期且超 ST_RECONCILE_TTL → 放弃，判 FAILURE 并留记录。"""
    monkeypatch.setattr(test_settings, "reconcile_ttl", 60)
    await _seed_pending(task_store, patch_redis, with_session=False, age=3600)

    result = await reconcile.run_reconcile()

    assert result["stats"] == {"abandoned": 1}
    assert task_store.rows[TASK]["status"] == "FAILURE"


async def test_recheck_backoff_skips_recent(task_store, patch_redis, fake_billing,
                                            test_settings):
    """同一批任务不该被每分钟反复查——reconcile_checked_at 做退避。"""
    await _seed_pending(task_store, patch_redis)
    task_store.rows[TASK]["data"]["reconcile_checked_at"] = task_store.now()

    result = await reconcile.run_reconcile()
    assert result["scanned"] == 0


async def test_callback_enqueued_after_reconcile(task_store, patch_redis,
                                                 fake_billing, test_settings,
                                                 queue_events):
    await _seed_pending(task_store, patch_redis, callback_url="http://cb/hook")
    fake_billing.charges[TASK] = {"request_id": "r", "direction": "settle"}

    await reconcile.run_reconcile()
    assert queue_events.notify == [(TASK, 1, 0)]


async def test_reconcile_lock_prevents_double_run(task_store, patch_redis,
                                                  fake_billing, test_settings):
    await _seed_pending(task_store, patch_redis)
    first = await reconcile.run_reconcile()
    second = await reconcile.run_reconcile()

    assert "scanned" in first
    assert second == {"skipped": "locked"}


# ---------------------------------------------------------------------------
# 槽位校准
# ---------------------------------------------------------------------------


async def test_recalibrate_fixes_both_directions(task_store, patch_redis,
                                                 test_settings):
    """校准必须双向修：虚高会让用户被永久限流，虚低会让用户超发。"""
    await task_store.create("a_" + "1" * 32, 42, "/x", {"token_hash": TH})
    await task_store.create("b_" + "2" * 32, 42, "/x", {"token_hash": TH})

    await slots.reset(TH, 9)                            # 虚高
    await reconcile.recalibrate_slots()
    assert await slots.current(TH) == 2

    # 换个 key 避开重入锁
    patch_redis._data.pop("st:sweep:slots", None)
    await slots.reset(TH, 0)                            # 虚低
    await reconcile.recalibrate_slots()
    assert await slots.current(TH) == 2


async def test_recalibrate_ignores_terminal(task_store, patch_redis, test_settings):
    await task_store.create("a_" + "1" * 32, 42, "/x", {"token_hash": TH})
    task_store.rows["a_" + "1" * 32]["status"] = "SUCCESS"
    await slots.reset(TH, 5)

    await reconcile.recalibrate_slots()
    assert await slots.current(TH) == 5     # 该 token 无活跃任务 → 不在真值表里


# ---------------------------------------------------------------------------
# 结果清理
# ---------------------------------------------------------------------------


async def test_purge_clears_body_keeps_status_row(task_store, patch_redis,
                                                  monkeypatch, test_settings):
    """AC-32：只清结果体，状态行保留（客户端仍能查到「任务成功但已过期」）。"""
    monkeypatch.setattr(test_settings, "result_ttl_seconds", 100)
    await task_store.create(TASK, 42, "/x", {
        "token_hash": TH, "upstream_response": "gzipped-payload",
        "upstream_status": 200,
    })
    row = task_store.rows[TASK]
    row["status"] = "SUCCESS"
    row["finish_time"] = task_store.now() - 500

    result = await reconcile.purge_results()

    assert result["purged"] == 1
    assert row["status"] == "SUCCESS"                   # 状态行还在
    assert row["data"]["upstream_response"] == ""
    assert row["data"]["result_purged"] is True


async def test_purge_skips_fresh_results(task_store, patch_redis, monkeypatch,
                                         test_settings):
    monkeypatch.setattr(test_settings, "result_ttl_seconds", 86400)
    await task_store.create(TASK, 42, "/x", {"upstream_response": "payload"})
    task_store.rows[TASK]["status"] = "SUCCESS"
    task_store.rows[TASK]["finish_time"] = task_store.now()

    assert (await reconcile.purge_results())["purged"] == 0
    assert task_store.rows[TASK]["data"]["upstream_response"] == "payload"
