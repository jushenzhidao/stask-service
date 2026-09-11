"""管理面：鉴权边界 + 看板 API + 动态配置。

安全断言是本文件的重点：管理面能改全局配置，鉴权一旦松了就是提权漏洞。
"""

from __future__ import annotations

import pytest

from app.services import dynconf
from tests.conftest import AUTH, stored_body

ADMIN = {"X-Admin-Key": "admin-secret"}


@pytest.fixture
def admin_client(client, monkeypatch, test_settings):
    """启用管理面的客户端（settings.admin_key 非空）。"""
    monkeypatch.setattr(test_settings, "admin_key", "admin-secret")
    return client


# ---------------------------------------------------------------------------
# 鉴权边界
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", [
    "/admin", "/admin/api/overview", "/admin/api/tasks", "/admin/api/config",
])
def test_disabled_admin_returns_404(client, test_settings, path):
    """未配置 ADMIN_KEY 时整个管理面 404。

    404 而非 401：不泄露「这里有个管理后台」。默认不开启——忘配密钥
    不等于裸奔。
    """
    assert client.get(path).status_code == 404


def test_wrong_key_401(admin_client):
    resp = admin_client.get("/admin/api/overview", headers={"X-Admin-Key": "nope"})
    assert resp.status_code == 401


def test_missing_key_401(admin_client):
    assert admin_client.get("/admin/api/overview").status_code == 401


def test_user_sk_cannot_access_admin(admin_client):
    """终端用户 sk **不能**进管理面——否则任何用户都能改全局配置（提权）。"""
    from tests.conftest import AUTH

    assert admin_client.get("/admin/api/overview", headers=AUTH).status_code == 401


def test_bearer_form_accepted(admin_client):
    """兼容 Bearer 写法，方便 curl 与 fetch 复用同一套 header。"""
    resp = admin_client.get("/admin/api/overview",
                            headers={"Authorization": "Bearer admin-secret"})
    assert resp.status_code == 200


def test_dashboard_page_served(admin_client):
    """页面本体不鉴权（空壳），数据全靠带密钥的 API。"""
    resp = admin_client.get("/admin")
    assert resp.status_code == 200
    assert "stask-service" in resp.text
    # 密钥绝不能被烘进页面
    assert "admin-secret" not in resp.text


def test_dashboard_guards_storage_access(admin_client):
    """回归：sessionStorage 必须包在 try/catch 里。

    实测踩过——无痕模式/沙箱 iframe/file:// 下访问 sessionStorage 抛
    SecurityError，裸调用会让脚本在第一行就停止执行，登录框都出不来，
    页面全白（agent-browser snapshot 显示 "(empty page)"）。
    """
    html = admin_client.get("/admin").text
    # 除 store 门面内部，不得再有裸的 sessionStorage 调用
    assert "sessionStorage.getItem" not in html.replace(
        "try { return sessionStorage.getItem(k) || \"\"; }", "")
    assert "const store = {" in html
    # 初始化失败必须回登录页，否则 login 与 app 双双 hidden = 白屏
    assert 'if ($("app").hidden && $("login").hidden) logout(' in html


def test_dashboard_has_no_emoji(admin_client):
    """P0 规则：功能图标一律内联 SVG，不用 emoji。"""
    import re

    pattern = re.compile(
        "[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF"
        "\U0001FA00-\U0001FAFF\U0001F000-\U0001F0FF]"
    )
    assert not pattern.search(admin_client.get("/admin").text)


# ---------------------------------------------------------------------------
# 动态配置
# ---------------------------------------------------------------------------


def test_config_snapshot_shape(admin_client):
    d = admin_client.get("/admin/api/config", headers=ADMIN).json()
    assert d["override_count"] == 0
    keys = {i["key"] for g in d["groups"] for i in g["items"]}
    assert {"max_slots", "rate_limit", "retry_max", "sweep_enabled"} <= keys
    # 只读项必须带原因，看板要展示给运维看
    assert all(i["reason"] for i in d["immutable"])


def test_config_write_takes_effect(admin_client):
    resp = admin_client.put("/admin/api/config", headers=ADMIN,
                            json={"max_slots": 99})
    assert resp.status_code == 200
    assert resp.json()["override_count"] == 1

    item = next(i for g in resp.json()["groups"] for i in g["items"]
                if i["key"] == "max_slots")
    assert item["value"] == 99
    assert item["overridden"] is True


async def test_dynconf_overrides_settings(patch_redis, test_settings):
    """读取优先级：Redis 覆盖 > settings。"""
    assert await dynconf.get_int("max_slots") == test_settings.max_slots
    await dynconf.set_many({"max_slots": 42})
    assert await dynconf.get_int("max_slots") == 42


async def test_dynconf_reset_falls_back(patch_redis, test_settings):
    await dynconf.set_many({"max_slots": 42})
    await dynconf.reset(["max_slots"])
    assert await dynconf.get_int("max_slots") == test_settings.max_slots


def test_reset_endpoint_selective(admin_client):
    """按 key 重置：只回落指定项，其余覆盖保留。"""
    admin_client.put("/admin/api/config", headers=ADMIN,
                     json={"max_slots": 55, "retry_max": 3})
    resp = admin_client.post("/admin/api/config/reset", headers=ADMIN,
                             json=["max_slots"])
    assert resp.status_code == 200
    assert resp.json()["override_count"] == 1        # retry_max 仍被覆盖


def test_reset_endpoint_all(admin_client):
    """省略 body 即清空全部覆盖。

    用 POST 而非 DELETE：DELETE 带请求体在部分 HTTP 客户端与代理上会被
    静默丢弃，实测 TestClient.delete() 就不接受 json 参数。
    """
    admin_client.put("/admin/api/config", headers=ADMIN,
                     json={"max_slots": 55, "retry_max": 3})
    resp = admin_client.post("/admin/api/config/reset", headers=ADMIN)
    assert resp.status_code == 200
    assert resp.json()["override_count"] == 0


def test_reset_unknown_key_rejected(admin_client):
    resp = admin_client.post("/admin/api/config/reset", headers=ADMIN,
                             json=["not_a_real_key"])
    assert resp.status_code == 400


@pytest.mark.parametrize("key", [
    "database_url", "redis_url", "upstream_allowlist", "callback_secret",
    "async_deny_prefixes", "gateway_platform", "auth_mode",
])
def test_immutable_keys_rejected(admin_client, key):
    """安全边界：这些项永不可运行时改。

    upstream_allowlist 可写 = 把「防 sk 打到野地址」的防线挂到网上；
    deny_prefixes 可写 = 攻击面收口点被打开。
    """
    resp = admin_client.put("/admin/api/config", headers=ADMIN, json={key: "x"})
    assert resp.status_code == 400
    assert "cannot be changed at runtime" in resp.json()["error"]["message"]


def test_unknown_key_rejected(admin_client):
    """白名单机制：未登记的键默认拒绝（不是黑名单）。"""
    resp = admin_client.put("/admin/api/config", headers=ADMIN,
                            json={"totally_made_up": 1})
    assert resp.status_code == 400


def test_out_of_range_rejected_atomically(admin_client):
    """区间校验失败**整批拒绝**——半套配置比旧配置更危险。"""
    resp = admin_client.put("/admin/api/config", headers=ADMIN,
                            json={"max_slots": 5, "retry_max": 9999})
    assert resp.status_code == 400
    # 合法的那一项也不能生效
    assert admin_client.get("/admin/api/config",
                            headers=ADMIN).json()["override_count"] == 0


def test_bool_coercion(admin_client):
    for raw in ("false", False, "off", "0"):
        admin_client.put("/admin/api/config", headers=ADMIN,
                         json={"sweep_enabled": raw})
        item = next(i for g in admin_client.get("/admin/api/config", headers=ADMIN)
                    .json()["groups"] for i in g["items"] if i["key"] == "sweep_enabled")
        assert item["value"] is False


async def test_dynconf_survives_redis_outage(patch_redis, monkeypatch, test_settings):
    """Redis 挂了必须回落 env——动态配置是增强，不能成为可用性单点。"""
    async def boom(*_a, **_kw):
        raise ConnectionError("redis down")

    monkeypatch.setattr(patch_redis, "hgetall", boom)
    assert await dynconf.get_int("max_slots") == test_settings.max_slots


async def test_rate_limit_reads_dynconf(client, patch_redis, test_settings):
    """端到端：在管理面把限流调成 1，下一次提交立刻被拦。"""
    await dynconf.set_many({"rate_limit": 1})
    body = {"model": "dall-e-3", "prompt": "x"}
    from tests.conftest import AUTH

    codes = [client.post("/async/v1/images/generations", json=body,
                         headers=AUTH).status_code for _ in range(3)]
    assert codes[0] == 202
    assert 429 in codes[1:]


# ---------------------------------------------------------------------------
# 看板 API
# ---------------------------------------------------------------------------


async def test_task_detail_never_leaks_secrets(admin_client, task_store, patch_redis):
    """脱敏纪律不因为是管理面就放松：不返回 sk、不返回请求体与结果原文。

    同时守一条新增的边界：``private_data`` 里若被宿主写了渠道 ``key``
    （Gemini/Vertex 渠道会写），管理面也绝不吐出去——那是 new-api 自己都
    标了 ``json:"-"`` 的字段。这里用一条任务同时验证 data 与 private 两侧。
    """
    from app.services import tokensession

    task_id = "img_" + "1" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": "dall-e-3",
        **stored_body("request_body", b'{"prompt":"top secret prompt"}'),
        **stored_body("upstream_response", b'{"url":"https://cdn/secret.png"}'),
        "response_bytes": 33,
    })
    # 模拟宿主往 private_data 写渠道密钥（new-api 原生行为）
    task_store.rows[task_id]["private_data"] = {"key": "sk-upstream-chan-secret"}
    await tokensession.store(task_id, "sk-test-token")

    resp = admin_client.get(f"/admin/api/tasks/{task_id}", headers=ADMIN)
    assert resp.status_code == 200
    text = resp.text
    assert "sk-test-token" not in text
    assert "top secret prompt" not in text
    assert "secret.png" not in text
    assert "sk-upstream-chan-secret" not in text
    assert resp.json()["token_session"]["exists"] is True
    assert resp.json()["response_bytes"] == 33


def test_task_detail_404(admin_client):
    assert admin_client.get(f"/admin/api/tasks/img_{'9' * 32}",
                            headers=ADMIN).status_code == 404


async def test_requeue_only_for_active(admin_client, task_store, queue_events):
    task_id = "img_" + "2" * 32
    await task_store.create(task_id, "/x", {"token_hash": "th"})

    resp = admin_client.post(f"/admin/api/tasks/{task_id}/requeue", headers=ADMIN)
    assert resp.status_code == 200
    assert queue_events.execute == [task_id]

    task_store.rows[task_id]["status"] = "SUCCESS"
    assert admin_client.post(f"/admin/api/tasks/{task_id}/requeue",
                             headers=ADMIN).status_code == 409


def test_invalid_status_filter_422(admin_client):
    assert admin_client.get("/admin/api/tasks?status=BOGUS",
                            headers=ADMIN).status_code == 422


# ---------------------------------------------------------------------------
# task_id 检索契约：精确 vs 前缀
# ---------------------------------------------------------------------------


@pytest.fixture
async def three_tasks(task_store):
    """img_aaa… / img_aab… / vid_aaa… ——前缀可区分、精确可区分。"""
    ids = ["img_" + "a" * 32, "img_" + "a" * 31 + "b", "vid_" + "a" * 32]
    for tid in ids:
        await task_store.create(tid, "/x", {"model": "dall-e-3"})
    return ids


async def test_task_id_is_exact_match(admin_client, three_tasks):
    """精确匹配只命中完全相同的 task_id。

    回归：旧实现是 ``task_id in row["task_id"]`` 片段匹配，替身与生产
    （``WHERE task_id = :tid``）行为不一致——测试绿、线上返回空。
    """
    full = three_tasks[0]
    d = admin_client.get(f"/admin/api/tasks?task_id={full}", headers=ADMIN).json()
    assert [i["task_id"] for i in d["items"]] == [full]
    assert d["total"] == 1

    # 片段不再命中任何行（这正是用户看到的「任务不存在」）
    frag = admin_client.get(f"/admin/api/tasks?task_id={full[:10]}",
                            headers=ADMIN).json()
    assert frag["total"] == 0


async def test_task_id_prefix_matches_all_with_prefix(admin_client, three_tasks):
    d = admin_client.get("/admin/api/tasks?task_id_prefix=img_", headers=ADMIN).json()
    assert sorted(i["task_id"] for i in d["items"]) == sorted(three_tasks[:2])
    assert d["total"] == 2


async def test_task_id_prefix_is_anchored(admin_client, three_tasks):
    """前缀是锚定的：中间片段不匹配（否则又退化成全表扫描的 LIKE '%x%'）。"""
    d = admin_client.get("/admin/api/tasks?task_id_prefix=aaa", headers=ADMIN).json()
    assert d["total"] == 0


def test_task_id_and_prefix_together_400(admin_client):
    resp = admin_client.get(
        f"/admin/api/tasks?task_id=img_{'a' * 32}&task_id_prefix=img_", headers=ADMIN)
    assert resp.status_code == 400
    assert "cannot be used together" in resp.json()["error"]["message"]


@pytest.mark.parametrize("param", ["task_id", "task_id_prefix"])
@pytest.mark.parametrize("bad", ["img%", "img\\_"])
def test_wildcard_characters_rejected_400(admin_client, param, bad):
    """``%`` / ``\\`` 一律拒绝：允许它们等于把 LIKE 通配符注入交给用户。"""
    resp = admin_client.get(f"/admin/api/tasks?{param}={bad}", headers=ADMIN)
    assert resp.status_code == 400


def test_real_taskstore_validates_before_db():
    """替身之外，直接压一遍**生产**校验函数。

    替身复用了 ``_validate_search_params``，若生产 ``search()`` 哪天忘了调用
    它，替身仍会校验、测试仍绿——这条断言把生产侧的调用点钉住。
    """
    import asyncio
    import inspect

    from app.services import taskstore

    assert "_validate_search_params(" in inspect.getsource(taskstore.search)
    with pytest.raises(ValueError, match="cannot be used together"):
        asyncio.run(taskstore.search(task_id="a", task_id_prefix="b"))


def test_dashboard_offers_exact_and_prefix_modes(admin_client):
    """页面必须让用户显式选择匹配方式，且不再宣称支持「片段」。"""
    html = admin_client.get("/admin").text
    assert 'id="fTaskIdMode"' in html
    assert '<option value="prefix">' in html
    assert 'q.set($("fTaskIdMode").value === "prefix" ? "task_id_prefix" : "task_id"' in html
    assert 'placeholder="task_id 片段"' not in html
    # 400 必须落到任务面板内的错误位，而不是静默失败
    assert 'id="tkErr"' in html


def test_job_trigger(admin_client):
    resp = admin_client.post("/admin/api/jobs/stale", headers=ADMIN)
    assert resp.status_code == 200
    assert "scanned" in resp.json()


def test_unknown_job_404(admin_client):
    assert admin_client.post("/admin/api/jobs/nope",
                             headers=ADMIN).status_code == 404


# ---------------------------------------------------------------------------
# 调度视图（PRD R-21）
# ---------------------------------------------------------------------------


def test_schedule_view_requires_admin(admin_client):
    """调度视图含归组键（可能带 token 指纹），**不得**对用户面开放。"""
    assert admin_client.get("/admin/api/schedule").status_code == 401
    assert admin_client.get(
        "/admin/api/schedule", headers=AUTH).status_code == 401


async def test_schedule_view_reports_planned_batches_and_requeue(
    admin_client, task_store, patch_redis
):
    """三类等待必须各归各位：等时刻 / 等凑批 / 等槽。

    这条用例同时是「不加新功能就把孤儿函数接上」的验收：调度视图的数据源
    就是此前无人调用的 ``batching.stats`` 与 ``dispatch.stats``。
    """
    from app.services import dispatch, taskstore

    now = taskstore.now()
    # 计划中（延迟未到点），落在一个明确的小时桶里
    await task_store.create("dl_" + "1" * 32, "/x", {
        "token_hash": "th", "model": "m", "batch_state": "scheduled",
        "scheduled_at": now + 7200,
    })
    # 等待凑批的批次成员
    await task_store.create("img_" + "2" * 32, "/v1/images/generations", {
        "token_hash": "th", "model": "m", "slot_model": "m", "slot_flags": 0,
        "batch_state": "waiting", "batch_size": 5, "batch_wait": 60,
        "batch_due_at": now + 60,
    })
    from app.services import batching

    await batching.join("img_" + "2" * 32, "m", batch_size=5, batch_wait=60)
    # 等待槽的重排任务
    await dispatch.requeue("img_" + "2" * 32, backoff_ceiling=60)

    resp = admin_client.get("/admin/api/schedule", headers=ADMIN)
    assert resp.status_code == 200
    body = resp.json()

    assert body["planned_total"] == 1
    assert body["planned_by_hour"], "按小时分桶不得为空"
    bucket = body["planned_by_hour"][0]
    assert bucket["bucket"] % 3600 == 0, "桶边界必须对齐整小时"
    assert bucket["count"] == 1
    assert any(b["key"] == "m" for b in body["batches"])
    assert body["requeue_pending"] >= 1


async def test_slot_watermark_reports_in_flight_and_limit(
    admin_client, task_store, patch_redis
):
    """AC-62：按 (模型, token) 给出在途占用与上限。

    两处最容易做错的地方：
    1. ``in_flight`` 必须只数**真正持第二层**的任务——等待期任务掩码为 0，
       若按「活跃行数」统计就会显示「明明满了其实空闲」；
    2. 必须同时给 ``limit``，否则「满了」这个结论无从判断。
    """
    await task_store.create("img_" + "7" * 32, "/v1/images/generations", {
        "token_hash": "th-abc", "model": "dall-e-3", "slot_model": "dall-e-3",
        "slot_flags": 0b011,          # 占了第一层 + 第二层
    })
    # 等待期任务：不该被算进第二层占用
    await task_store.create("img_" + "8" * 32, "/v1/images/generations", {
        "token_hash": "th-abc", "model": "dall-e-3", "slot_model": "dall-e-3",
        "slot_flags": 0, "batch_state": "waiting",
    })

    resp = admin_client.get("/admin/api/slots", headers=ADMIN)
    assert resp.status_code == 200
    row = next(r for r in resp.json()["slots"] if r["token_hash"] == "th-abc")
    assert row["in_flight"] == 1, "等待期任务不得被算成在途占用"
    assert row["model"] == "dall-e-3"
    assert "limit" in row and "global_limit" in row


def test_slot_watermark_requires_admin(admin_client):
    assert admin_client.get("/admin/api/slots").status_code == 401
