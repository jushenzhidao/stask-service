"""管理面：看板页面 + 看板 API + 动态配置读写。

鉴权走 ``ADMIN_KEY``（`deps/admin.py`）——未配置时**整个管理面 404**。

脱敏纪律（与 `/ops` 一致，不因为是管理面就放松）：
- 绝不返回用户令牌（会话只给存在性 + TTL）；
- 绝不返回请求体与结果原文（只给字节数、Content-Type、路径、模型）；
- 配置读写只覆盖 `dynconf.MUTABLE` 白名单，连接串/密钥/白名单永不可写。

页面本体是单文件 HTML（`app/static/admin.html`），零构建步骤。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse

from app.config import settings
from app.deps.admin import require_admin
from app.logging import log
from app.schemas import ACTIVE
from app.services import dynconf, slots, sweeper, taskstore, tokensession

router = APIRouter(prefix="/admin")

_STATIC = Path(__file__).resolve().parent.parent / "static"


@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def dashboard() -> HTMLResponse:
    """看板页面。

    页面本身**不鉴权**——它只是一个空壳，所有数据都要带密钥调 API 才拿得到。
    密钥存在浏览器 sessionStorage，不落 URL。管理面未启用时同样 404。
    """
    from app.deps.admin import admin_enabled

    if not admin_enabled():
        raise HTTPException(404, "not found")
    page = _STATIC / "admin.html"
    if not page.exists():
        raise HTTPException(500, "dashboard asset missing")
    return HTMLResponse(page.read_text(encoding="utf-8"))


@router.get("/api/overview")
async def overview(
    window: int = Query(3600, ge=60, le=7 * 86400),
    _: None = Depends(require_admin),
) -> dict:
    """概览：窗口内指标 + 运行时信息。"""
    try:
        data = await taskstore.metrics(window)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    data["service"] = {
        "version": settings.app_version,
        "env": settings.app_env,
        "platform": settings.gateway_platform,
        "channel_id": settings.channel_id,
        "upstream_default": settings.upstream_base_url,
        "sweep_enabled": await dynconf.get_bool("sweep_enabled"),
        "override_count": (await dynconf.snapshot())["override_count"],
    }
    return data


@router.get("/api/slots")
async def slot_watermark(
    limit: int = Query(50, ge=1, le=500),
    _: None = Depends(require_admin),
) -> dict:
    """按 (模型, token) 的在途占用与上限 Top N（PRD R-22 / AC-62）。

    回答的是运维最常问的那个问题：「闸门到底是满的还是坏的」。

    两个字段必须同时给，缺一个就会误判：
    - ``in_flight`` 来自 tasks 表的掩码谓词（**只数真正持该层**的任务，见
      ``SQL_HOLDS_LAYER2``）；若按「活跃行数」统计，等待期任务会被算成占用，
      于是「看起来满了」而实际是空闲；
    - ``limit`` 来自当前策略快照。它与 ``in_flight`` 的口径必须来自同一套
      语义，否则「满了」这个结论无从判断。

    同时回 ``global`` 一层：第二层没满而第三层满了是**跨 token 的**饱和，
    只看 (模型, token) 会得出「明明没人用却是 429」的错觉。

    管理面才看得到 token_hash（与 ``/ops`` 只给调用者自己的占用不同）——
    PRD §4.4 明确要求按 (模型, token) 列出，那必然要跨 token。
    """
    from app.services import modelpolicy

    usage = await taskstore.active_counts_by_model_token()
    policies = await dynconf.get_model_policies()
    cfg = await dynconf.get_runtime_config()

    rows: list[dict[str, Any]] = []
    for (token_hash, model), in_flight in usage.items():
        policy = modelpolicy.resolve(
            model=model, policies=policies,
            default_limit_per_token=cfg.max_slots,
        )
        rows.append({
            "token_hash": token_hash,
            "model": model,
            "in_flight": in_flight,
            "limit": policy.limit_model_token,
            "global_limit": policy.limit_global,
            "source": policy.source,
        })
    rows.sort(key=lambda r: (-r["in_flight"], r["model"], r["token_hash"]))
    return {"slots": rows[:limit], "total": len(rows)}


@router.get("/api/schedule")
async def schedule_overview(_: None = Depends(require_admin)) -> dict:
    """调度视图（PRD R-21）：计划中任务（按小时分桶）+ 等待中批次 + 重排积压。

    **必须挂在管理面，不能挂用户面的 ``/ops``**：批次归组键在
    ``token_model`` 维度下含 token_hash 前缀、也可能是客户端自定义串，
    暴露给任意已鉴权调用者等于泄露别人家的 key 指纹。管理面才看得到全局面貌。

    三类数据合在一个端点：运维判断「系统是不是在等」时，这三者是同一个问题的
    三个侧面（等时刻 / 等凑批 / 等槽），分开看会来回切页面。
    """
    from app.services import batching, dispatch

    now = taskstore.now()
    planned = await taskstore.scheduled_overview(now)
    return {
        "planned_by_hour": planned,
        "planned_total": sum(item["count"] for item in planned),
        "batches": (await batching.stats())["batches"],
        "requeue_pending": (await dispatch.stats())["requeue_pending"],
    }


@router.get("/api/tasks")
async def list_tasks(
    status: str = Query("", pattern="^(QUEUED|IN_PROGRESS|SUCCESS|FAILURE|CANCELED)?$"),
    model: str = Query("", max_length=128),
    task_id: str = Query("", max_length=64),
    task_id_prefix: str = Query("", max_length=64),
    since: int = Query(0, ge=0, le=7 * 86400),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=10_000),
    _: None = Depends(require_admin),
) -> dict:
    """任务列表（分页 + 筛选）。不含请求体与结果原文。

    ``task_id`` 是精确匹配；前缀检索使用 ``task_id_prefix``，避免前导通配符
    令 task_id 索引失效。两者同时提供会返回统一的参数错误。
    """
    try:
        params: dict[str, Any] = {
            "status": status, "model": model, "task_id": task_id,
            "since_seconds": since, "limit": limit, "offset": offset,
        }
        if task_id_prefix:
            params["task_id_prefix"] = task_id_prefix
        return await taskstore.search(**params)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/api/tasks/{task_id}")
async def task_detail(task_id: str, _: None = Depends(require_admin)) -> dict:
    """单任务详情（脱敏，走 ``get_meta`` 元数据投影，不拉结果体大字段）。

    ``fail_reason`` 现在带上游的具体错误消息（``upstream 400: <message>``），
    看板不必再让人回 DB 解 ``upstream_response`` 才知道为什么失败。

    ``artifacts`` / ``result_url`` 是产出侧的事实；``private_result_url``
    是写进 new-api 原生 ``private_data`` 列的那一份——两者应当一致，
    不一致就说明终态落库时 private 补丁没写上（宿主看板会显示不出结果）。
    """
    task = await taskstore.get_meta(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    data: dict = task.get("data") or {}
    private: dict = task.get("private_data") or {}
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "fail_reason": task.get("fail_reason", ""),
        "progress": task.get("progress", ""),
        "channel_id": task.get("channel_id", 0),
        "user_id": task.get("user_id", 0),
        "created_at": task.get("created_at", 0),
        "start_time": task.get("start_time", 0),
        "finish_time": task.get("finish_time", 0),
        "model": data.get("model", ""),
        "request_method": data.get("request_method", ""),
        "request_path": data.get("request_path", ""),
        "request_query": data.get("request_query", ""),
        "upstream_base_url": data.get("upstream_base_url", ""),
        "upstream_status": data.get("upstream_status", 0),
        "upstream_content_type": data.get("upstream_content_type", ""),
        "response_bytes": data.get("response_bytes", 0),
        "response_encoding": data.get("upstream_response_encoding", ""),
        "request_body_encoding": data.get("request_body_encoding", ""),
        "result_purged": bool(data.get("result_purged", False)),
        "body_truncated": bool(data.get("body_truncated", False)),
        "dispatch_epoch": data.get("dispatch_epoch", 0),
        "idempotency_key": data.get("idempotency_key", ""),
        "callback_url": data.get("callback_url", ""),
        "callback_delivered": data.get("callback_delivered"),
        # 产出侧
        "artifacts": data.get("artifacts", []),
        "artifact_count": data.get("artifact_count", 0),
        "artifact_parser": data.get("artifact_parser", ""),
        "result_url": data.get("result_url", ""),
        "private_result_url": private.get("result_url", ""),
        "slots_in_use": await slots.current(str(data.get("token_hash") or "")),
        "token_session": await tokensession.session_info(task_id),
    }


@router.post("/api/tasks/{task_id}/requeue")
async def requeue(task_id: str, _: None = Depends(require_admin)) -> dict:
    """重投一个卡住的任务（排障用）。

    **只对非终态任务开放**，且**绝不清派发锁**——锁在就意味着一次调用
    可能在飞。这里只是把消息重新丢进队列，真正的防重仍由派发锁把关：
    如果锁还在，worker 会直接跳过而不是再打一次上游。

    换句话说：这个按钮救的是「消息丢了」，不是「我想再跑一次」。
    """
    task = await taskstore.get_meta(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    if task["status"] not in ACTIVE:
        raise HTTPException(409, f"task already terminal: {task['status']}")

    from app.queue import publish_execute

    await publish_execute(task_id)
    log.warning("task requeued from admin: task_id={}", task_id)
    return {"task_id": task_id, "status": task["status"], "requeued": True}


@router.get("/api/config")
async def read_config(_: None = Depends(require_admin)) -> dict:
    """可热改配置的全量视图 + 只读项及其原因。"""
    return await dynconf.snapshot()


@router.put("/api/config")
async def write_config(
    updates: dict[str, Any] = Body(...),
    mode: str = Query("replace", pattern="^(replace|merge)$"),
    _: None = Depends(require_admin),
) -> dict:
    """批量更新覆盖值。白名单外的键一律拒绝，校验失败整批回退。

    ``?mode=merge`` 时，映射型配置项（``model_policies``）按**顶层键合并**：
    只覆盖/新增本次写到的模型条目，未写到的保持原值——不必再「先读全表 →
    整表写回」。默认 ``replace`` 保持既有整表替换语义。
    """
    try:
        return await dynconf.set_many(updates, mode=mode)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/api/config/reset")
async def reset_config(
    keys: list[str] | None = Body(None),
    _: None = Depends(require_admin),
) -> dict:
    """删除覆盖值回落 env。``keys`` 为空/省略则清空全部覆盖。

    用 POST 而不是 DELETE：DELETE 带请求体在很多 HTTP 客户端与代理上
    行为不一致（有的直接丢掉 body）。
    """
    try:
        return await dynconf.reset(keys)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/api/jobs/{job}")
async def run_job(job: str, _: None = Depends(require_admin)) -> dict:
    """手工触发定时任务（卡死收敛 / 超龄判死 / 槽位校准 / 结果清理）。"""
    jobs = {
        "stale": sweeper.sweep_stale,
        "overdue": sweeper.sweep_overdue,
        "slots": sweeper.recalibrate_slots,
        "purge": sweeper.purge_results,
    }
    handler = jobs.get(job)
    if handler is None:
        raise HTTPException(404, f"unknown job: {job} (valid: {sorted(jobs)})")
    return await handler()
