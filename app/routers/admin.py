"""管理面：看板页面 + 看板 API + 动态配置读写。

鉴权走 ``ST_ADMIN_KEY``（`deps/admin.py`）——未配置时**整个管理面 404**。

脱敏纪律（与 `/ops` 一致，不因为是管理面就放松）：
- 绝不返回用户 sk（令牌会话只给存在性 + TTL）；
- 绝不返回请求体与结果原文（只给字节数、Content-Type、路径、模型）；
- 配置读写只覆盖 `dynconf.MUTABLE` 白名单，连接串/密钥/白名单永不可写。

页面本体是单文件 HTML（`app/static/admin.html`），零构建步骤——不引入
node 工具链是有意的：这个服务的部署应该是「一个镜像 + 一个 compose」，
为一个内部看板搭前端构建链不划算。
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
from app.services import dynconf, reconcile, slots, taskstore, tokensession

router = APIRouter(prefix="/admin")

_STATIC = Path(__file__).resolve().parent.parent / "static"


@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def dashboard() -> HTMLResponse:
    """看板页面。

    页面本身**不鉴权**——它只是一个空壳，所有数据都要带密钥调 API 才拿得到。
    密钥存在浏览器 sessionStorage，不落 URL（URL 会进 nginx access log
    和浏览器历史）。管理面未启用时同样 404，不泄露存在性。
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
    """概览：窗口内指标 + 队列健康 + 运行时信息。"""
    try:
        data = await taskstore.metrics(window)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    data["service"] = {
        "version": settings.app_version,
        "env": settings.app_env,
        "platform": settings.gateway_platform,
        "upstream_default": settings.newapi_base_url,
        "sweep_enabled": await dynconf.get_bool("sweep_enabled"),
        "override_count": (await dynconf.snapshot())["override_count"],
    }
    return data


@router.get("/api/tasks")
async def list_tasks(
    status: str = Query("", pattern="^(SUBMITTED|IN_PROGRESS|SUCCESS|FAILURE|CANCELED)?$"),
    model: str = Query("", max_length=128),
    task_id: str = Query("", max_length=64),
    task_id_prefix: str = Query("", max_length=64),
    reconcile_only: bool = False,
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
            "reconcile_only": reconcile_only, "since_seconds": since,
            "limit": limit, "offset": offset,
        }
        if task_id_prefix:
            params["task_id_prefix"] = task_id_prefix
        return await taskstore.search(**params)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/api/tasks/{task_id}")
async def task_detail(task_id: str, _: None = Depends(require_admin)) -> dict:
    """单任务详情（脱敏）。

    走 ``get_meta`` 而非 ``get``：这里返回的字段没有一个来自结果原文，
    脱敏纪律本来就禁止返回结果体——没必要把 10MB 的 ``upstream_response``
    拉过 DB 连接再丢掉。
    """
    task = await taskstore.get_meta(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    data: dict = task.get("data") or {}
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "fail_reason": task.get("fail_reason", ""),
        "progress": task.get("progress", ""),
        "user_id": task.get("user_id", 0),
        "channel_id": task.get("channel_id", 0),
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
        "result_purged": bool(data.get("result_purged", False)),
        "body_truncated": bool(data.get("body_truncated", False)),
        "dispatch_epoch": data.get("dispatch_epoch", 0),
        "idempotency_key": data.get("idempotency_key", ""),
        "callback_url": data.get("callback_url", ""),
        "callback_delivered": data.get("callback_delivered"),
        "reconcile_pending": bool(data.get("reconcile_pending", False)),
        "reconcile_reason": data.get("reconcile_reason", ""),
        "reconcile_charge_id": data.get("reconcile_charge_id", ""),
        "slots_in_use": await slots.current(str(data.get("token_hash") or "")),
        "token_session": await tokensession.session_info(task_id),
    }


@router.post("/api/tasks/{task_id}/requeue")
async def requeue(task_id: str, _: None = Depends(require_admin)) -> dict:
    """重投一个卡住的任务（排障用）。

    **只对非终态任务开放**，且**绝不清派发锁**——锁在就意味着可能已经
    调用过上游、可能已扣费。这里只是把消息重新丢进队列，真正的防重仍由
    派发锁把关：如果锁还在，worker 会把它转成待对账而不是再打一次上游。

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
    _: None = Depends(require_admin),
) -> dict:
    """批量更新覆盖值。白名单外的键一律拒绝，校验失败整批回退。"""
    try:
        return await dynconf.set_many(updates)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/api/config/reset")
async def reset_config(
    keys: list[str] | None = Body(None),
    _: None = Depends(require_admin),
) -> dict:
    """删除覆盖值回落 env。``keys`` 为空/省略则清空全部覆盖。

    用 POST 而不是 DELETE：DELETE 带请求体在很多 HTTP 客户端与代理上
    行为不一致（有的直接丢掉 body），curl 也需要额外参数。语义上这是个
    动作而非资源删除，POST 更稳。
    """
    try:
        return await dynconf.reset(keys)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/api/jobs/{job}")
async def run_job(job: str, _: None = Depends(require_admin)) -> dict:
    """手工触发定时任务（对账 / 槽位校准 / 卡死扫描 / 结果清理）。"""
    jobs = {
        "reconcile": reconcile.run_reconcile,
        "slots": reconcile.recalibrate_slots,
        "stale": reconcile.sweep_stale,
        "purge": reconcile.purge_results,
    }
    handler = jobs.get(job)
    if handler is None:
        raise HTTPException(404, f"unknown job: {job} (valid: {sorted(jobs)})")
    return await handler()
