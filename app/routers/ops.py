"""运维观测端点。

鉴权复用终端用户令牌（``require_caller``）——本服务没有独立的管理身份，
这些端点只暴露聚合统计与单任务诊断，不含任何敏感内容：
- 不返回用户 sk（令牌会话只给存在性 + TTL）；
- 不返回结果原文（只给字节数与 Content-Type）；
- 不返回请求体（只给路径与模型）。
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.deps.auth import Caller, require_caller
from app.services import reconcile, slots, taskstore, tokensession

router = APIRouter(prefix="/ops")


@router.get("/stats")
async def stats(caller: Caller = Depends(require_caller)) -> dict:
    """状态分布 + 调用者自己的槽位占用。"""
    return {
        "status_counts": await taskstore.counts_by_status(),
        "my_slots_in_use": await slots.current(caller.token_hash),
    }


@router.get("/tasks/{task_id}")
async def task_detail(task_id: str, _: Caller = Depends(require_caller)) -> dict:
    """单任务诊断视图（脱敏）。"""
    task = await taskstore.get_meta(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    data: dict = task.get("data") or {}
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "fail_reason": task.get("fail_reason", ""),
        "created_at": task.get("created_at", 0),
        "start_time": task.get("start_time", 0),
        "finish_time": task.get("finish_time", 0),
        "channel_id": task.get("channel_id", 0),
        "model": data.get("model", ""),
        "request_path": data.get("request_path", ""),
        "upstream_base_url": data.get("upstream_base_url", ""),
        "upstream_status": data.get("upstream_status", 0),
        "upstream_content_type": data.get("upstream_content_type", ""),
        "response_bytes": data.get("response_bytes", 0),
        "result_purged": bool(data.get("result_purged", False)),
        "dispatch_epoch": data.get("dispatch_epoch", 0),
        "reconcile_pending": bool(data.get("reconcile_pending", False)),
        "reconcile_reason": data.get("reconcile_reason", ""),
        "callback_delivered": data.get("callback_delivered"),
        "token_session": await tokensession.session_info(task_id),
    }


@router.post("/reconcile/run")
async def run_reconcile(_: Caller = Depends(require_caller)) -> dict:
    """手工触发一轮对账（排障用；定时任务每分钟自动跑）。"""
    return await reconcile.run_reconcile()


@router.post("/slots/recalibrate")
async def recalibrate(_: Caller = Depends(require_caller)) -> dict:
    """手工触发槽位校准。"""
    return await reconcile.recalibrate_slots()
