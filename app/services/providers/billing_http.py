"""billing 服务适配（地址由 ``ST_BILLING_SVC_URL`` 配置）。

统一前缀 ``/api/v1``；鉴权恒为 ``Authorization: Bearer <终端用户 sk- 令牌>``，
user_id 由令牌解析（本服务不指定 user_id，也不缓存跨用户数据）。

本服务只用三个端点（不做任何资金操作——freeze/settle/cancel 一概不碰）：

- ``POST /auth/inspect`` → 200 **扁平** ``{"valid":true,"user_id","token_id"}`` / 401
- ``GET  /billing/balance`` → 200 ``{"data":{"balance","balance_usd","frozen",...}}``
- ``GET  /billing/logs?attr_filter=task_id=xxx&direction=...&from=&to=``
  → 200 ``{"data":{"logs":[...],"total":N}}``

注意响应包络**不一致**：``/auth/inspect`` 是扁平的，其余走
``writeResult`` 的 ``{"data": ...}`` 包络。这是 billing 服务的既有形态，
适配层负责吸收差异，不要求上游改。
"""

from __future__ import annotations

from typing import Any

from app.config import settings
from app.logging import log
from app.schemas import UserIdentity
from app.services import httpc
from app.services.providers import BillingError

#: 扣费方向：freeze 只是预冻结，settle/charge 才是真实结算。
#: 对账判「上游确实扣了钱」只认后两者——预冻结在上游失败时会被回滚。
_SETTLED_DIRECTIONS = frozenset({"settle", "charge"})


def _err_body(resp: Any) -> str:
    try:
        data = resp.json()
        return str(data.get("error") or data)[:200]
    except Exception:
        return str(resp.text)[:200]


class HttpBillingProvider:
    def _client(self):
        return httpc.shared_client(
            base_url=settings.billing_svc_url, timeout=settings.http_timeout
        )

    async def inspect(self, raw_token: str) -> UserIdentity | None:
        resp = await self._client().post(
            "/api/v1/auth/inspect",
            headers={"Authorization": f"Bearer {raw_token}"},
        )
        if resp.status_code != 200:
            log.debug("identity introspection rejected: status={}", resp.status_code)
            return None
        data = resp.json()
        if not data.get("valid"):
            return None
        return UserIdentity(
            user_id=int(data["user_id"]), token_id=int(data.get("token_id", 0))
        )

    async def balance(self, raw_token: str) -> float:
        """可用余额（USD）。

        取 ``balance_usd``（billing 已按 quota/500000 折算）；缺失时回落
        ``balance`` 原始 quota 再自行折算——两个字段都没有则视为查询异常
        抛错，而不是当 0 处理（当 0 会让所有用户被压到 1 槽）。
        """
        resp = await self._client().get(
            "/api/v1/billing/balance",
            headers={"Authorization": f"Bearer {raw_token}"},
        )
        if resp.status_code != 200:
            raise BillingError(resp.status_code, _err_body(resp))
        payload = resp.json().get("data") or {}
        if "balance_usd" in payload:
            return float(payload["balance_usd"])
        if "balance" in payload:
            return float(payload["balance"]) / 500000.0
        raise BillingError(502, "balance field missing in billing response")

    async def find_charge(self, raw_token: str, *, task_id: str,
                          since: int, until: int) -> dict | None:
        """按 task_id 反查扣费记录（设计 §8 超时对账）。

        两级策略：
        1. 精确匹配 ``attr_filter=task_id={task_id}``——要求上游
           把 ``X-Task-Id`` 头写进 billing attrs（OPEN-DECISIONS 待确认）；
        2. 精确匹配无结果时**不**自动降级到模糊匹配——模糊匹配
           （token + 时间窗）会把用户同期的其他调用误判成本任务的扣费，
           补记出一个假的 SUCCESS。降级由调用方（reconcile）在明确知道
           上游不记该头时显式开启。

        返回 None = 窗口内确认无记录（可判 FAILURE）；
        抛 BillingError = 查询本身失败（必须保持挂起，绝不判死）。
        """
        params: dict[str, Any] = {
            "attr_filter": f"task_id={task_id}",
            "from": since,
            "to": until,
            "page": 1,
            "page_size": 20,
        }
        if settings.reconcile_biz_type:
            params["biz_type"] = settings.reconcile_biz_type
        resp = await self._client().get(
            "/api/v1/billing/logs",
            headers={"Authorization": f"Bearer {raw_token}"},
            params=params,
        )
        if resp.status_code != 200:
            raise BillingError(resp.status_code, _err_body(resp))
        payload = resp.json().get("data") or {}
        logs = payload.get("logs") or []
        for item in logs:
            if str(item.get("direction", "")).lower() in _SETTLED_DIRECTIONS:
                return dict(item)
        return None
