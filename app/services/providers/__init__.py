"""外部服务适配层：Protocol 端口 + 实现分离，工厂按配置装配。

本服务只依赖一个外部控制面服务：**billing**（身份内省 / 余额 / 消费日志）。
红线：这三件事一律走 HTTP，禁止跨服务直连 new-api 的库。

为什么要 Protocol 而不是直接 import 实现：测试里换 FakeBilling 只需实现
三个方法，不必起 respx 拦全部端点；将来 billing 换实现（比如直接问
new-api 的 ``/api/user/self``）也只加一个 provider 文件。
"""

from __future__ import annotations

from typing import Protocol

from app.config import settings
from app.schemas import UserIdentity


class ProviderError(Exception):
    """外部服务调用失败的基类。"""


class BillingError(ProviderError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message

    @property
    def retryable(self) -> bool:
        """5xx 与 409（锁竞争）是瞬时态，可退避重试；其余 4xx 确定性失败。"""
        return self.status >= 500 or self.status == 409


class BillingProvider(Protocol):
    async def inspect(self, raw_token: str) -> UserIdentity | None:
        """令牌 → 身份。无效令牌返回 None（不抛异常——401 是正常业务分支）。"""
        ...

    async def balance(self, raw_token: str) -> float:
        """当前可用余额（USD）。查询失败抛 BillingError。"""
        ...

    async def find_charge(self, raw_token: str, *, task_id: str,
                          since: int, until: int) -> dict | None:
        """按 task_id 反查消费日志中的扣费记录（超时对账用）。

        返回 None = 窗口内确认无记录；抛 BillingError = 查询本身失败
        （两者语义完全不同：前者判 FAILURE，后者必须保持挂起）。
        """
        ...


def _build_billing() -> BillingProvider:
    from app.services.providers.billing_newapi import NewapiBillingProvider

    if settings.billing_svc_url:
        return NewapiBillingProvider()
    raise ProviderError("ST_BILLING_SVC_URL is required")


#: 模块级单例（测试用 monkeypatch 替换）
billing: BillingProvider = _build_billing()
