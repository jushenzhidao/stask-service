"""参考单价（ref_price）——只用于算并发槽位数，不参与任何资金计算。

ADR-003 决策：**配置为主 + 兜底默认值**，不查 new-api 定价接口。理由是
提交链路已经有 inspect + balance 两个外部 RTT，再加一个定价查询会让 P99
不可控；而 ref_price 的精度只影响闸门松紧（差 2 倍无非是 5 槽还是 10 槽），
不影响一分钱的正确性。

配置形态：``ST_REF_PRICE_{MODEL}``，模型名规整规则见 ``_env_suffix``。
例：
    ST_REF_PRICE_DALL_E_3=0.08
    ST_REF_PRICE_GPT_IMAGE_1=0.04
    ST_REF_PRICE_DEFAULT=0.04
"""

from __future__ import annotations

import os
import re
from functools import lru_cache

from app.config import settings

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def model_slug(model: str) -> str:
    """模型名 → task_id 前缀片段（小写、非字母数字替 ``_``、截断 16）。

    task_id 总长必须 ≤53（tasks.task_id 是 varchar(64)，留余量）：
    16 + 1 + 32 = 49。空模型名回落 ``task``。
    """
    slug = _NON_ALNUM.sub("_", (model or "").strip().lower()).strip("_")
    return (slug[:16].rstrip("_") or "task")


def _env_suffix(model: str) -> str:
    """模型名 → 环境变量后缀（大写，非字母数字替 ``_``）。"""
    return _NON_ALNUM.sub("_", (model or "").strip().lower()).strip("_").upper()


@lru_cache(maxsize=256)
def _lookup_env(suffix: str) -> float | None:
    """读 ``ST_REF_PRICE_{SUFFIX}``。

    这是 config.py「禁止散读 os.environ」纪律的**唯一例外**并且是有意的：
    键名是动态的（模型名不可枚举），pydantic-settings 表达不了这种形态。
    用 lru_cache 保证每个模型只读一次环境变量。
    """
    if not suffix:
        return None
    raw = os.environ.get(f"ST_REF_PRICE_{suffix}")
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def ref_price(model: str, default_price: float | None = None) -> float:
    """模型参考单价（USD/次）。

    优先级：``ST_REF_PRICE_{MODEL}`` 环境变量 > ``default_price``（调用方
    从 dynconf 取的兜底值）> ``settings.ref_price_default``。

    按模型的单价仍只从 env 读——模型名不可枚举，做成 Hash 里的动态键会让
    白名单校验失去意义（任意键都能写）。要热调某个模型的单价，改
    ``ref_price_default`` 或走 env + 重启。
    """
    hit = _lookup_env(_env_suffix(model))
    if hit is not None:
        return hit
    fallback = default_price if default_price is not None else settings.ref_price_default
    return max(fallback, 1e-9)


def slots_for(balance: float | None, model: str, *,
              max_slots: int | None = None,
              default_price: float | None = None) -> int:
    """并发槽位数（设计 §6）：``clamp(floor(balance / ref_price), 1, max_slots)``。

    语义 = 余额付得起几个在途任务。余额未知（billing 抖动）→ 回落 1，
    既不硬拒也不放开。下限恒为 1：余额不足的判定归 new-api relay
    （它会返回 402，任务落 FAILURE），本服务不做资金判定。

    ``max_slots`` / ``default_price`` 由调用方从 dynconf 注入（可运行时热改）；
    不传则回落 settings，保持本函数纯同步、可被单测直接调用。
    """
    if balance is None:
        return 1
    limit = max_slots if max_slots is not None else settings.max_slots
    raw = int(balance // ref_price(model, default_price))
    return max(1, min(raw, limit))


async def slots_for_live(balance: float | None, model: str) -> int:
    """``slots_for`` 的动态配置版本（提交链路用）。"""
    from app.services import dynconf

    config = await dynconf.get_runtime_config()
    return slots_for(
        balance, model,
        max_slots=config.max_slots,
        default_price=config.ref_price_default,
    )
