# ADR-003: ref_price 走配置 + 兜底默认值，不查定价接口

## Status

Accepted (2026-08-21) —— 对应设计文档开放问题③

## Background

并发额度公式 `slots = clamp(floor(balance / ref_price), 1, ST_MAX_SLOTS)`
需要一个「参考单价」。来源有两个选择：环境变量配置，或运行时查 new-api
的定价接口。

## Decision

**配置为主**：`ST_REF_PRICE_{MODEL}`（模型名非字母数字替 `_` 后大写），
未命中回落 `ST_REF_PRICE_DEFAULT`。

关键认知：**ref_price 不参与任何资金计算**。它只决定一个用户能同时跑几个
任务，是个并发闸门。真正的扣费在 new-api relay 内部按它自己的定价执行，
和这个值毫无关系。

既然如此，配置的精度差 2 倍无非是 5 槽还是 10 槽的区别；而查定价接口要在
提交链路上再加一个外部 RTT——提交已经有 inspect + balance 两个了，P99 不
可控。用户等的是「毫秒返回 task_id」，不是「准确的槽位数」。

## Consequences

**正面**：提交链路零额外依赖；新模型未配置时自动走默认值，不会因为定价
接口不认识这个模型就报错。

**负面**：模型定价变化后需要手工更新 env。可接受——这个值本来就是个数量级
估算，不需要跟着实际定价走。

**实现注意**：`ST_REF_PRICE_{MODEL}` 的键名是动态的（模型名不可枚举），
pydantic-settings 表达不了。`app/services/pricing.py::_lookup_env` 是
「禁止散读 os.environ」纪律的唯一例外，用 `lru_cache` 保证每个模型只读一次。

## Related

SPEC §6 / AC-07；`app/services/pricing.py`；OPEN-DECISIONS ③。
