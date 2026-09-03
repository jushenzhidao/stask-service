# ADR-002: 上游 5xx 默认不重试（`ST_RETRY_MAX=0`）

## Status

Accepted (2026-08-21) —— 对应设计文档开放问题①

## Background

设计文档 §8 写「5xx / 网络错 → 退避重试 ≤3 次 → FAILURE」，但同时在开放问题①里承认：**new-api relay 返回 5xx 时，它的预扣配额是否确定回滚，尚未确认**。

这两件事是矛盾的。如果 relay 在 5xx 路径上没有回滚预扣，那么 worker 每重试一次就可能多扣一次费。而 stask 的核心卖点是"计费零代码——资金操作全部在 new-api 原生 relay 内闭环"，本服务没有任何退款通道（开放问题②同样未决）。

在退款通道不存在的前提下，重试的期望收益（少量瞬时故障自愈）远小于期望损失（用户被重复扣费且无法自动纠正）。

## Decision

**代码保留完整的退避重试能力，但默认关闭**：

- `ST_RETRY_MAX = 0` —— HTTP 5xx 直接落 `FAILURE`，不重试。
- `ST_RETRY_MAX_CONNECT = 2` —— **连接层错误单独放行**：`httpx.ConnectError` / `ConnectTimeout` / DNS 失败等，表示请求根本没到达上游，relay 未执行、零扣费风险，重试安全。
- 读超时（`ReadTimeout`）不算连接层错误 —— 请求已进 relay，可能已扣费，走 §8 超时对账路径（绝不判死）。

确认 new-api 5xx 回滚语义后，把 `ST_RETRY_MAX` 改成 3 即可开启，无需改代码。

## Consequences

**正面**：零双扣风险；用户看到的失败是真失败，语义干净。

**负面**：上游偶发 5xx（如渠道瞬时抖动）会直接暴露给用户，需要客户端自己重试。这是可接受的——客户端重试带 `Idempotency-Key` 时本服务不会重建任务，语义仍然安全。

**待办**：向 new-api 侧确认 relay 5xx 的配额回滚行为，确认后改配置并在此 ADR 追加 Resolution。

## Related

OPEN-DECISIONS 条目 ①②；SPEC AC-16。
