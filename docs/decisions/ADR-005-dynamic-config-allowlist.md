# ADR-005: 运行时配置只开放白名单子集

## Status

Accepted (2026-08-23)

## Background

原设计里所有配置都只在启动时从 env 读一次（`app/config.py` 单例 +
「禁止散读 os.environ」纪律）。改任何配置都要重启，这对运营性质的旋钮
不合理——某个模型涨价要调 `ref_price`、某用户压测要临时放宽 `max_slots`、
确认 relay 回滚语义后要开 `retry_max`，都不该重启网关。

但「配置可在页面改」会直接冲撞现有纪律，而且有些配置项**开放即漏洞**。

## Decision

分三层，只有第三层可运行时改：

| 层 | 内容 | 可热改 |
|---|---|---|
| 启动项 | 连接串、键前缀、platform 标识 | 否 —— 改了等于换一个服务，在途任务全部失联 |
| 安全项 | `upstream_allowlist`、`callback_allowlist`、`callback_secret`、`billing_svc_url`、`async_allow/deny_prefixes` | 否 |
| 运营项 | 单价、槽上限、限流、重试、超时、TTL、开关 | 是 |

实现在 `app/services/dynconf.py`，读取优先级 **Redis Hash 覆盖 > env > 代码默认值**。

三个关键设计：

1. **白名单而非黑名单**（`MUTABLE` 字典）。新增配置项默认不可热改，要开必须
   显式登记。用黑名单的话，将来新增一个敏感项忘了加进黑名单就直接暴露了。
2. **Redis 不可用回落 env**。动态配置是增强，绝不能成为可用性单点。
3. **批量写整批校验**。区间校验失败整批拒绝，不做部分成功——半套配置比旧配置
   更危险（比如 `worker_timeout` 生效了但配套的 `dispatch_lock_margin` 没生效）。

## Consequences

**正面**：运营旋钮改完即生效；看板上能看到哪些项被覆盖了（不再跟随 `.env`），
以及只读项各自的原因，运维不必翻代码。

**负面**：
- 配置真值来源从「一个 `.env`」变成「`.env` + Redis 覆盖」两处。缓解：看板明确
  标注哪些被覆盖，且提供「全部回落 `.env`」一键操作。
- 热路径多一次 Redis 读。缓解：进程内缓存 TTL 5s，摊薄到几乎为零。
- 多副本时配置生效有最长 5s 的偏差窗口。**有意接受**——为强一致做 pub/sub
  失效广播的复杂度远超收益，这些都是运营旋钮而非资金判定。

**为什么 `upstream_allowlist` 绝对不能开**：worker 携带用户真实 sk 调用上游，
allowlist 是「防 sk 被打到野地址」的最后一道应用层防线（nginx 头覆盖是第一道，
但直连 8000 端口的流量绕过 nginx）。让它可写等于给了一条通过管理面把所有用户
凭证导向任意地址的路径。

## Related

`app/services/dynconf.py`；`app/routers/admin.py`；SPEC AC-34；README「运行时配置」。
