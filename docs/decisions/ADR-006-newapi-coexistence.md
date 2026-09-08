# ADR-006: 与 new-api 共存契约（外部任务不影响内部任务）

## Status: Accepted (2026-09-07)

## Background

本服务复用最新版 new-api 的 `tasks` 表（零建表，ADR-001）。new-api 自身有
任务轮询（`UpdateVideoTasks` 按 platform 找 adaptor）与超时清理
（`sweepTimedOutTasks`：`progress != '100%'` 且非终态且 `submit_time` 超 24h
→ 标 FAILURE 并按 `quota` 退款）。必须保证双方零冲突。

## Decision

外部任务行满足以下契约：

| 列 | 取值 | 效果 |
|---|---|---|
| `platform` | 自定义值 `stask`（非 suno/mj） | `GetTaskAdaptorFunc` 返回 nil，`updateVideoTasks` 报 "video adaptor not found" 只记日志，任务不动 |
| `channel_id` | **必须是 new-api 中真实存在的渠道 id**（`CHANNEL_ID`，可用禁用的占位渠道） | 见下方「轮询顺序」——渠道不存在会导致批量误判 FAILURE |
| `task_id` | `{model_slug}_{fingerprint32}`（本服务生成，恒非空） | `GetUpstreamTaskID()` 回退到 task_id 列，非空 → 不进 null 判死分支 |
| `quota` | 恒 0 | 即便被上游超时清理误标 FAILURE，退款金额也是 0，零资金影响 |
| `user_id` | 恒 0 | 本服务不做 key 管理 |
| `status` / `progress` | new-api 原生枚举：`NOT_START`/`IN_PROGRESS`/`SUCCESS`/`FAILURE` + `0%`/`100%` | 共享表对上游工具（看板、SQL 巡检）保持可读 |

### 已核实的轮询顺序（service/task_polling.go @ main，2026-09-07）

```
RunTaskPollingOnce:
  GetAllUnFinishSyncTasks           # WHERE progress != '100%' AND status 非终态，无 platform 过滤
  → GetUpstreamTaskID() == "" ?     # 空 → TaskBulkUpdateByID 强制 FAILURE
  →                                 #（我们回退到 task_id 列，恒非空，命不中）
  → DispatchPlatformUpdate → updateVideoTasks（按 channel_id 分组并发）:
      1. CacheGetChannel(channelId) 失败
         → 该渠道全部任务 TaskBulkUpdateByID 强制 FAILURE（无 CAS 无退款）  ← 最危险
      2. GetTaskAdaptorFunc(platform) == nil
         → return "video adaptor not found"，只记日志，任务不动           ← 安全出口
```

**关键结论**：`CacheGetChannel` 在 adaptor nil 检查**之前**——
platform 自定义值只能保证走到第 2 步安全退出，但走不到第 2 步的前提是
第 1 步不炸。所以 `channel_id` 必须真实存在（`channelsIDM` 含禁用渠道，
禁用状态的占位渠道即可）。`CHANNEL_ID<=0` 时应用启动打告警。

另外两条防线（与轮询无关）：

时间防线：本服务的超龄判死 sweeper（`TASK_MAX_LIFETIME_SECONDS`，默认
6h）**先于** new-api 的 24h 清理线收敛自己的行——正常情况下上游的清理
永远碰不到我们的活跃行；即便碰到，quota=0 使其零资金影响（双保险）。

写读侧 WHERE 恒带 `platform = 'stask'`，绝不动别人的行。

## Consequences

- 正面：外部任务与 new-api 内部任务在同一张表上零冲突共存；上游零改动。
- 正面：不再需要 `STASK_RUNNING` 这类私有 progress 值。
- 负面：轮询日志里会出现 "adaptor not found" 噪音（new-api 侧，无害）。
- 负面：tasks 表仍与上游同实例——彻底解耦需独立存储 + 迁移（见 ADR-001）。

## Related ADRs
- ADR-001（复用 tasks 表）
- 取代旧行为：`SUBMITTED` 初始态与 `STASK_RUNNING` progress 伪装。
