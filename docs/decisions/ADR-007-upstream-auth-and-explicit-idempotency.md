# ADR-007: 上游鉴权前置、user_id 落表、原生状态机、显式幂等（v0.3）

日期：2026-09-08 ｜ 状态：已采纳（v0.3.1 修订，见文末）

## 背景

v0.2 的取舍是「零鉴权、零计费、自动幂等」：Authorization 盲透传，无效 key
要等到 worker 执行才发现（任务 FAILURE）；tasks.user_id 恒 0；同请求体
字节级指纹自动去重。实际运营暴露三个问题：

1. 无效 key / 余额耗尽的提交照样建任务、占槽、入队——资源被垃圾请求消耗，
   客户端还要异步轮询才知道失败；
2. user_id=0 使共享表里本服务的行无法按用户归属检索；
3. 自动幂等对「故意重跑同一 prompt」不友好（必须记得带盐），且字节级
   指纹对 JSON 键序、空白敏感，语义上并不可靠。

## 决策

### 1. 提交前用客户端 key 调上游鉴权 + 余额预检（`services/upstream.py`）

- `GET /dashboard/billing/subscription` + `/usage`（OpenAI 兼容，挂
  new-api TokenAuth）并发调用：无效 key → 401；余额 ≤ 0 → 402；
  上游控制面不可用 → 502。三者都**不建任务**。
- 结果按 token_hash 进程内缓存 60s，正向结果才缓存。
- **计费仍零代码**：预扣/退款/流水由上游 relay 在任务执行时自理；
  余额预检只是准入闸门，不是资金判定。
- 鉴权失败的任务根本不存在，自然「不重试」。

### 2. user_id 落表

- 账单端点不回显 user_id（实测 new-api 源码确认），因此从共享库
  `tokens` 表按 key 直查（`key` 列 = sk 去 `sk-` 前缀取 `-` 首段，
  对齐 TokenAuth 解析）。best-effort：查不到落 0，不阻塞提交。
- 这是继 tasks 表之后第二处共库依赖（只读一列）。

### 3. 状态机全用 new-api 原生枚举

    SUBMITTED → QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED

- `SUBMITTED`：行已落库、入队未确认（毫秒级瞬态；停留 = 入队失败，
  sweep_stale 兜底）；`QUEUED`：入队确认，对外 202 的初始可见态。
- 外部系统语义映射：已提交=SUBMITTED，排队中=QUEUED，处理中=IN_PROGRESS。
- new-api `ToVideoStatus` 把 SUBMITTED/QUEUED 归 queued，上游看板可读。
- 取消仅允许 SUBMITTED/QUEUED（`PENDING` 元组）。

### 4. 幂等改为显式（Stripe/OpenAI 语义）

- 默认**不去重**：task_id = `{slug}_{uuid4hex}`，每次提交都是新任务；
- 带 `Idempotency-Key` 头才幂等：task_id =
  `{slug}_{sha256(token_hash|key)[:32]}`，同 token 同 key 恒回放；
- 请求指纹、`idem_ttl`/`idem_pending_ttl`/`idem_replay_wait` 配置全部
  删除（占位 TTL/等待窗口改为模块内常量）。

### 5. 可观测性精简

- taskiq 链路 trace 改用官方 `TaskiqInstrumentor`（logfire 无
  instrument_taskiq；已存在的 broker 必须显式 `instrument_broker`）；
  queue.py 不再手动挂 OpenTelemetryMiddleware；
- `instrument_fastapi(excluded_urls="/healthz")` 过滤探针噪音
  （子串匹配，业务路径均在 /async、/admin、/ops 下，不误伤）；
- broker 加 `unacknowledged_lock_timeout=60`：修复 worker 在
  XAUTOCLAIM 扫描中途崩溃时 pending 消息永久卡死；
- 删除 `logfire_service_name` 配置（固定 stask-web / stask-worker）。

## 后果

- 提交链路多两跳上游 HTTP（60s 缓存摊薄）；上游控制面故障时提交不可用
  （502）——有意取舍：无法鉴权就不放行。
- tokens 表只读依赖使「上游只是 HTTP 服务」的抽象破了一角（user_id
  拿不到时优雅降级为 0，不影响功能）。
- 旧行的 NOT_START 状态不再被识别为活跃态——升级前需清空在途任务或
  手工 UPDATE 为 QUEUED（本次明确不做兼容）。

## v0.3.1 修订（同日，提交效率优化）

### 鉴权改共享库单 SQL 直查（替换两跳 HTTP）

- tasks 表本就与 new-api 共库，`tokens ⋈ users` 一条 JOIN 拿全
  「key 有效性、token 状态/过期/额度、用户状态/额度、user_id」——
  提交链路鉴权成本从两跳 HTTP（几十 ms）降到一次 uniqueIndex 查询；
- 判定语义逐条对齐 new-api `model.ValidateUserToken`（源码核实）：
  token status 2/3→401、4→402；expired_time 过期→401；user status≠1→401；
  非 unlimited 且 remain_quota≤0→402；user quota≤0→402；软删除行不可见；
- 缓存改双层且**可持久**：Redis `st:auth:{token_hash}`
  （`AUTH_CACHE_TTL_SECONDS`，默认 300s，跨进程共享、重启不丢；值只存
  user_id 绝不存 key）+ 进程内 5s 短缓存挡 Redis RTT。负向不缓存；
- 上游对额度的最终判定权不变：缓存窗口内漏放的提交由 relay 在执行时
  拒绝（任务 FAILURE），零资金风险。

### 状态机再精简：QUEUED 起步（SUBMITTED 删除）

- 落库即 QUEUED，删掉「INSERT SUBMITTED + 入队后 CAS QUEUED」的第二次
  DB UPDATE——提交链路少一次写；
- 「入队未确认」不需要独立状态：入队调用失败→提交链路当场 CAS FAILURE；
  进程崩溃→行停在 QUEUED 且 dispatch_epoch=0，sweep_stale 按
  「锁不在 + 从未派发」重投。语义与 SUBMITTED 版完全等价，少一个状态。

### 提交链路最终形态（DB 写恰好 1 次，Redis 3-4 次）

    限流(Redis) → 鉴权(本地缓存/Redis/单SQL) → [幂等占位(Redis)]
    → 占槽(Redis) → INSERT QUEUED → 令牌会话(Redis) → kiq(Redis) → 202
