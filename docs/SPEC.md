# Spec — stask-service v1.0（规格即契约）

> 生成日期：2026-08-21
> 基于：`docs/stask-service-design.md` v1.1（定稿评审）
> 状态：已确认（开放问题①③⑥已裁决，见 `docs/decisions/`）

本文件是**团队内部契约**：范围、API、数据、Token、验收标准全部锁定。
不在本 Spec 列表内的功能一律不做；开发中的任何改动走 §13 变更流程。

---

## 1. 产品定义

- **一句话描述**：把同步 HTTP 生成接口变成长任务——加 `/async` 前缀提交，毫秒返回本地 task_id，结果异步取回。
- **目标用户**：接入同步生成 API 的应用开发者（客户端不愿为一次 60~180s 的出图请求挂住连接）。
- **核心问题**：同步生成接口耗时长，HTTP 连接易被中间层掐断；而在上游里引入任务模型成本高、风险大。本服务在**不改上游一行代码**的前提下把同步接口变成任务接口，资金操作全部留在上游内部闭环。
- **定位边界**：本服务是一个**独立异步队列服务**，职责只有「同步 → 异步的队列化」。上游（new-api 是默认实现）对本项目而言**只是一个 HTTP 服务**——渠道选择、配额扣费、限流、消费日志全在上游，本服务零资金动作。
- **已知耦合**：任务行落在与上游同实例的 `tasks` 表上（ADR-001），这是当前唯一一处非 HTTP 依赖；反转它需要独立存储 + 迁移方案，不在本轮范围。

---

## 2. MVP 范围（锁定）

| 优先级 | 功能 | 验收标准摘要 | RICE |
|---|---|---|---|
| P0 | 提交 `POST/PUT /async/{path}` 通配转任务 | 202 + task_id + Location 头，毫秒返回 | 高 |
| P0 | 幂等占位（`Idempotency-Key`） | 同键重放同 task_id；真并发 409 | 高 |
| P0 | 路径准入（allow/deny 前缀） | deny 优先；未命中 allow → 403 | 高 |
| P0 | upstream 寻址与三防线校验 | 头覆盖 + allowlist + scheme/userinfo 校验 | 高 |
| P0 | 余额并发额度占槽 | slots 公式；超限 429 + Retry-After | 高 |
| P0 | worker 派发锁 + 上游调用 + 四类分流 | 锁在不重发；2xx/4xx/5xx/超时各自归位 | 高 |
| P0 | 查询 `GET /async/{path}/{task_id}` 与字节级回放 | 202/200/错误码重放三态 | 高 |
| P0 | 长轮询 `?wait=N` | 终态即返，超时返 202 | 中 |
| P0 | 取消 `DELETE /async/{path}/{task_id}` | 排队中 CANCELED；执行中 409 | 中 |
| P1 | 超时对账（reconcile_pending 三态收敛） | 有扣费补 SUCCESS / 无记录 FAILURE / 查不到挂起 | 高 |
| P1 | 回调 `X-Callback-Url`（HMAC 签名 + 重试） | 终态推送，签名可验 | 中 |
| P1 | 结果 TTL 清理 | 到期清空 `upstream_response`，状态行保留 | 中 |
| P1 | 健康检查与 ops 观测端点 | live/ready + 状态分布 | 中 |

---

## 3. 明确不做（Out-of-Scope — 锁定）

| 不做 | 原因 | 何时考虑 |
|---|---|---|
| 自建计费/冻结/结算 | 资金全在上游内部闭环，本服务零资金动作 | 永不 |
| 建任何 MySQL 表 | 复用 new-api `tasks` 表，靠 `platform` 列划分自有行 | 永不 |
| 跨服务直连 new-api/billing 的数据库 | 红线：余额/日志一律 HTTP | 永不 |
| 执行中任务的真中止 | 上游是同步调用，无法中断；执行中取消返 409 | 上游支持 abort 后 |
| 流式（SSE/chunked）响应任务化 | 任务模型与流式语义冲突，本期只做一问一答型 | v2.0 |
| 上游 task_id 反查（atask 的 `gw:tidx`） | 本服务的上游是同步接口，不产生上游任务 id | 永不 |
| HELD 挂起态与金丝雀排空 | 无冻结即无挂起收口需求（设计 §1 明示） | 永不 |
| 结果外置对象存储 | 10MB 上限 + gzip 已覆盖出图/TTS；开放问题⑤留 OPEN | 有超限告警后 |
| 多租户隔离/管理台 UI | MVP 无此需求 | v2.0 |

---

## 4. 技术架构（锁定，版本锚定）

| 层 | 技术 | 版本 | 锁定原因 |
|---|---|---|---|
| Web 框架 | FastAPI | 0.115.14 | 与 atask-service 对齐，团队零学习成本 |
| ASGI | uvicorn / gunicorn | 0.34.0 / 23.0.0 | UvicornWorker + preload，post-fork 惰性单例 |
| 数据校验 | pydantic / pydantic-settings | 2.11.7 / 2.9.1 | 无前缀配置单例 |
| ORM/驱动 | SQLAlchemy[asyncio] / asyncmy | 2.0.41 / 0.2.10 | 只用 `text()` 原生 SQL，ORM 仅作映射说明 |
| Redis | redis (asyncio) | 5.2.1 | `decode_responses=True`，Lua 原子操作 |
| HTTP | httpx[http2] | 0.28.1 | 共享 AsyncClient 连接池 |
| 队列 | taskiq / taskiq-redis | 0.11.18 / 1.0.2 | ListQueueBroker + RedisScheduleSource |
| 日志 | loguru | 0.7.3 | stdlib 桥接，`backtrace/diagnose=False` |
| 测试 | pytest / pytest-asyncio / respx | 8.3.5 / 0.26.0 / 0.22.0 | `asyncio_mode=auto`，手写 FakeRedis |
| Lint/Type | ruff / mypy | 0.11.13 / 1.15.0 | mypy 做成一个 pytest 用例 |
| 部署 | Docker Compose（web + worker + 独立 Redis） | - | Redis 独立实例（ADR-004） |
| 认证 | 透传终端用户 `Bearer sk-...`，身份由 billing `/auth/inspect` 判定 | - | 本服务不签发任何凭证 |

**依赖钉版唯一处 = `pyproject.toml`**，不写 requirements.txt。

---

## 5. API 端点清单（锁定）

| Method | Path | 功能 | 认证 | 请求 | 响应 |
|---|---|---|---|---|---|
| POST/PUT | `/async/{path:path}` | 提交任务 | Bearer sk | 原文 path/query/body；头 `Idempotency-Key`、`X-Callback-Url`、`X-Delay-Seconds`、`X-Execute-After`、`X-Batch-Size`、`X-Batch-Wait`、`X-Batch-Key` | `202 {task_id,status,created_at,scheduled_at,batch_key,batch_state,replayed}` + `Location` |
| GET | `/async/{path:path}` （末段为 task_id） | 查询/回放 | 无（task_id 即凭证） | `?wait=0..60` | `202` 进行中 / `200` 原生回放 / 重放上游错误码 |
| DELETE | `/async/{path:path}` （末段为 task_id） | 取消 | 无 | - | `200 {task_id,status:CANCELED}` / `409` |
| GET | `/healthz/live` | 存活探针 | 无 | - | `200 {"status":"ok"}` |
| GET | `/healthz/ready` | 就绪探针 | 无 | - | `200`/`503` + checks |
| GET | `/ops/stats` | 状态分布 + 队列观测 | Bearer sk（管理令牌） | - | `200` |
| GET | `/ops/tasks/{task_id}` | 单任务诊断视图（不含 sk、不含结果体） | Bearer sk | - | `200`/`404` |
| POST | `/ops/reconcile/run` | 手工触发一轮对账 | Bearer sk | - | `200 {scanned,stats}` |

管理面（`X-Admin-Key`；`ADMIN_KEY` 未配置时**全部 404**）：

| Method | Path | 功能 | 请求 | 响应 |
|---|---|---|---|---|
| GET | `/admin` | 看板页面（单文件 HTML，零构建） | - | `200` / `404` |
| GET | `/admin/api/overview` | 概览指标 | `?window=` | `200` |
| GET | `/admin/api/tasks` | 任务列表（分页 + 筛选） | `status/model/task_id/reconcile_only/since/limit/offset` | `200 {total,items}` |
| GET | `/admin/api/tasks/{task_id}` | 任务详情（脱敏） | - | `200` / `404` |
| POST | `/admin/api/tasks/{task_id}/requeue` | 重投队列（**不清派发锁**） | - | `200` / `409` |
| GET | `/admin/api/config` | 读运行时配置 + 只读项原因 | - | `200` |
| PUT | `/admin/api/config` | 批量写覆盖值（白名单外拒绝，整批校验） | JSON 对象 | `200` / `400` |
| POST | `/admin/api/config/reset` | 重置回落 env（POST 而非 DELETE：DELETE 带 body 在客户端/代理上行为不一致） | `keys` 数组或 `null` | `200` |
| POST | `/admin/api/jobs/{job}` | 触发 `reconcile`/`stale`/`slots`/`purge` | - | `200` / `404` |

错误响应统一 `{"error": {"message","type","param","code"}}`。

---

## 6. 数据模型（锁定 — 复用 new-api `tasks` 表，零建表）

`task_id` 形态：`{model_slug}_{uuid4hex}`（`model_slug` = 模型名小写、非 `[a-z0-9]` 替 `_`、截断 16 字符；总长 ≤ 53）。
`platform` = `stask`（`GATEWAY_PLATFORM`），**所有写操作 WHERE 必带**。
`channel_id`：执行前 0，执行后从响应头尽力回填（开放问题④保守取头）。

`data` JSON 列字段契约：

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | str | 恒 `"stask"` |
| `model` | str | 浅解析 body 提取；缺失为 `""` |
| `token_hash` | str | sha256(raw_token)[:32]，限流/占槽/校准的口径 |
| `idempotency_key` | str | 可空 |
| `callback_url` | str | 可空 |
| `request_method` / `request_path` / `request_query` | str | 剥前缀后的原文 |
| `request_headers` | obj | 已剔除 Authorization / Cookie / Host / Content-Length 等 |
| `request_body` | str(b64) 或 obj | 超 `BODY_MAX_BYTES` 则只存摘要 + `body_truncated:true` |
| `upstream_base_url` | str | 提交时校验通过的值，worker 只认它 |
| `freeze_amount` | int | 恒 `0` — 让 atask sweeper 天然跳过 |
| `settled` | bool | 恒 `true` — 同上 |
| `inflight_slot` | bool | 占槽标记，终态清为 `false` |
| `upstream_response` | str | gzip+b64 的响应原文，TTL 到期清空 |
| `upstream_content_type` | str | 回放时原样回设 |
| `upstream_status` | int | 上游 HTTP 状态码（错误码重放依据） |
| `dispatch_epoch` | int | 派发轮次，防重投观测 |
| `reconcile_pending` | bool | 超时标记，对账扫描依据 |
| `reconcile_checked_at` | int | 上次对账时间（秒） |
| `scheduled_at` | int | 计划执行时刻（unix 秒）；`0` = 无延迟。延迟任务保持 `QUEUED`，等待期不占槽；同时是 sweeper 豁免与生命期计时口径的依据 |
| `batch_state` | str | `""` / `immediate` / `scheduled`（延迟未到点）/ `waiting`（批次成员或等槽）/ `releasing` / `released` |
| `batch_size` / `batch_wait` | int | 生效的 N/T（客户端头覆盖后的值，非策略原值） |
| `batch_key` | str | 批次归组键，`""` = 未使用 |
| `slot_flags` | int | 三层槽占位掩码（1=token / 2=(模型,token) / 4=模型全局）。**释放的唯一依据**，绝不按当前配置重算 |
| `slot_model` | str | 占槽用的归一化模型名。占与释放必须同一字符串 |

> 注：本表仍保留 `freeze_amount` / `settled` / `inflight_slot` / `reconcile_*` 等
> **已不存在的字段**（v0.3 去计费化后移除，占槽标记改为 `slot_flags` 掩码）。
> 本表待一次完整对齐；新增字段请以上方几行为准。

延迟/定时下发（`X-Delay-Seconds` / `X-Execute-After`）的完整需求与验收线见
`docs/PRD-scheduling-and-concurrency.md`（R-01~R-13 / AC-37~AC-45），架构裁决见
`docs/ARCH-scheduling-and-concurrency.md`。要点：等待期不占并发槽（R-04）、
兜底扫描豁免未到点任务（R-05）、生命期起点取 `max(created_at, scheduled_at)`（R-06）、
延迟上限由令牌 TTL 反推（超限在提交时即 `400 delay_too_long`）。

状态机：`QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`。无 HELD、无孤儿判死；计划任务与批次成员**不引入新状态**，一律保持 `QUEUED`（等待期天然可取消）。

---

## 7. 页面清单

无 UI。本服务是纯 API 网关，交付物为 OpenAPI（FastAPI 自动生成 `/docs`）+ `deploy/nginx.conf` 样例。

---

## 8. 设计 Token

不适用（无前端产物）。CLI/日志输出遵循 atask 同款 loguru 格式，不使用任何 emoji 字符。

---

## 9. 验收标准（EARS 格式，锁定 — QA 唯一依据）

| 编号 | 功能 | EARS 验收标准 | 优先级 |
|---|---|---|---|
| AC-01 | 提交 | When 客户端 POST 合法 `/async/v1/images/generations`，系统**必须**在落库后返回 `202` + `task_id` + `Location` 头 | P0 |
| AC-02 | 路径准入 | If 请求路径命中 `ASYNC_DENY_PREFIXES`，系统**必须**返回 `403`，且 deny 判定优先于 allow | P0 |
| AC-03 | 路径准入 | If 请求路径未命中 `ASYNC_ALLOW_PREFIXES`，系统**必须**返回 `403` | P0 |
| AC-04 | 方法准入 | If 请求方法不是 POST/PUT，系统**必须**返回 `405` | P0 |
| AC-05 | 幂等 | While 同一 `Idempotency-Key` 已回填 task_id，系统**必须**返回同一 task_id 而不重建任务 | P0 |
| AC-06 | 幂等 | If 同一 `Idempotency-Key` 真并发且占位未回填，系统**必须**返回 `409`，绝不重建 | P0 |
| AC-07 | 额度 | Where 余额为 `B`、参考单价为 `P`，系统**必须**按 `clamp(floor(B/P), 1, MAX_SLOTS)` 计算槽位数 | P0 |
| AC-08 | 额度 | If 在途任务数已达槽位上限，系统**必须**返回 `429` 并携带 `Retry-After` 头 | P0 |
| AC-09 | 回滚 | If 落库失败，系统**必须**归还幂等占位（CAS）并归还并发槽 | P0 |
| AC-10 | upstream | `UPSTREAM_ALLOWLIST` 非空时，若 `X-Upstream-Base-Url` 的 host 不在其中，系统**必须**返回 `400`；留空则**不限制**（与 `CALLBACK_ALLOWLIST` 同语义），但 scheme / userinfo / query 校验（AC-11）始终生效 | P0 |
| AC-11 | upstream | If upstream URL 含 userinfo 或 scheme 非 http(s)，系统**必须**返回 `400` | P0 |
| AC-12 | 执行 | When worker 出队，系统**必须**先 CAS `SUBMITTED→IN_PROGRESS`，失败即放弃（不重复执行） | P0 |
| AC-13 | 派发锁 | If 派发锁已被占用，系统**必须不**再次调用上游，并将任务标记为 `reconcile_pending` | P0 |
| AC-14 | 分流 | When 上游返回 2xx，系统**必须**落 `SUCCESS` 并存储 gzip 响应原文与 Content-Type | P0 |
| AC-15 | 分流 | When 上游返回 4xx，系统**必须**落 `FAILURE` 并保存原文与状态码用于重放 | P0 |
| AC-16 | 分流 | When 上游返回 5xx 且 `RETRY_MAX=0`，系统**必须**直接落 `FAILURE`，不重试 | P0 |
| AC-17 | 分流 | When 调用超时，系统**必须不**判死，而是保持非终态并标记 `reconcile_pending` | P0 |
| AC-18 | 释放 | When 任务进入终态，系统**必须**释放并发槽、清除令牌会话、置 `inflight_slot=false` | P0 |
| AC-19 | 查询 | While 任务为 SUBMITTED/IN_PROGRESS，系统**必须**返回 `202` + `{task_id,status,created_at}` | P0 |
| AC-20 | 回放 | While 任务为 SUCCESS，系统**必须**返回 `200` + 字节级一致的上游响应体与原 Content-Type | P0 |
| AC-21 | 回放 | While 任务为 FAILURE 且有上游状态码，系统**必须**重放该状态码与原文 | P0 |
| AC-22 | 回放 | If 结果已被 TTL 清理，系统**必须**返回 `410` + `{"error":{...}}` | P0 |
| AC-22b | 回放 | While 任务为 CANCELED，系统**必须**返回 `200` + 状态视图（它从未调用上游，无原文可放，也不是失败） | P0 |
| AC-23 | 长轮询 | While `?wait=N`（0<N≤60）且任务在窗口内转终态，系统**必须**立即返回终态响应 | P1 |
| AC-24 | 取消 | While 任务为 SUBMITTED，系统**必须**迁移为 CANCELED 并释放槽，零资金动作 | P0 |
| AC-25 | 取消 | While 任务为 IN_PROGRESS，系统**必须**返回 `409` | P0 |
| AC-26 | 对账 | While 任务 `reconcile_pending` 且消费日志按 `X-Task-Id` 查到成功扣费，系统**必须**补记 `SUCCESS` 并告警 result 缺失 | P1 |
| AC-27 | 对账 | While 任务 `reconcile_pending` 且窗口内确认无任何扣费记录，系统**必须**落 `FAILURE` | P1 |
| AC-28 | 对账 | If 对账查询本身失败，系统**必须**保持挂起，超 `RECONCILE_TTL` 转人工告警 | P1 |
| AC-29 | 回调 | When 任务转终态且提供 `X-Callback-Url`，系统**必须**推送含 `X-Stask-Signature` 的 HMAC-SHA256 签名 | P1 |
| AC-30 | 安全 | 系统**必须不**将用户 sk 写入 tasks 表、日志或任何 HTTP 响应 | P0 |
| AC-31 | 时间 | 读 tasks 表时间列**必须**经 `as_unix_seconds` 归一；写侧恒写 unix 秒，SQL 时间谓词用裸列比较以保留索引 | P0 |
| AC-32 | 清理 | When 结果超过 `result_ttl_seconds`，系统**必须**清空 `upstream_response` 但保留状态行 | P1 |
| AC-33 | 卡死兜底 | If 任务落库后入队消息丢失（长时间无进展且未被标记待对账），系统**必须**将其标记 `reconcile_pending` 交由对账收敛，且**不得**直接判死 | P0 |
| AC-34 | 动态配置 | If 请求修改白名单外的配置项（连接串 / 密钥 / upstream 白名单 / 路径准入等），系统**必须**拒绝并返回 400；区间校验失败**必须**整批回退 | P0 |
| AC-35 | 管理鉴权 | If `ADMIN_KEY` 未配置，所有管理端点**必须**返回 404；已配置时缺失或错误密钥**必须**返回 401，且终端用户 sk **不得**通过 | P0 |
| AC-36 | 管理脱敏 | 管理端点**必须不**返回用户 sk、请求体原文或响应体原文 | P0 |

---

## 10. 边界与约束

- Python ≥ 3.12；MySQL 与 new-api 共享实例，连接预算 `进程数 × (pool_size + max_overflow) ≤ max_connections × 0.8`。
- Redis 独立实例，AOF `everysec`；键统一前缀 `st:`。
- 提交体上限 2MB，响应体上限 10MB（超限落 `FAILURE` + `response_too_large`）。
- `?wait` 上限 60s，必须 < nginx `proxy_read_timeout`（样例 65s）。
- 不支持流式响应、不支持 GET 型生成接口任务化。
- 性能目标：提交链路 P99 < 80ms（不含 billing RTT），单 worker 并发 64。

---

## 10.1 如何开启攒批（运维速查）

攒批是**可选**能力：不配置时所有任务保持「收到即发」，与改造前完全一致。
开启方式有两条，可以叠加。

### 方式 A：服务端策略（推荐，对客户端透明）

**某个模型要开启攒批，就必须在模型策略表 `model_policies` 里给它写一条**——
没有单独的「开启」布尔位，**`batch >= 2` 本身就是开关**。没写到的模型一律
`batch=0`（不攒批、收到即发）。

```bash
# 热改，5 秒内对新提交生效；校验失败整批拒绝并 400
curl -X PUT "$BASE/admin/api/config" \
  -H "X-Admin-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"model_policies": {"dall-e-3": {"batch": 10, "batch_wait": 60}}}'
```

> **写入语义：默认整表替换，可选合并（`?mode=merge`）。**
> 配置存在 Redis 的一个 hash 字段里（值是一整个 JSON 串），写入即覆盖该字段。
> 默认（`replace`）是**整表替换**：**给第二个模型开启时必须把已有条目一起带上**，
> 否则第一个模型的策略会被静默清掉——它不再攒批、也不再受并发上限约束，而请求侧
> 毫无异常，没人会发现。
>
> 不想承担这个心智负担就加 **`?mode=merge`**（看板上是配置区的「覆盖 / 合并」下拉）：
> 只覆盖/新增本次写到的模型条目，未写到的保持原值。合并粒度到**顶层键（模型名）**
> 为止，同名条目整条替换（不做字段级深合并）。
>
> ```bash
> # 推荐：合并写入（已有条目自动保留，不必先读全表）
> curl -X PUT "$BASE/admin/api/config?mode=merge" -H "X-Admin-Key: $ADMIN_KEY" \
>   -H 'Content-Type: application/json' \
>   -d '{"model_policies": {"sora": {"batch":5, "batch_wait":30}}}'
>
> # 整表替换（默认，不带 mode）：先 GET 现表 → 本地合并 → 再整表 PUT
> curl -s "$BASE/admin/api/config" -H "X-Admin-Key: $ADMIN_KEY" \
>   | jq '.groups[].items[] | select(.key=="model_policies") | .value'
> curl -X PUT "$BASE/admin/api/config" -H "X-Admin-Key: $ADMIN_KEY" \
>   -H 'Content-Type: application/json' \
>   -d '{"model_policies": {"dall-e-3": {"batch":10,"batch_wait":60},
>                            "sora":     {"batch":5, "batch_wait":30}}}'
> ```
>
> 两条语义都有测试钉着：整表替换见
> `test_dynconf_hotreload.py::test_model_policies_write_replaces_whole_table`，
> 合并见 `::test_model_policies_merge_keeps_untouched_entries`。

策略表的键支持三种写法，**按优先级从高到低**命中一个：

| 键写法 | 含义 | 例 |
|---|---|---|
| 模型名 | 精确匹配归一化后的模型名 | `"dall-e-3"` |
| 端点前缀（以 `/` 开头） | 最长前缀匹配 | `"/v1/images"` |
| `__default__` | 兜底，仅在以上都没命中时用 | `"__default__"` |

可用字段（写侧逐项校验，越界/拼错/非法 JSON 一律整批拒绝）：

| 字段 | 含义 | 范围 |
|---|---|---|
| `batch` | 攒够多少条放行（N） | 0–1000（`0`/`1` = 不攒批） |
| `batch_wait` | 最长等待秒数（T） | 1–3600，且须满足下方 TTL 约束 |
| `limit_per_token` | 该 token 总在途上限 | 0–10000（`0` = 回落 `MAX_SLOTS`） |
| `limit_model_token` | (模型, token) 上限。**`>0` 即强制排队** | 0–10000 |
| `limit_global` | 模型全局上限（多 key 合计不超发的唯一保证） | 0–10000 |

两条硬约束（写侧会拦，报 400）：

1. **`batch >= 2` 必须同时给 `batch_wait`** —— 否则「只发了 3 条却声明 N=100」
   的批次可能永远等不到放行。
2. **`batch_wait + 执行时长 + 余量 ≤ SK_SESSION_TTL_SECONDS`** —— 令牌只在 Redis
   且绝不落库，等过头 = 到点取不到令牌 = 100% `token_missing` 失败。
   这条比「任务最大生命期」更紧，是等待时长的真天花板（当前 TTL 7h ⇒ 上限约 6.8h）。

### 方式 B：客户端请求头（逐请求，无需服务端配置）

| 头 | 含义 | 非法时 |
|---|---|---|
| `X-Batch-Size: N` | 期望批量 | 非正整数 / 超 1000 → 400 `invalid_batch_size` |
| `X-Batch-Wait: T` | 最长等待秒 | 超 `MAX_BATCH_WAIT_SECONDS`(默认 300) → 400 `batch_wait_too_long` |
| `X-Batch-Key: <≤64>` | 显式归组键（可跨模型混批） | 超长**截断不报错**；非法字符替换为 `_` |

`X-Batch-Size` **可以单独开启攒批**（即使服务端没配策略）——这是「客户端主动要求
凑批」的正当用法。只给 N 不给 T 时，T 自动兜底为 `MAX_BATCH_WAIT_SECONDS`
（AC-57：不得无限等待）。逐字段覆盖：只声明 N 时 T 仍取服务端策略值。

#### `X-Batch-Key` 的语义与用法

它是**批次的名字**，只决定「谁和谁算同一批」，**不改变 N/T，也不是攒批开关**。
不带它时批次名就是**归一化模型名**，即同模型的所有请求（含其他 token）进同一批
（`BATCH_GROUP_BY=model` 的默认口径，见下方「归组维度」）。

```bash
# 不带 Key：按模型名归组
#   → 202 {"batch_key":"doubao-seedream-5-0-pro-260628","batch_state":"waiting"}
# 带 Key：自成一档，与别人的流量互不干扰
-H 'X-Batch-Key: order-20260911-a'
#   → 202 {"batch_key":"order-20260911-a","batch_state":"waiting"}
```

三个用途：①把自己的请求圈起来，既不被别人的流量「带跑」（提前凑满 N 被放行）、
也不被「拖住」（等满 T）；②同一 Key 下**不同模型可混批**（默认不会跨模型混批），
适合「一次业务动作产生多个请求、要求对齐下发」；③多租户按档切分。

三个边界：

1. **头名必须用连字符**（`X-Batch-Key`）。写成 `X_Batch_Key` 会被 nginx 按默认
   `underscores_in_headers off` **整条丢弃**，表现为「头不生效」且无任何报错——
   服务端会按默认维度归组，`batch_key` 回落成模型名。
2. **键值建议只用 ASCII**：白名单是 `[A-Za-z0-9._:-]`，其余字符（含中文）一律替换成
   `_`。不同的中文键可能归一到同一个下划线串而**意外合并**成同一批。
3. **键名公开且无鉴权**：谁知道这个字符串谁就能进同一批，别用 `batch1`/`test`
   这类可猜名，建议带业务前缀与日期/租户标识。

「它是不是开关」的判据同样简单：**服务端策略 `batch >= 2` 或客户端
`X-Batch-Size >= 2` 才入批**；两者都没有时任务走「收到即发」，此时 Key 被直接丢弃
（`batch_key` 返回空串）。所以只带 Key 的客户端在策略被删除后会**静默退化**为
收到即发，要客户端侧自保就得显式带上 `X-Batch-Size`。

> nginx 透传规则（部署视角）：`proxy_set_header` 是**覆盖**语义，只影响显式列出的头
> （`deploy/nginx.conf` 里是 `X-Upstream-Base-Url` / `Host` / `X-Real-IP` /
> `X-Forwarded-*`），**未列出的自定义头默认原样转发**，不必逐个登记。唯一例外就是
> 上面第 1 条的 `underscores_in_headers`。

### 总开关与「谁来决定是否攒批」

`BATCH_ENABLED`（热改项 `batch_enabled`）**默认就是 `true`**——它是**允许**攒批，
不是**启用**攒批。实际是否攒批由两层决定，任一层不满足就不攒批：

| 层 | 谁控制 | 怎么算作「要攒批」 |
|---|---|---|
| 总开关 | 运维（配置中心热改，或 env） | `batch_enabled = true`（默认） |
| 逐模型/逐请求 | 配置中心策略 **或** 客户端请求头 | 策略 `batch >= 2`（或 `limit_model_token/limit_global > 0`）；客户端 `X-Batch-Size >= 2` |

所以：**总开关保持开着**，是否攒批交给「配置中心的模型策略」或「客户端参数」。
只有线上需要全局止血时才把 `batch_enabled` 关掉（此时客户端头也开不起来）。

### 归组维度

批次归组键默认按**归一化模型名**（`BATCH_GROUP_BY=model`）——跨 token 合并、
批次更大、N 更容易触发。另一种是 `token_model`（按 `token_hash + model`，
与并发维度对齐，但每个 token 各自成批、批次显著变小）。两者都不改变
「放行时各自占各自 token 的槽」这一事实。详见 ARCH §6 Q5 注。

### 验证是否生效

```bash
curl -s "$BASE/admin/api/schedule" -H "X-Admin-Key: $ADMIN_KEY" | jq
# batches[]     : 当前每个批次的归组键、成员数、剩余等待秒
# planned_by_hour: 计划中任务（延迟未到点）按小时分桶
# requeue_pending: 等槽重排的积压量
```

提交后看 202 响应的 `batch_state`：`waiting` = 在批次里等 N/T，
`""` = 未参与批次（`batch_key` 同时为空）。

### 紧急止血

```bash
# 全局关闭攒批（热改，5 秒内生效）
curl -X PUT "$BASE/admin/api/config" -H "X-Admin-Key: $ADMIN_KEY" \
  -H 'Content-Type: application/json' -d '{"batch_enabled": false}'
```

关闭后的精确行为（**开关语义是精确的，只停它该停的那件事**）：

| 对象 | 行为 |
|---|---|
| 新提交的任务 | 不再入批，回到「收到即发」（除非策略里还有分层上限要排队） |
| 已在等待的批次成员 | 不再由 T 触发整批放行；约 2 分钟内被兜底扫描 `sweep_stale` **逐条**放行——仍会执行，只是不再成批 |
| **延迟任务** | **不受影响**，照常在 `scheduled_at` 到点放行 |
| **占槽失败的退避重排** | **不受影响**，照常重试 |

后两行是刻意的：批次 T 触发与到期通道共用同一个 ticker（实现复用），但语义无关。
早前止血开关是在 ticker 入口一刀切，会把延迟任务与重排一起停掉——而延迟任务在
等待期被 sweeper 豁免（不能重投也不能判死），没有第二条恢复路径，只能等超龄判死。
现在开关只在 `batching.tick_once` 内部跳过批次放行那一段。

止血开关只有**一个**事实源：热改白名单里的 `batch_enabled`（不再有第二个读 env 的
判定点——同一开关两套语义会让「管理面止血」与「改 env 止血」效果不同）。

### 版本锚定

- 攒批 N/T 双触发：`batching` 模块；放行单点 `dispatch.release`（占槽唯一位置）
- 批次索引（`st:batch:*`）丢失由 `sweep_stale` 每 2 分钟按 DB 事实重建

---

## 11. 内嵌已知坑

| 坑 | 技术栈指纹 | 根因 | 修法 |
|---|---|---|---|
| tasks 表时间列混入毫秒 | mysql/new-api-tasks | new-api 原生任务模块用 UnixMilli 写法 | 读侧 `as_unix_seconds` 兜底归一；SQL 谓词恒带 `platform='stask'`，只命中本服务写的秒值行，故可裸比较走索引 |
| 共享表误改他人行 | mysql/new-api-tasks | tasks 表被 new-api + atask + stask 三方写 | 所有 UPDATE/SELECT 的 WHERE 必带 `platform = :p` |
| atask sweeper 误扫 stask 行 | atask-service | sweeper 按 `settled != true` 找未结算任务 | data 恒写 `freeze_amount:0, settled:true`，天然跳过 |
| taskiq `with_labels(delay=)` 不生效 | taskiq-redis/ListQueueBroker | ListQueueBroker 不支持 delay 标签 | 延迟任务一律走 `schedule_by_time` |
| gunicorn preload + 全局连接池 | gunicorn/preload_app | fork 前建连接会在子进程间共享 socket | 引擎/Redis/HTTP 客户端全部惰性单例 |
| loguru `diagnose=True` 泄露 sk | loguru | 异常回溯打印帧局部变量，含 raw_token | 固定 `backtrace=False, diagnose=False` |
| 队列 at-least-once 造成双扣 | taskiq | 崩溃重投会再次调上游 | 派发锁 SET NX，锁在即不重发，转对账 |

---

## 12. 端到端验证步骤

```bash
# 1. 安装与静态检查
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m ruff check app tests
.venv/bin/python -m mypy app/
.venv/bin/python -m pytest tests/ -q          # 断言全绿

# 2. 起服务（需 MySQL/Redis/上游 或用 compose）
docker compose up -d
curl -sf http://127.0.0.1:8000/healthz/ready | jq .

# 3. 核心成功流
TASK=$(curl -s -X POST http://127.0.0.1:8000/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" -H "Content-Type: application/json" \
  -H "Idempotency-Key: e2e-001" \
  -d '{"model":"dall-e-3","prompt":"a red cube","n":1}' \
  -D /tmp/h.txt | jq -r .task_id)
grep -i '^location:' /tmp/h.txt           # 断言：Location: /async/v1/images/generations/{task_id}
curl -s -o /dev/null -w '%{http_code}\n' \
  "http://127.0.0.1:8000/async/v1/images/generations/$TASK"          # 断言：202
curl -s "http://127.0.0.1:8000/async/v1/images/generations/$TASK?wait=60" | jq .
                                          # 断言：200 + 上游原生 images 响应体

# 4. 幂等重放
curl -s -X POST http://127.0.0.1:8000/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" -H "Idempotency-Key: e2e-001" \
  -H "Content-Type: application/json" -d '{"model":"dall-e-3","prompt":"x"}' | jq -r .task_id
                                          # 断言：与 $TASK 相同

# 5. 关键错误流：路径硬拒
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  http://127.0.0.1:8000/async/api/user/self -H "Authorization: Bearer sk-xxx"
                                          # 断言：403

# 6. 取消
NEW=$(curl -s -X POST http://127.0.0.1:8000/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" -H "Content-Type: application/json" \
  -d '{"model":"dall-e-3","prompt":"y"}' | jq -r .task_id)
curl -s -X DELETE "http://127.0.0.1:8000/async/v1/images/generations/$NEW" | jq .status
                                          # 断言：CANCELED（若已被 worker 领走则 409）
```

---

## 13. 变更记录

| 日期 | 变更 | 原因 | 影响范围 |
|---|---|---|---|
| 2026-08-21 | Spec v1.0 建立 | 基于设计 v1.1 + 三项开放问题裁决 | 全量 |
| 2026-08-21 | `RETRY_MAX` 默认 0 | ADR-002：5xx 回滚语义未确认，保守不重试 | §5 worker |
| 2026-08-21 | ref_price 走配置 + 兜底 | ADR-003：提交链路不引入额外 RTT | §6 额度 |
| 2026-08-21 | Redis 独立实例 | ADR-004：故障域隔离 | 部署 |
| 2026-08-23 | 补齐卡死任务兜底扫描（AC-33） | `stale_active` 已实现但零调用方：入队消息丢失的任务永久停在 SUBMITTED 且永久占槽 | §5 worker、新增 sweeper |
| 2026-08-23 | 运行时配置白名单（AC-34） | ADR-005：运营旋钮可热改，安全项永久只读 | 新增 `dynconf` |
| 2026-08-23 | 管理面 + 单文件看板（AC-35/36） | 独立 `ADMIN_KEY`，未配置则全部 404 | 新增 `/admin` |
| 2026-08-23 | 单进程部署模式 | 简化部署：一条命令起 web+worker+scheduler | 新增 `app/standalone.py` |
| 2026-09-07 | 定位与命名去 newapi 化 | 本服务是独立异步队列服务，上游只是一个 HTTP 服务；`newapi_base_url`→`upstream_base_url`（不保留旧 env 名），`billing_newapi`→`billing_http` | §1 措辞、`app/config.py`、`app/services/providers/`；**无契约变更** |
