# Spec — stask-service v1.1（规格即契约）

> 首次生成：2026-08-21（基于设计 v1.1 定稿评审 + 开放问题①③⑥裁决）
> 对齐日期：**2026-09-13**（对当前 `main` 代码事实做了一次完整重对齐，见 §13）
> 状态：已确认
> 需求与架构明细：`docs/PRD-scheduling-and-concurrency.md`（延迟 / 攒批 / 三层闸门）、
> `docs/ARCH-scheduling-and-concurrency.md`、`docs/decisions/ADR-*.md`（含 `OPEN-DECISIONS.md`）

本文件是**团队内部契约**：范围、API、数据、Token、验收标准全部锁定。
不在本 Spec 列表内的功能一律不做；开发中的任何改动走 §13 变更流程。

> **本文件的可信度取决于它与代码是否一致。** 代码侧原有 6 道机械守卫（ruff/mypy 自检、
> emoji 扫描、孤儿函数、孤儿类、`SELECT *` 白名单），但**没有一条**校验本文件——
> 2026-09-13 实测：端点清单里写着早就不存在的端点、验收标准仍在描述已删除的计费与
> 对账子系统，漂移累积 6 天而三道门禁全绿。
>
> 现已补上 `tests/test_spec_contract.py` 的 **5 条守卫**，机械守住 §5 端点清单 ⇄
> `app.routes`（双向 + 方法归属）、§4 版本 ⇄ `pyproject.toml`、正文引用的仓库内路径
> 存在性、正文不得残留已废概念、服务版本单一事实源；**§13 变更记录作为追加式历史一律豁免**。
> 守卫本身由 `scripts/mutate_spec_contract.py`（`make mutate`，8 条变异）自证不是空跑。
>
> 因此改动相关代码时**仍须同步修订本文件**——守卫会告诉你哪里没改齐，但不会替你改。

---

## 1. 产品定义

- **一句话描述**：把同步 HTTP 生成接口变成长任务——加 `/async` 前缀提交，毫秒返回本地 task_id，结果异步取回。
- **目标用户**：接入同步生成 API 的应用开发者（客户端不愿为一次 60~180s 的出图请求挂住连接）。
- **核心问题**：同步生成接口耗时长，HTTP 连接易被中间层掐断；而在上游里引入任务模型成本高、风险大。本服务在**不改上游一行代码**的前提下把同步接口变成任务接口，资金操作全部留在上游内部闭环。
- **定位边界**：本服务是一个**独立异步队列服务**，职责只有「同步 → 异步的队列化」。上游（new-api 是默认实现）对本项目而言**只是一个 HTTP 服务**——渠道选择、配额扣费、限流、消费日志全在上游，本服务零资金动作（`quota` 恒写 `0`）。
- **已知耦合**：任务行落在与上游同实例的 `tasks` 表上（ADR-001），这是当前唯一一处非 HTTP 依赖；反转它需要独立存储 + 迁移方案，不在本轮范围。

---

## 2. MVP 范围（锁定）

| 优先级 | 功能 | 验收标准摘要 | RICE |
|---|---|---|---|
| P0 | 提交 `POST/PUT /async/{path}` 通配转任务 | 202 + task_id + Location 头，毫秒返回 | 高 |
| P0 | 幂等占位（`Idempotency-Key`） | 同键重放同 task_id 且回报库中**原始值**；真并发 409 | 高 |
| P0 | 路径准入（allow/deny 前缀） | deny 优先；未命中 allow → 403 | 高 |
| P0 | upstream 寻址与三防线校验 | 头覆盖 + allowlist + scheme/userinfo 校验 | 高 |
| P0 | 分层并发闸门（三层槽位） | 占与释放同源（`slot_flags` 掩码）；满额行为由 `reject_when_full` 决定 | 高 |
| P0 | worker 派发锁 + 上游调用 + 四类分流 | 锁在不重发；2xx/4xx/5xx/超时各自归位 | 高 |
| P0 | 查询 `GET /async/{path}/{task_id}` 与字节级回放 | 202/200/错误码重放三态 | 高 |
| P0 | 长轮询 `?wait=N` | 终态即返，超时返 202 | 中 |
| P0 | 取消 `DELETE /async/{path}/{task_id}` | 排队中 CANCELED；执行中 409 | 中 |
| P1 | 卡死兜底收敛（`sweep_stale`） | 超龄无进展按持槽与否分流：重投 / 判死 | 高 |
| P1 | 延迟下发与攒批（`X-Delay-Seconds` / `X-Execute-After` / `X-Batch-*`） | 等待期不占槽；批次 N/T 双触发放行 | 高 |
| P1 | 制品归一化（`artifacts` / `result_url`） | 扩展名优先、字段名兜底；空清单合法 | 中 |
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
| **主动对账（消费日志反查补记 SUCCESS）** | 随去计费化一并删除：本服务零资金动作，无「有没有扣费」这个标的；超时结果的归属由 `sweep_stale` 收敛 | 永不 |
| 执行中任务的真中止 | 上游是同步调用，无法中断；执行中取消返 409 | 上游支持 abort 后 |
| 流式（SSE/chunked）响应任务化 | 任务模型与流式语义冲突，本期只做一问一答型 | v2.0 |
| 上游 task_id 反查 | 本服务的上游是同步接口，不产生上游任务 id | 永不 |
| HELD 挂起态与金丝雀排空 | 无冻结即无挂起收口需求 | 永不 |
| 结果外置对象存储 | 10MB 上限 + gzip 已覆盖出图/TTS；开放问题⑤留 OPEN | 有超限告警后 |
| 多租户隔离/管理台 UI 定制 | MVP 无此需求（看板为单文件只读视图） | v2.0 |

---

## 4. 技术架构（锁定，版本锚定）

> **版本锚定有两处事实源**：**依赖**版本看 `pyproject.toml`；**服务**版本看
> `app/__init__.py` 的 `__version__`（`pyproject.toml` 通过 `dynamic = ["version"]`
> 从它派生，故打包元数据与运行时永远一致）。本表必须与两者都对得上。
>
> 这两条约束由 `tests/test_spec_contract.py` **机械守住**——它们曾经都漂移过：
> 本表长期写着 redis `5.2.1` 而 `pyproject.toml` 是 `8.1.0`；服务版本则同时存在
> `pyproject` 0.1.0 / `config.py` 0.2.0 / `.env.example` 0.2.0 三个值，而没有任何
> 东西为此报错。

| 层 | 技术 | 版本 | 锁定原因 |
|---|---|---|---|
| Web 框架 | FastAPI | 0.115.14 | 与团队既有服务对齐，零学习成本 |
| ASGI | uvicorn / gunicorn | 0.34.0 / 23.0.0 | UvicornWorker + preload，post-fork 惰性单例 |
| 数据校验 | pydantic / pydantic-settings | 2.11.7 / 2.9.1 | 无前缀配置单例 |
| ORM/驱动 | SQLAlchemy[asyncio] / asyncmy | 2.0.41 / 0.2.10 | 只用 `text()` 原生 SQL，ORM 仅作映射说明 |
| DB 加密依赖 | cryptography | 44.0.0 | MySQL 8 `caching_sha2_password` 必需，缺它 `/healthz/ready` 报 db fail |
| Redis | redis (asyncio) | 8.1.0 | `decode_responses=True` + Lua；**8.x 是 taskiq-redis 1.2 的硬要求** |
| HTTP | httpx[http2] | 0.28.1 | 共享 AsyncClient 连接池 |
| 队列 | taskiq / taskiq-redis | 0.12.6 / 1.2.3 | **`RedisStreamBroker`** + `ListRedisScheduleSource`（见下） |
| 日志 | loguru | 0.7.3 | stdlib 桥接，`backtrace/diagnose=False` |
| 可观测性 | logfire[fastapi,httpx] | 5.0.0 | 可选（`LOGFIRE_ENABLED`），未配 token 静默降级 |
| 测试 | pytest / pytest-asyncio / respx | 8.3.5 / 0.26.0 / 0.22.0 | `asyncio_mode=auto`，手写 FakeRedis |
| Lint/Type | ruff / mypy | 0.11.13 / 1.15.0 | mypy 做成一个 pytest 用例 |
| 部署 | Docker Compose（web + worker + 独立 Redis） | - | Redis 独立实例（ADR-004） |
| 认证 | `AUTH_MODE`：`generic`（默认，零动作、key 由上游判定）/ `newapi`（共库直查 `tokens ⋈ users` 做鉴权+余额预检） | - | 本服务不签发任何凭证（ADR-007） |

**为什么是 `RedisStreamBroker` 而不是 `ListQueueBroker`**：后者 `BRPOP` 取走即删，
worker 崩溃时在飞消息直接蒸发；Stream + consumer group 是 at-least-once，配
`XADD MAXLEN ~` 近似裁剪（`QUEUE_STREAM_MAXLEN`）。

**日志口径**（内部审计口径，勿按「脱敏」预期使用）：内容**不脱敏**（签名 URL 原样上报），
唯一屏蔽项是**凭证头名**（AC-30）；`LOG_LEVEL` 默认 DEBUG，**WARNING 为硬地板**
（调到 ERROR 应急降噪时 WARN 及以上仍必然落两侧）；stderr 侧只打 `{message}`，
`extra` 仅在 logfire 可见。logfire 上报**不脱敏但不上报大文件**——三层体量闸门
（业务摘要 4KB / `OTEL_*_ATTRIBUTE_VALUE_LENGTH_LIMIT` / 管道内 `_BodyCapProcessor` 裁 body）；
metrics 默认**关**（无消费方）；停机 flush 在 `web lifespan` 与 `worker shutdown`。

**依赖钉版唯一处 = `pyproject.toml`**，不写 requirements.txt。

---

## 5. API 端点清单（锁定）

### 用户面

认证列写「Bearer」= 只要求带**任意非空** Bearer 令牌，有效性由上游判定，本服务不外呼。

| Method | Path | 功能 | 认证 | 请求 | 响应 |
|---|---|---|---|---|---|
| POST/PUT | `/async/{path:path}` | 提交任务 | Bearer | 原文 path/query/body；头 `Idempotency-Key`、`X-Callback-Url`、`X-Upstream-Base-Url`、`X-Delay-Seconds`、`X-Execute-After`、`X-Batch-Size`、`X-Batch-Wait`、`X-Batch-Key` | `202 {task_id,status,created_at,scheduled_at,batch_key,batch_state,replayed}` + `Location` |
| GET | `/async/{path:path}`（末段为 task_id） | 查询/回放 | 无（task_id 即凭证） | `?wait=0..60` | `202` 进行中 / `200` 原生回放或 CANCELED 视图 / 重放上游错误码 / `410` 结果已清理 |
| DELETE | `/async/{path:path}`（末段为 task_id） | 取消 | 无 | - | `200 {task_id,status:CANCELED}` / `409` |
| 其他方法 | `/async/{path:path}` | 方法准入 | - | - | `405` |
| GET | `/healthz/live` | 存活探针（无依赖） | 无 | - | `200 {"status":"ok"}` |
| GET | `/healthz/ready` | 就绪探针（DB / Redis 检查 + 配置上报） | 无 | - | `200`/`503` + checks |
| GET | `/ops/stats` | 状态分布 + **调用者自己**的槽位占用 | Bearer | - | `200 {status_counts,my_slots_in_use}` |
| GET | `/ops/tasks/{task_id}` | 单任务诊断视图（不含 sk、不含结果体） | Bearer | - | `200`/`404` |
| POST | `/ops/sweep/stale` | 手工触发一轮卡死收敛 | Bearer | - | `200 {killed,rescheduled,...}` |
| POST | `/ops/slots/recalibrate` | 手工触发槽位校准 | Bearer | - | `200 {...}` |

`/ops/*` 只暴露聚合统计与本任务诊断，**不返回**用户令牌、结果原文、请求体——
这也是它只需「任意 Bearer」的前提。

### 管理面

`X-Admin-Key`（也兼容 `Authorization: Bearer <ADMIN_KEY>`，方便 curl 与浏览器 fetch 复用同一套写法）；
**`ADMIN_KEY` 未配置时全部 404**（不是 401——404 不泄露「这里有个后台」）。

| Method | Path | 功能 | 请求 | 响应 |
|---|---|---|---|---|
| GET | `/admin`、`/admin/` | 看板页面（单文件 HTML，零构建） | - | `200` / `404` |
| GET | `/admin/api/overview` | 概览指标 + 运行时信息 | `?window=60..604800`（默认 3600） | `200` |
| GET | `/admin/api/slots` | 三层槽位占用水位（按 (模型, token) 列出） | `?limit=1..500`（默认 50） | `200 {slots,total}` |
| GET | `/admin/api/schedule` | 调度视图（计划任务按小时分桶 + 等待中批次 + 重排积压） | - | `200 {planned_by_hour,planned_total,batches,requeue_pending}` |
| GET | `/admin/api/tasks` | 任务列表（分页 + 筛选） | `status/model/task_id/task_id_prefix/since/limit/offset` | `200 {total,items}` |
| GET | `/admin/api/tasks/{task_id}` | 任务详情（脱敏） | - | `200` / `404` |
| POST | `/admin/api/tasks/{task_id}/requeue` | 重投队列（**不清派发锁**） | - | `200` / `409` |
| GET | `/admin/api/config` | 读运行时配置 + 只读项原因 | - | `200` |
| PUT | `/admin/api/config` | 批量写覆盖值（白名单外拒绝，整批校验）；**`?mode=merge`** 合并写入 | JSON 对象 | `200` / `400` |
| POST | `/admin/api/config/reset` | 重置回落 env（POST 而非 DELETE：DELETE 带 body 在客户端/代理上行为不一致） | `keys` 数组或 `null` | `200` |
| POST | `/admin/api/jobs/{job}` | 触发 `stale` / `overdue` / `slots` / `purge` | - | `200` / `404` |

`/admin/api/tasks` 的参数约束（越界 → `400`）：`task_id` 精确匹配、`task_id_prefix`
前缀检索，**两者不可同用**，且都禁止 `%` / `\`（前导通配符会让 `task_id` 索引失效）；
`since` 0–604800；`limit` 1–200；`offset` 0–10000。

`/admin/api/schedule` **必须挂在管理面**：批次归组键在 `token_model` 维度下含
token_hash 前缀、也可能是客户端自定义串，暴露给任意已鉴权调用者等于泄露别人家的 key 指纹。

错误响应统一 `{"error": {"message","type","param","code"}}`。

**幂等回放的回报口径**（`POST` 带 `Idempotency-Key` 命中已有 task_id 时）：
响应里的 `status` / `created_at` / `scheduled_at` / `batch_key` / `batch_state`
**全部是库里那一行的原始值**，本次请求携带的调度头与分批头一律不生效，
`replayed=true` 标明这是回放。含义：若命中的任务已经结束（FAILURE/SUCCESS/CANCELED），
响应里就是那个**终态**——不得粉饰成 `QUEUED`，也不得用回放时刻冒充创建时刻；
否则客户端会把一条早已死掉的任务当成"刚入队、正在跑"，在同一个 key 上无限重试。
（要真正发起一次新的尝试，客户端必须换一个 `Idempotency-Key` 或去掉该头。）

---

## 6. 数据模型（锁定 — 复用 new-api `tasks` 表，零建表）

`task_id` 形态：`{model_slug}_{uuid4hex}`（`model_slug` = 模型名小写、非 `[a-z0-9]` 替 `_`、截断 16 字符；总长 ≤ 53）。
`platform` = `stask`（`GATEWAY_PLATFORM`），**所有读写 WHERE 必带**。
`channel_id` = 本服务写入的**独立渠道号**（`CHANNEL_ID`，ADR-006），**不从上游响应头回填**。
该值必须是 new-api 中**真实存在**的渠道 id：new-api 轮询 `updateVideoTasks` 里
`CacheGetChannel(channel_id)` 在 adaptor nil 检查**之前**执行，渠道不存在会把该渠道下
全部任务无 CAS 批量强制 FAILURE（详见 §11）。`CHANNEL_ID<=0` 在生产启动即报错。
`action` = 剥前缀后的请求路径（截断 32 字符）；`quota` 恒写 `0`。

**表列**（非 JSON）：

| 列 | 说明 |
|---|---|
| `task_id` / `platform` / `action` | 见上 |
| `status` | `QUEUED` / `IN_PROGRESS` / `SUCCESS` / `FAILURE` / `CANCELED`（new-api 原生枚举） |
| `progress` | new-api 兼容列，本服务恒 `'0%'` |
| `fail_reason` | 失败原因；new-api 的 `GetResultURL()` 先读 `result_url`，为空才回落它 |
| `user_id` | 归属信息，**可为 0**（`AUTH_MODE=generic` 时为 0）；不参与计费 |
| `channel_id` / `quota` | 见上 |
| `submit_time` / `start_time` / `finish_time` / `created_at` / `updated_at` | **unix 秒** |
| `data` | JSON，字段契约见下表 |
| `private_data` | new-api 的 `TaskPrivateData`（宿主标 `json:"-"`，可能有渠道 key）。本服务**只逐键白名单读取**（`result_url` / `upstream_task_id`），绝不整列返回 |

**`data` JSON 字段契约**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | str | 恒 `"stask"` |
| `model` | str | 浅解析 body 提取；缺失为 `""` |
| `token_hash` | str | sha256(raw_token)[:32]，限流/占槽/校准的口径 |
| `idempotency_key` | str | 可空；**显式**幂等键（ADR-007；已无自动幂等） |
| `callback_url` | str | 可空 |
| `request_method` / `request_path` / `request_query` | str | 剥前缀后的原文 |
| `request_headers` | obj | 剔除逐跳头与凭证头，其余原样保留转发 |
| `request_body` / `request_body_encoding` | str | ≤ `PLAIN_MAX_BYTES` 且合法 UTF-8 存明文，否则 gzip+b64 |
| `body_truncated` | bool | 超 `BODY_MAX_BYTES` 时只存摘要 |
| `upstream_base_url` | str | 提交时校验通过的值，worker 只认它 |
| `upstream_response` / `upstream_response_encoding` | str | 回放原文（明文或 gzip+b64），TTL 到期清空 |
| `upstream_content_type` | str | 回放时原样回设 |
| `upstream_status` | int | 上游 HTTP 状态码（错误码重放依据）；**连接/超时类失败为 0** |
| `response_bytes` | int | 响应体字节数 |
| `dispatch_epoch` | int | 派发轮次；`0` = 从未派发（`sweep_stale` 分流判据） |
| `scheduled_at` | int | 计划执行时刻（unix 秒）；`0` = 无延迟。延迟任务保持 `QUEUED`，等待期不占槽；同时是 sweeper 豁免与生命期计时口径的依据 |
| `batch_state` | str | `""` / `immediate` / `scheduled`（延迟未到点）/ `waiting`（批次成员或等槽）/ `releasing` / `released` |
| `batch_size` / `batch_wait` | int | 生效的 N/T（客户端头覆盖后的值，非策略原值） |
| `batch_key` | str | 批次归组键，`""` = 未使用 |
| `batch_policy_source` | str | 该次生效的策略来源（策略表 / 客户端头 / 回落） |
| `batch_due_at` | int | 批次到期时刻（tick 判据） |
| `slot_flags` | int | 三层槽占位掩码（1=token / 2=(模型,token) / 4=模型全局）。**释放的唯一依据**，绝不按当前配置重算 |
| `slot_model` | str | 占槽用的归一化模型名。占与释放必须同一字符串 |
| `requeue_attempts` | int | 退避重排次数（`dispatch.requeue` 每次读它算下次退避时长） |
| `result_purged` | bool | 结果已被 TTL 清理 |
| `result_url` | str | 从响应体提取的主制品 URL |
| `artifact_parser` | str | 命中的制品解析器名 |
| `artifact_count` | int | 制品数量 |
| `artifacts` | arr | 归一化制品清单（**空清单合法**） |
| `callback_delivered` | bool \| null | 三态：`null` 从未回调 / `true` 已送达 / `false` 重试耗尽 |

> **投影白名单的单一事实源是 `taskstore` 的 `_META_STR_KEYS` / `_META_INT_KEYS` /
> `_META_BOOL_KEYS` / `_META_TRISTATE_KEYS` / `_META_JSON_KEYS` 常量组。**
> 新增字段若属于「释放 / 判死所需」，**必须**同时进对应的组——否则 sweeper 与
> `flow` 取消拿到的元数据行里没有它，会按错误掩码释放（一层不还 = 槽永久泄漏）
> 或把还在等放行的任务当僵尸判死。JSON 数值比较必须 `+ 0`
> （`data ->> '$.x'` 回 LONGTEXT，字符串比较下 `'900' < '1000'` 为假）。

延迟/定时下发（`X-Delay-Seconds` / `X-Execute-After`）的完整需求与验收线见
`docs/PRD-scheduling-and-concurrency.md`（R-01~R-13 / AC-37~AC-45），架构裁决见
`docs/ARCH-scheduling-and-concurrency.md`。要点：等待期不占并发槽（R-04）、
兜底扫描豁免未到点任务（R-05）、生命期起点取 `max(created_at, scheduled_at)`（R-06）、
延迟上限由令牌 TTL 反推（超限在提交时即 `400 delay_too_long`）。

状态机：`QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`。无 HELD、无孤儿判死；计划任务与批次成员**不引入新状态**，一律保持 `QUEUED`（等待期天然可取消）。

---

## 7. 页面清单

无前端产物。本服务是纯 API 网关，交付物为 OpenAPI（FastAPI 自动生成 `/docs`）
+ 单文件只读看板（`app/static/`，零构建）+ `deploy/nginx.conf` 样例。

---

## 8. 设计 Token

不适用（无前端产物）。CLI/日志输出遵循团队同款 loguru 格式，**不使用任何 emoji 字符**
（由 `test_misc.test_no_emoji_in_source` 机械守卫生效）。

---

## 9. 验收标准（EARS 格式，锁定 — QA 唯一依据）

> 编号 `AC-01`~`AC-36` 属本文件；`AC-37` 及之后属 `PRD-scheduling-and-concurrency.md`
> （延迟 / 攒批 / 三层闸门）。**不要重编号已有条目**——PRD、ARCH、测试都用这些编号互指。

| 编号 | 功能 | EARS 验收标准 | 优先级 |
|---|---|---|---|
| AC-01 | 提交 | When 客户端 POST 合法 `/async/v1/images/generations`，系统**必须**在落库后返回 `202` + `task_id` + `Location` 头 | P0 |
| AC-02 | 路径准入 | If 请求路径命中 `ASYNC_DENY_PREFIXES`，系统**必须**返回 `403`，且 deny 判定优先于 allow | P0 |
| AC-03 | 路径准入 | If 请求路径未命中 `ASYNC_ALLOW_PREFIXES`，系统**必须**返回 `403` | P0 |
| AC-04 | 方法准入 | If 请求方法不是 POST/PUT（提交）/ GET（查询）/ DELETE（取消），系统**必须**返回 `405` | P0 |
| AC-05 | 幂等 | While 同一 `Idempotency-Key` 已回填 task_id，系统**必须**返回同一 task_id 而不重建任务，且回报库里那一行的 `status` / `created_at` / `scheduled_at` 原值 | P0 |
| AC-06 | 幂等 | If 同一 `Idempotency-Key` 真并发且占位未回填，系统**必须**返回 `409`，绝不重建 | P0 |
| AC-07 | 并发闸门 | Where 生效策略声明了分层上限，系统**必须**在放行时原子占用对应层并把掩码写入 `slot_flags`；**分层上限只在放行通道 `dispatch.release` 判定**（提交时无效） | P0 |
| AC-08 | 并发闸门 | If 目标层已达上限：`reject_when_full=1` 时系统**必须**在提交时返回 `429` + `Retry-After` 并在响应体标明满的是哪一层；`reject_when_full=0`（**默认**）时系统**必须不**返回 429，而是排队等待放行 | P0 |
| AC-09 | 回滚 | If 落库或入队失败，系统**必须**归还幂等占位（CAS）并归还**已占的**并发槽；任务行**必须保留为 `FAILURE` 而不删除**（入队是「响应可能丢失」的操作，删行会让重试重建第二个任务、上游被调两次） | P0 |
| AC-10 | upstream | `UPSTREAM_ALLOWLIST` 非空时，若 `X-Upstream-Base-Url` 的 host 不在其中，系统**必须**返回 `400`；留空则**不限制**（与 `CALLBACK_ALLOWLIST` 同语义），但 scheme / userinfo / query 校验（AC-11）始终生效 | P0 |
| AC-11 | upstream | If upstream URL 含 userinfo 或 scheme 非 http(s)，系统**必须**返回 `400` | P0 |
| AC-12 | 执行 | When worker 出队，系统**必须**先 CAS `QUEUED→IN_PROGRESS`，失败即放弃（不重复执行） | P0 |
| AC-13 | 派发锁 | If 派发锁已被占用，系统**必须不**再次调用上游——**锁在绝不重发**；锁由 TTL（`WORKER_TIMEOUT` + margin）自然过期，不主动释放 | P0 |
| AC-14 | 分流 | When 上游返回 2xx，系统**必须**落 `SUCCESS` 并存储 gzip 响应原文与 Content-Type | P0 |
| AC-15 | 分流 | When 上游返回 4xx，系统**必须**落 `FAILURE` 并保存原文与状态码用于重放 | P0 |
| AC-16 | 分流 | When 上游返回 5xx 且 `RETRY_MAX=0`（默认），系统**必须**直接落 `FAILURE`，不重试（上游可能有副作用） | P0 |
| AC-16b | 分流 | When 连接层失败（请求**未到达**上游，重试零副作用），系统**必须**按 `RETRY_MAX_CONNECT`（默认 2）退避重试；耗尽后落 `FAILURE`（`upstream unreachable`） | P0 |
| AC-17 | 分流 | When 上游调用超时或传输中断（请求已发出、结果拿不回），系统**必须**落 `FAILURE`（`upstream timeout/broken`）、`upstream_status=0`，且**不得**重试 | P0 |
| AC-18 | 释放 | When 任务进入终态，系统**必须**按 `slot_flags` 掩码**恰好**归还它占过的层、清除令牌会话；**绝不**按当前配置重算层数（那会还掉别人的槽） | P0 |
| AC-19 | 查询 | While 任务为 `QUEUED` / `IN_PROGRESS`，系统**必须**返回 `202` + `{task_id,status,created_at}` | P0 |
| AC-20 | 回放 | While 任务为 SUCCESS，系统**必须**返回 `200` + 字节级一致的上游响应体与原 Content-Type | P0 |
| AC-21 | 回放 | While 任务为 FAILURE 且有上游状态码，系统**必须**重放该状态码与原文 | P0 |
| AC-22 | 回放 | If 结果已被 TTL 清理，系统**必须**返回 `410` + `{"error":{...}}` | P0 |
| AC-22b | 回放 | While 任务为 CANCELED，系统**必须**返回 `200` + 状态视图（它从未调用上游，无原文可放，也不是失败） | P0 |
| AC-23 | 长轮询 | While `?wait=N`（0<N≤60）且任务在窗口内转终态，系统**必须**立即返回终态响应 | P1 |
| AC-24 | 取消 | While 任务为 `QUEUED`，系统**必须**迁移为 `CANCELED` 并按掩码释放槽，零资金动作 | P0 |
| AC-25 | 取消 | While 任务为 `IN_PROGRESS`，系统**必须**返回 `409` | P0 |
| AC-26 | 兜底收敛 | While 任务超龄无进展、派发锁已过期且 `dispatch_epoch=0`（消息丢失），系统**必须**重投，且**必须**按是否持槽分流：已持槽（`slot_flags>0`）直接重投，未持槽（批次成员 / 计划任务）走 `dispatch.release` 占槽——**不得**绕过三层闸门直接投递 | P0 |
| AC-27 | 兜底收敛 | While 任务超龄无进展、派发锁已过期且 `dispatch_epoch>0`（已派发过，结果不可得），系统**必须**判死 `FAILURE`，**不得**重投 | P0 |
| AC-28 | 兜底收敛 | If 派发锁仍存活（一次调用可能在飞），系统**必须**跳过本轮，等锁过期后的下一轮再判；Redis 不可用时**必须**保守视为在飞 | P0 |
| AC-29 | 回调 | When 任务转终态且提供 `X-Callback-Url`，系统**必须**推送含 `X-Stask-Signature` 的 HMAC-SHA256 签名；**严格环境下 `CALLBACK_SECRET` 为空必须阻断启动**（无密钥 = 签名头缺失 = 本 AC 失效，不允许静默降级） | P1 |
| AC-30 | 安全 | 系统**必须不**将用户 sk 写入 tasks 表、日志或任何 HTTP 响应；日志内容其余部分按**内部审计口径不脱敏**，唯一屏蔽项是凭证头名 | P0 |
| AC-31 | 时间 | 读 tasks 表时间列**必须**经 `as_unix_seconds` 归一；写侧恒写 unix 秒，SQL 时间谓词用裸列比较以保留索引 | P0 |
| AC-32 | 清理 | When 结果超过 `result_ttl_seconds`，系统**必须**清空 `upstream_response` 但保留状态行 | P1 |
| AC-33 | 生命期 | When 任务超过 `TASK_MAX_LIFETIME_SECONDS`，系统**必须**判死 `FAILURE`；计时起点取 `max(created_at, scheduled_at)`（两谓词合取实现，保证 `created_at` 走索引） | P0 |
| AC-34 | 动态配置 | If 请求修改白名单外的配置项（连接串 / 密钥 / upstream 白名单 / 路径准入等），系统**必须**拒绝并返回 400；区间校验失败**必须**整批回退 | P0 |
| AC-35 | 管理鉴权 | If `ADMIN_KEY` 未配置，所有管理端点**必须**返回 `404`；已配置时缺失或错误密钥**必须**返回 `401`，且终端用户 sk **不得**通过 | P0 |
| AC-36 | 管理脱敏 | 管理端点**必须不**返回用户 sk、请求体原文或响应体原文 | P0 |
| AC-36b | 运维面鉴权 | `/ops/*` **必须**只要求携带任意非空 Bearer 令牌（不校验有效性），且**必须不**返回用户令牌原文、结果原文或请求体；需要跨 token 视角的数据（如按 (模型, token) 的水位）**必须**只挂管理面 | P0 |

---

## 10. 边界与约束

- Python ≥ 3.12；MySQL 与 new-api 共享实例，连接预算 `进程数 × (pool_size + max_overflow) ≤ max_connections × 0.8`。
- Redis 独立实例，AOF `everysec`；键统一前缀 `st:`。
- 提交体上限 2MB，响应体上限 10MB（超限落 `FAILURE` + `response_too_large`）。
- 明文落库上限 32KB（`PLAIN_MAX_BYTES`）：≤ 此值且合法 UTF-8 存明文，否则 gzip+b64。
- `?wait` 上限 60s，必须 < nginx `proxy_read_timeout`（样例 65s）。
- 令牌会话 TTL（`SK_SESSION_TTL_SECONDS`，默认 7h）**必须 > `TASK_MAX_LIFETIME_SECONDS`（6h）**；
  它同时是延迟上限的天花板（延迟 + 执行 + 余量 ≤ 本值）。
- 不支持流式响应、不支持 GET 型生成接口任务化。
- 性能目标：提交链路 P99 < 80ms（不含上游鉴权 RTT），单 worker 并发 64。
- 可观测性：logfire 可选（默认关）；metrics 默认关（无消费方）。
- 本服务**不做 key 管理**：`AUTH_MODE=generic` 时提交前零动作，任务有效性由上游在执行时判定。

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
| `limit_model_token` | (模型, token) 上限。`>0` 即启用该层（满额行为见 `reject_when_full`） | 0–10000 |
| `limit_global` | 模型全局上限（多 key 合计不超发的唯一保证） | 0–10000 |
| `reject_when_full` | 分层上限**满额时怎么办**：`0`=排队（默认，提交即 202、永不 429）；`1`=提交时原子占三层，满则 429 + `Retry-After`（不再排队、不依赖 tick） | 0–1，须与 `batch<2` 且至少一个分层上限 `>0` 同用 |

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
| 逐模型/逐请求 | 配置中心策略 **或** 客户端请求头 | 策略 `batch >= 2`；或 `limit_model_token/limit_global > 0` **且未开 `reject_when_full`**（这两层默认只能由放行通道 `dispatch.release` 判定，故必须排队）；客户端 `X-Batch-Size >= 2` |

所以：**总开关保持开着**，是否攒批交给「配置中心的模型策略」或「客户端参数」。
只有线上需要全局止血时才把 `batch_enabled` 关掉（此时客户端头也开不起来）。

> **陷阱（不报错、不告警）**：`batch_enabled=false` 还有一个副作用——它令第二/三层静默失效。
> `queued = batch_enabled and policy.queues`，为 false 时走立即路径，而立即路径只调
> 单层 `slots.acquire`（只认第一层）——表现就是「配了没生效」。

### 归组维度

批次归组键默认按**归一化模型名**（`BATCH_GROUP_BY=model`）——跨 token 合并、
批次更大、N 更容易触发。另一种是 `token_model`（按 `token_hash + model`，
与并发维度对齐，但每个 token 各自成批、批次显著变小）。两者都不改变
「放行时各自占各自 token 的槽」这一事实。**这是 2026-09-11 的产品决定**，
与 ARCH §6 Q5 的原始裁决不同，属**有意的偏离**（已在 ARCH §6 Q5 注记录）。

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
- **Redis 索引丢失必须三侧对称回补**：执行侧 `sweeper.sweep_stale`、延迟侧
  `sweeper._rearm_due_index`、批次侧 `sweeper._rebuild_batch_index`。只补一侧即缺陷
  （批次侧静默失效、不报错，最难发现）

---

## 11. 内嵌已知坑

| 坑 | 技术栈指纹 | 根因 | 修法 |
|---|---|---|---|
| tasks 表时间列混入毫秒 | mysql/new-api-tasks | new-api 原生任务模块用 UnixMilli 写法 | 读侧 `as_unix_seconds` 兜底归一；写侧恒写 unix 秒；SQL 谓词恒带 `platform='stask'`，只命中本服务写的秒值行，故可裸比较走索引 |
| 共享表误改他人行 | mysql/new-api-tasks | tasks 表被 new-api 与本服务共写 | 所有读写 SQL 的 WHERE 必带 `platform = :p` |
| 同表其他消费者的筛选条件误扫本服务行 | new-api | 宿主 `GetAllUnFinishSyncTasks` **不带 platform 过滤**（只按 `progress != '100%'` 且状态非 SUCCESS/FAILURE） | 本服务行**确实进了**宿主轮询扫描，靠 `platform='stask'` 在 `relay.GetTaskAdaptor` 返回 nil 而短路。**一旦出现名为 `stask` 的 new-api 任务插件，宿主就会真的轮询并改写本服务任务状态**；另注意 `CacheGetChannel` 在 adaptor nil 检查**之前**执行（见下） |
| 渠道不存在导致批量误杀 | new-api | 轮询里 `CacheGetChannel(channel_id)` 先于 adaptor nil 检查；渠道不存在时该渠道下全部任务被无 CAS 批量强制 FAILURE | `CHANNEL_ID` 必须是 new-api 中**真实存在**的渠道 id（禁用状态也行，缓存含禁用渠道）；`CHANNEL_ID<=0` 生产启动即报错 |
| 配额清零防被其他消费者误判 | mysql/new-api-tasks | 其他消费者按结算/配额条件筛选 | 本服务行恒写 `quota=0`（列）且 `source="stask"`；生命周期由 `TASK_MAX_LIFETIME_SECONDS`(6h) 先于上游 24h 清理线判死 |
| `X_Batch_Key` 这类下划线头被丢弃 | nginx | 默认 `underscores_in_headers off`，**静默整条丢弃** | 客户端自定义头一律用连字符；nginx 侧不改此默认（放宽会引入头注入面） |
| taskiq `with_labels(delay=)` 不生效 | taskiq-redis | `RedisStreamBroker` 不支持该标签 | 延迟任务一律走 `schedule_by_time` / `ListRedisScheduleSource` |
| gunicorn preload + 全局连接池 | gunicorn/preload_app | fork 前建连接会在子进程间共享 socket | 引擎/Redis/HTTP 客户端全部惰性单例 |
| loguru `diagnose=True` 泄露 sk | loguru | 异常回溯打印帧局部变量，含 raw_token | 固定 `backtrace=False, diagnose=False` |
| 队列 at-least-once 造成重复调用上游 | taskiq | Stream consumer group 崩溃重投会再次调上游 | 派发锁 SET NX，**锁在即不重发**；超时/已派发结果按 AC-27 判死 |
| 批次 JSON 统计键集漂移 | — | 同一组键在两处手写，改一处必 KeyError | 键集单一常量派生（`batching._TICK_STAT_KEYS` / `_REBUILD_STAT_KEYS`）；2026-09-11 线上 KeyError 教训 |

---

## 12. 端到端验证步骤

```bash
# 1. 安装与静态检查
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m ruff check app tests scripts gunicorn.conf.py
.venv/bin/python -m mypy app/
.venv/bin/python -m pytest tests/ -q          # 断言全绿（当前基线 487 passed）

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

# 4. 幂等重放（断言：task_id 与上面相同，且 status/created_at 是库里原值）
curl -s -X POST http://127.0.0.1:8000/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" -H "Idempotency-Key: e2e-001" \
  -H "Content-Type: application/json" -d '{"model":"dall-e-3","prompt":"x"}' \
  | jq '{task_id,status,replayed}'

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

# 7. 管理面（需 ADMIN_KEY；未配置则断言 404）
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/admin/api/overview
curl -s "http://127.0.0.1:8000/admin/api/schedule" -H "X-Admin-Key: $ADMIN_KEY" | jq
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
| 2026-09-07 | 定位与命名去 newapi 化 | 本服务是独立异步队列服务，上游只是一个 HTTP 服务；`newapi_base_url`→`upstream_base_url`（不保留旧 env 名） | §1 措辞、`app/config.py`；**无契约变更** |
| 2026-09-08 | 上游鉴权前置 + 显式幂等（ADR-007） | 引入 `AUTH_MODE`（`generic` 默认 / `newapi` 共库直查）；`Idempotency-Key` 成为**唯一**幂等语义，删除自动幂等与 `IDEM_*` 配置 | §4 认证、§5、`upstream.py`、`submit.py`；AC-05/06 口径收紧 |
| 2026-09-08 | 通用上游 + 热路径性能（ADR-008） | 删除类型归一化拼接；热路径读改 `get_meta` 轻量投影（`SELECT *` 仅留白名单） | §6、`taskstore`；AC-31 配套 |
| 2026-09-08 | 队列换 `RedisStreamBroker` | `ListQueueBroker` 的 `BRPOP` 取走即删，worker 崩溃丢在飞消息 | §4、`queue.py` |
| 2026-09-11 | 调度下发与细粒度并发控制 | 延迟 / 攒批 / **三层闸门**落地（第三层才是「多 key 合计不超发」的保证） | §2、§5、新增 §10.1；AC-07/08 改写为分层口径 |
| 2026-09-11 | 执行 / sweeper 返回结构化摘要 | 接通 taskiq-admin 可视化；任务体恒返回摘要 dict，失败靠 `ok=False` 绝不 raise | §5 |
| 2026-09-11 | 制品归一化解析 | 扩展名优先、字段名声明兜底；**空清单合法**；解析异常绝不上抛 | §6、§2 |
| 2026-09-12 | 模型策略表支持 `?mode=merge` | 整表替换会静默清掉未写到的模型条目 | §5、§10.1 |
| 2026-09-12 | 日志全量口径 + logfire 三层体量闸门 | 内部审计口径：内容不脱敏但不上报大文件；metrics 默认关 | §4 日志行 |
| 2026-09-12 | 幂等回放回报库中真实状态与原始时刻 | 回放不得把已 FAILURE 的任务粉饰成 `QUEUED`，否则客户端无限重试 | §5 回放口径；AC-05 补「原值」要求 |
| 2026-09-12 | 分层上限满额行为可选 `reject_when_full` + 429 标明层级 | 默认排队（永不 429）会令客户端失去背压信号，需要可按模型选择 | §10.1；AC-08 拆分两种语义 |
| **2026-09-13** | **SPEC 与实现重对齐（v1.0 → v1.1）** | 变更记录停更于 09-07，此间 ADR-007/008、三层闸门、攒批、`?mode=merge`、`reject_when_full` 等全部未入账；端点清单含幽灵端点、AC 仍描述已删除的计费与对账子系统、字段表含已废字段、并引用了不存在的文件（`docs/stask-service-design.md` / `app/services/providers/` / ADR-003） | 全量：§2 范围、§3 增补、§4 版本锚定（redis 5.2.1→8.1.0、taskiq 0.11.18/1.0.2→0.12.6/1.2.3、补 cryptography/logfire、broker 更正）、§5 端点清单重写、§6 字段表重写、§9 修 AC-07/08/12/13/17/18/19/24 并新增 AC-16b/36b、§11 坑表增补、§10.1 补 `batch_enabled` 副作用与三侧回补 |

| **2026-09-13** | **契约守卫 + 版本单一事实源（P1）** | 上一条对齐只治了**当前**漂移，契约层仍**零守卫**——同样的漂移会再次发生，而 CI 依旧全绿 | 新增 `tests/test_spec_contract.py`（5 条：§5 端点 ⇄ `app.routes` **双向**一致 + 方法归属、正文已废概念 denylist、正文路径引用存在性、§4 版本必须出自 pyproject、服务版本单一事实源）；新增 `scripts/mutate_spec_contract.py`（8 条变异自证，逐条确认守卫真的会红）；服务版本唯一事实源落到 `app/__init__.py` 的 `__version__`，`pyproject.toml` 改 `dynamic = ["version"]` 从它派生，`Settings.app_version` 默认值与 `.env.example` 同步引用 |

| **2026-09-13** | **注册表与入口覆盖门禁（P2）** | 不变式「新增 taskstore 函数须同步测试替身登记表」与「被调度/被路由的入口必须有真的调用一遍的用例」此前**只有纪律、没有门禁** | `tests/test_misc.py` 新增 2 条：`test_taskstore_test_double_covers_every_public_function`（公开函数须进 `_TASKSTORE_FUNCS` 或新增的 `_TASKSTORE_PURE_FUNCS`；登记项须与真实实现和替身**双向**存在）、`test_no_direct_name_import_of_stateful_taskstore_functions`（直接名字导入会让猴子补丁失效）；新增 `tests/test_entrypoints.py`（11 条入口冒烟）与 `tests/entry_coverage_plugin.py` + `make entries`（判据是 **code object 真的被执行过**，不是名字出现过）；`Makefile` 增 `entries` 目标 |

| **2026-09-13** | **P3：文档守卫推广 + `OPTIMIZATION.md` 订正** | `docs/OPTIMIZATION.md` 是孤儿文档，且**让读者去找一个本仓不存在的文件**（`submit_v2.py`，无实现也无引用）；死引用守卫此前只覆盖 SPEC，同类问题在其他文档无人管 | 第 3 条守卫由「SPEC 正文路径」**推广为「全部 `docs/*.md` + README 的路径与 `.py` 引用」**（含裸文件名形态——`submit_v2.py` 正是这种没有目录前缀的形态，只查带前缀路径会整类漏掉），带 `_HISTORICAL_DOCS` / `_EXTERNAL_PY_REFS` 两张**写明理由**的豁免表；`OPTIMIZATION.md` 按逐项复核结果订正（6 项中 5 项仍成立、第 6 项标注**未落地**），加「历史快照、非事实源」抬头；`scripts/mutate_spec_contract.py` 加固：pytest 退出码 4/5（节点失效）判为**变异无效**而非「如期变红」——旧版会把节点改名读成一次假绿 |

| **2026-09-13** | **`taskstore.py` 拆成包（纯结构变更）** | 单文件 1163 行，远超可读区间；文件内原有的「写 / 读」两处分节注释已经暗示了真实的关注点边界 | `app/services/taskstore.py` → `app/services/taskstore/`：`__init__.py`（**全量再导出**原命名空间，契约不变）+ `_base` / `_projection` / `_write` / `_batch` / `_read` / `_sweeper` / `_admin_query`，**最大单文件 255 行**。每个函数体经 AST 比对**逐字未变**、54 个顶层名字零丢失。配套：`conftest` 的测试替身改为 patch「包 + `pkgutil` 自动枚举的子模块」（包内跨模块调用的绑定在子模块命名空间里，只 patch 包够不着 → 会静默打真库）；注册表守卫补「再导出完整性」与「再导出但包外无调用方」两条；孤儿守卫的限定前缀改为「消费者实际会写的包名」；同步 README 文件树、`models.py` docstring、PRD / ARCH 的路径引用 |

| **2026-09-13** | **生产「忘配就静默失去保护」三面收口（P4）** | 复核发现致命启动校验在**生产默认不生效**：`Dockerfile` 不注入任何 ENV、compose 也不设 `APP_ENV`，容器里 `APP_ENV` 恒为配置默认值 `dev` → `CHANNEL_ID` / taskiq-admin 两条 fail-fast 形同虚设；且 `_STRICT_ENVS` 是 **fail-open**（`APP_ENV=prodd` 拼错即静默宽松）；另有 `CALLBACK_SECRET` 为空时回调**不带签名**，与 AC-29「必须签名」直接冲突且无任何启动提醒 | ① `main.py` 的严格判定改为 **fail-closed**（`_LENIENT_ENVS` 白名单之外一律严格，含拼错与置空）；② 新增 `_check_callback_secret()`：严格环境空密钥阻断启动（沿用 `CHANNEL_ID` 同一口径）；③ `Dockerfile` 加 `ENV APP_ENV=prod`（镜像即生产产物）；④ compose 加 `APP_ENV: ${APP_ENV:?...}`（沿用既有必填惯用法，缺失即解析失败）；⑤ AC-29 措辞补上该前置条件；⑥ `.env.example` 写明 fail-closed 语义与生产必须显式声明 |

> **历史行保留原样（append-only）。** 上表中 `ref_price` / ADR-003、`SUBMITTED`、
> `reconcile_*`、`inflight_slot` 等描述所对应的机制均已失效——去计费化（v0.2 起）
> 删除了资金与对账链路，状态机收敛为 `QUEUED→IN_PROGRESS→终态`，占槽标记改为
> `slot_flags` 掩码。保留原行仅为审计追溯，**不要照它们实现**。
