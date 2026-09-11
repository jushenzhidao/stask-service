# stask-service — 通用 HTTP 异步任务服务

把一个**同步 HTTP 接口**变成长任务：给目标路径加 `/async` 前缀提交，毫秒返回
本地 `task_id`，结果异步取回。除此之外没有别的职责——**不做计费、不做 key
管理**，只做队列化 worker。Authorization 原样透传给上游。

**上游只是一个 HTTP 服务**（new-api 是默认实现，换任何同步生成接口只要进
allowlist 即可）。鉴权、渠道选择、配额扣费、限流全部在上游内部闭环。

```
nginx（同一域名）
├─ /async/ → stask-service          其余 → 上游 HTTP
│
stask web    提交：鉴权（AUTH_MODE）→ 幂等占位（可选）→ 占槽 → 落库 QUEUED → 令牌会话 → 入队 → 202
stask worker 执行：派发锁 → 用户令牌调上游同步接口 → 响应即终态落库 → 释放槽/清会话/回调
上游          鉴权 / 渠道 / 扣费 / 限流，零改动
```

状态机：`QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`
（new-api tasks 表原生枚举，两活跃态，ADR-006）。

鉴权按 `AUTH_MODE` 分流（ADR-007）：

| 模式 | 行为 |
|---|---|
| `generic`（默认） | 零动作——key 有效性由上游在任务执行时判定，提交链路零额外开销 |
| `newapi` | 共享库单 SQL 直查 tokens ⋈ users：key 有效性 + 状态/额度预检 + user_id 回查；无效 key 提交即 401，额度耗尽 402，DB 挂 502 不放行。双层缓存（进程内 5s + Redis 300s），只缓存正向结果 |

> **已知耦合（有意保留）**：任务行落在与 new-api 同实例的 `tasks` 表上，靠
> `platform='stask'` + 独立 `channel_id` + `quota=0` 与上游任务零冲突共存
> （契约见 ADR-006）。彻底解耦需独立存储 + 迁移方案。

---

## 快速开始

```bash
make setup       # 建 venv、装依赖、生成 .env
make check       # 三项门禁：ruff + mypy + pytest（不需要 MySQL/Redis）
make up          # 起全套（web + worker + redis）
```

`make` 无参数列出所有命令。两种运行形态按需选：

| 形态 | 命令 | 适用 |
|---|---|---|
| 四容器 | `make up` | 标准部署：web + worker + redis + taskiq-admin（看板，必选） |
| 本机单进程 | `make standalone` | 本地开发、单机试用，一条命令起全套且**免 .env** |

接真实环境时 `.env` 至少改四项：`UPSTREAM_BASE_URL`（上游地址）、
`SQL_DSN`（指向 tasks 表所在库，new-api 的 Go DSN 格式）、`UPSTREAM_ALLOWLIST`、
`TASKIQ_ADMIN_API_TOKEN`（看板必填，缺失时 compose 直接报错）。
环境变量名 = 配置字段名的大写形式，**无 `ST_` 前缀**；容器内地址（Redis /
数据库 / 看板）写死在 `docker-compose.yml` 的 `environment` 里，不重复配置。
上游地址由 `UPSTREAM_BASE_URL` 控制，`UPSTREAM_ALLOWLIST` 留空即不限制。
**必须配 `CHANNEL_ID`**：在 new-api 建一个占位渠道（可禁用）并填其 id——渠道不存在会被上游轮询批量误判 FAILURE（详见 ADR-006）。

---

## API

| Method | Path | 说明 |
|---|---|---|
| POST/PUT | `/async/{path}` | 提交。202 + `{task_id,status,replayed}` + `Location` 头，**默认不去重**；带 `Idempotency-Key` 头才幂等 |
| GET | `/async/{path}/{task_id}` | 查询。202 进行中 / 200 原文回放 / 重放上游错误码；`?wait=60` 长轮询 |
| DELETE | `/async/{path}/{task_id}` | 取消。排队中 → CANCELED；执行中 → 409 |
| GET | `/healthz/live` `/healthz/ready` | 探针 |
| GET | `/ops/stats` `/ops/tasks/{id}` | 观测（带 Bearer 即可，脱敏） |
| POST | `/ops/sweep/stale` `/ops/slots/recalibrate` | 手工触发兜底任务 |

提交示例：

```bash
curl -X POST https://api.example.com/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" \
  -H "Content-Type: application/json" \
  -H "X-Callback-Url: https://myapp.com/hook" \
  -H "X-Upstream-Base-Url: https://newapi.com" \
  -d '{"model":"dall-e-3","prompt":"a red cube","n":1}'
# → 202 {"task_id":"dall_e_3_ab12...","status":"QUEUED","scheduled_at":0,
#        "batch_key":"","batch_state":"","replayed":false}
#   Location: /async/v1/images/generations/dall_e_3_ab12...

curl "https://api.example.com/async/v1/images/generations/dall_e_3_ab12...?wait=60"
# → 200 + 与直接调同步接口**字节级一致**的响应体
```

- `X-Upstream-Base-Url` 可选（默认读 env `UPSTREAM_BASE_URL`），host 必须
  在 allowlist 内。
- **显式幂等（可选）**：默认每次提交都是**新任务**，`task_id = {slug}_{uuid4}`。
  带 `Idempotency-Key` 时改为确定的 `task_id = {slug}_{sha256(token_hash‖key)[:32]}`——
  同一 token 的同一 key 恒映射同一任务，重试只需复用同一个 key；回放返回**原始**
  排期（本次请求带的调度头/分批头不生效）。事实源是 `tasks` 表，Redis 只护
  「创建链路在飞」的几百毫秒窗口。

管理面（`X-Admin-Key` 鉴权，未配置密钥时全部 404）：

| Method | Path | 说明 |
|---|---|---|
| GET | `/admin` | 看板页面 |
| GET | `/admin/api/overview?window=` | 概览指标 |
| GET | `/admin/api/tasks?status=&model=&task_id=&task_id_prefix=&limit=&offset=` | 任务列表 |
| GET | `/admin/api/tasks/{id}` | 任务详情（脱敏） |
| POST | `/admin/api/tasks/{id}/requeue` | 重投队列（不清派发锁） |
| GET/PUT | `/admin/api/config` | 读写运行时配置 |
| POST | `/admin/api/config/reset` | 重置回落 env |
| POST | `/admin/api/jobs/{stale\|overdue\|slots\|purge}` | 手工触发定时任务 |

---

## 模型策略表（`model_policies`）：按模型控制攒批与并发

`model_policies` 是**唯一的逐模型调度开关**——没有「开启攒批」这种布尔位，
**`batch >= 2` 本身就是开关**。没写到的模型一律 `batch=0`：不攒批、收到即发，
行为与改造前完全一致。

### 怎么改：三条路径（读取优先级从高到低）

| # | 路径 | 写入方式 | 生效 | 写侧校验 |
|---|---|---|---|---|
| 1 | **管理面 API**（推荐） | `PUT /admin/api/config`（可加 `?mode=merge`）+ `X-Admin-Key` | ≤5s | **有**——非法整批 400，一个值都不改 |
| 2 | Redis 覆盖值 | `HSET st:dynconf model_policies '<JSON>'` | ≤5s | **无**——坏值被读侧丢弃并回落 env，日志留 `dynconf value invalid, ignoring` |
| 3 | env `MODEL_POLICIES` | 写 `.env` 后重启 | 重启 | 启动时校验，非法 JSON 直接起不来 |

读取序为 **Redis 覆盖 > env > 代码默认**。**写侧默认整表替换**（Redis / env 两条路径
天然如此），管理面 API 额外提供 **`?mode=merge`**——只覆盖/新增写到的模型条目，
未写到的保持原值。且 Redis 里一旦存在该字段，env 那份**整份被忽略**（改了 `.env`
却不生效，先查 Redis 有没有残留覆盖值）。

推荐走路径 1：**加模型用 `merge`，别用默认的整表替换**。

```bash
BASE=https://<stask 入口>
ADMIN_KEY=<管理密钥>

# 1) 加一个模型（merge：已有条目自动保留，不必先读全表）
curl -X PUT "$BASE/admin/api/config?mode=merge" \
  -H "X-Admin-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"model_policies": {
         "doubao-seedream-5-0-pro-260628": {"batch": 3, "batch_wait": 30}
       }}'

# 2) 整表替换（默认，不带 mode）：**没带上的模型会被清掉**，慎用。
#    真要整表写，先读出现值 → 本地合并 → 再写回：
curl -s "$BASE/admin/api/config" -H "X-Admin-Key: $ADMIN_KEY" \
  | jq '.groups[].items[] | select(.key=="model_policies") | .value'

# 3) 回退到 env 值（删除覆盖值）
curl -X POST "$BASE/admin/api/config/reset" \
  -H "X-Admin-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '["model_policies"]'
```

> **`merge` 的粒度到顶层键（模型名）为止**：只覆盖/新增本次写到的模型，
> 同名条目**整条替换**（不做字段级深合并——那会让「改一个字段」与「删一个字段」
> 不可区分，排障时也说不清生效了哪套参数）。看板上是配置区按钮行的
> 「覆盖 / 合并」下拉；未知 mode 一律 422，不会静默退化成 replace。

> ⚠️ **网关没放开管理面时**（不少部署只对外暴露 `/async/`，`/admin` 被上游
> SPA 吞掉，返回 200 但内容是 HTML）路径 1 走不通：改走内网
> `http://<容器>:8000/admin`，或用路径 2 直连 Redis——**注意它绕过校验**。

### 键与字段

> 本节与下面两节的要点，**同样就近标注在管理看板的 `model_policies` 项下方**
> （即 `GET /admin/api/config` 返回的 `note` 字段）——页面上改配置时不必翻本文档。

键三选一，按优先级命中最先匹配的一条：**精确模型名**（小写归一）>
**端点前缀**（如 `/v1/images`，最长匹配）> `__default__`（兜底）。

| 字段 | 含义 | 范围 |
|---|---|---|
| `batch` | 攒够多少条放行（N） | 0–1000，`>= 2` 才攒批 |
| `batch_wait` | 最长等待秒数（T）。**`batch >= 2` 时必须显式给** | 1–3600 |
| `limit_per_token` | 该 token 总在途上限 | 0–10000，`0` = 回落 `MAX_SLOTS` |
| `limit_model_token` | (模型, token) 上限，**`> 0` 即强制排队** | 0–10000 |
| `limit_global` | 模型全局上限（多 key 合计不超发的唯一保证） | 0–10000 |

两条写侧硬约束（违反即 400）：`batch >= 2` 必须同时给 `batch_wait`；
`batch_wait + 执行时长 + 余量 ≤ SK_SESSION_TTL_SECONDS`——令牌只在 Redis 且
绝不落库，等过头必然以 `token_missing` 100% 失败。

### 放行延迟按「T + cron 相位」估算

生产实测：**N 触发**从第 N 条入批到整批下发在 **2~7s**（取决于放行任务的消费
相位）；**T 触发**的实际等待
≈ `batch_wait` + **0~15s**（`tick` 每分钟 1 次、内部自旋 4 轮 × 15s 的相位差）
+ 约 3s 放行链路。即 `batch_wait=30` 的真实放行在 **40~50s** 量级，不是 30s。

### 不改服务端配置的旁路

客户端可用请求头**逐请求**开启/覆盖攒批，无需动策略表：

| 头 | 作用 | 备注 |
|---|---|---|
| `X-Batch-Size: N` | 声明批量（N），**可单独开启攒批** | 只给 N 不给 T 时，T 兜底为 `MAX_BATCH_WAIT_SECONDS` |
| `X-Batch-Wait: T` | 声明最长等待（T） | 超 `MAX_BATCH_WAIT_SECONDS` → 400 `batch_wait_too_long` |
| `X-Batch-Key: <≤64>` | **批次的名字**：决定「谁和谁算同一批」 | **不改变 N/T，也不是攒批开关**；可强制跨模型混批 |

`X-Batch-Key` 的用法与坑：

```bash
# 默认（不带 Key）：按模型名归组 —— 同模型的**所有**请求（含其他 token）进同一批
curl -X POST "$BASE/async/v1/images/generations" ... \
  -d '{"model":"doubao-seedream-5-0-pro-260628", ...}'
#   → 202 {"batch_key":"doubao-seedream-5-0-pro-260628","batch_state":"waiting"}

# 带 Key：自成一档，与别人的流量互不干扰（批量与放行时刻只和自己人凑）
curl -X POST "$BASE/async/v1/images/generations" ... \
  -H 'X-Batch-Key: order-20260911-a' \
  -d '{"model":"doubao-seedream-5-0-pro-260628", ...}'
#   → 202 {"batch_key":"order-20260911-a","batch_state":"waiting"}
```

- **头名必须用连字符**：`X_Batch_Key` 会被 nginx 按默认 `underscores_in_headers off` 整条丢弃，
  表现为「头不生效」（服务端按默认维度归组，`batch_key` 回落成模型名）
- **键值建议只用 ASCII**：白名单是 `A-Za-z0-9._:-`，其余字符（含中文）一律替换成 `_`——
  不同的中文键可能归一到同一个下划线串而**意外合并**
- 键名是**公开且无鉴权**的：谁知道这个字符串谁就能进同一批，别用 `batch1` 这类可猜名，
  建议带上业务前缀与日期/租户标识
- 它能解决什么：①自己的请求不被别人的流量「带跑」或「拖住」；②同一 Key 下不同模型可混批，
  把一次业务动作产生的多个请求对齐下发

完整语义见 [`docs/SPEC.md`](docs/SPEC.md) §10.1。

---

## 与 new-api 的共存契约（ADR-006）

外部任务复用最新版 new-api `tasks` 表，与其内部任务零冲突：

| 条件 | 效果 |
|---|---|
| `platform='stask'`（非 suno/mj） | `GetTaskAdaptorFunc` 返回 nil，原生任务轮询天然跳过 |
| `channel_id` = **new-api 中真实存在的渠道**（`CHANNEL_ID`，占位渠道可禁用） | 轮询按渠道分组；渠道必须存在——CacheGetChannel 失败先于 adaptor nil 检查，会把整组任务批量误判 FAILURE |
| `quota = 0` | 上游 24h 超时清理即便动到本行，退款也是 0，零资金影响 |
| `status`/`progress` 原生枚举 | 共享表对上游看板/巡检工具可读 |
| 生命期 6h（`TASK_MAX_LIFETIME_SECONDS`）| 先于上游 24h 清理线自行收敛，正常情况上游永远碰不到我们的活跃行 |

写读侧 WHERE 恒带 `platform`，绝不动别人的行。

---

## 设计红线

1. **零资金动作**——不冻结、不结算、不对账；扣费成败是上游和用户之间的事。
2. **令牌不落库不进日志**——只放 Redis 会话（`SK_SESSION_TTL_SECONDS`，默认 7h），
   终态即清。
   loguru 固定 `backtrace=False, diagnose=False`。
3. **tasks 表时间列必须归一**——读侧 `as_unix_seconds`；SQL 侧写入恒为秒，
   时间谓词用裸列比较（不再包裹表达式，否则吃不到索引）。
   表被 new-api 原生模块用 UnixMilli 写过，两侧都不能省。
4. **写读侧 WHERE 必带 `platform`**——共享表，绝不动别人的行。
5. **派发锁在 = 一次调用已发出 = 绝不再调上游**——队列 at-least-once，防重全靠锁。
6. **超时/传输断判 FAILURE 且不重试**——请求可能已产生副作用，重发才是事故；
   结果拿不回来，留挂着毫无意义。
7. **安全项不可运行时改**——白名单类配置只读 env，管理面拒绝写入（ADR-005）。

---

## 关键决策

| 决策 | 取值 | 依据 |
|---|---|---|
| 幂等 | **显式**：默认随机 `task_id`，带 `Idempotency-Key` 才去重 | 去重语义与 Stripe/OpenAI 一致；不带头的重试就是两次提交，不静默吞掉请求 |
| 共存 | platform + 独立渠道 + quota=0 + 6h 生命期 | ADR-006：外部任务不影响 new-api 内部任务 |
| 5xx 重试 | 默认关（`RETRY_MAX=0`） | ADR-002：上游调用可能有副作用 |
| Redis | 独立实例 + `st:` 键前缀 | ADR-004：故障域隔离，前缀是误配时的第二道防线 |
| 建表 | 零 | ADR-001：复用上游同实例 `tasks` 表 |
| 动态配置 | 白名单子集可热改 | ADR-005：安全项开放等于把防线挂到网上 |

未决事项见 [`docs/decisions/OPEN-DECISIONS.md`](docs/decisions/OPEN-DECISIONS.md)。

---

## 目录结构

```
app/
├── config.py         配置单例（env 名 = 字段名大写无前缀，禁止散读 os.environ）
├── db.py             MySQL 惰性引擎（零建表）
├── redis.py          键规范 + Lua 脚本（全部集中在此）
├── errors.py         {"error":{...}} 统一形制
├── queue.py          taskiq broker/scheduler/发布门面
├── main.py           应用装配
├── standalone.py     单进程模式（web + worker + scheduler 同事件循环）
├── static/           admin.html 单文件看板（零构建）
├── deps/             auth（Bearer 提取，不外呼）/ ratelimit（滑窗）/ admin（管理密钥）
├── routers/          proxy（/async 通配）/ ops / admin（看板 + 配置）
└── services/
    ├── admission.py    路径准入 + upstream 三防线 + 头清洗
    ├── idem.py         显式幂等（Idempotency-Key → 确定 task_id + 创建窗口占位）
    ├── submit.py       提交链路（失败即回滚）
    ├── execute.py      worker 执行（派发锁 + 分流）
    ├── flow.py         查询 / 字节级回放 / 长轮询 / 取消
    ├── sweeper.py      卡死收敛 / 超龄判死 / 槽位校准 / 结果清理
    ├── taskstore.py    tasks 表原生 SQL（数据访问单点）
    ├── dynconf.py      运行时配置白名单（Redis > env > 默认）
    ├── slots.py        并发槽（固定上限）
    ├── tokensession.py 用户令牌会话（唯一查询处）
    ├── codec.py        gzip + b64
    └── notify.py       HMAC 签名回调
```

## 兜底扫描（四只 sweeper）

| 任务 | 频率 | 作用 |
|---|---|---|
| 卡死收敛 | 每 2 分钟 | 锁过期 + 从未派发（消息丢失）→ 重投；锁过期 + 派发过（结果不可得）→ 判死 |
| 超龄判死 | 每 5 分钟 | 超过最大生命期（6h）仍非终态 → FAILURE，先于 new-api 24h 清理线收敛 |
| 槽位校准 | 每 5 分钟 | Redis 计数按 tasks 表事实双向修正 |
| 结果清理 | 每小时 | 清空超期 `upstream_response`，状态行保留（查询返回 410）|

## 测试

440+ 个用例，不依赖真实 MySQL / Redis / 上游：
Redis 用手写 FakeRedis（Lua 按 `app/redis.py` 常量做等价 Python 实现），
tasks 表用 InMemoryTaskStore（保留 CAS 与 data 合并语义），
出站 HTTP 用 respx 拦截（未声明的请求立即失败），
`mypy` 与 `ruff` 各做成一个 pytest 用例。
