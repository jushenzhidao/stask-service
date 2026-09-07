# stask-service — 同步转异步任务网关

给 new-api 的同步生成 API（出图 / TTS / 视频）加 `/async` 前缀即任务化：
毫秒返回本地 `task_id`，结果异步取回。

**计费零代码**——资金操作全部在 new-api 原生 relay 内闭环，本服务不做任何
冻结/结算/退款动作。atask-service（异步任务网关）零改动，两者共享 `tasks`
表但靠 `platform` 列划分自有行。

```
nginx（同一域名）
├─ /async/ → stask-service          其余 → new-api
│
stask web    提交：幂等占位 + 余额额度占槽 → 落库 SUBMITTED → 令牌会话 → 入队 → 202
stask worker 执行：派发锁 → 用户 sk 调 new-api relay → 响应即终态落库 → 释放槽/清会话/回调
new-api      渠道选择 / 配额扣费 / 限流 / 消费日志，零改动
```

状态机：`SUBMITTED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`。
无冻结、无孤儿判死、无 HELD。

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
| 三容器 | `make up` | 标准部署，worker 可独立扩副本 |
| 本机单进程 | `make standalone` | 本地开发、单机试用，一条命令起全套且**免 .env** |

worker 需要扩副本或要滚动重启 web 而不中断在途任务时，从 `standalone` 换到 `up`。

接真实环境时 `.env` 至少改三项：`ST_DATABASE_URL`（指向 new-api 那个库）、
`ST_BILLING_SVC_URL`、`ST_UPSTREAM_ALLOWLIST`。

---

## 管理看板

```bash
make admin-key   # 生成密钥写入 .env 并输出访问地址
make up
open http://127.0.0.1:8000/admin
```

单文件 HTML，零构建步骤、零 npm。四个页签：

- **概览** — 窗口内任务量、成功率、在途数、待对账数；状态分布、耗时分位（p50/p95/p99）、失败原因 TopN、模型分布
- **任务** — 按状态/模型/task_id 片段筛选分页；详情含派发轮次、对账原因、令牌会话 TTL、回调状态
- **配置** — 运行时热改，保存即生效
- **运维** — 手工触发四个定时任务、查看待对账队列、重投卡住的任务

`ST_ADMIN_KEY` 留空时整个管理面返回 **404**（不是 401）——默认不开启，也不泄露
"这里有个后台"。密钥与终端用户 sk 完全分离：复用 sk 会让任何普通用户都能改
全局配置。

## 运行时配置

`.env` 里标 `[热改]` 的项可在看板改，无需重启。读取优先级
**Redis 覆盖 > env > 代码默认值**；Redis 不可用时自动回落 env——动态配置是增强，
不是可用性单点。

可热改的都是运营旋钮：参考单价、槽上限、限流、重试次数、各类超时与 TTL、
定时任务开关。多副本部署时其余进程最多 5 秒后生效（本地缓存 TTL）。

**永不可热改**（`app/services/dynconf.py::IMMUTABLE_REASONS` 有逐条原因）：
连接串、键前缀、platform 标识、`upstream_allowlist`、`callback_allowlist`、
回调密钥、`async_allow/deny_prefixes`。这些是启动项或安全项——upstream 白名单
可写就等于把"防 sk 打到野地址"的防线挂到网上。

采用**白名单**而非黑名单：新增配置项默认不可热改，要开必须显式登记进 `MUTABLE`。
反过来的话，新增一个敏感项忘了加黑名单就直接暴露了。

---

## API

| Method | Path | 说明 |
|---|---|---|
| POST/PUT | `/async/{path}` | 提交。202 + `{task_id,status}` + `Location` 头 |
| GET | `/async/{path}/{task_id}` | 查询。202 进行中 / 200 原文回放 / 重放上游错误码；`?wait=30` 长轮询 |
| DELETE | `/async/{path}/{task_id}` | 取消。排队中 → CANCELED；执行中 → 409 |
| GET | `/healthz/live` `/healthz/ready` | 探针 |
| GET | `/ops/stats` `/ops/tasks/{id}` | 观测（用户 sk 鉴权，脱敏） |
| POST | `/ops/reconcile/run` `/ops/slots/recalibrate` | 手工触发定时任务 |

管理面（`X-Admin-Key` 鉴权，未配置密钥时全部 404）：

| Method | Path | 说明 |
|---|---|---|
| GET | `/admin` | 看板页面 |
| GET | `/admin/api/overview?window=` | 概览指标 |
| GET | `/admin/api/tasks?status=&model=&task_id=&reconcile_only=&limit=&offset=` | 任务列表 |
| GET | `/admin/api/tasks/{id}` | 任务详情（脱敏） |
| POST | `/admin/api/tasks/{id}/requeue` | 重投队列（不清派发锁） |
| GET/PUT | `/admin/api/config` | 读写运行时配置 |
| POST | `/admin/api/config/reset` | 重置回落 env |
| POST | `/admin/api/jobs/{reconcile\|stale\|slots\|purge}` | 手工触发定时任务 |

提交示例：

```bash
curl -X POST https://api.example.com/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key" \
  -H "X-Callback-Url: https://myapp.com/hook" \
  -d '{"model":"dall-e-3","prompt":"a red cube","n":1}'
# → 202 {"task_id":"dall_e_3_ab12...","status":"SUBMITTED"}
#   Location: /async/v1/images/generations/dall_e_3_ab12...

curl "https://api.example.com/async/v1/images/generations/dall_e_3_ab12...?wait=60"
# → 200 + 与直接调同步接口**字节级一致**的响应体
```

---

## 设计红线

1. **禁止跨服务读库**——余额、身份、消费日志一律走 billing 的 HTTP 接口。
2. **sk 不落库不进日志**——只放 Redis 令牌会话（TTL 48h），终态即清。
   loguru 固定 `backtrace=False, diagnose=False`，异常回溯不带帧变量。
3. **tasks 表时间列必须归一**——读侧 `as_unix_seconds`，SQL 侧 `_secs(col)`。
   表被 new-api 原生模块用 UnixMilli 写过，两侧都不能省。
4. **写侧 WHERE 必带 `platform`**——三方共享表，绝不动别人的行。
5. **超时绝不判死**——上游可能已成功并扣费，标记 `reconcile_pending` 走对账。
6. **派发锁在 = 可能已扣费 = 绝不重发**——队列 at-least-once，防重全靠锁。
7. **安全项不可运行时改**——白名单类配置只读 env，管理面拒绝写入。

---

## 关键决策

| 决策 | 取值 | 依据 |
|---|---|---|
| 5xx 重试 | **默认关**（`ST_RETRY_MAX=0`） | ADR-002：relay 5xx 是否回滚预扣未确认，重试可能双扣 |
| ref_price 来源 | 配置 + 兜底默认值 | ADR-003：提交链路不引入额外 RTT，精度只影响闸门松紧 |
| Redis | 独立实例 + `st:` 键前缀 | ADR-004：故障域隔离，前缀是误配时的第二道防线 |
| 建表 | 零 | 复用 new-api `tasks` 表 |
| 动态配置 | 白名单子集可热改 | ADR-005：安全项开放等于把防线挂到网上 |

完整规格见 [`docs/SPEC.md`](docs/SPEC.md)（12 章节契约 + 34 条 EARS 验收标准），
未决事项见 [`docs/decisions/OPEN-DECISIONS.md`](docs/decisions/OPEN-DECISIONS.md)。

---

## 目录结构

```
app/
├── config.py         配置单例（ST_ 前缀，禁止散读 os.environ）
├── db.py             MySQL 惰性引擎（零建表）
├── redis.py          键规范 + Lua 脚本（全部集中在此）
├── errors.py         {"error":{...}} 统一形制
├── queue.py          taskiq broker/scheduler/发布门面
├── main.py           应用装配
├── standalone.py     单进程模式（web + worker + scheduler 同事件循环）
├── static/           admin.html 单文件看板（零构建）
├── deps/             auth（Bearer + 内省）/ ratelimit（滑窗）/ admin（管理密钥）
├── routers/          proxy（/async 通配）/ ops / admin（看板 + 配置）
└── services/
    ├── admission.py    路径准入 + upstream 三防线 + 头清洗
    ├── submit.py       提交链路（失败即回滚）
    ├── execute.py      worker 执行（派发锁 + 四类分流）
    ├── flow.py         查询 / 字节级回放 / 长轮询 / 取消
    ├── reconcile.py    超时对账三态 / 槽位校准 / 卡死扫描 / 结果清理
    ├── taskstore.py    tasks 表原生 SQL（数据访问单点）
    ├── dynconf.py      运行时配置白名单（Redis > env > 默认）
    ├── idem.py         幂等原子占位
    ├── slots.py        并发槽
    ├── tokensession.py 用户 sk 会话（唯一查询处）
    ├── codec.py        gzip + b64
    ├── notify.py       HMAC 签名回调
    └── providers/      billing 适配（Protocol + 实现分离）
```

## 兜底扫描（四只 sweeper）

| 任务 | 频率 | 作用 |
|---|---|---|
| 超时对账 | 每分钟 | 挂起任务查消费日志定性（三态：补记成功 / 判失败 / 保持挂起）|
| 卡死扫描 | 每 2 分钟 | **入队消息丢失**的任务交给对账，否则永久停在 SUBMITTED 并吃掉一个并发额度 |
| 槽位校准 | 每 5 分钟 | Redis 计数按 tasks 表事实双向修正（虚高会永久限流用户，虚低会让用户超发）|
| 结果清理 | 每小时 | 清空超期 `upstream_response`，状态行保留 |

## 测试

200 个用例，不依赖真实 MySQL / Redis / new-api / billing：
Redis 用手写 FakeRedis（Lua 按 `app/redis.py` 常量做等价 Python 实现），
tasks 表用 InMemoryTaskStore（保留 CAS 与 data 合并语义），
出站 HTTP 用 respx 拦截（未声明的请求立即失败），
`mypy` 与 `ruff` 各做成一个 pytest 用例。
