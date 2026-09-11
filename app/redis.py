"""Redis 客户端、键规范与 Lua 脚本。

原则：Redis 只放"丢了能重建"的状态——限流计数、幂等占位、并发槽、
派发锁。事实源永远是 tasks 表，定时校准负责从事实重建。

唯一的例外是 ``app/services/tokensession.py`` 的用户令牌会话（键
``{p}:sk:{task_id}``）：它必须放 Redis，因为 worker 执行时请求上下文已结束，
而令牌绝不能落库（红线）。丢失的最坏后果是任务无法执行 → 判死 FAILURE。
本服务零资金动作，不会造成钱款损失。

键前缀：全部经 ``_k()`` 拼 ``REDIS_KEY_PREFIX``（默认 ``st``）。即便误连
别家实例也不会撞键（ADR-004 的第二道防线）。
"""

from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings

# 连接池优化：高并发场景下连接复用
r = aioredis.from_url(
    settings.redis_url,
    decode_responses=True,
    max_connections=100,
    socket_keepalive=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


def _k(suffix: str) -> str:
    return f"{settings.redis_key_prefix}:{suffix}"


# ---- 键规范（全部集中在此，业务模块只 import 常量）----
K_IDEM = _k("idem:{task_id}")                 # 显式幂等占位（task_id 即幂等键）
K_RL = _k("rl:{subject}")                     # 滑动窗口限流（subject = token_hash）
K_SLOT = _k("slot:{token_hash}")              # 并发槽 · 第一层：token 总量
K_MSLOT = _k("mslot:{token_hash}:{model}")    # 并发槽 · 第二层：(模型, token)
K_GSLOT = _k("gslot:{model}")                 # 并发槽 · 第三层：模型全局（对齐上游渠道容量）
K_BATCH = _k("batch:{key}")                    # 攒批成员队列（ZSET，score=入批时刻 → FIFO）
K_BATCH_DUE = _k("batch:due")                  # 攒批到期索引（ZSET，member=归组键，score=放行时刻）
K_DUE = _k("due")                             # 单任务重排索引（ZSET，member=task_id，score=重试时刻）
K_SK = _k("sk:{task_id}")                     # 用户令牌会话（终态即清）
K_DISPATCH = _k("dispatch:{task_id}")         # 派发锁（防重复调用上游的核心）
K_SWEEP_LOCK = _k("sweep:{job}")              # 定时任务重入锁
K_AUTH = _k("auth:{token_hash}")              # 鉴权正向缓存（只存 user_id，不存 key）
K_STATUS = _k("status:{task_id}")             # 状态 write-through 缓存（长轮询卸 DB）


# ---- 滑动窗口限流：ARGV = [now_ms, window_ms, limit] ----
LUA_RATE_LIMIT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
local n = redis.call('ZCARD', KEYS[1])
if n >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1] .. ':' .. math.random(1000000000))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"""

# ---- 并发槽占用：ARGV = [limit, ttl_seconds]，不超上限才 +1 ----
# TTL 兜底：进程在「占槽后、落库前」崩溃时槽位不永久泄漏——键整体过期后
# 由定时校准（sweeper.recalibrate_slots）对照 tasks 表活跃任务数回写。
LUA_SLOT_ACQUIRE = """
local n = redis.call('INCR', KEYS[1])
if n > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
"""

LUA_SLOT_RELEASE = """
local n = redis.call('DECR', KEYS[1])
if n < 0 then redis.call('SET', KEYS[1], 0) end
return 1
"""

# ---- 三层并发槽 · 一次原子占用 ----
#
# KEYS = [token 槽, (模型,token) 槽, 模型全局槽]
# ARGV = [token 上限, (模型,token) 上限, 模型全局上限, ttl]
# 上限 **0 = 该层不启用**（不占、不判），让纯 token 层行为与旧版逐字节一致。
#
# 返回位掩码：1=token层 2=(模型,token)层 4=模型全局层；0 = 未占到任何层。
# 掩码是**释放的唯一依据**——业务侧把它落进 data.slot_flags，释放时按位回退。
# 不能用「当前配置」重算该释放哪几层：配置热改后模型上限可能从 0 变成 3，
# 按新配置释放就会 DECR 一个从未 INCR 过的键（还掉别人的槽）。
#
# 三层在同一次 EVAL 内判定，任一层超限就在**同一个原子块内**回滚已占的层，
# 不存在「外层占了、内层失败、外层没人还」的窗口。
LUA_SLOT_ACQUIRE3 = """
local mask = 0
if tonumber(ARGV[1]) > 0 then
  if redis.call('INCR', KEYS[1]) > tonumber(ARGV[1]) then
    redis.call('DECR', KEYS[1])
    return 0
  end
  mask = mask + 1
end
if tonumber(ARGV[2]) > 0 then
  if redis.call('INCR', KEYS[2]) > tonumber(ARGV[2]) then
    redis.call('DECR', KEYS[2])
    if mask >= 1 then redis.call('DECR', KEYS[1]) end
    return 0
  end
  mask = mask + 2
end
if tonumber(ARGV[3]) > 0 then
  if redis.call('INCR', KEYS[3]) > tonumber(ARGV[3]) then
    redis.call('DECR', KEYS[3])
    if mask >= 2 then redis.call('DECR', KEYS[2]) end
    if mask >= 1 then redis.call('DECR', KEYS[1]) end
    return 0
  end
  mask = mask + 4
end
if mask >= 1 then redis.call('EXPIRE', KEYS[1], ARGV[4]) end
if mask >= 2 then redis.call('EXPIRE', KEYS[2], ARGV[4]) end
if mask >= 4 then redis.call('EXPIRE', KEYS[3], ARGV[4]) end
return mask
"""

# ---- 三层并发槽 · 按占位掩码释放 ----
# KEYS / ARGV = [位掩码]。只回退真正占过的层，每层带下溢保护。
LUA_SLOT_RELEASE3 = """
local mask = tonumber(ARGV[1])
if mask >= 1 then
  if redis.call('DECR', KEYS[1]) < 0 then redis.call('SET', KEYS[1], 0) end
end
if mask >= 2 then
  if redis.call('DECR', KEYS[2]) < 0 then redis.call('SET', KEYS[2], 0) end
end
if mask >= 4 then
  if redis.call('DECR', KEYS[3]) < 0 then redis.call('SET', KEYS[3], 0) end
end
return 1
"""

# ---- 攒批 · 原子入批 ----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [task_id, now, due_at, model, ttl]
# 返回 [入批后成员总数, 本批权威到期时刻]（调用方据前者判 N 是否达标）。
#
# 到期时刻用 **ZADD NX** 只由首个成员写定：T 是「自本批开始攒起」的窗口，
# 后续成员若都刷新 deadline，涓涓细流会让这个批次永远等不到放行。
#
# 入批与计数在同一次 EVAL 内完成，多副本并发提交时 ZCARD==N 不可能双触发
# （谁把计数推过阈值，谁就负责触发放行）。
#
# 必须把 **ZSCORE 的真实值**回给调用方，不能让它拿自己算的 due_at 落库：
# NX 命中（本批已有更早的 deadline）时两者不同，而 rebuild_from_db 正是按
# DB 里的 batch_deadline 取 min 重建到期索引 —— 落了偏晚的值，Redis 丢数据
# 后整批的放行时刻就会集体后移，T 语义失真。
LUA_BATCH_JOIN = """
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[2], 'NX', ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return {redis.call('ZCARD', KEYS[1]), redis.call('ZSCORE', KEYS[2], ARGV[4])}
"""

# ---- 攒批 · 原子摘取整批成员（放行的唯一入口）----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [model]
# 返回被摘走的 task_id 列表；空列表 = 本批已被别人（或上一轮）取走。
#
# **摘取本身就是互斥**：DEL 成员键与 ZREM 到期键在同一 EVAL 内完成，
# N 触发与 T 触发并发时只有一个能拿到成员列表——所以不需要额外的放行锁。
# 返回空列表时也顺手清掉到期索引，避免空批次在 ZSET 里留下残渣反复被扫。
LUA_BATCH_CLAIM = """
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return members
"""

# ---- 攒批 · 成员退批（取消时用）----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [task_id, model]
# 被取消的成员必须从计数里摘掉：一批声明 N=100 而其中 5 条被取消，
# 计数就永远差 5 条到不了 N，只能干等 T 兜底，等待时长凭空变长。
#
# 摘完若批次已空，顺手清掉到期索引——否则 ticker 每轮都会捞到这个空批次
# 并触发一次无成员的放行（无害但纯浪费）。
LUA_BATCH_LEAVE = """
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[2])
end
return 1
"""

# ---- CAS 删除（值匹配才删）：幂等占位归还专用，绝不误删他人占位 ----
LUA_CAS_DELETE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""
