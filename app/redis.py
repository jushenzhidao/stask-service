"""Redis 客户端、键规范与 Lua 脚本。

原则：Redis 只放"丢了能重建"的状态——缓存、限流计数、幂等占位、并发槽、
派发锁。事实源永远是 tasks 表，定时校准负责从事实重建。

唯一的例外是 ``app/services/tokensession.py`` 的用户 sk 会话（键
``{p}:sk:{task_id}``）：它必须放 Redis，因为 worker 执行时请求上下文已结束，
而 sk 绝不能落库（红线）。丢失的最坏后果是任务无法执行 → 走对账收敛，
本服务无资金动作，不会造成钱款损失。

键前缀：全部经 ``_k()`` 拼 ``ST_REDIS_KEY_PREFIX``（默认 ``st``）。即便误连
atask 的实例（``gw:*``）也不会撞键（ADR-004 的第二道防线）。
"""

from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings

# 连接池优化：高并发场景下连接复用
r = aioredis.from_url(
    settings.redis_url,
    decode_responses=True,
    max_connections=100,  # 连接池上限（默认仅 50）
    socket_keepalive=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


def _k(suffix: str) -> str:
    return f"{settings.redis_key_prefix}:{suffix}"


# ---- 键规范（全部集中在此，业务模块只 import 常量）----
K_INSPECT = _k("inspect:{token_hash}")        # 身份内省缓存（ST_INSPECT_CACHE_TTL）
K_BALANCE = _k("balance:{token_hash}")        # 余额缓存（ST_BALANCE_CACHE_TTL）
K_IDEM = _k("idem:{token_hash}:{key}")        # 幂等占位/回填（pending → task_id）
K_RL = _k("rl:{subject}")                     # 滑动窗口限流（subject = token_hash 或 ip）
K_SLOT = _k("slot:{token_hash}")              # 并发槽占用计数
K_SK = _k("sk:{task_id}")                     # 用户令牌会话（终态即清）
K_DISPATCH = _k("dispatch:{task_id}")         # 派发锁（防双扣的核心）
K_SWEEP_LOCK = _k("sweep:{job}")              # 定时任务重入锁


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
# 由定时校准（reconcile.recalibrate_slots）对照 tasks 表活跃任务数回写。
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

# ---- CAS 删除（值匹配才删）：幂等占位归还专用，绝不误删已回填的 task_id ----
LUA_CAS_DELETE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""
