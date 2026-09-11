"""统一测试基建：内存版 Redis / 内存 taskstore / respx 出站拦截 / 受控 settings。

原则：单测不依赖真实 MySQL / Redis / 上游。外部边界只有两处——
- HTTP 出站（上游调用与回调推送）：respx 拦截；
- Redis：手写 FakeRedis（``decode_responses=True`` 语义，覆盖用到的命令子集，
  Lua 脚本按 ``app.redis`` 里的常量做等价 Python 实现）。

为什么不用 fakeredis 库：Lua 脚本的行为是本服务并发正确性的核心
（占槽的 INCR-超限-DECR 回滚、幂等的 CAS 删除），必须能在测试里**逐行
断言语义**。第三方库把 Lua 丢给真解释器，一旦行为不符只能猜。
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
import respx

# ---------------------------------------------------------------------------
# FakeRedis
# ---------------------------------------------------------------------------


def _as_score(value: Any) -> float:
    """ZSET 边界值 → float。真 Redis 接受 '-inf' / '+inf' 字面量。"""
    text_value = str(value).strip()
    if text_value in ("-inf", "inf", "+inf"):
        return float(text_value.replace("+", ""))
    return float(text_value.lstrip("("))


class _FakePipeline:
    """FakeRedis 的最小 pipeline（见 ``FakeRedis.pipeline``）。"""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._queued: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def zadd(self, key: str, mapping: dict[str, Any],
             nx: bool = False) -> _FakePipeline:
        self._queued.append(("zadd", (key, mapping), {"nx": nx}))
        return self

    def expire(self, key: str, seconds: int) -> _FakePipeline:
        self._queued.append(("expire", (key, seconds), {}))
        return self

    async def execute(self) -> list[Any]:
        results = []
        for name, args, kwargs in self._queued:
            results.append(await getattr(self._redis, name)(*args, **kwargs))
        self._queued.clear()
        return results

    async def __aenter__(self) -> _FakePipeline:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class FakeRedis:
    """内存版异步 Redis。值一律按 str 存取（对齐 decode_responses=True）。"""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._expires: dict[str, float] = {}

    # ---- 内部 ----

    def _alive(self, key: str) -> bool:
        exp = self._expires.get(key)
        if exp is not None and exp <= time.time():
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return False
        return True

    @staticmethod
    def _s(value: Any) -> str:
        return value if isinstance(value, str) else str(value)

    # ---- 通用 ----

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    async def delete(self, *keys: str) -> int:
        n = 0
        for key in keys:
            if self._alive(key) and key in self._data:
                n += 1
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return n

    async def expire(self, key: str, seconds: int) -> bool:
        if not (self._alive(key) and key in self._data):
            return False
        self._expires[key] = time.time() + seconds
        return True

    async def ttl(self, key: str) -> int:
        """对齐 Redis 语义：不存在 -2；无过期 -1；否则剩余秒。"""
        if not self._alive(key):
            return -2
        exp = self._expires.get(key)
        return -1 if exp is None else max(0, int(exp - time.time()))

    # ---- STRING ----

    async def get(self, key: str) -> str | None:
        if not self._alive(key):
            return None
        value = self._data.get(key)
        return value if isinstance(value, str) else None

    async def set(self, key: str, value: Any, ex: int | None = None,
                  nx: bool = False, **_: Any) -> Any:
        if nx and self._alive(key) and key in self._data:
            return None
        self._data[key] = self._s(value)
        if ex is not None:
            self._expires[key] = time.time() + ex
        else:
            self._expires.pop(key, None)
        return True

    async def incr(self, key: str) -> int:
        cur = int(self._data.get(key, "0")) if self._alive(key) else 0
        cur += 1
        self._data[key] = str(cur)
        return cur

    # ---- HASH（dynconf 覆盖值用）----

    async def hgetall(self, key: str) -> dict[str, str]:
        if not self._alive(key):
            return {}
        value = self._data.get(key)
        return dict(value) if isinstance(value, dict) else {}

    async def hset(self, key: str, mapping: dict[str, Any] | None = None,
                   **_: Any) -> int:
        current = self._data.get(key) if self._alive(key) else None
        if not isinstance(current, dict):
            current = {}
        for field, value in (mapping or {}).items():
            current[str(field)] = self._s(value)
        self._data[key] = current
        return len(mapping or {})

    async def hdel(self, key: str, *fields: str) -> int:
        current = self._data.get(key) if self._alive(key) else None
        if not isinstance(current, dict):
            return 0
        n = 0
        for field in fields:
            if str(field) in current:
                del current[str(field)]
                n += 1
        self._data[key] = current
        return n

    # ---- ZSET（攒批成员表 / 到期索引 / 延迟下发索引）----
    #
    # 存储形态：dict[member] = score。真 Redis 里 ZSET 按 score 排序，
    # 这里排序在读侧做（zrange/zrangebyscore），写侧只维护映射。

    def _z(self, key: str) -> dict[str, float]:
        if not self._alive(key):
            return {}
        cur = self._data.get(key)
        return cur if isinstance(cur, dict) else {}

    async def zadd(self, key: str, mapping: dict[str, Any],
                   nx: bool = False) -> int:
        z = self._z(key)
        added = 0
        for member, score in mapping.items():
            m = self._s(member)
            if nx and m in z:          # NX：已存在则不覆盖 score
                continue
            if m not in z:
                added += 1
            z[m] = float(score)
        self._data[key] = z
        return added

    async def zcard(self, key: str) -> int:
        return len(self._z(key))

    async def zrange(self, key: str, start: int, end: int) -> list[str]:
        ordered = [m for m, _ in sorted(self._z(key).items(), key=lambda kv: kv[1])]
        if end == -1:
            return ordered[start:]
        return ordered[start : end + 1]

    async def zrangebyscore(self, key: str, min_score: Any, max_score: Any,
                            start: int = 0, num: int | None = None) -> list[str]:
        lo, hi = _as_score(min_score), _as_score(max_score)
        hits = [
            m
            for m, s in sorted(self._z(key).items(), key=lambda kv: kv[1])
            if lo <= s <= hi
        ]
        hits = hits[start:]
        return hits[:num] if num is not None else hits

    async def zrem(self, key: str, *members: Any) -> int:
        z = self._z(key)
        n = 0
        for member in members:
            if self._s(member) in z:
                del z[self._s(member)]
                n += 1
        self._data[key] = z
        return n

    async def zscore(self, key: str, member: Any) -> float | None:
        return self._z(key).get(self._s(member))

    # ---- PIPELINE ----

    def pipeline(self) -> _FakePipeline:
        """最小 pipeline：只支持本服务用到的写命令。

        真 redis 的 pipeline 把命令缓冲后一次性发送；这里同步排队、``execute``
        时按序调用等价命令。只实现被实际使用的 ``zadd`` / ``expire``——
        多实现一个方法就等于多一处与真 Redis 语义可能不符的替身。
        """
        return _FakePipeline(self)

    # ---- Lua ----

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        from app.redis import (
            LUA_BATCH_CLAIM,
            LUA_BATCH_JOIN,
            LUA_BATCH_LEAVE,
            LUA_CAS_DELETE,
            LUA_RATE_LIMIT,
            LUA_SLOT_ACQUIRE,
            LUA_SLOT_ACQUIRE3,
            LUA_SLOT_RELEASE,
            LUA_SLOT_RELEASE3,
        )

        key = str(args[0])
        keys = [str(a) for a in args[:numkeys]]
        argv = [str(a) for a in args[numkeys:]]

        # 攒批入批：ZADD 成员 + ZADD NX 到期 + EXPIRE + 返回 [ZCARD, ZSCORE]。
        # 第二项是本批**权威**到期时刻：NX 命中时它就是首个成员写的那个值，
        # 调用方必须落库这个值而不是自己算的（真 Redis 回的是字符串，
        # 这里也照样返回 str，否则测试会掩盖调用方的类型处理漏洞）。
        if script == LUA_BATCH_JOIN:
            await self.zadd(keys[0], {argv[0]: float(argv[1])})
            await self.zadd(keys[1], {argv[3]: float(argv[2])}, nx=True)
            await self.expire(keys[0], int(argv[4]))
            score = await self.zscore(keys[1], argv[3])
            return [
                await self.zcard(keys[0]),
                None if score is None else str(score),
            ]

        # 攒批摘取：取全部成员后原子删批 + 清到期索引。
        # 第二次调用必然返回空列表——这正是 N/T 双触发下的互斥保证。
        if script == LUA_BATCH_CLAIM:
            members = await self.zrange(keys[0], 0, -1)
            await self.delete(keys[0])
            await self.zrem(keys[1], argv[0])
            return members

        # 攒批退批（取消用）：摘掉成员；批空则连到期索引一起清。
        if script == LUA_BATCH_LEAVE:
            await self.zrem(keys[0], argv[0])
            if await self.zcard(keys[0]) == 0:
                await self.delete(keys[0])
                await self.zrem(keys[1], argv[1])
            return 1

        if script == LUA_CAS_DELETE:
            if self._alive(key) and self._data.get(key) == argv[0]:
                self._data.pop(key, None)
                self._expires.pop(key, None)
                return 1
            return 0

        if script == LUA_RATE_LIMIT:
            now_ms, window_ms, limit = int(argv[0]), int(argv[1]), int(argv[2])
            entries: list[int] = self._data.get(key) if self._alive(key) else []
            entries = [t for t in (entries or []) if t > now_ms - window_ms]
            if len(entries) >= limit:
                self._data[key] = entries
                return 0
            entries.append(now_ms)
            self._data[key] = entries
            self._expires[key] = time.time() + window_ms / 1000
            return 1

        if script == LUA_SLOT_ACQUIRE:
            limit = int(argv[0])
            cur = int(self._data.get(key, "0")) if self._alive(key) else 0
            if cur + 1 > limit:
                return 0                       # INCR 后立即 DECR 回滚，净效果为不变
            self._data[key] = str(cur + 1)
            if len(argv) > 1:
                self._expires[key] = time.time() + int(argv[1])
            return 1

        if script == LUA_SLOT_RELEASE:
            cur = int(self._data.get(key, "0")) if self._alive(key) else 0
            self._data[key] = str(max(0, cur - 1))
            return 1

        # 三层占槽：逐层 INCR，任一层超限就回滚**本次已占的层**并返回 0。
        # 返回位掩码而非布尔——释放必须按掩码回退，见 services/slots.py。
        # 上限 0 = 该层不启用（既不占也不判），与真脚本一致。
        if script == LUA_SLOT_ACQUIRE3:
            limits = [int(argv[0]), int(argv[1]), int(argv[2])]
            ttl = int(argv[3])
            mask = 0
            for idx, limit in enumerate(limits):
                if limit <= 0:
                    continue
                slot_key = keys[idx]
                cur = int(self._data.get(slot_key, "0")) if self._alive(slot_key) else 0
                if cur + 1 > limit:
                    for done in range(idx):          # 同块内回滚，无跨层失败窗口
                        if mask & (1 << done):
                            prev = keys[done]
                            self._data[prev] = str(max(0, int(self._data[prev]) - 1))
                    return 0
                self._data[slot_key] = str(cur + 1)
                mask |= 1 << idx
            for idx in range(3):
                if mask & (1 << idx):
                    self._expires[keys[idx]] = time.time() + ttl
            return mask

        # 三层释放：只回退掩码里标记过的层（下溢拉回 0）。
        if script == LUA_SLOT_RELEASE3:
            mask = int(argv[0])
            for idx in range(3):
                if not mask & (1 << idx):
                    continue
                slot_key = keys[idx]
                cur = int(self._data.get(slot_key, "0")) if self._alive(slot_key) else 0
                self._data[slot_key] = str(max(0, cur - 1))
            return 1

        raise AssertionError(f"unexpected Lua script: {script[:60]}")

    # ---- 测试辅助 ----

    def dump(self) -> dict[str, Any]:
        return {k: v for k, v in self._data.items() if self._alive(k)}


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


#: 所有 ``from app.redis import r`` 的消费方（新增模块时必须同步这张表，
#: 否则该模块会连真 Redis，测试在无 Redis 环境下静默挂起）
_REDIS_CONSUMERS = (
    "app.deps.ratelimit",
    "app.services.idem",
    "app.services.slots",
    "app.services.tokensession",
    "app.services.execute",
    "app.services.sweeper",
    "app.services.dynconf",
    "app.services.upstream",
    "app.services.batching",
    "app.services.dispatch",
    "app.healthz",
)


@pytest.fixture
def patch_redis(monkeypatch: pytest.MonkeyPatch, fake_redis: FakeRedis) -> FakeRedis:
    import importlib

    for name in _REDIS_CONSUMERS:
        module = importlib.import_module(name)
        monkeypatch.setattr(module, "r", fake_redis, raising=False)
    return fake_redis


@pytest.fixture(autouse=True)
def _reset_dynconf_cache():
    """dynconf 有 5s 进程内缓存——不清会让上个用例写的覆盖值串到下个用例。"""
    from app.services import dynconf

    dynconf._cache = {}
    dynconf._cache_at = 0.0
    yield
    dynconf._cache = {}
    dynconf._cache_at = 0.0


# ---------------------------------------------------------------------------
# InMemoryTaskStore
# ---------------------------------------------------------------------------


class InMemoryTaskStore:
    """内存版 tasks 表：保留 CAS 语义与 data 合并语义（真实 SQL 的等价实现）。"""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.now_value: int | None = None
        #: 记录走过哪些读取入口（"get" = SELECT * / "get_meta" = 逐字段投影）。
        #: 热路径误用 get 会拉整行（request_body ≤2MB、upstream_response ≤10MB），
        #: 这是不变式 6 的违反——用断言把它钉住，而不是靠人记得。
        self.reads: list[str] = []

    def _now(self) -> int:
        return self.now_value if self.now_value is not None else int(time.time())

    # ---- 与 taskstore 同名的接口 ----

    async def create(self, task_id: str, action: str, data: dict,
                     user_id: int = 0) -> None:
        from app.config import settings

        ts = self._now()
        self.rows[task_id] = {
            "task_id": task_id, "platform": "stask", "action": action,
            "status": "QUEUED", "fail_reason": "", "progress": "0%",
            "submit_time": ts, "start_time": 0, "finish_time": 0,
            "created_at": ts, "updated_at": ts,
            "data": json.loads(json.dumps(data)), "user_id": user_id,
            "channel_id": settings.channel_id, "quota": 0,
            # new-api 原生列，真实链路里允许 NULL —— 用 {} 表达同一语义
            # （只有 SUCCESS 落终态时才会被 JSON_MERGE_PATCH 写入）
            "private_data": {},
        }

    async def exists(self, task_id: str) -> bool:
        return task_id in self.rows

    async def cas(self, task_id: str, from_statuses: tuple[str, ...], to_status: str,
                  patch: dict | None = None, fail_reason: str = "",
                  private_patch: dict | None = None) -> bool:
        from app.schemas import TERMINAL

        row = self.rows.get(task_id)
        if row is None or row["status"] not in from_statuses:
            return False
        ts = self._now()
        row["status"] = to_status
        row["updated_at"] = ts
        row["fail_reason"] = (fail_reason or "")[:500]
        if to_status == "IN_PROGRESS":
            row["start_time"] = ts
        if to_status in TERMINAL:
            row["finish_time"] = ts
            row["progress"] = "100%"
        row["data"] = {**row["data"], **(patch or {})}
        if private_patch:
            # 真实 SQL 是 JSON_MERGE_PATCH(COALESCE(private_data, '{}'), patch)
            row["private_data"] = {**(row.get("private_data") or {}), **private_patch}
        return True

    async def patch_data(self, task_id: str, patch: dict) -> None:
        row = self.rows.get(task_id)
        if row is None:
            return
        row["updated_at"] = self._now()
        row["data"] = {**row["data"], **patch}

    async def get(self, task_id: str) -> dict | None:
        self.reads.append("get")
        row = self.rows.get(task_id)
        return json.loads(json.dumps(row)) if row else None

    def _meta(self, row: dict) -> dict:
        """元数据投影替身：复用真实的 ``_meta_row_to_dict`` 归一逻辑。

        关键是先把 ``data`` 的值**按 MySQL ``->>`` 的语义字符串化**再交给
        归一函数——直接把原生 bool/int 塞进去会掩盖真实链路上
        ``'false'`` 是真值字符串这类问题，替身就失去了防护意义。

        ``get_meta`` 与 ``stale_active`` / ``overdue_active`` 共用此投影：
        真实实现里三者的 SELECT 列表也是同一份 ``_META_SELECT``。
        """
        from app.services import taskstore as real

        def as_json_text(value: Any) -> str | None:
            if isinstance(value, bool):
                return "true" if value else "false"
            if isinstance(value, str):
                return value
            return json.dumps(value, ensure_ascii=False)

        raw: dict[str, Any] = {col: row.get(col) for col in real._META_COLUMNS}
        for key in real._META_DATA_KEYS:
            raw[key] = (
                as_json_text(row["data"][key]) if key in row["data"] else None
            )
        # private_data 的白名单投影：真实 SQL 是
        # ``private_data ->> '$.k' AS private_k``，同样恒回字符串
        private = row.get("private_data") or {}
        for key in real._PRIVATE_STR_KEYS:
            raw[f"private_{key}"] = (
                as_json_text(private[key]) if key in private else None
            )
        return real._meta_row_to_dict(raw)

    async def get_meta(self, task_id: str) -> dict | None:
        self.reads.append("get_meta")
        row = self.rows.get(task_id)
        return self._meta(row) if row is not None else None

    async def get_status(self, task_id: str) -> str | None:
        row = self.rows.get(task_id)
        return row["status"] if row else None

    async def counts_by_status(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for row in self.rows.values():
            out[row["status"]] = out.get(row["status"], 0) + 1
        return out

    async def active_counts_by_token(self) -> dict[str, int]:
        """只数真正占了**第一层**的任务（与真实 SQL 的掩码谓词同口径）。

        等待期任务（批次成员/计划任务）掩码为 0，必须排除——否则一次校准
        就把闸门拉到远超真实占用。
        """
        from app.schemas import ACTIVE

        out: dict[str, int] = {}
        for row in self.rows.values():
            if row["status"] not in ACTIVE:
                continue
            if int(row["data"].get("slot_flags") or 0) % 2 != 1:
                continue
            th = str(row["data"].get("token_hash") or "")
            if th:
                out[th] = out.get(th, 0) + 1
        return out

    async def active_counts_by_model_token(self) -> dict[tuple[str, str], int]:
        """只数真正占了**第二层** (模型, token) 的任务。"""
        from app.schemas import ACTIVE

        out: dict[tuple[str, str], int] = {}
        for row in self.rows.values():
            if row["status"] not in ACTIVE:
                continue
            mask = int(row["data"].get("slot_flags") or 0)
            if (mask // 2) % 2 != 1:
                continue
            th = str(row["data"].get("token_hash") or "")
            model = str(row["data"].get("slot_model") or "")
            if th and model:
                out[(th, model)] = out.get((th, model), 0) + 1
        return out

    async def active_counts_by_model(self) -> dict[str, int]:
        """只数真正占了**第三层**（模型全局）的任务。"""
        from app.schemas import ACTIVE

        out: dict[str, int] = {}
        for row in self.rows.values():
            if row["status"] not in ACTIVE:
                continue
            if int(row["data"].get("slot_flags") or 0) < 4:
                continue
            model = str(row["data"].get("slot_model") or "")
            if model:
                out[model] = out.get(model, 0) + 1
        return out

    async def stale_active(self, stale_seconds: int, limit: int = 200) -> list[dict]:
        from app.schemas import ACTIVE

        cutoff = self._now() - stale_seconds
        return [
            self._meta(row) for row in self.rows.values()
            if row["status"] in ACTIVE and row["updated_at"] < cutoff
            # 与真实 SQL 同谓词：未到 scheduled_at 的计划任务不算卡死
            and int(row["data"].get("scheduled_at") or 0) <= cutoff
        ][:limit]

    async def overdue_active(self, lifetime_seconds: int, limit: int = 200) -> list[dict]:
        from app.schemas import ACTIVE

        cutoff = self._now() - lifetime_seconds
        return [
            self._meta(row) for row in self.rows.values()
            if row["status"] in ACTIVE and row["created_at"] < cutoff
            # 生命期起点 = max(created_at, scheduled_at)，等价于两个谓词合取
            and int(row["data"].get("scheduled_at") or 0) < cutoff
        ][:limit]

    async def claim_for_release(self, task_id: str) -> bool:
        """条件更新的等价实现：QUEUED 且处于可放行的等待态才抢到放行权。

        必须**如实复刻条件**（而不是无脑返 True）：N 触发与 T 触发同时命中
        同一条任务时，这里是唯一挡住重复下发的地方。替身放宽了条件，
        「双触发只放行一次」这条断言就永远测不出问题。
        """
        from app.schemas import QUEUED

        row = self.rows.get(task_id)
        if row is None or row["status"] != QUEUED:
            return False
        if str(row["data"].get("batch_state", "waiting")) not in ("waiting", "scheduled"):
            return False
        row["data"]["batch_state"] = "releasing"
        row["updated_at"] = self._now()
        return True

    async def unclaim_for_release(self, task_id: str, *,
                                  restore: str = "waiting") -> None:
        # 与真实实现同口径：退回调用方传来的抢占前状态（不能靠 scheduled_at 反推）
        await self.patch_data(task_id, {"batch_state": restore})

    async def batch_waiting(self, limit: int = 500) -> list[dict]:
        """真实 SQL 的投影列：task_id / model / batch_key / due_at / size。

        ``batch_due_at`` 与 ``batch_size`` 在真实链路里经 ``+ 0`` 转成数值，
        这里显式 int 化——替身回字符串的话，重建时的 ``min()`` 会做字符串
        比较（'900' < '1000' 为假），刚好掩盖不变式 13 要防的那类 bug。
        """
        from app.schemas import QUEUED

        out: list[dict] = []
        for row in sorted(self.rows.values(), key=lambda r: r["submit_time"]):
            data = row["data"]
            if row["status"] != QUEUED or data.get("batch_state") != "waiting":
                continue
            out.append({
                "task_id": row["task_id"],
                "model": str(data.get("model") or ""),
                "batch_key": str(data.get("batch_key") or ""),
                "batch_due_at": int(data.get("batch_due_at") or 0),
                "batch_size": int(data.get("batch_size") or 0),
            })
            if len(out) >= limit:
                break
        return out

    async def pending_scheduled(self, now: int, limit: int = 500) -> list[dict]:
        """尚未到点的计划任务（st:due 索引回补的事实源）。

        如实复刻 SQL 的三个条件（QUEUED / batch_state=scheduled /
        scheduled_at > now），否则「回补」用例会测不出漏补与错补。
        """
        from app.schemas import QUEUED

        out: list[dict] = []
        for row in sorted(self.rows.values(), key=lambda r: r["submit_time"]):
            data = row["data"]
            if row["status"] != QUEUED or data.get("batch_state") != "scheduled":
                continue
            scheduled_at = int(data.get("scheduled_at") or 0)
            if scheduled_at <= now:
                continue
            out.append({"task_id": row["task_id"], "scheduled_at": scheduled_at})
            if len(out) >= limit:
                break
        return out

    async def scheduled_overview(self, now: int, limit: int = 500) -> list[dict]:
        """计划中任务按小时分桶（与真实 SQL 同口径：只数未到点的 scheduled）。"""
        from app.schemas import QUEUED

        buckets: dict[int, int] = {}
        for row in self.rows.values():
            data = row["data"]
            if row["status"] != QUEUED or data.get("batch_state") != "scheduled":
                continue
            scheduled_at = int(data.get("scheduled_at") or 0)
            if scheduled_at <= now:
                continue
            bucket = (scheduled_at // 3600) * 3600
            buckets[bucket] = buckets.get(bucket, 0) + 1
        return [{"bucket": b, "count": buckets[b]}
                for b in sorted(buckets)][:limit]

    async def batch_counts_by_model(self) -> dict[str, int]:
        from app.schemas import QUEUED

        out: dict[str, int] = {}
        for row in self.rows.values():
            data = row["data"]
            if row["status"] != QUEUED or data.get("batch_state") != "waiting":
                continue
            model = str(data.get("model") or "")
            out[model] = out.get(model, 0) + 1
        return out

    async def search(self, *, status: str = "", model: str = "", task_id: str = "",
                     task_id_prefix: str = "",
                     since_seconds: int = 0, limit: int = 50, offset: int = 0) -> dict:
        # 校验必须与 taskstore._validate_search_params 完全一致：替身不校验，
        # 就测不出「非法参数是否返回 400」。
        from app.services.taskstore import _validate_search_params

        _validate_search_params(
            task_id=task_id, task_id_prefix=task_id_prefix,
            since_seconds=since_seconds, limit=limit, offset=offset,
        )
        rows = list(reversed(list(self.rows.values())))
        cutoff = self._now() - since_seconds if since_seconds > 0 else 0

        def keep(row: dict) -> bool:
            data = row["data"]
            if status and row["status"] != status:
                return False
            if model and data.get("model") != model:
                return False
            if task_id and row["task_id"] != task_id:
                return False
            if task_id_prefix and not row["task_id"].startswith(task_id_prefix):
                return False
            if cutoff and row["created_at"] <= cutoff:
                return False
            return True

        hits = [r for r in rows if keep(r)]
        page = hits[offset:offset + limit]
        items = []
        for row in page:
            data = row["data"]
            finish, start = row["finish_time"], row["start_time"]
            items.append({
                "task_id": row["task_id"], "status": row["status"],
                "fail_reason": row["fail_reason"], "channel_id": row["channel_id"],
                "created_at": row["created_at"], "start_time": start,
                "finish_time": finish,
                "duration": (finish - start) if (finish and start and finish >= start) else 0,
                "model": data.get("model", ""),
                "request_path": data.get("request_path", ""),
                "upstream_status": data.get("upstream_status", 0),
                "response_bytes": data.get("response_bytes", 0),
                "result_purged": bool(data.get("result_purged")),
                "artifact_count": int(data.get("artifact_count") or 0),
                "result_url": str(data.get("result_url") or ""),
            })
        return {"total": len(hits), "items": items, "limit": limit, "offset": offset}

    async def metrics(self, window_seconds: int = 3600) -> dict:
        from app.schemas import ACTIVE

        cutoff = self._now() - window_seconds
        rows = [r for r in self.rows.values() if r["created_at"] > cutoff]
        counts: dict[str, int] = {}
        for row in rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        done = counts.get("SUCCESS", 0) + counts.get("FAILURE", 0)
        durations = sorted(
            r["finish_time"] - r["start_time"] for r in rows
            if r["status"] == "SUCCESS" and r["start_time"]
            and r["finish_time"] >= r["start_time"]
        )

        def pct(q: float) -> int:
            return durations[min(len(durations) - 1, int(len(durations) * q))] \
                if durations else 0

        fails: dict[str, int] = {}
        models: dict[str, int] = {}
        for row in rows:
            if row["status"] == "FAILURE":
                reason = row["fail_reason"] or "unknown"
                fails[reason] = fails.get(reason, 0) + 1
            key = row["data"].get("model") or "unknown"
            models[key] = models.get(key, 0) + 1

        active = [r for r in self.rows.values() if r["status"] in ACTIVE]
        return {
            "window_seconds": window_seconds,
            "status_counts": counts,
            "total": sum(counts.values()),
            "success_rate": round(counts.get("SUCCESS", 0) / done, 4) if done else None,
            "active_total": len(active),
            "duration_seconds": {
                "count": len(durations), "p50": pct(0.50), "p95": pct(0.95),
                "p99": pct(0.99), "max": durations[-1] if durations else 0,
            },
            "top_failures": [{"reason": k, "count": v} for k, v in
                             sorted(fails.items(), key=lambda x: -x[1])[:10]],
            "top_models": [{"model": k, "count": v} for k, v in
                           sorted(models.items(), key=lambda x: -x[1])[:10]],
        }

    async def purge_expired_results(self, ttl_seconds: int, limit: int = 200) -> int:
        cutoff = self._now() - ttl_seconds
        n = 0
        for row in self.rows.values():
            data = row["data"]
            if data.get("result_purged"):
                continue
            if not data.get("upstream_response"):
                continue
            if not (1 <= row["finish_time"] <= cutoff):
                continue
            data["upstream_response"] = ""
            data["upstream_response_encoding"] = ""
            data["result_purged"] = True
            n += 1
            if n >= limit:
                break
        return n

    def now(self) -> int:
        return self._now()


#: 需要被替换 taskstore 引用的模块
_TASKSTORE_CONSUMERS = (
    "app.services.submit",
    "app.services.execute",
    "app.services.flow",
    "app.services.sweeper",
    "app.services.notify",
    "app.services.batching",
    "app.services.dispatch",
    "app.routers.ops",
)

#: taskstore 上被业务调用的函数名（逐名替换，漏一个就会打真 DB）
_TASKSTORE_FUNCS = (
    "create", "exists", "cas", "patch_data", "get", "get_meta", "get_status",
    "counts_by_status", "active_counts_by_token",
    "active_counts_by_model_token", "active_counts_by_model",
    "stale_active", "overdue_active", "purge_expired_results", "now",
    "search", "metrics",
    "claim_for_release", "unclaim_for_release",
    "batch_waiting", "pending_scheduled", "batch_counts_by_model",
    "scheduled_overview",
)


@pytest.fixture
def task_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryTaskStore:
    import importlib

    store = InMemoryTaskStore()
    real = importlib.import_module("app.services.taskstore")
    for name in _TASKSTORE_FUNCS:
        monkeypatch.setattr(real, name, getattr(store, name))
    for mod_name in _TASKSTORE_CONSUMERS:
        module = importlib.import_module(mod_name)
        if hasattr(module, "taskstore"):
            monkeypatch.setattr(module, "taskstore", store)
    return store


# ---------------------------------------------------------------------------
# 队列发布拦截
# ---------------------------------------------------------------------------


class QueueEvents:
    def __init__(self) -> None:
        self.execute: list[str] = []
        self.notify: list[tuple[str, int, int]] = []
        #: N 触发的整批放行投递（model, source）
        self.release_batch: list[tuple[str, str]] = []


@pytest.fixture
def queue_events(monkeypatch: pytest.MonkeyPatch) -> QueueEvents:
    """拦截发布门面。

    注意 ``app.queue`` 的 import 会连 Redis 建 broker 对象（不发请求，
    只构造），所以这里可以安全 import。
    """
    import app.queue as queue_mod

    events = QueueEvents()

    async def _execute(task_id: str) -> None:
        events.execute.append(task_id)

    async def _notify(task_id: str, attempt: int = 1, delay_seconds: int = 0) -> None:
        events.notify.append((task_id, attempt, delay_seconds))

    async def _release_batch(model: str, source: str = "batch") -> None:
        events.release_batch.append((model, source))

    monkeypatch.setattr(queue_mod, "publish_execute", _execute)
    monkeypatch.setattr(queue_mod, "publish_notify", _notify)
    monkeypatch.setattr(queue_mod, "publish_release_batch", _release_batch)
    return events


# ---------------------------------------------------------------------------
# settings / respx / app client
# ---------------------------------------------------------------------------


@pytest.fixture
def test_settings(monkeypatch: pytest.MonkeyPatch):
    """收紧到可预测的取值（各用例可再覆盖单个字段）。"""
    from app.config import settings

    monkeypatch.setattr(settings, "async_allow_prefixes", ("/v1/images", "/v1/audio"))
    monkeypatch.setattr(settings, "async_deny_prefixes", ("/api/", "/console/"))
    monkeypatch.setattr(settings, "upstream_allowlist", ("newapi:3000", "127.0.0.1:3000"))
    monkeypatch.setattr(settings, "upstream_base_url", "http://newapi:3000")
    monkeypatch.setattr(settings, "max_slots", 10)
    monkeypatch.setattr(settings, "rate_limit", 1000)
    monkeypatch.setattr(settings, "retry_max", 0)
    monkeypatch.setattr(settings, "retry_max_connect", 0)
    monkeypatch.setattr(settings, "worker_timeout", 5)
    monkeypatch.setattr(settings, "poll_interval_seconds", 0.01)
    monkeypatch.setattr(settings, "callback_secret", "test-secret")
    monkeypatch.setattr(settings, "callback_allowlist", ())
    monkeypatch.setattr(settings, "channel_id", 990)
    # 下面两项必须钉死：它们会读到**开发者本机的 .env**，不钉住的话
    # 「管理面未启用应 404」「非生产环境只告警」这类断言会随本地配置漂移。
    monkeypatch.setattr(settings, "admin_key", "")     # 需要管理面的用例自己覆盖
    monkeypatch.setattr(settings, "app_env", "test")   # 避免本机 APP_ENV=prod 触发 fail-fast
    return settings


@pytest.fixture
def respx_router() -> Iterator[respx.MockRouter]:
    """任何未声明的出站请求立即失败（assert_all_mocked=True）。"""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


@pytest.fixture
def upstream_auth(monkeypatch: pytest.MonkeyPatch):
    """鉴权替身：默认放行（user_id=42）。

    用例可通过 ``upstream_auth.fail = UpstreamAuthError(...)`` 改为拒绝，
    或断言 ``upstream_auth.calls`` 验证调用参数。
    """
    from app.services import upstream

    upstream.clear_cache()

    class _Stub:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self.fail: Exception | None = None
            self.info = upstream.AuthInfo(user_id=42)

        async def __call__(self, raw_token: str,
                           token_hash: str) -> upstream.AuthInfo:
            self.calls.append((raw_token, token_hash))
            if self.fail is not None:
                raise self.fail
            return self.info

    stub = _Stub()
    monkeypatch.setattr(upstream, "authenticate", stub)
    return stub


@pytest.fixture
def client(patch_redis, task_store, queue_events, test_settings, upstream_auth):
    """带全套替身的 ASGI 测试客户端。"""
    from fastapi.testclient import TestClient

    from app.main import create_app

    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


AUTH = {"Authorization": "Bearer sk-test-token"}

# ---------------------------------------------------------------------------
# 体的落库形态助手
# ---------------------------------------------------------------------------

#: 用例里构造落库体时的默认明文阈值（与 ``Settings.plain_max_bytes`` 一致）
PLAIN_MAX = 32 * 1024


def stored_body(key: str, raw: bytes, plain_max_bytes: int = PLAIN_MAX) -> dict[str, str]:
    """构造 ``data`` 里的「体 + 编码标记」键对。

    体与它的 ``*_encoding`` 兄弟字段必须同写——只写体不写标记，读侧会按
    ``plain`` 解释一串 base64，是最容易在用例里犯的错。把配对关系收在
    这一个函数里，用例就不可能只写一半。
    """
    from app.services import codec

    encoded, encoding = codec.encode(raw, plain_max_bytes)
    return {key: encoded, f"{key}_encoding": encoding}


def read_body(data: dict, key: str) -> bytes:
    """``stored_body`` 的逆操作：按落库的编码标记取回原始字节。"""
    from app.services import codec

    return codec.decode(str(data.get(key) or ""), str(data.get(f"{key}_encoding") or ""))
