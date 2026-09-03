"""统一测试基建：内存版 Redis / 内存 taskstore / respx 出站拦截 / 受控 settings。

原则：单测不依赖真实 MySQL / Redis / new-api / billing。外部边界只有两处——
- HTTP 出站（billing provider 与 relay 调用）：respx 拦截；
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

    # ---- Lua ----

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        from app.redis import (
            LUA_CAS_DELETE,
            LUA_RATE_LIMIT,
            LUA_SLOT_ACQUIRE,
            LUA_SLOT_RELEASE,
        )

        key = str(args[0])
        argv = [str(a) for a in args[numkeys:]]

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
    "app.services.identity",
    "app.services.execute",
    "app.services.reconcile",
    "app.services.dynconf",
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
    """dynconf 有 5s 进程内缓存——不清会让上个用例写的覆盖值串到下个用例。

    autouse：这类全局状态泄漏一旦发生，症状是「单跑通过、全量跑失败」，
    极难定位。宁可每个用例都付一次清理成本。
    """
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

    def _now(self) -> int:
        return self.now_value if self.now_value is not None else int(time.time())

    # ---- 与 taskstore 同名的接口 ----

    async def create(self, task_id: str, user_id: int, action: str, data: dict) -> None:
        ts = self._now()
        self.rows[task_id] = {
            "task_id": task_id, "platform": "stask", "action": action,
            "status": "SUBMITTED", "fail_reason": "", "progress": "STASK_RUNNING",
            "submit_time": ts, "start_time": 0, "finish_time": 0,
            "created_at": ts, "updated_at": ts,
            "data": json.loads(json.dumps(data)), "user_id": user_id,
            "channel_id": 0, "quota": 0,
        }

    async def cas(self, task_id: str, from_statuses: tuple[str, ...], to_status: str,
                  patch: dict | None = None, fail_reason: str = "",
                  channel_id: int | None = None) -> bool:
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
        if channel_id:
            row["channel_id"] = channel_id
        row["data"] = {**row["data"], **(patch or {})}
        return True

    async def patch_data(self, task_id: str, patch: dict) -> None:
        row = self.rows.get(task_id)
        if row is None:
            return
        row["updated_at"] = self._now()
        row["data"] = {**row["data"], **patch}

    async def get(self, task_id: str) -> dict | None:
        row = self.rows.get(task_id)
        return json.loads(json.dumps(row)) if row else None

    async def get_meta(self, task_id: str) -> dict | None:
        """元数据投影替身：复用真实的 ``_meta_row_to_dict`` 归一逻辑。

        关键是先把 ``data`` 的值**按 MySQL ``->>`` 的语义字符串化**再交给
        归一函数——直接把原生 bool/int 塞进去会掩盖真实链路上
        ``'false'`` 是真值字符串这类问题，替身就失去了防护意义。
        """
        from app.services import taskstore as real

        row = self.rows.get(task_id)
        if row is None:
            return None

        def as_json_text(value: Any) -> str | None:
            if value is None and False:  # pragma: no cover - 占位不可达
                return None
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
        return real._meta_row_to_dict(raw)

    async def get_status(self, task_id: str) -> str | None:
        row = self.rows.get(task_id)
        return row["status"] if row else None

    async def counts_by_status(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for row in self.rows.values():
            out[row["status"]] = out.get(row["status"], 0) + 1
        return out

    async def active_counts_by_token(self) -> dict[str, int]:
        from app.schemas import ACTIVE

        out: dict[str, int] = {}
        for row in self.rows.values():
            if row["status"] not in ACTIVE:
                continue
            th = str(row["data"].get("token_hash") or "")
            if th:
                out[th] = out.get(th, 0) + 1
        return out

    async def reconcile_pending(self, limit: int = 50) -> list[dict]:
        from app.schemas import ACTIVE

        out = []
        recheck_before = self._now() - 60
        for row in self.rows.values():
            data = row["data"]
            if row["status"] in ACTIVE and data.get("reconcile_pending") \
                    and int(data.get("reconcile_checked_at") or 0) < recheck_before:
                out.append(json.loads(json.dumps(row)))
            if len(out) >= limit:
                break
        return out

    async def stale_active(self, stale_seconds: int, limit: int = 200) -> list[str]:
        from app.schemas import ACTIVE

        cutoff = self._now() - stale_seconds
        return [
            tid for tid, row in self.rows.items()
            if row["status"] in ACTIVE and row["updated_at"] < cutoff
        ][:limit]

    async def search(self, *, status: str = "", model: str = "", task_id: str = "",
                     task_id_prefix: str = "", reconcile_only: bool = False,
                     since_seconds: int = 0, limit: int = 50, offset: int = 0) -> dict:
        # 校验必须与 taskstore._validate_search_params 完全一致：替身不校验，
        # 就测不出「非法参数是否返回 400」——测试绿灯而线上是另一套行为。
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
            if reconcile_only and not data.get("reconcile_pending"):
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
                "reconcile_pending": bool(data.get("reconcile_pending")),
                "reconcile_reason": data.get("reconcile_reason", ""),
                "result_purged": bool(data.get("result_purged")),
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
            "pending_reconcile": sum(
                1 for r in active if r["data"].get("reconcile_pending")),
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
    "app.services.reconcile",
    "app.services.notify",
    "app.routers.ops",
)

#: taskstore 上被业务调用的函数名（逐名替换，漏一个就会打真 DB）
_TASKSTORE_FUNCS = (
    "create", "cas", "patch_data", "get", "get_meta", "get_status",
    "counts_by_status", "active_counts_by_token", "reconcile_pending",
    "stale_active", "purge_expired_results", "now",
    "search", "metrics",
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
# billing provider
# ---------------------------------------------------------------------------


class FakeBilling:
    """可编程的 billing 替身。

    ``charges`` 按 task_id 存放"上游确实扣了费"的记录；
    ``fail_find`` 置 True 时 ``find_charge`` 抛 BillingError（模拟查询失败，
    对账必须保持挂起而不是判死——这是第二态与第三态的区分点）。
    """

    def __init__(self) -> None:
        self.valid = True
        self.user_id = 42
        self.balance_value: float | None = 10.0
        self.balance_error = False
        self.charges: dict[str, dict] = {}
        self.fail_find = False
        self.inspect_calls = 0
        self.balance_calls = 0

    async def inspect(self, raw_token: str):
        from app.schemas import UserIdentity

        self.inspect_calls += 1
        if not self.valid:
            return None
        return UserIdentity(user_id=self.user_id, token_id=7)

    async def balance(self, raw_token: str) -> float:
        from app.services.providers import BillingError

        self.balance_calls += 1
        if self.balance_error:
            raise BillingError(503, "billing down")
        return float(self.balance_value or 0.0)

    async def find_charge(self, raw_token: str, *, task_id: str,
                          since: int, until: int) -> dict | None:
        from app.services.providers import BillingError

        if self.fail_find:
            raise BillingError(500, "log query failed")
        return self.charges.get(task_id)


@pytest.fixture
def fake_billing(monkeypatch: pytest.MonkeyPatch) -> FakeBilling:
    import importlib

    provider = FakeBilling()
    for name in ("app.services.providers", "app.services.identity",
                 "app.services.reconcile"):
        module = importlib.import_module(name)
        monkeypatch.setattr(module, "billing", provider, raising=False)
    return provider


# ---------------------------------------------------------------------------
# 队列发布拦截
# ---------------------------------------------------------------------------


class QueueEvents:
    def __init__(self) -> None:
        self.execute: list[str] = []
        self.notify: list[tuple[str, int, int]] = []


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

    monkeypatch.setattr(queue_mod, "publish_execute", _execute)
    monkeypatch.setattr(queue_mod, "publish_notify", _notify)
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
    monkeypatch.setattr(settings, "newapi_base_url", "http://newapi:3000")
    monkeypatch.setattr(settings, "max_slots", 10)
    monkeypatch.setattr(settings, "ref_price_default", 1.0)
    monkeypatch.setattr(settings, "rate_limit", 1000)
    monkeypatch.setattr(settings, "retry_max", 0)
    monkeypatch.setattr(settings, "retry_max_connect", 0)
    monkeypatch.setattr(settings, "worker_timeout", 5)
    monkeypatch.setattr(settings, "poll_interval_seconds", 0.01)
    monkeypatch.setattr(settings, "idem_replay_wait_seconds", 0.2)
    monkeypatch.setattr(settings, "callback_secret", "test-secret")
    monkeypatch.setattr(settings, "callback_allowlist", ())
    return settings


@pytest.fixture
def respx_router() -> Iterator[respx.MockRouter]:
    """任何未声明的出站请求立即失败（assert_all_mocked=True）。"""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


@pytest.fixture
def client(patch_redis, task_store, fake_billing, queue_events, test_settings):
    """带全套替身的 ASGI 测试客户端。"""
    from fastapi.testclient import TestClient

    from app.main import create_app

    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


AUTH = {"Authorization": "Bearer sk-test-token"}
