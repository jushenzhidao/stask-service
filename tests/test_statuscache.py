"""``statuscache``（长轮询的 Redis write-through 缓存）的行为用例。

**为什么单独立一个文件。** 2026-09-13 复核实测：这个模块此前**测试里零引用**，
且不在 conftest 的 ``_REDIS_CONSUMERS`` 里。两条叠加的后果是——它内部那层
``except Exception: pass`` 会把「连不上真 Redis」咽掉，于是整条缓存路径在测试里
实际上是个**空操作**，而 522 个用例全绿。就算键格式写错、TTL 写错、命中判断
整个反过来，也不会有任何用例变红。

这里钉的是「缓存到底有没有在起作用」（写穿 / 命中 / 未命中回落 / 故障降级），
而不是「函数被调用过」。三类：

1. 单元：键、TTL、命中、未命中、Redis 故障降级；
2. ``flow._probe_status`` 的真实行为（这段代码**没有被替身替换**，可以真跑）：
   命中缓存时不许打 DB、未命中时回落 DB 并回填；
3. 写穿的**结构断言**：真实 ``create``/``cas`` 在测试里被替身整体替换，
   「写了缓存」没有任何行为出口，只能静态断言它确实调了 ``statuscache.set``。
"""

from __future__ import annotations

from pathlib import Path

from app.redis import K_STATUS
from app.schemas import IN_PROGRESS, QUEUED
from app.services import flow, statuscache, taskstore

ROOT = Path(__file__).resolve().parents[1]

#: 形态合法的 task_id（``{slug}_{32hex}``）
_TASK = "demo_0123456789abcdef0123456789abcdef"


def _key(task_id: str = _TASK) -> str:
    return K_STATUS.format(task_id=task_id)


# ---------------------------------------------------------------------------
# 1. 单元：键 / TTL / 命中 / 未命中 / 故障降级
# ---------------------------------------------------------------------------


async def test_set_then_get_roundtrip(patch_redis):
    await statuscache.set(_TASK, IN_PROGRESS)
    assert await statuscache.get(_TASK) == IN_PROGRESS


async def test_set_uses_contract_key_and_ttl(patch_redis):
    """键格式与 TTL 是契约。

    键写错 = **永远不命中**，而这一错误在行为上完全不可见（只是每次都回落 DB），
    正是「没有测试就发现不了」的典型。
    """
    await statuscache.set(_TASK, QUEUED)
    assert await patch_redis.get(_key()) == QUEUED
    ttl = await patch_redis.ttl(_key())
    # 替身用 ``int(exp - now)`` 截断，刚写完是 3599；真 Redis 同刻返回 3600。
    # 断言区间而不是等值——这条要钉的是「TTL 被设成了 _TTL 这个量级」，
    # 不是替身的取整方式。
    assert statuscache._TTL - 2 <= ttl <= statuscache._TTL, f"TTL 不在预期量级: {ttl}"


async def test_get_miss_returns_none(patch_redis):
    """未命中返回 None —— 调用方据此回落 DB，绝不能返回空串冒充状态。"""
    assert await statuscache.get("never_seen_0123456789abcdef0123456789") is None


async def test_set_swallows_redis_failure(patch_redis, monkeypatch):
    """缓存写失败必须静默：正确性不依赖缓存，抛出去会把提交链路一起带崩。"""
    async def boom(*args, **kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(patch_redis, "set", boom)
    await statuscache.set(_TASK, QUEUED)          # 不抛即通过


async def test_get_swallows_redis_failure_and_returns_none(patch_redis, monkeypatch):
    """Redis 整体不可用时，长轮询退化为 DB 轮询（返回 None 而非抛错）。"""
    async def boom(*args, **kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(patch_redis, "get", boom)
    assert await statuscache.get(_TASK) is None


# ---------------------------------------------------------------------------
# 2. flow._probe_status 的真实行为（这段没被替身替换）
# ---------------------------------------------------------------------------


async def test_probe_status_prefers_cache_without_hitting_db(
    task_store, patch_redis, monkeypatch
):
    """命中缓存时**必须不打 DB** —— 这正是这个缓存存在的全部理由。"""
    await statuscache.set(_TASK, IN_PROGRESS)

    async def boom(*args, **kwargs):
        raise AssertionError("命中缓存却打了 DB —— 缓存等于没起作用")

    monkeypatch.setattr(taskstore, "get_status", boom)
    assert await flow._probe_status(_TASK) == IN_PROGRESS


async def test_probe_status_falls_back_to_db_and_backfills(
    task_store, patch_redis
):
    """未命中 → 回落 DB 一次 → 回填，之后同一任务的其它等待者都命中缓存。"""
    await taskstore.create(_TASK, "/v1/images/generations", {"model": "m"})
    await patch_redis.delete(_key())              # 模拟键过期 / Redis 刚重启
    assert await statuscache.get(_TASK) is None

    assert await flow._probe_status(_TASK) == QUEUED    # 回落 DB
    assert await statuscache.get(_TASK) == QUEUED       # 且回填


# ---------------------------------------------------------------------------
# 3. 写穿的结构断言（真实实现在测试里不会被执行）
# ---------------------------------------------------------------------------


def test_real_write_path_does_write_through():
    """真实 ``create`` / ``cas`` 必须调用 ``statuscache.set``。

    **为什么只能是结构断言**：测试里 ``taskstore.create`` / ``cas`` 被
    ``InMemoryTaskStore`` 整体替换，连 MySQL 的那份真实实现**根本不会执行**，
    所以「写穿」在测试里没有任何行为出口。丢了写穿 ⇒ 缓存永远为空 ⇒
    长轮询永远回落 DB（性能退化，不是功能故障），**所有用例照样绿**。
    """
    import ast

    src = (ROOT / "app" / "services" / "taskstore" / "_write.py").read_text("utf-8")
    tree = ast.parse(src)

    for fname in ("create", "cas"):
        node = next((n for n in tree.body
                     if isinstance(n, ast.AsyncFunctionDef) and n.name == fname), None)
        assert node is not None, f"_write.py 里找不到 {fname} —— 守卫定位已失效"
        called = {
            n.func.attr
            for n in ast.walk(node)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "statuscache"
        }
        assert "set" in called, (
            f"`{fname}` 没有调用 statuscache.set —— write-through 丢了。"
            "长轮询会永远回落 DB（性能退化且无测试能发现）。"
        )
