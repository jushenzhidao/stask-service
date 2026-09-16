"""``gunicorn.conf.py`` 的启动期推导与告警。

这个文件是**唯一**直读 ``os.environ`` 的地方，且只在 master 进程里跑一次——
写错不会被任何用例自然覆盖，只等到生产启动（甚至等到压力上来）才显形。
这里用「受控 env + exec 模块 + 假 server」把两条启动告警钉住。

为什么值得钉：``GUNICORN_WORKERS`` 手设值**优先于**预算推导，而「改它要重算
连接数」以前只写在 ``.env`` 注释里，且那段注释本身就与值互相矛盾（按 workers=3
算着 80，实际是 8 → 180）。超支不会让启动失败，代价要等打爆共享 MySQL、
连累 new-api 才付——正是最该由机械门禁兜住的那类。
"""

from __future__ import annotations

import types
from pathlib import Path
from typing import Any

import pytest

_CONF = Path(__file__).resolve().parents[1] / "gunicorn.conf.py"

#: 推导会读到的 env。每个用例先清空，免得读到开发者本机的 .env 值
_ENV_KEYS = (
    "GUNICORN_WORKERS",
    "DB_POOL_SIZE",
    "DB_MAX_OVERFLOW",
    "DB_MAX_CONNECTIONS",
    "DB_WEB_CONNECTION_SHARE",
    "POLL_WAIT_MAX_SECONDS",
    "GUNICORN_TIMEOUT",
    "GUNICORN_GRACEFUL_TIMEOUT",
)


class _CapturingLog:
    """假 ``server.log``：把 warning 按 ``%`` 格式化后留档，不碰真实 logging 装配。"""

    def __init__(self) -> None:
        self.warnings: list[str] = []

    def info(self, msg: str, *args: Any) -> None:
        return None

    def warning(self, msg: str, *args: Any) -> None:
        self.warnings.append(msg % args if args else msg)


def _load(monkeypatch: pytest.MonkeyPatch, **env: str) -> dict[str, Any]:
    """在受控环境下 exec 配置模块，返回它的命名空间。"""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    ns: dict[str, Any] = {}
    exec(compile(_CONF.read_text(), str(_CONF), "exec"), ns)
    return ns


def _warnings(ns: dict[str, Any]) -> list[str]:
    """跑一遍 ``on_starting``（gunicorn master 的启动钩子），收集它打出的告警。"""
    server = types.SimpleNamespace(log=_CapturingLog())
    ns["on_starting"](server)
    return server.log.warnings


def test_auto_derived_workers_are_quiet(monkeypatch: pytest.MonkeyPatch) -> None:
    """模板默认（不手设 GUNICORN_WORKERS）→ 走预算推导，且不告警。

    这条同时防「告警写成恒真」：判据若写错方向，这里会红。
    """
    ns = _load(monkeypatch)
    assert ns["workers"] == ns["_BY_AUTO"] == 3
    assert _warnings(ns) == []


def test_manual_workers_over_budget_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    """手设 8（线上实际出现过的值）→ 必须告警，并给出「哪几个数、怎么改」。"""
    ns = _load(monkeypatch, GUNICORN_WORKERS="8")
    warnings = _warnings(ns)
    assert len(warnings) == 1, "应当且只应当告警一次"

    text = warnings[0]
    assert "DB 连接预算超支" in text
    assert "160" in text, "web 侧连接 8 × (10+10)"
    assert "240" in text, "160 × 1.5 安全系数"
    assert "DB_MAX_CONNECTIONS=151" in text
    assert "自动推导" in text and "3" in text, "应给出自动推导值作为出路"
    assert "12" in text, "151 / (1.5 × 8) 的每 worker 连接上限建议"


def test_at_auto_value_is_silent(monkeypatch: pytest.MonkeyPatch) -> None:
    """钉住阈值本身：贴着线（3 × 20 × 1.5 = 90 ≤ 151）不该误报。"""
    ns = _load(monkeypatch, GUNICORN_WORKERS="3")
    assert _warnings(ns) == []


def test_auto_derivation_also_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    """没手设也照查：最少 2 worker 是硬下限，预算极小时它同样顶破预算。

    这不是"手设才有问题"——把守卫限定在「env 里有 GUNICORN_WORKERS」会漏掉
    这类配置（池开得大 / 预算填得小），同样是会打爆 MySQL 的写法。
    """
    ns = _load(monkeypatch, DB_MAX_CONNECTIONS="30")
    assert ns["workers"] == ns["_BY_AUTO"] == 2
    assert ns["workers"] * ns["_DB_PER_WORKER"] * ns["_DB_ALERT_FACTOR"] > 30
    assert len(_warnings(ns)) == 1
