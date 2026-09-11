"""其余单元：时间归一、codec 往返、回调签名、健康检查、ops、mypy 门禁。

对应 AC-29 / AC-30 / AC-31。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from app import main
from app.config import normalize_database_url, settings
from app.services import admission, codec, notify, taskstore
from app.services.admission import AdmissionError
from tests.conftest import AUTH, stored_body

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 时间归一（AC-31）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    (1755000000, 1755000000),               # 秒，原样
    (1755000000123, 1755000000),            # 毫秒 → 秒
    ("1755000000", 1755000000),
    (None, 0),
    ("", 0),
    ("bogus", 0),
    (0, 0),
])
def test_as_unix_seconds(value, expected):
    """tasks 表被 new-api 原生模块用 UnixMilli 写过，读侧必须归一。"""
    assert taskstore.as_unix_seconds(value) == expected


def test_sql_time_predicates_are_bare_columns():
    """时间谓词必须裸列比较，否则 sweeper/看板的 range 条件吃不到索引。

    前提是写侧恒写 unix 秒（见下一个用例）+ WHERE 恒带 platform，
    所以 SQL 侧不再需要 ``IF(col > 1e11, col DIV 1000, col)`` 包裹。
    这条是机械门禁：包裹一旦被写回来就失败。
    """
    source = (ROOT / "app" / "services" / "taskstore.py").read_text("utf-8")
    body = source.split('"""', 2)[2]          # 去掉模块 docstring 里的说明文字
    assert "DIV 1000" not in body
    assert str(taskstore._UNIX_MS_THRESHOLD) not in body.replace(
        "_UNIX_MS_THRESHOLD = 100_000_000_000", ""
    )


def test_write_side_timestamps_are_seconds():
    """裸列比较的前提：本服务写入的时间列恒为秒，绝不写毫秒。"""
    assert taskstore.now() < taskstore._UNIX_MS_THRESHOLD


# ---------------------------------------------------------------------------
# codec
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    b"",
    b"{}",
    b'{"data":[{"url":"https://cdn/a.png"}]}',
    bytes(range(256)) * 100,                # 二进制（音频/图片）
    "中文内容测试".encode(),
])
def test_codec_roundtrip(payload):
    """任何输入、任一分支，编解码都必须字节级还原。"""
    assert codec.decode(*codec.encode(payload, codec.PLAIN_MAX_DEFAULT)) == payload


@pytest.mark.parametrize("payload", [
    b"",
    b"{}",
    b'{"data":[{"url":"https://cdn/a.png"}]}',
    "中文内容测试".encode(),
])
def test_small_utf8_payloads_stay_plain(payload):
    """小体 UTF-8 → 明文直存，DB 里肉眼可读（本轮改造的核心诉求）。"""
    encoded, encoding = codec.encode(payload, codec.PLAIN_MAX_DEFAULT)
    assert encoding == codec.PLAIN
    assert encoded == payload.decode()


def test_non_utf8_payload_forced_to_gzip():
    """非 UTF-8 字节无法进 JSON 列 → 无条件压缩，与体积阈值无关。"""
    encoded, encoding = codec.encode(b"\xff\xfe\x00", codec.PLAIN_MAX_DEFAULT)
    assert encoding == codec.GZIP_B64
    assert codec.decode(encoded, encoding) == b"\xff\xfe\x00"


def test_codec_compresses_large_json():
    """超阈值的 JSON 压缩率应显著——b64 会 ×4/3，压不动就是净亏。"""
    payload = json.dumps({"data": [{"url": "https://cdn/x.png"} for _ in range(200)]}
                         ).encode()
    encoded, encoding = codec.encode(payload, plain_max_bytes=1024)
    assert encoding == codec.GZIP_B64
    assert len(encoded) < len(payload) * 0.5


def test_plain_max_bytes_is_the_only_switch():
    """阈值恰好等于体长时仍走明文（边界取 <=）。"""
    payload = b"x" * 100
    assert codec.encode(payload, 100)[1] == codec.PLAIN
    assert codec.encode(payload, 99)[1] == codec.GZIP_B64


def test_codec_rejects_garbage():
    with pytest.raises(ValueError):
        codec.decode("!!!not-base64!!!", codec.GZIP_B64)


def test_codec_rejects_unknown_encoding():
    """未知编码标记必须显式报错，不能静默当明文——静默会把压缩数据
    当原文回放给客户端，是最难查的一类污染。"""
    with pytest.raises(ValueError):
        codec.decode("whatever", "zstd+b64")


def test_legacy_empty_encoding_treated_as_plain():
    """空标记 = 明文。仅为容忍手工插入/外部写入的行，不为兼容旧版本。"""
    assert codec.decode('{"a":1}', "") == b'{"a":1}'


# ---------------------------------------------------------------------------
# 回调签名（AC-29）
# ---------------------------------------------------------------------------


def test_signature_is_verifiable(test_settings):
    body = b'{"task_id":"x","status":"SUCCESS"}'
    ts = 1755000000
    got = notify.sign(body, ts)

    expected = hmac.new(b"test-secret", f"{ts}.".encode() + body,
                        hashlib.sha256).hexdigest()
    assert got == f"sha256={expected}"


def test_signature_omitted_without_secret(monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "callback_secret", "")
    assert notify.sign(b"x", 1) == ""


def test_signature_binds_timestamp(test_settings):
    """时间戳参与签名 → 重放攻击可被接收方按窗口拒绝。"""
    assert notify.sign(b"x", 1) != notify.sign(b"x", 2)


@pytest.mark.parametrize("url,allowed", [
    ("http://cb.example/hook", True),
    ("https://cb.example/hook", True),
    ("ftp://cb.example/hook", False),
    ("http://user:pw@cb.example/hook", False),
    ("not-a-url", False),
])
def test_callback_url_validation(test_settings, url, allowed):
    assert notify._url_allowed(url) is allowed


def test_callback_allowlist_enforced(monkeypatch, test_settings):
    """空 allowlist = 不限制，但那样回调就是 SSRF 出口，生产必须配。"""
    monkeypatch.setattr(test_settings, "callback_allowlist", ("cb.example",))
    assert notify._url_allowed("http://cb.example/hook") is True
    assert notify._url_allowed("http://169.254.169.254/latest/meta-data") is False


async def test_callback_delivery_signs_request(task_store, patch_redis,
                                               test_settings, respx_router):
    task_id = "img_" + "d" * 32
    await task_store.create(task_id, "/x", {
        "callback_url": "http://cb.example/hook", "upstream_status": 200,
    })
    task_store.rows[task_id]["status"] = "SUCCESS"
    route = respx_router.post("http://cb.example/hook").mock(
        return_value=httpx.Response(200)
    )

    await notify.deliver(task_id)

    req = route.calls[0].request
    assert req.headers["x-stask-signature"].startswith("sha256=")
    assert req.headers["x-stask-timestamp"].isdigit()
    payload = json.loads(req.content)
    assert payload["task_id"] == task_id
    assert payload["status"] == "SUCCESS"
    # 回调体绝不含结果原文（可能好几 MB）
    assert "upstream_response" not in payload
    assert task_store.rows[task_id]["data"]["callback_delivered"] is True


async def test_callback_retries_with_backoff(task_store, patch_redis, test_settings,
                                             respx_router, queue_events):
    task_id = "img_" + "e" * 32
    await task_store.create(task_id, "/x", {"callback_url": "http://cb.example/h"})
    task_store.rows[task_id]["status"] = "FAILURE"
    respx_router.post("http://cb.example/h").mock(return_value=httpx.Response(500))

    await notify.deliver(task_id, attempt=1)

    assert queue_events.notify == [(task_id, 2, 2)]     # 指数退避 2^1


async def test_callback_exhausts_and_records(task_store, patch_redis, monkeypatch,
                                             test_settings, respx_router,
                                             queue_events):
    monkeypatch.setattr(test_settings, "callback_max_attempts", 2)
    task_id = "img_" + "f" * 32
    await task_store.create(task_id, "/x", {"callback_url": "http://cb.example/h"})
    task_store.rows[task_id]["status"] = "FAILURE"
    respx_router.post("http://cb.example/h").mock(return_value=httpx.Response(500))

    await notify.deliver(task_id, attempt=2)

    assert queue_events.notify == []
    assert task_store.rows[task_id]["data"]["callback_delivered"] is False


# ---------------------------------------------------------------------------
# 健康检查与 ops
# ---------------------------------------------------------------------------


def test_healthz_live_has_no_dependencies(client):
    """liveness 必须零依赖——DB 宕机时重启进程治不了病。"""
    resp = client.get("/healthz/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_healthz_ready_reports_channel_id_without_gating(client, monkeypatch):
    """ADR-006：channel_id 缺失只**上报**、不门禁。

    它是致命配置（上游会无 CAS 批量误杀在途任务），但在 dev/测试环境里
    channel_id=0 是合法的——若拿它判 503，本地和 CI 就永远起不来。
    所以放进 ``config`` 段让监控去抓，而不是混进 ``checks`` 里摘流量。
    """
    from app import healthz

    class _Session:
        async def execute(self, _q):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

    monkeypatch.setattr(healthz, "get_session_factory", lambda: (lambda: _Session()))
    monkeypatch.setattr(settings, "channel_id", 0)

    resp = client.get("/healthz/ready")
    assert resp.status_code == 200
    assert resp.json()["checks"] == {"redis": "ok", "db": "ok"}
    assert resp.json()["config"] == {"channel_id": 0}


@pytest.mark.parametrize("raw,host,port,db,password", [
    # new-api 的 SQL_DSN（Go DSN，官方示例）
    ("root:123456@tcp(localhost:3306)/oneapi", "localhost", 3306, "oneapi", "123456"),
    # 省略端口 → 补 3306
    ("root:123456@tcp(127.0.0.1)/oneapi", "127.0.0.1", 3306, "oneapi", "123456"),
    # 密码里带 @ 和 / ：必须以 @tcp( 为锚点切，不能按第一个 @ 切
    ("root:p@ss/w0rd@tcp(db:3306)/oneapi", "db", 3306, "oneapi", "p@ss/w0rd"),
    # 混种：SQLAlchemy 壳 + Go 的 tcp() + 重复问号（曾经让服务直接起不来）
    ("mysql+asyncmy://root:pwd@tcp(h:3306)/db??charset=utf8mb4", "h", 3306, "db", "pwd"),
    # 标准写法原样保留
    ("mysql+asyncmy://root:pwd@h:3306/db?charset=utf8mb4", "h", 3306, "db", "pwd"),
    # 同步 scheme 补成 asyncmy
    ("mysql://root:pwd@h:3306/db", "h", 3306, "db", "pwd"),
])
def test_database_url_accepts_newapi_sql_dsn(raw, host, port, db, password):
    """tasks 表与 new-api 共用，连接串要能直接吃上游那份（Go DSN）。"""
    from sqlalchemy.engine import make_url

    url = make_url(normalize_database_url(raw))
    assert (url.host, url.port, url.database, url.password) == (host, port, db, password)
    assert url.drivername == "mysql+asyncmy"


def test_sql_dsn_env_alias_is_accepted(monkeypatch):
    """``SQL_DSN`` 是 new-api 的变量名——两边共用同一份配置，不必再抄一遍。"""
    from app.config import Settings

    monkeypatch.setenv("SQL_DSN", "root:pwd@tcp(db:3306)/oneapi?parseTime=True")
    # _env_file=None：别让本机 .env 干扰
    built = Settings(_env_file=None)
    assert built.database_url == "mysql+asyncmy://root:pwd@db:3306/oneapi?charset=utf8mb4"


def test_settings_have_no_env_prefix():
    """环境变量名 = 字段名大写，不带 ST_ 前缀。

    这是**契约**而不是实现细节：.env 的键名、compose 的 environment、
    gunicorn.conf.py 的直读全部依赖它。一旦有人把前缀加回来，存量 .env
    会静默失效——所以钉住。
    """
    from app.config import Settings

    assert Settings.model_config.get("env_prefix", "") == ""


def test_empty_upstream_allowlist_means_unrestricted(monkeypatch):
    """留空 = 不限制，与 CALLBACK_ALLOWLIST 同一套语义。

    放行意味着 X-Upstream-Base-Url 头能决定令牌发往哪里，所以只有 scheme /
    userinfo / query 这几道硬校验还在（它们防的是 SSRF 手法，与白名单无关）。
    """
    monkeypatch.setattr(settings, "upstream_allowlist", ())
    assert admission.resolve_upstream("http://anything.example.com:9999") == (
        "http://anything.example.com:9999"
    )
    for bad, code in [
        ("ftp://x.com", "upstream_invalid_scheme"),
        ("http://u:p@x.com", "upstream_userinfo"),
        ("http://x.com/?a=1", "upstream_invalid"),
    ]:
        with pytest.raises(AdmissionError) as ei:
            admission.resolve_upstream(bad)
        assert ei.value.code == code


def test_taskiq_admin_url_without_token_is_silent_failure(monkeypatch):
    """看板改为必选后，URL 有、token 空 = 静默不上报——必须在启动期点出来。"""
    monkeypatch.setattr(settings, "app_env", "dev")
    monkeypatch.setattr(settings, "taskiq_admin_url", "http://taskiq-admin:3000")
    monkeypatch.setattr(settings, "taskiq_admin_api_token", "")
    main._check_taskiq_admin()                       # 非生产：只告警

    monkeypatch.setattr(settings, "app_env", "prod")
    with pytest.raises(RuntimeError, match="TASKIQ_ADMIN_API_TOKEN"):
        main._check_taskiq_admin()


def test_taskiq_admin_check_skipped_without_url(monkeypatch):
    """没配 URL（本机 standalone）就不该拿 token 说事。"""
    monkeypatch.setattr(settings, "app_env", "prod")
    monkeypatch.setattr(settings, "taskiq_admin_url", "")
    monkeypatch.setattr(settings, "taskiq_admin_api_token", "")
    main._check_taskiq_admin()


@pytest.mark.parametrize("app_env,should_raise", [
    ("dev", False), ("test", False), ("prod", True), ("PRODUCTION", True),
])
def test_channel_id_missing_is_fatal_only_in_prod(monkeypatch, app_env, should_raise):
    """生产环境 channel_id=0 必须 fail-fast：带病启动比起不来危险得多。"""
    monkeypatch.setattr(settings, "app_env", app_env)
    monkeypatch.setattr(settings, "channel_id", 0)

    if should_raise:
        with pytest.raises(RuntimeError, match="CHANNEL_ID"):
            main._warn_coexistence_risks()
    else:
        main._warn_coexistence_risks()      # 非生产只告警，不阻断


async def test_ops_stats(client, task_store):
    await task_store.create("a_" + "1" * 32, "/x", {"token_hash": "th"})
    resp = client.get("/ops/stats", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["status_counts"]["QUEUED"] == 1


async def test_ops_task_detail_never_leaks_sk_or_body(client, task_store,
                                                      patch_redis):
    """AC-30：诊断端点只给存在性与 TTL，绝不返回令牌本体或结果原文。"""
    from app.services import tokensession

    task_id = "img_" + "9" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": "dall-e-3",
        **stored_body("request_body", b'{"prompt":"secret prompt"}'),
        **stored_body("upstream_response", b'{"url":"x"}'),
        "response_bytes": 11,
    })
    await tokensession.store(task_id, "sk-test-token")

    resp = client.get(f"/ops/tasks/{task_id}", headers=AUTH)
    assert resp.status_code == 200
    body = resp.text
    assert "sk-test-token" not in body
    assert "secret prompt" not in body
    assert resp.json()["token_session"]["exists"] is True
    assert resp.json()["response_bytes"] == 11


def test_ops_requires_auth(client):
    assert client.get("/ops/stats").status_code == 401


# ---------------------------------------------------------------------------
# 静态检查门禁
# ---------------------------------------------------------------------------


def test_mypy_clean():
    """把类型检查做成一个测试用例——CI 里跑 pytest 就等于跑了 mypy。"""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "app/"],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_ruff_clean():
    proc = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "app", "tests"],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_no_emoji_in_source():
    """团队 P0 规则：源码与文档中不得出现 emoji 作为功能标识。"""
    import re

    pattern = re.compile(
        "[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF"
        "\U0001FA00-\U0001FAFF\U0001F000-\U0001F0FF]"
    )
    offenders = []
    for path in list(ROOT.glob("app/**/*.py")) + list(ROOT.glob("tests/**/*.py")) \
            + list(ROOT.glob("docs/**/*.md")) + [ROOT / ".env.example"]:
        text = path.read_text(encoding="utf-8")
        if pattern.search(text):
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"emoji found in: {offenders}"


#: 允许调用 ``taskstore.get``（= ``SELECT *``，含 request_body / upstream_response
#: 大字段）的模块白名单。**只有真正需要原始体的路径可以在这里**：
#: - ``execute.py``：worker 要把 request_body 发给上游；
#: - ``flow.py``：终态回放要把 upstream_response 原文还给客户端。
#:
#: 其余读取一律走 ``get_meta`` 投影。这条不变式（设计 §6 第 6 条）用测试
#: 钉住而不是靠人记得——热路径误用 get 会静默拉回每条最多 12MB 的列，
#: 功能完全正常、只是把 DB 带宽烧穿，评审很难看出来。
_GET_ALLOWLIST = {"services/execute.py", "services/flow.py"}


def test_select_star_only_in_allowlisted_modules():
    """``taskstore.get``（SELECT *）只允许出现在白名单模块里。

    背景（实测）：到期通道与批次放行曾是每任务一次 ``SELECT *``，而它们只用
    到几个标量字段。一轮 200 条到期放行的额外 DB 流量，按提交体中位数计约
    数 MB、按 2MB 上限计约 800MB——纯属浪费，且**没有任何行为测试会发现**
    （返回值完全正确）。故用结构断言把读取入口锁死。
    """
    import re

    call = re.compile(r"taskstore\.get\(")
    offenders = []
    for path in ROOT.glob("app/**/*.py"):
        rel = str(path.relative_to(ROOT / "app"))
        if rel in _GET_ALLOWLIST:
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if call.search(line):
                offenders.append(f"{rel}:{lineno}")
    assert not offenders, (
        "这些位置用了 taskstore.get（SELECT *），应改为 get_meta 投影；"
        "若确实需要原始体，请把它加进 _GET_ALLOWLIST 并写明理由：\n  "
        + "\n  ".join(offenders)
    )


#: 允许「生产代码无调用方」的 services 公开函数，**每条都必须写明理由**。
#: 白名单是豁免清单，不是垃圾桶——不写理由就等于默许死代码堆积。
_ORPHAN_ALLOWLIST = {
    # 测试专用的缓存清理钩子。Redis 层靠 TTL 自然过期，进程内层只有测试需要
    # 在用例之间重置。docstring 已声明「测试用」。
    "clear_cache",
    # 为未实现的看板需求（PRD R-21/R-22）预留的读取入口，口径已对齐
    # batch_waiting()。见其 docstring。
    "batch_counts_by_model",
}


def test_no_orphan_service_functions():
    """``app/services`` 里不得存在「没人调用」的公开函数。

    这一类缺陷（实现了但没有任何调用方）**测试发现不了**——被测函数本身是
    绿的，缺的只是调用点。实测过一次：``artifacts.parse_for_store`` 是文档里
    写明的「唯一入口」，但 ``execute.py`` 内联了同样的四个字段，于是
    ``parse_for_store`` 成了只被测试调用的死代码。危害不是「多几行」，
    而是**同一个契约有了两份实现**：改了一份、另一份静默漂移，
    而测试盯着的恰好是不跑的那份。

    判据：模块外无引用，且模块内除定义行外也无引用（路由与 cron 任务用
    装饰器注册，故有装饰器的一律跳过）。测试文件的引用**不算数**——
    「只被测试调用」正是本测试要抓的信号。

    **匹配口径要按「名字是否有歧义」分两档**（这里踩过两次坑）：

    - 裸名匹配（默认）：函数名在全仓唯一时，`from ...idem import new_task_id`
      这类导入后直呼的名字也算调用。早期的「只认模块限定」版本会因为
      漏掉这种形式而**大量误报**。
    - 限定匹配：名字在多个模块里都有同名定义时（如 `stats` 同时存在于
      `batching` / `dispatch` / `ops`），裸名无法判断指向谁，必须写成
      `batching.stats`。早期版本一律按裸名匹配，于是 `ops.py` 的路由处理器
      `stats` 把 `batching.stats` 的孤儿身份盖住了——**同名不同物**是这类
      扫描最常见的假阴性来源。
    """
    import ast
    import re
    from collections import defaultdict

    app_src = {p: p.read_text(encoding="utf-8")
               for p in (ROOT / "app").rglob("*.py")}

    def top_level_defs(source: str) -> list:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        return [n for n in tree.body
                if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)]

    # 名字 → 定义了它的模块集合（用于判断名字是否有歧义）
    owners: dict[str, set] = defaultdict(set)
    for path, source in app_src.items():
        for node in top_level_defs(source):
            owners[node.name].add(path)

    offenders = []
    for path, source in sorted(app_src.items()):
        if "services" not in path.parts:
            continue
        own = re.escape(path.stem)
        for node in top_level_defs(source):
            if node.name.startswith("_") or node.decorator_list:
                continue
            if node.name in _ORPHAN_ALLOWLIST:
                continue
            bare = r"\b" + re.escape(node.name) + r"\b"
            if len(owners[node.name]) > 1:
                # 有歧义：只认 "<模块名>.<函数名>" 形式的限定引用
                outside = sum(
                    len(re.findall(rf"\b{own}\.{bare}", s))
                    for q, s in app_src.items() if q != path)
            else:
                outside = sum(
                    len(re.findall(bare, s))
                    for q, s in app_src.items() if q != path)
            inside = len(re.findall(bare, source)) - 1     # 减去定义行
            if outside == 0 and inside == 0:
                offenders.append(
                    f"{node.name} ({path.relative_to(ROOT)})")
    assert not offenders, (
        "以下 services 公开函数没有任何调用方。要么接上调用点、要么删除；"
        "确实要保留（如为未实现需求预留）请加进 _ORPHAN_ALLOWLIST 并写明理由：\n  "
        + "\n  ".join(offenders)
    )
