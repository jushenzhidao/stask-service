"""查询 / 回放 / 长轮询 / 取消。

对应 AC-19 ~ AC-25、AC-22（410）。

**对外形态口径（2026-09-17 修订）**：

- ``status`` 对外恒为**小写**（``queued``/``in_progress``/``success``/``failure``/
  ``canceled``），库内那一行恒为大写原生枚举——见 ``schemas.public_status``；
- 终态 **JSON** 结果体顶层恒有 ``status``（回放时注入；本地失败/过期时补上）；
- 终态 **非 JSON** 结果体（音频/图片/纯文本）保持**字节级一致**，此时状态由
  ``X-Stask-Task-Status`` 响应头携带——往 bytes 里加键就是损坏制品；
- 三条分支都挂 ``X-Stask-*`` 状态头。

`非 JSON 字节级一致` 仍是产品定位的关键承诺：客户端把 ``/async`` 去掉后，
二进制结果必须和直接调同步接口完全一致。
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from app.schemas import (
    ACTIVE,
    CANCELED,
    FAILURE,
    IN_PROGRESS,
    QUEUED,
    SUCCESS,
    TERMINAL,
    public_status,
)
from app.services import slots, statuscache, tokensession
from tests.conftest import stored_body

BASE = "/async/v1/images/generations"
TASK = "dall_e_3_" + "b" * 32
TH = "tokenhash0000000000000000000000"

#: 状态旁路头（三条分支共用）
H_STATUS = "x-stask-task-status"
H_TASK_ID = "x-stask-task-id"
H_UPSTREAM_STATUS = "x-stask-upstream-status"


async def _seed(task_store, status="QUEUED", **data_over) -> str:
    # slot_flags 是终态释放的依据（按掩码按位回退）：立即路径提交时占的是
    # 第一层 = FLAG_TOKEN，缺了它取消/终态就一层都不还。
    await task_store.create(TASK, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "slot_flags": slots.FLAG_TOKEN, "slot_model": "dall-e-3",
        "upstream_response": "", "upstream_response_encoding": "",
        "upstream_content_type": "", "upstream_status": 0,
        **data_over,
    })
    task_store.rows[TASK]["status"] = status
    if status in ("SUCCESS", "FAILURE", "CANCELED"):
        task_store.rows[TASK]["finish_time"] = task_store.now()
    return TASK


def _assert_status_header(resp, expected: str) -> None:
    """状态头必须恒在，且与小写形态一致。"""
    assert resp.headers[H_STATUS] == expected
    assert resp.headers[H_TASK_ID] == TASK


# ---------------------------------------------------------------------------
# 查询三态
# ---------------------------------------------------------------------------


async def test_pending_returns_202(client, task_store):
    """AC-19。"""
    await _seed(task_store)
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 202
    body = resp.json()
    assert body["task_id"] == TASK
    assert body["status"] == "queued"
    assert "created_at" in body
    # updated_at 让客户端能区分「一直在推进」与「真的卡死」——只有 created_at
    # 时，排队 10 分钟且持续重排的任务与死掉的任务看起来完全一样。
    assert "updated_at" in body
    _assert_status_header(resp, "queued")


def test_status_case_is_lowercase_everywhere():
    """对外形态恒小写，库内行仍是大写原生枚举（ADR-006 共存）。"""
    assert [public_status(s) for s in (*ACTIVE, CANCELED)] == [
        "queued", "in_progress", "canceled"]
    # 库内常量本身不得被改动
    assert QUEUED == "QUEUED" and CANCELED == "CANCELED"
    # 单一映射必须覆盖全部状态常量，否则新增状态会静默漏掉小写化
    assert {QUEUED, IN_PROGRESS, SUCCESS, FAILURE, CANCELED} == set(ACTIVE + TERMINAL)
    assert public_status(IN_PROGRESS) == "in_progress"
    assert public_status(SUCCESS) == "success"
    assert public_status(FAILURE) == "failure"


def test_public_form_never_equals_internal_form():
    """两套形态**必须不同**：映射不得退化成恒等。

    这是「无需兼容旧版本」这条裁决的机械钉子。若有人为了「兼容老客户端」把
    `public_status` 改成原值直通（或加一条按请求头切换形态的分支），
    下面三条断言会立刻变红——否则那种改动是**静默**的：响应照样 200，
    只是形态悄悄回到大写，而既有用例只断言「等于某个小写值」，
    很容易被一起改掉。
    """
    for state in (*ACTIVE, *TERMINAL):
        rendered = public_status(state)
        assert rendered != state, f"{state} 的对外形态与库内形态相同，映射已退化"
        assert rendered == rendered.lower(), f"{rendered!r} 不是小写"
        assert public_status(rendered) == rendered, "映射必须幂等（小写再转仍是自身）"


async def test_success_replays_json_with_status_injected(client, task_store):
    """AC-20（修订）：上游 JSON 结果体**原键值全保留**，顶层追加 status。

    修订前的措辞是「字节级一致（含空白与键序）」，理由是客户端可能在做
    签名校验。2026-09-17 起改为：JSON 体注入状态字段，**非 JSON 体仍字节级
    一致**——签名校验场景（图片/音频/纯文本）不受影响，JSON 场景下客户端
    要按新契约解析。
    """
    payload = b'{"created":1,   "data":[{"b64_json":"AAAA"}]}'
    await _seed(task_store, "SUCCESS",
                **stored_body("upstream_response", payload),
                upstream_content_type="application/json; charset=utf-8",
                upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    # Content-Type 仍原样回设（上游声明什么就是什么）
    assert resp.headers["content-type"] == "application/json; charset=utf-8"
    assert resp.json() == {
        "created": 1,
        "data": [{"b64_json": "AAAA"}],
        "status": "success",
    }
    _assert_status_header(resp, "success")
    assert resp.headers[H_UPSTREAM_STATUS] == "200"


async def test_gzip_stored_response_is_replayed_with_injection(client, task_store):
    """超阈值落 gzip 的 JSON 体，同样注入状态（编码标记驱动解码）。"""
    payload = b'{"data":"' + b"y" * 2000 + b'"}'
    await _seed(task_store, "SUCCESS",
                # plain_max_bytes=64 → 强制走 gzip 分支
                **stored_body("upstream_response", payload, 64),
                upstream_content_type="application/json", upstream_status=200)

    assert task_store.rows[TASK]["data"]["upstream_response_encoding"] == "gzip+b64"
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.json() == {"data": "y" * 2000, "status": "success"}
    _assert_status_header(resp, "success")


async def test_upstream_status_key_is_overridden_by_ours(client, task_store):
    """上游原文顶层已有 ``status`` 键时，以**本服务**的实际状态为准。

    这是有意的覆盖而非追加：客户端读顶层 ``status`` 就是为了知道**本任务**的
    终态，若保留上游那个（异步协议里常是 ``processing``/``succeeded``）反而会
    误导。上游原值不丢——``data.upstream_response`` 里仍是不含注入的原样副本。
    """
    payload = b'{"status":"succeeded","data":[{"url":"http://x/1.png"}]}'
    await _seed(task_store, "SUCCESS",
                **stored_body("upstream_response", payload),
                upstream_content_type="application/json", upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    body = resp.json()
    assert body["status"] == "success", "本服务的状态必须覆盖上游同名键"
    assert body["data"] == [{"url": "http://x/1.png"}], "其余键值必须原样保留"
    # 原文副本仍是上游那一份（未被注入污染）
    stored = task_store.rows[TASK]["data"]["upstream_response"]
    assert "\"status\":\"succeeded\"" in stored.replace(" ", "")


async def test_text_plain_json_like_body_is_not_injected(client, task_store):
    """``Content-Type`` 不含 json → **不注入**，即使内容恰好像 JSON。

    判定以 Content-Type 为准（与 codec 的显式编码标记同一思路），不做内容
    嗅探：``text/plain`` 里放一段 JSON 文本是上游的自由，我们无权改写它。
    """
    payload = b'{"ok":true}'
    await _seed(task_store, "SUCCESS",
                upstream_response=payload.decode(),
                upstream_response_encoding="plain",
                upstream_content_type="text/plain", upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.content == payload, "非 JSON 体必须原样，不得注入"
    _assert_status_header(resp, "success")


async def test_binary_response_replayed(client, task_store):
    """TTS 返回 audio/mpeg 二进制——回放不得被 JSON 化，也不得注入状态。

    二进制体必然走 gzip 分支（JSON 列存不了非法 UTF-8），这里顺带
    验证「非 UTF-8 → 自动压缩」在读写两侧闭环。

    状态注入在这里**必须**缺席（注入即损坏音频），客户端靠 ``X-Stask-*``
    头拿状态——这正是状态头存在的首要理由。
    """
    audio = bytes(range(256)) * 4
    await _seed(task_store, "SUCCESS",
                **stored_body("upstream_response", audio),
                upstream_content_type="audio/mpeg", upstream_status=200)

    assert task_store.rows[TASK]["data"]["upstream_response_encoding"] == "gzip+b64"
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.content == audio
    assert resp.headers["content-type"] == "audio/mpeg"
    _assert_status_header(resp, "success")


@pytest.mark.parametrize("upstream_status", [400, 401, 402, 429, 500])
async def test_failure_replays_upstream_status_and_body(client, task_store,
                                                        upstream_status):
    """AC-21：重放上游状态码 + 原文（不包装成本地 error 形制）。

    上游原文的 ``error`` 结构必须原样保留，只在顶层追加 ``status``。
    """
    body = b'{"error":{"message":"quota exceeded","type":"insufficient_quota"}}'
    await _seed(task_store, "FAILURE",
                **stored_body("upstream_response", body),
                upstream_content_type="application/json",
                upstream_status=upstream_status)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == upstream_status
    payload = resp.json()
    assert payload["error"] == {
        "message": "quota exceeded", "type": "insufficient_quota"}
    assert payload["status"] == "failure"
    _assert_status_header(resp, "failure")
    assert resp.headers[H_UPSTREAM_STATUS] == str(upstream_status)


async def test_canceled_returns_status_view(client, task_store):
    """CANCELED 从没调过上游，没有原文可回放——返回 200 + 状态视图。

    它不是「失败」（用户自己取消的），不该被塞进 502 的 error 形制。
    """
    await _seed(task_store, "CANCELED")
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "canceled"
    _assert_status_header(resp, "canceled")


async def test_local_failure_uses_error_envelope(client, task_store):
    """无上游原文的本地失败 → 统一 {"error": {...}} 形制 + 顶层 status。"""
    await _seed(task_store, "FAILURE")
    task_store.rows[TASK]["fail_reason"] = "token session missing"

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_no_body"
    assert "token session missing" in resp.json()["error"]["message"]
    assert resp.json()["status"] == "failure"
    _assert_status_header(resp, "failure")


async def test_purged_result_returns_410(client, task_store):
    """AC-22：结果已 TTL 清理 → 410（不是 404）。

    404 会让客户端以为 task_id 写错了；410 明确表达「存在过、已过期」。
    """
    await _seed(task_store, "SUCCESS", result_purged=True, upstream_status=200)
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "result_expired"
    assert resp.json()["status"] == "success", "410 报的仍是任务真实状态"
    _assert_status_header(resp, "success")
    assert resp.headers["x-stask-result-expired"] == "true"


async def test_corrupted_gzip_payload_returns_500(client, task_store):
    """标记为 gzip 却解不开 → 500（存储损坏，不是客户端的问题）。

    注意：**必须**带 ``gzip+b64`` 标记才走解压路径。不带标记的同样内容
    是合法明文，会被原样回放——这正是显式编码标记要达到的效果，
    读侧不再靠"试着解一下"猜形态。
    """
    await _seed(task_store, "SUCCESS",
                upstream_response="!!!not-base64!!!",
                upstream_response_encoding="gzip+b64",
                upstream_status=200)
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 500
    assert resp.json()["error"]["code"] == "result_corrupted"
    _assert_status_header(resp, "success")


async def test_plain_payload_is_never_treated_as_encoded(client, task_store):
    """明文标记下，恰好像 base64 的内容也必须原样回放（不做嗅探）。

    ``eyJvayI6dHJ1ZX0=`` 是合法 base64（解开是 ``{"ok":true}``）。靠嗅探
    的实现会把它解码成别的东西——这是显式标记替代嗅探的直接理由。
    它同时是「非 JSON → 字节级一致」的钉子：``text/plain`` 下不得注入。
    """
    payload = b"eyJvayI6dHJ1ZX0="
    await _seed(task_store, "SUCCESS",
                upstream_response=payload.decode(),
                upstream_response_encoding="plain",
                upstream_content_type="text/plain", upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.content == payload


def test_unknown_task_404(client):
    assert client.get(f"{BASE}/dall_e_3_{'f' * 32}").status_code == 404


def test_path_without_task_id_404(client):
    """形态预筛：纯路径不该被当成 task_id 去查库。"""
    assert client.get(BASE).status_code == 404


# ---------------------------------------------------------------------------
# 长轮询
# ---------------------------------------------------------------------------


async def test_long_poll_returns_on_terminal(client, task_store, test_settings):
    """AC-23：窗口内转终态 → 立即返回终态响应。

    TestClient 的请求跑在独立线程的事件循环里，所以用真线程延迟翻转
    状态（asyncio.create_task 在这里跨不过循环边界）。
    """
    await _seed(task_store)
    payload = b'{"done":true}'

    def flip_after_delay() -> None:
        time.sleep(0.05)
        task_store.rows[TASK]["status"] = "SUCCESS"
        task_store.rows[TASK]["data"].update({
            **stored_body("upstream_response", payload),
            "upstream_content_type": "application/json",
            "upstream_status": 200,
        })
        # 必须**同时**刷影子缓存：真实链路的终态迁移走 cas，而 cas 会
        # write-through 到 statuscache（长轮询正是靠它秒级看见终态）。
        # 只改行不刷缓存的话，长轮询会一直命中曾经回填的「进行中」，
        # 直到 TTL 到期都看不到终态——生产里不可能出现这种状态。
        asyncio.run(statuscache.set(TASK, "SUCCESS"))

    threading.Thread(target=flip_after_delay, daemon=True).start()
    resp = client.get(f"{BASE}/{TASK}?wait=3")
    assert resp.status_code == 200
    assert resp.json() == {"done": True, "status": "success"}
    _assert_status_header(resp, "success")


async def test_long_poll_timeout_returns_202(client, task_store, test_settings):
    await _seed(task_store)
    resp = client.get(f"{BASE}/{TASK}?wait=1")
    assert resp.status_code == 202


def test_wait_exceeding_max_is_rejected(client, test_settings):
    """?wait 上限受 POLL_WAIT_MAX_SECONDS 约束（必须 < nginx read timeout）。"""
    assert client.get(f"{BASE}/{TASK}?wait=99999").status_code == 422


# ---------------------------------------------------------------------------
# 取消
# ---------------------------------------------------------------------------


async def test_cancel_pending_task(client, task_store, patch_redis):
    """AC-24：排队中 → CANCELED，释放槽，零资金动作。"""
    await _seed(task_store)
    await slots.acquire(TH, 10)
    await tokensession.store(TASK, "sk-test-token")

    resp = client.delete(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    # 对外小写；**库内那一行仍是大写原生枚举**（两者不是同一个东西）
    assert resp.json()["status"] == "canceled"
    _assert_status_header(resp, "canceled")

    row = task_store.rows[TASK]
    assert row["status"] == "CANCELED"
    assert row["progress"] == "100%"
    assert await slots.current(TH) == 0
    assert await tokensession.get(TASK) is None


async def test_cancel_in_progress_409(client, task_store):
    """AC-25：执行中不可取消——上游是同步调用，HTTP 没有中止语义。"""
    await _seed(task_store, "IN_PROGRESS")
    resp = client.delete(f"{BASE}/{TASK}")
    assert resp.status_code == 409
    assert "in progress" in resp.json()["error"]["message"]


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
async def test_cancel_terminal_409(client, task_store, status):
    await _seed(task_store, status)
    assert client.delete(f"{BASE}/{TASK}").status_code == 409


def test_cancel_unknown_404(client):
    assert client.delete(f"{BASE}/dall_e_3_{'e' * 32}").status_code == 404
