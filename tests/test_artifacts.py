"""制品解析：三级降级、真实厂商形态、反例与鲁棒性。

解析层是**增强**而非成败判据：任何输入都不得抛异常，空清单是合法结果。
用例按「一级 known → 二级 walk → 三级 inline」的降级顺序组织，另加
一组反例（请求回显、非法 URL）与鲁棒性（环、超深、超量）。
"""

from __future__ import annotations

import base64
import json

import pytest

from app.services import artifacts


def _keys(items: list[artifacts.Artifact]) -> list[str]:
    return [a.key for a in items]


def _urls(items: list[artifacts.Artifact]) -> list[str]:
    return [a.url for a in items]


# ---------------------------------------------------------------------------
# 第一级：已知路径（真实厂商形态）
# ---------------------------------------------------------------------------


def test_seedream_data_url_list() -> None:
    """doubao-seedream 图片任务：``data[].url`` —— 截图里报警的那条。"""
    payload = {
        "model": "doubao-seedream-4-0-250828",
        "created": 1757300000,
        "data": [
            {"url": "https://ark-content.tos-cn-beijing.volces.com/a.jpeg?X-Tos-Expires=86400"},
            {"url": "https://ark-content.tos-cn-beijing.volces.com/b.jpeg?X-Tos-Expires=86400"},
        ],
        "usage": {"generated_images": 2},
    }
    result = artifacts.parse_result(payload)
    assert result.tier == "known"
    assert _keys(result.items) == ["image", "image_2"]
    assert all(a.type == "image" for a in result.items)
    # 扩展名推断 MIME，且预签名 query 不能干扰
    assert {a.mime_type for a in result.items} == {"image/jpeg"}
    assert artifacts.primary_url(result.items).endswith("a.jpeg?X-Tos-Expires=86400")


def test_seedance_video_and_last_frame() -> None:
    """Seedance 视频任务：``content.video_url`` + ``last_frame_url``。"""
    payload = {
        "id": "cgt-2026-xyz",
        "status": "succeeded",
        "content": {
            "video_url": "https://ark-content.tos-cn-beijing.volces.com/v.mp4?sig=1",
            "last_frame_url": "https://ark-content.tos-cn-beijing.volces.com/f.png?sig=2",
        },
    }
    result = artifacts.parse_result(payload)
    assert result.tier == "known"
    assert _keys(result.items) == ["video", "image"]
    assert result.items[0].mime_type == "video/mp4"
    assert result.items[1].mime_type == "image/png"
    # primary 偏向 video（用户真正想看的产出）
    assert artifacts.primary_url(result.items).endswith("v.mp4?sig=1")


def test_openai_images_nested_data() -> None:
    """OpenAI 兼容层常见的 ``data.data[]`` 双层信封。"""
    payload = {"code": "success", "data": {"data": [{"url": "https://cdn.example.com/x.webp"}]}}
    result = artifacts.parse_result(payload)
    assert result.tier == "known"
    assert _urls(result.items) == ["https://cdn.example.com/x.webp"]
    assert result.items[0].mime_type == "image/webp"


def test_bytes_and_str_payload_accepted() -> None:
    """上游原文是 bytes（``resp.content``）——落库路径的真实入参形态。"""
    raw = json.dumps({"data": [{"url": "https://cdn.example.com/x.png"}]}).encode()
    assert _urls(artifacts.parse(raw)) == ["https://cdn.example.com/x.png"]
    assert _urls(artifacts.parse(raw.decode())) == ["https://cdn.example.com/x.png"]


# ---------------------------------------------------------------------------
# 第二级：键名递归
# ---------------------------------------------------------------------------


def test_walk_unknown_vendor_shape() -> None:
    """没命中已知路径，靠键名兜住：任意深度的 ``*_url`` / ``images[]``。"""
    payload = {
        "outputs": {"render": {"artifacts": [{"image_url": "https://cdn.example.com/deep.png"}]}}
    }
    result = artifacts.parse_result(payload)
    assert result.tier == "walk"
    assert _urls(result.items) == ["https://cdn.example.com/deep.png"]


def test_kind_by_extension_over_key_hint() -> None:
    """键名说 image，扩展名是 mp4 —— 扩展名更可靠，判 video。

    两级都必须遵守这条：第一级走 ``result`` envelope 的精确路径，第二级走
    键名递归；判定规则不一致会让同一个 URL 因命中级别不同而类型不同。
    """
    by_known = artifacts.parse_result({"result": {"image_url": "https://cdn.example.com/clip.mp4"}})
    assert by_known.tier == "known"
    assert by_known.items[0].type == "video"
    assert by_known.items[0].mime_type == "video/mp4"

    by_walk = artifacts.parse_result({"payload_wrap": {"cover_image": "https://c.example.com/x.mp4"}})
    assert by_walk.tier == "walk"
    assert by_walk.items[0].type == "video"


def test_walk_bare_string_list() -> None:
    payload = {"result": {"audios": ["https://cdn.example.com/a.mp3"]}}
    result = artifacts.parse_result(payload)
    assert result.tier == "walk"
    assert result.items[0].type == "audio"
    assert result.items[0].mime_type == "audio/mpeg"


# ---------------------------------------------------------------------------
# 第三级：inline base64
# ---------------------------------------------------------------------------


def test_inline_b64_json() -> None:
    """``b64_json``：无任何可回源 URL 时的最后兜底。"""
    blob = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"0" * 64).decode()
    result = artifacts.parse_result({"data": [{"b64_json": blob}]})
    assert result.tier == "inline"
    assert result.items[0].inline is True
    assert result.items[0].url == ""
    # inline 体量大：落库只留标记，不写回 data
    assert result.items[0].to_dict() == {
        "key": "image",
        "type": "image",
        "mime_type": "image/png",
        "inline": True,
    }


def test_inline_data_uri_mime_wins() -> None:
    blob = base64.b64encode(b"1" * 64).decode()
    result = artifacts.parse_result({"out": f"data:image/gif;base64,{blob}"})
    assert result.tier == "inline"
    assert result.items[0].mime_type == "image/gif"
    assert result.items[0].type == "image"


def test_url_preferred_over_inline() -> None:
    """URL 与 inline 同时存在时只取 URL（更省库、可回源）。"""
    blob = base64.b64encode(b"1" * 64).decode()
    payload = {"data": [{"url": "https://cdn.example.com/x.png", "b64_json": blob}]}
    result = artifacts.parse_result(payload)
    assert result.tier == "known"
    assert len(result.items) == 1
    assert result.items[0].url == "https://cdn.example.com/x.png"


# ---------------------------------------------------------------------------
# 反例：不该被认成制品
# ---------------------------------------------------------------------------


def test_request_echo_subtrees_not_collected() -> None:
    """请求回显子树不能当成产出——输入图是最常见的误报源。

    注意根级裸 ``image`` 键**不在**排除名单里：响应体里的 ``image`` 绝大多数
    厂商都是产出（Ark/fal/Kling 皆然），排掉会漏报真制品。只有语义明确属于
    请求侧的容器键（``prompt``/``messages``/``input``/``parameters`` ...）才排除。
    """
    payload = {
        "prompt": "see https://ref.example.com/style.png",
        "messages": [{"image_url": "https://user-upload.example.com/ref.png"}],
        "input": {"image_url": "https://user-upload.example.com/input.png"},
        "parameters": {"reference_url": "https://user-upload.example.com/ref2.png"},
        "usage": {"generated_images": 1},
    }
    result = artifacts.parse_result(payload)
    assert result.items == []
    assert result.tier == "none"


def test_invalid_urls_rejected() -> None:
    payload = {
        "data": [
            {"url": "ftp://example.com/x.png"},
            {"url": "javascript:alert(1)"},
            {"url": "  "},
            {"url": None},
            {"url": 123},
        ]
    }
    assert artifacts.parse(payload) == []


def test_dedup_same_url() -> None:
    payload = {
        "data": [
            {"url": "https://cdn.example.com/same.png"},
            {"url": "https://cdn.example.com/same.png"},
        ]
    }
    assert len(artifacts.parse(payload)) == 1


@pytest.mark.parametrize("payload", [None, "", b"", "not json", b"\xff\xfe", 42, [], {}])
def test_malformed_never_raises(payload: object) -> None:
    """畸形输入恒不抛 —— 否则成功任务会被改判 FAILURE。"""
    result = artifacts.parse_result(payload)
    assert result.items == []
    assert result.tier == "none"


# ---------------------------------------------------------------------------
# 鲁棒性
# ---------------------------------------------------------------------------


def test_cyclic_payload_terminates() -> None:
    """自引用结构不能死循环（dict 是 Python 对象时可能成环）。"""
    node: dict[str, object] = {"url": "https://cdn.example.com/x.png"}
    node["self"] = node
    assert len(artifacts.parse({"result": node})) == 1


def test_artifact_cap_enforced() -> None:
    payload = {"data": [{"url": f"https://cdn.example.com/{i}.png"} for i in range(500)]}
    assert len(artifacts.parse(payload)) <= 64


def test_deep_nesting_terminates() -> None:
    node: dict[str, object] = {"url": "https://cdn.example.com/deep.png"}
    for _ in range(200):
        node = {"wrap": node}
    artifacts.parse(node)  # 只要不抛/不挂即可


# ---------------------------------------------------------------------------
# 落库形态
# ---------------------------------------------------------------------------


def test_parse_for_store_fields() -> None:
    payload = {"data": [{"url": "https://cdn.example.com/x.png"}]}
    patch = artifacts.parse_for_store(payload)
    assert patch == {
        "artifacts": [
            {
                "key": "image",
                "type": "image",
                "mime_type": "image/png",
                "url": "https://cdn.example.com/x.png",
            }
        ],
        "result_url": "https://cdn.example.com/x.png",
        "artifact_count": 1,
        "artifact_parser": "known",
    }


def test_parse_for_store_empty_is_explicit() -> None:
    """纯文本任务无制品：字段也要写全，看板才能区分「无」与「未解析」。"""
    patch = artifacts.parse_for_store({"choices": [{"text": "hello"}]})
    assert patch["artifacts"] == []
    assert patch["artifact_count"] == 0
    assert patch["result_url"] == ""
    assert patch["artifact_parser"] == "none"
