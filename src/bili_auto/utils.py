"""
通用工具函数模块。

存放所有无副作用、不涉及具体业务逻辑的纯函数，
包括时间格式化、字符串处理、HTTP 辅助、WBI 签名等。
"""

import base64
import hashlib
import re
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, cast

import httpx
import qrcode
from qrcode import constants as qrcode_constants

from bili_auto.config import API_CLIENT_TIMEOUT, BASE_HEADERS


def now_iso() -> str:
    """统一输出 UTC 时间字符串，便于 Redis 中的状态调试与下载记录。"""
    return datetime.now(timezone.utc).isoformat()


def get_stream_url(stream: dict) -> str | None:
    """兼容 B 站字段大小写差异，提取流地址。"""
    return stream.get("baseUrl") or stream.get("base_url")


def parse_duration_text(duration_text: str) -> int:
    """将 B 站动态接口的 duration_text 字符串解析为整秒数。

    支持两种格式：
      - "mm:ss"     → 分钟:秒
      - "hh:mm:ss"  → 小时:分钟:秒
    无法解析时返回 0。

    Args:
        duration_text: B 站返回的时长字符串，例如 "12:34" 或 "1:23:45"。

    Returns:
        整秒数，例如 "12:34" → 754。
    """
    try:
        parts = duration_text.strip().split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return 0
    except (ValueError, AttributeError):
        return 0


def sanitize_title(text: str) -> str:
    """标题/作者名转安全文件名：去除首尾符号，内部符号替换为下划线，合并连续下划线。

    Args:
        text: 原始文本。

    Returns:
        安全文件名，无法解析时返回 "unknown"。
    """
    cleaned = re.sub(r"[^\w\u4e00-\u9fff]", "_", text)
    cleaned = cleaned.strip("_")
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned or "unknown"


def cookies_to_string(cookies: httpx.Cookies) -> str:
    """把 httpx 的 cookie 容器序列化成标准请求头格式。

    httpx 在存在同名 cookie 且 domain/path 不同时，直接按名称读取会抛 CookieConflict，
    这里改为遍历底层 cookie jar，并按名称保留最后一个值用于请求头。

    Args:
        cookies: httpx 客户端的 cookies 属性。

    Returns:
        以 "; " 分隔的 cookie 字符串，形如 "key1=val1; key2=val2"。
    """
    cookie_map: dict[str, str] = {}
    for cookie in cookies.jar:
        if cookie.value is not None:
            cookie_map[cookie.name] = cookie.value
    return "; ".join(f"{key}={value}" for key, value in cookie_map.items())


def extract_bili_jct(cookie: str) -> str | None:
    """从 Cookie 字符串中提取 bili_jct（CSRF token）。"""
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("bili_jct="):
            return part.split("=", 1)[1]
    return None


async def extract_bvid(url: str) -> str | None:
    """兼容 b23 短链，必要时先跟随跳转再提取 BV 号。

    Args:
        url: 视频链接（支持 b23 短链或包含 BV 号的完整链接）。

    Returns:
        提取到的 BV 号字符串，未找到则返回 None。
    """
    if "b23.tv" in url:
        async with httpx.AsyncClient(
            headers=BASE_HEADERS,
            follow_redirects=True,
            timeout=API_CLIENT_TIMEOUT,
        ) as client:
            resp = await client.get(url)
        url = str(resp.url)

    m = re.search(r"BV([a-zA-Z0-9]{10})", url)
    return "BV" + m.group(1) if m else None


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
    headers: dict[str, str] | None = None,
) -> dict:
    """统一处理 JSON 接口请求，便于复用错误检查。

    Args:
        client: 共享的 httpx AsyncClient 实例。
        url: 请求地址。
        params: URL 查询参数。
        headers: 额外的请求头（与 client 默认 headers 合并）。

    Returns:
        解析后的 JSON dict。

    Raises:
        httpx.HTTPStatusError: HTTP 状态码非 2xx 时抛出。
    """
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


# -----------------------------
# WBI 签名（B 站反爬参数）
# -----------------------------
async def get_wbi_key(client: httpx.AsyncClient) -> str:
    """从 B 站 nav 接口获取 WBI 签名所需的 img_key + sub_key。

    Args:
        client: 共享的 httpx AsyncClient 实例。

    Returns:
        img_key 与 sub_key 拼接后的字符串。
    """
    resp = await client.get("https://api.bilibili.com/x/web-interface/nav")
    data = resp.json()
    img_url = data["data"]["wbi_img"]["img_url"]
    sub_url = data["data"]["wbi_img"]["sub_url"]

    img_key = img_url.split("/")[-1].split(".")[0]
    sub_key = sub_url.split("/")[-1].split(".")[0]

    return img_key + sub_key


def wbi_sign(params: dict, wbi_key: str) -> dict:
    """对请求参数进行 WBI 签名，原地修改并返回传入的 dict。

    签名流程：
      1. 添加 wts（Unix 时间戳）
      2. 参数按 key 排序拼接
      3. 拼接 wbi_key 后计算 MD5，结果填入 w_rid

    Args:
        params: 待签名的请求参数字典（会被原地修改）。
        wbi_key: 从 get_wbi_key() 获取的签名密钥。

    Returns:
        签名后的 params dict（与传入的是同一个对象）。
    """
    params["wts"] = int(time.time())
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    md5 = hashlib.md5()
    md5.update((sorted_params + wbi_key).encode("utf-8"))
    params["w_rid"] = md5.hexdigest()
    return params


# -----------------------------
# 二维码生成
# -----------------------------
def build_qrcode_data_url(qrcode_url: str) -> str:
    """在服务端直接生成二维码图片，返回 data URL 供浏览器原样展示。

    Args:
        qrcode_url: B 站返回的二维码内容 URL。

    Returns:
        base64 编码的 PNG data URL。
    """
    qr = qrcode.QRCode(error_correction=qrcode_constants.ERROR_CORRECT_L)
    qr.add_data(qrcode_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buffer = BytesIO()
    cast(Any, img).save(buffer, "PNG")
    img_base64 = base64.b64encode(buffer.getvalue()).decode()
    return "data:image/png;base64," + img_base64


# -----------------------------
# 码流选择
# -----------------------------
def select_best_video_stream(videos: list[dict]) -> dict:
    """按清晰度 ID、分辨率和码率选择最高可用视频流。

    Args:
        videos: B 站 dash.video 数组。

    Returns:
        最优视频流 dict。
    """
    return max(
        videos,
        key=lambda item: (
            int(item.get("id", 0)),
            int(item.get("height", 0)),
            int(item.get("bandwidth", 0)),
        ),
    )


def select_best_audio_stream(audios: list[dict]) -> dict:
    """音频流按码率选择最高可用档位。

    Args:
        audios: B 站 dash.audio 数组。

    Returns:
        最优音频流 dict。
    """
    return max(audios, key=lambda item: int(item.get("bandwidth", 0)))
