import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from secrets import token_hex
from typing import Any, cast

import httpx
import qrcode
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Security
from fastapi.responses import HTMLResponse
from fastapi.security.api_key import APIKeyHeader
from qrcode import constants as qrcode_constants

from bili_auto.downloader import (
    DOWNLOAD_LOCK_KEY,
    async_main as _run_downloader,
    r as downloader_r,
)

load_dotenv()

# -----------------------------
# 配置
# -----------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
API_KEY = os.getenv("API_KEY", "")

REDIS_KEY = os.getenv("REDIS_KEY", "bili:downloaded")
COOKIE_REDIS_KEY = os.getenv("COOKIE_REDIS_KEY", "bili:auth:cookie")
LOGIN_REDIS_PREFIX = os.getenv("LOGIN_REDIS_PREFIX", "bili:login:")
VIDEO_REDIS_PREFIX = os.getenv("VIDEO_REDIS_PREFIX", "bili:video:")
SCAN_FAV_LOCK_KEY = os.getenv("SCAN_FAV_LOCK_KEY", "bili:scan_fav:lock")
SCAN_FAV_LOCK_TTL_SECONDS = int(os.getenv("SCAN_FAV_LOCK_TTL_SECONDS", "1800"))
LOGIN_POLL_INTERVAL_SECONDS = int(os.getenv("LOGIN_POLL_INTERVAL_SECONDS", "10"))
LOGIN_MAX_POLLS = int(os.getenv("LOGIN_MAX_POLLS", "5"))
LOGIN_KEY_TTL_SECONDS = int(os.getenv("LOGIN_KEY_TTL_SECONDS", "600"))
CLIENT_TIMEOUT = httpx.Timeout(30.0, connect=10.0, read=60.0)

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bili_auto.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

r = cast(Any, redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=True))


@asynccontextmanager
async def lifespan(_: FastAPI):
    """使用 FastAPI 新版生命周期接口，在应用退出时关闭 Redis 连接。"""
    try:
        yield
    finally:
        await r.aclose()
        await downloader_r.aclose()


# -----------------------------
# API 鉴权
# -----------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: str | None = Security(_api_key_header)) -> None:
    """全局依赖：API_KEY 已配置时，校验请求头中的 X-API-Key。"""
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_api_key)])

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}

AUTH_HEADERS = {
    **BASE_HEADERS,
    "Origin": "https://www.bilibili.com",
}


# -----------------------------
# 工具函数
# -----------------------------
def now_iso() -> str:
    """统一输出 UTC 时间字符串，便于 Redis 中的状态调试。"""
    return datetime.now(timezone.utc).isoformat()


def login_state_key(qrcode_key: str) -> str:
    """拼出二维码登录状态在 Redis 中对应的 key。"""
    return f"{LOGIN_REDIS_PREFIX}{qrcode_key}"


def video_state_key(bvid: str) -> str:
    """拼出单个视频在 Redis 中的元数据 key。"""
    return f"{VIDEO_REDIS_PREFIX}{bvid}"


async def save_login_state(qrcode_key: str, mapping: dict[str, str]) -> None:
    """将二维码状态写入 Redis，并刷新过期时间。"""
    key = login_state_key(qrcode_key)
    await r.hset(key, mapping=mapping)
    await r.expire(key, LOGIN_KEY_TTL_SECONDS)


async def load_login_state(qrcode_key: str) -> dict[str, str] | None:
    """读取 Redis 中的二维码状态，不存在时返回 None。"""
    data = await r.hgetall(login_state_key(qrcode_key))
    return data or None


async def save_cookie(cookie: str) -> None:
    """将当前登录态仅保存到 Redis，不再落地本地文件。"""
    await r.set(COOKIE_REDIS_KEY, cookie)


async def load_cookie() -> str | None:
    """从 Redis 读取当前登录 Cookie。"""
    return await r.get(COOKIE_REDIS_KEY)


async def acquire_scan_lock() -> str | None:
    """尝试获取扫描锁，成功时返回本次锁令牌，失败时返回 None。"""
    lock_token = token_hex(16)
    locked = await r.set(SCAN_FAV_LOCK_KEY, lock_token, ex=SCAN_FAV_LOCK_TTL_SECONDS, nx=True)
    return lock_token if locked else None


async def release_scan_lock(lock_token: str) -> None:
    """只在当前请求仍持有锁时释放，避免误删其他请求的新锁。"""
    current_token = await r.get(SCAN_FAV_LOCK_KEY)
    if current_token == lock_token:
        await r.delete(SCAN_FAV_LOCK_KEY)


def cookies_to_string(cookies: httpx.Cookies) -> str:
    """把 httpx 的 cookie 容器序列化成标准请求头格式。

    httpx 在存在同名 cookie 且 domain/path 不同时，直接按名称读取会抛 CookieConflict，
    这里改为遍历底层 cookie jar，并按名称保留最后一个值用于请求头。
    """
    cookie_map: dict[str, str] = {}
    for cookie in cookies.jar:
        if cookie.value is not None:
            cookie_map[cookie.name] = cookie.value
    return "; ".join(f"{key}={value}" for key, value in cookie_map.items())


def get_stream_url(stream: dict) -> str | None:
    """兼容 B 站字段大小写差异，提取流地址。"""
    return stream.get("baseUrl") or stream.get("base_url")


def build_qrcode_data_url(qrcode_url: str) -> str:
    """在服务端直接生成二维码图片，浏览器可以原样展示。"""
    qr = qrcode.QRCode(error_correction=qrcode_constants.ERROR_CORRECT_L)
    qr.add_data(qrcode_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buffer = BytesIO()
    cast(Any, img).save(buffer, "PNG")
    img_base64 = base64.b64encode(buffer.getvalue()).decode()
    return "data:image/png;base64," + img_base64


def build_login_page(qrcode_key: str, qrcode_data_url: str) -> str:
    """返回一个可直接扫码的页面，前端无需再自己拼图片。"""
    qrcode_key_json = json.dumps(qrcode_key)
    return f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>B 站扫码登录</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: linear-gradient(135deg, #f7efe6 0%, #d8ecff 100%);
      font-family: "PingFang SC", "Hiragino Sans GB", sans-serif;
      color: #1f2937;
    }}
    .panel {{
      width: min(92vw, 420px);
      padding: 28px;
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 20px 50px rgba(15, 23, 42, 0.15);
      text-align: center;
      backdrop-filter: blur(12px);
    }}
    img {{
      width: min(72vw, 280px);
      height: min(72vw, 280px);
      border-radius: 16px;
      background: #fff;
      padding: 12px;
      box-sizing: border-box;
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 26px;
    }}
    p {{
      margin: 8px 0;
      line-height: 1.6;
    }}
    .meta {{
      color: #4b5563;
      font-size: 14px;
      word-break: break-all;
    }}
    .status {{
      margin-top: 18px;
      padding: 14px;
      border-radius: 14px;
      background: #f8fafc;
    }}
  </style>
</head>
<body>
  <main class=\"panel\">
    <h1>B 站扫码登录</h1>
    <p>请直接使用 B 站 App 扫码，服务端会在后台每 10 秒轮询一次登录状态。</p>
    <img src=\"{qrcode_data_url}\" alt=\"B 站登录二维码\" />
    <div class=\"status\">
      <p id=\"status\">状态：等待扫码</p>
      <p id=\"message\">说明：二维码已生成，后台轮询已启动。</p>
      <p id=\"poll-count\" class=\"meta\">轮询次数：0 / {LOGIN_MAX_POLLS}</p>
      <p class=\"meta\">二维码 Key：{qrcode_key}</p>
    </div>
  </main>
  <script>
    const qrcodeKey = {qrcode_key_json};
    const statusEl = document.getElementById("status");
    const messageEl = document.getElementById("message");
    const pollCountEl = document.getElementById("poll-count");

    async function refreshLoginStatus() {{
      try {{
        const response = await fetch(`/login_poll?qrcode_key=${{encodeURIComponent(qrcodeKey)}}`, {{ cache: "no-store" }});
        const data = await response.json();

        statusEl.textContent = `状态：${{data.status || "unknown"}}`;
        messageEl.textContent = `说明：${{data.message || "暂无状态说明"}}`;
        pollCountEl.textContent = `轮询次数：${{data.poll_count || 0}} / {LOGIN_MAX_POLLS}`;

        if (["success", "expired", "failed", "not_found"].includes(data.status)) {{
          return;
        }}
      }} catch (error) {{
        messageEl.textContent = `说明：状态查询失败，${{error}}`;
      }}

      setTimeout(refreshLoginStatus, 2000);
    }}

    refreshLoginStatus();
  </script>
</body>
</html>
"""


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
    headers: dict[str, str] | None = None,
) -> dict:
    """统一处理 JSON 接口请求，便于复用错误检查。"""
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


async def extract_bvid(url: str):
    """兼容 b23 短链，必要时先跟随跳转再提取 BV 号。"""
    if "b23.tv" in url:
        async with httpx.AsyncClient(
            headers=BASE_HEADERS,
            follow_redirects=True,
            timeout=CLIENT_TIMEOUT,
        ) as client:
            resp = await client.get(url)
        url = str(resp.url)

    m = re.search(r"BV([a-zA-Z0-9]{10})", url)
    return "BV" + m.group(1) if m else None


async def poll_login_status_task(qrcode_key: str) -> None:
    """后台轮询 B 站登录状态，成功后把 Cookie 存入 Redis。"""
    poll_url = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"

    for attempt in range(1, LOGIN_MAX_POLLS + 1):
        current_state = await load_login_state(qrcode_key)
        if not current_state:
            return

        if current_state.get("status") in {"success", "expired", "failed"}:
            return

        async with httpx.AsyncClient(
            headers=AUTH_HEADERS,
            follow_redirects=True,
            timeout=CLIENT_TIMEOUT,
        ) as client:
            try:
                data = await fetch_json(client, poll_url, params={"qrcode_key": qrcode_key})
            except Exception as exc:
                await save_login_state(
                    qrcode_key,
                    {
                        "status": "failed",
                        "message": f"轮询异常：{exc}",
                        "poll_count": str(attempt),
                        "updated_at": now_iso(),
                    },
                )
                return

        poll_code = data.get("data", {}).get("code")

        if poll_code in (-2, 86101):
            await save_login_state(
                qrcode_key,
                {
                    "status": "pending",
                    "message": "等待扫码",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
        elif poll_code in (-4, 86090):
            await save_login_state(
                qrcode_key,
                {
                    "status": "pending",
                    "message": "已扫码，等待手机确认",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
        elif poll_code == 0:
            login_url = data["data"]["url"]
            async with httpx.AsyncClient(
                headers=AUTH_HEADERS,
                follow_redirects=True,
                timeout=CLIENT_TIMEOUT,
            ) as client:
                await client.get(login_url)
                cookie_str = cookies_to_string(client.cookies)

            await save_cookie(cookie_str)
            await save_login_state(
                qrcode_key,
                {
                    "status": "success",
                    "message": "登录成功，Cookie 已写入 Redis",
                    "poll_count": str(attempt),
                    "cookie": cookie_str,
                    "updated_at": now_iso(),
                },
            )
            return
        elif poll_code in (-5, 86038):
            await save_login_state(
                qrcode_key,
                {
                    "status": "expired",
                    "message": "二维码已过期，请刷新页面重新获取",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
            return
        else:
            await save_login_state(
                qrcode_key,
                {
                    "status": "failed",
                    "message": f"未知登录状态：{poll_code}",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
            return

        if attempt < LOGIN_MAX_POLLS:
            await asyncio.sleep(LOGIN_POLL_INTERVAL_SECONDS)

    await save_login_state(
        qrcode_key,
        {
            "status": "expired",
            "message": "轮询次数已达上限，请重新获取二维码",
            "poll_count": str(LOGIN_MAX_POLLS),
            "updated_at": now_iso(),
        },
    )


# -----------------------------
# 登录：获取二维码
# -----------------------------
@app.get("/")
async def index():
    """给浏览器一个简单入口，直接跳到二维码登录页。"""
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login_qrcode" />')


@app.get("/login_qrcode", response_class=HTMLResponse)
async def login_qrcode(background_tasks: BackgroundTasks):
    """生成二维码页面，并在响应返回后启动后台轮询任务。"""
    api = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"

    async with httpx.AsyncClient(
        headers=AUTH_HEADERS,
        follow_redirects=True,
        timeout=CLIENT_TIMEOUT,
    ) as client:
        resp = await fetch_json(client, api)

    if resp.get("code") != 0:
        raise HTTPException(status_code=502, detail={"error": "获取二维码失败", "raw": resp})

    qrcode_url = resp["data"]["url"]
    qrcode_key = resp["data"]["qrcode_key"]

    await save_login_state(
        qrcode_key,
        {
            "status": "pending",
            "message": "二维码已生成，等待扫码",
            "poll_count": "0",
            "cookie": "",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        },
    )

    background_tasks.add_task(poll_login_status_task, qrcode_key)
    qrcode_data_url = build_qrcode_data_url(qrcode_url)
    return HTMLResponse(build_login_page(qrcode_key, qrcode_data_url))


# -----------------------------
# 登录：轮询二维码状态
# -----------------------------
@app.get("/login_poll")
async def login_poll(qrcode_key: str):
    """读取 Redis 中的轮询结果，前端无需自己再访问 B 站接口。"""
    state = await load_login_state(qrcode_key)
    if not state:
        return {"status": "not_found", "message": "二维码状态不存在或已过期"}

    return {
        "status": state.get("status", "unknown"),
        "message": state.get("message", ""),
        "poll_count": int(state.get("poll_count", "0")),
        "created_at": state.get("created_at", ""),
        "updated_at": state.get("updated_at", ""),
        "has_cookie": bool(state.get("cookie")),
    }


# -----------------------------
# 扫描收藏夹
# -----------------------------

async def get_wbi_key(client):
    resp = await client.get("https://api.bilibili.com/x/web-interface/nav")
    data = resp.json()
    img_url = data["data"]["wbi_img"]["img_url"]
    sub_url = data["data"]["wbi_img"]["sub_url"]

    img_key = img_url.split("/")[-1].split(".")[0]
    sub_key = sub_url.split("/")[-1].split(".")[0]

    return img_key + sub_key

def wbi_sign(params: dict, wbi_key: str):
    # 1. 添加 wts
    params["wts"] = int(time.time())

    # 2. 参数按 key 排序
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

    # 3. 拼接 key 做 MD5
    md5 = hashlib.md5()
    md5.update((sorted_params + wbi_key).encode("utf-8"))
    params["w_rid"] = md5.hexdigest()

    return params



async def scan_fav(cookie: str, folder_name: str | None = None) -> list[str]:
    headers = {"Cookie": cookie, **BASE_HEADERS}

    async with httpx.AsyncClient(headers=headers, timeout=CLIENT_TIMEOUT) as client:
        # 获取 WBI key
        wbi_key = await get_wbi_key(client)

        # 获取 UID
        nav_resp = await fetch_json(client, "https://api.bilibili.com/x/web-interface/nav")
        uid = nav_resp["data"]["mid"]

        # 获取收藏夹列表
        fav_resp = await fetch_json(
            client,
            f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={uid}",
        )
        favs = fav_resp["data"]["list"]

        # -----------------------------
        # ⭐ 过滤收藏夹（新增逻辑）
        # -----------------------------
        if folder_name:
            favs = [f for f in favs if f["title"] == folder_name]
            if not favs:
                logger.info("未找到名称为《%s》的收藏夹", folder_name)
                return []

        new_bvs = []
        seen = set()

        for fav in favs:
            media_id = fav["id"]
            pn = 1

            while True:
                params = {"media_id": media_id, "pn": pn, "ps": 20}
                params = wbi_sign(params, wbi_key)

                resp = await fetch_json(
                    client,
                    "https://api.bilibili.com/x/v3/fav/resource/list",
                    params=params,
                )

                medias = resp["data"]["medias"]
                if not medias:
                    break

                for m in medias:
                    bv = m["bv_id"]
                    if bv and bv not in seen:
                        seen.add(bv)
                        if await r.sismember(REDIS_KEY, bv):
                            continue
                        download_status = await r.hget(video_state_key(bv), "download")
                        if download_status in ("ready", "downloading"):
                            continue
                        new_bvs.append(bv)

                pn += 1

    return new_bvs


async def enqueue_ready_video(bvid: str) -> None:
    """把待下载视频写入 Redis hash，并初始化下载状态字段。"""
    now = now_iso()
    await r.hset(
        video_state_key(bvid),
        mapping={
            "bvid": bvid,
            "download": "ready",
            "created_at": now,
            "updated_at": now,
        },
    )


@app.get("/scan_fav")
async def scan_fav_api(folder_name: str | None = None):
    """扫描收藏夹并把尚未处理的新视频写入 Redis ready 队列。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    lock_token = await acquire_scan_lock()
    if not lock_token:
        return {
            "status": "busy",
            "message": "scan_fav 正在扫描中，请稍后再试",
        }

    try:
        try:
            new_bvs = await scan_fav(cookie, folder_name)
        except httpx.HTTPStatusError as exc:
            return {
                "status": "failed",
                "message": f"扫描收藏夹失败，B站返回 HTTP {exc.response.status_code}",
                "raw": exc.response.text[:200],
            }

        queued = []
        for bv in new_bvs:
            await enqueue_ready_video(bv)
            queued.append(bv)

        return {"status": "ok", "queued": queued, "ready_count": len(queued)}
    finally:
        await release_scan_lock(lock_token)


# -----------------------------
# 下载触发接口
# -----------------------------
@app.post("/download")
async def trigger_download(background_tasks: BackgroundTasks):
    """
    手动触发下载队列消费。
    若下载锁已占用，返回 busy；否则启动后台下载任务。
    """
    lock_held = await r.exists(DOWNLOAD_LOCK_KEY)
    if lock_held:
        lock_since = await r.get(DOWNLOAD_LOCK_KEY)
        return {"status": "busy", "message": "下载任务正在执行中", "lock_since": lock_since}
    background_tasks.add_task(_run_downloader)
    return {"status": "ok", "message": "下载任务已启动"}


@app.get("/download/status")
async def download_status():
    """Query 下载锁状态，可用于确认下载任务是否正在运行。"""
    lock_since = await r.get(DOWNLOAD_LOCK_KEY)
    return {"running": bool(lock_since), "lock_since": lock_since}


# -----------------------------
# Cookie 保活接口（定期调用）
# -----------------------------
@app.get("/keep_alive")
async def keep_alive():
    """
    你可以用定时任务定期请求这个接口，
    它会访问一次需要登录的接口，帮助 Cookie 续期。
    """
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    headers = {"Cookie": cookie, **BASE_HEADERS}
    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=CLIENT_TIMEOUT,
    ) as client:
        response = await client.get("https://api.bilibili.com/x/web-interface/nav")

    try:
        data = response.json()
    except Exception:
        return {"status": "failed", "raw": response.text[:200]}

    if data.get("code") == 0:
        return {"status": "ok", "uname": data["data"]["uname"]}
    else:
        return {"status": "failed", "data": data}


# -----------------------------
# 健康检查
# -----------------------------
@app.get("/health")
async def health():
    """检查 Redis 连接、ffmpeg 可用性以及登录状态。"""
    result: dict = {}

    # Redis 连通性
    try:
        await r.ping()
        result["redis"] = "ok"
    except Exception as exc:
        result["redis"] = f"error: {exc}"

    # ffmpeg 可用性
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        first_line = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
        result["ffmpeg"] = first_line if proc.returncode == 0 else "error: ffmpeg exited non-zero"
    except FileNotFoundError:
        result["ffmpeg"] = "error: ffmpeg not found in PATH"
    except Exception as exc:
        result["ffmpeg"] = f"error: {exc}"

    # 登录状态
    cookie = await load_cookie()
    result["logged_in"] = bool(cookie)

    # B 站接口连通性（不需要登录）
    try:
        async with httpx.AsyncClient(headers=BASE_HEADERS, timeout=httpx.Timeout(10.0)) as client:
            resp = await client.get("https://api.bilibili.com/x/web-interface/nav")
        result["bilibili_api"] = "ok" if resp.status_code == 200 else f"http {resp.status_code}"
    except Exception as exc:
        result["bilibili_api"] = f"error: {exc}"

    overall = "ok" if all(v == "ok" or v is True for v in result.values()) else "degraded"
    return {"status": overall, **result}


# -----------------------------
# 启动
# -----------------------------
def main() -> None:
    """CLI 入口：启动 FastAPI 服务。"""
    import uvicorn

    host = os.getenv("SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("SERVICE_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
