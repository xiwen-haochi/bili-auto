"""
FastAPI 应用入口与路由定义模块。

负责：
  - FastAPI 实例创建、生命周期管理、API 鉴权
  - 所有 HTTP 路由（登录、扫描、下载触发等）

业务逻辑委托给 bilibili_api / downloader / redis_client 等模块，
本模块仅做路由编排。
"""

import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.security.api_key import APIKeyHeader

from bili_auto.bilibili_api import (
    delete_fav_item,
    fetch_all_up_video_dynamic,
    fetch_fav_all_items,
    fetch_followings,
    fetch_latest_up_video_dynamic,
    get_media_id_by_name,
    poll_login_status_task,
    scan_fav,
)
from bili_auto.config import (
    API_CLIENT_TIMEOUT,
    API_KEY,
    AUTH_HEADERS,
    BASE_HEADERS,
    DOWNLOAD_LOCK_KEY,
    DOWNLOAD_MODE,
    LOG_DIR,
    UP_DYNAMIC_PREFIX,
)
from bili_auto.downloader import async_main as _run_downloader
from bili_auto.redis_client import (
    acquire_scan_lock,
    enqueue_ready_video,
    get_video_download_status,
    is_video_downloaded,
    load_cookie,
    load_login_state,
    r,
    release_scan_lock,
    save_login_state,
)
from bili_auto.templates import build_login_page
from bili_auto.utils import (
    build_qrcode_data_url,
    extract_bili_jct,
    fetch_json,
    now_iso,
)

# -----------------------------
# 日志配置（API 服务专用）
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bili_auto.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# -----------------------------
# FastAPI 生命周期
# -----------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    """使用 FastAPI 新版生命周期接口，在应用退出时关闭 Redis 连接。"""
    try:
        yield
    finally:
        await r.aclose()


# -----------------------------
# API 鉴权
# -----------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: str | None = Depends(_api_key_header)) -> None:
    """全局依赖：API_KEY 已配置时，校验请求头中的 X-API-Key。"""
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_api_key)])


# -----------------------------
# 路由：首页
# -----------------------------
@app.get("/")
async def index():
    """给浏览器一个简单入口，直接跳到二维码登录页。"""
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login_qrcode" />')


# -----------------------------
# 路由：登录（获取二维码）
# -----------------------------
@app.get("/login_qrcode", response_class=HTMLResponse)
async def login_qrcode(background_tasks: BackgroundTasks):
    """生成二维码页面，并在响应返回后启动后台轮询任务。"""
    api = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"

    async with httpx.AsyncClient(
        headers=AUTH_HEADERS,
        follow_redirects=True,
        timeout=API_CLIENT_TIMEOUT,
    ) as client:
        resp = await fetch_json(client, api)

    if resp.get("code") != 0:
        raise HTTPException(
            status_code=502, detail={"error": "获取二维码失败", "raw": resp}
        )

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
# 路由：轮询登录状态
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
# 路由：扫描收藏夹
# -----------------------------
@app.get("/scan_fav")
async def scan_fav_api(folder_name: str | None = None):
    """扫描收藏夹并把尚未处理的新视频写入 Redis ready 队列。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}
    if not folder_name:
        return {"error": "folder_name is required"}

    lock_token = await acquire_scan_lock()
    if not lock_token:
        return {
            "status": "busy",
            "message": "scan_fav 正在扫描中，请稍后再试",
        }

    try:
        try:
            new_items = await scan_fav(cookie, folder_name)
        except httpx.HTTPStatusError as exc:
            return {
                "status": "failed",
                "message": f"扫描收藏夹失败，B站返回 HTTP {exc.response.status_code}",
                "raw": exc.response.text[:200],
            }

        queued = []
        bili_jct = extract_bili_jct(cookie)
        async with httpx.AsyncClient(
            headers={"Cookie": cookie, **BASE_HEADERS}, timeout=30
        ) as client:
            for item in new_items:
                bv = item["bv"]
                rid = item["rid"]
                media_id = item["media_id"]
                await enqueue_ready_video(bv, folder_name)
                queued.append(bv)

                ok = await delete_fav_item(client, media_id, rid, bili_jct)
                if ok:
                    logger.info("已从收藏夹删除 BV=%s (rid=%s)", bv, rid)
                else:
                    logger.warning("删除失败 BV=%s (rid=%s)", bv, rid)

        return {"status": "ok", "queued": queued, "ready_count": len(queued)}
    finally:
        await release_scan_lock(lock_token)


# -----------------------------
# 路由：查看收藏夹内容
# -----------------------------
@app.get("/fav_items")
async def fav_items(folder_name: str):
    """获取指定收藏夹的全部内容，返回 [{bv, rid, title}]。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    data = await fetch_fav_all_items(cookie, folder_name)
    return data


# -----------------------------
# 路由：从收藏夹删除单个视频
# -----------------------------
@app.get("/fav_delete")
async def fav_delete(folder_name: str, rid: int):
    """删除指定收藏夹中的指定视频（传入 rid）。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    bili_jct = extract_bili_jct(cookie)
    if not bili_jct:
        return {"error": "cookie missing bili_jct"}

    async with httpx.AsyncClient(
        headers={"Cookie": cookie, **BASE_HEADERS}, timeout=30
    ) as client:
        nav = await fetch_json(client, "https://api.bilibili.com/x/web-interface/nav")
        uid = nav["data"]["mid"]

        media_id = await get_media_id_by_name(client, uid, folder_name)
        if not media_id:
            return {"error": f"未找到名称为《{folder_name}》的收藏夹"}

        ok = await delete_fav_item(client, media_id, rid, bili_jct)

        if ok:
            return {
                "status": "ok",
                "folder_name": folder_name,
                "rid": rid,
                "message": "删除成功",
            }
        else:
            return {
                "status": "failed",
                "folder_name": folder_name,
                "rid": rid,
                "message": "删除失败",
            }


# -----------------------------
# 路由：触发下载
# -----------------------------
@app.post("/download")
async def trigger_download(background_tasks: BackgroundTasks):
    """手动触发下载队列消费。

    执行模式由环境变量 DOWNLOAD_MODE 控制：
      bg   （默认）：将下载注册为 FastAPI BackgroundTask，接口立即返回。
      sync          ：在当前请求内 await 下载完成，接口等待全部视频处理结束后才返回。

    若下载锁已占用，直接返回 busy 状态及当前进度。
    """
    lock_held = await r.exists(DOWNLOAD_LOCK_KEY)
    if lock_held:
        progress = await r.get(DOWNLOAD_LOCK_KEY)
        return {
            "status": "busy",
            "message": "下载任务正在执行中",
            "progress": progress,
        }

    if DOWNLOAD_MODE == "sync":
        await _run_downloader()
        return {"status": "ok", "message": "下载任务已完成（sync 模式）"}

    background_tasks.add_task(_run_downloader)
    return {"status": "ok", "message": "下载任务已启动（bg 模式）"}


# -----------------------------
# 路由：下载进度查询
# -----------------------------
@app.get("/download/status")
async def download_status():
    """查询下载锁状态及当前进度。

    返回字段：
      running  : 是否有下载任务正在执行
      progress : 当前进度，格式 "{done}-{total}"，例如 "1-4"；
                 任务未运行时为 null
    """
    progress = await r.get(DOWNLOAD_LOCK_KEY)
    return {"running": bool(progress), "progress": progress}


# -----------------------------
# 路由：获取指定 UP 主的视频动态
# -----------------------------
@app.get("/up_video_dynamic_all")
async def up_video_dynamic_all(uid: int):
    """获取指定 UP 主的全部视频动态，并将 BV 写入待下载队列。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    data = await fetch_all_up_video_dynamic(uid, cookie)
    if isinstance(data, dict) and "error" in data:
        return data

    queued = []
    for item in data:
        bv = item["bv"]
        if await is_video_downloaded(bv):
            continue
        download_status = await get_video_download_status(bv)
        if download_status in ("ready", "downloading", "done"):
            continue
        await enqueue_ready_video(bv)
        queued.append(bv)

    return {
        "status": "ok",
        "uid": uid,
        "queued": queued,
        "total_fetched": len(data),
    }


# -----------------------------
# 路由：检查多个 UP 主是否有新视频动态
# -----------------------------
@app.get("/check_up_new_video")
async def check_up_new_video(uids: list[int] = Query(...)):
    """检查多个 UP 主是否有新视频动态。

    - 首次见到该 UP：记录 dynamic_id，入队 BV
    - dynamic_id 未变化：跳过
    - dynamic_id 有变化：更新 dynamic_id，入队新 BV
    """
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    summary = {}
    for uid in uids:
        redis_key = f"{UP_DYNAMIC_PREFIX}{uid}"
        stored_id = await r.get(redis_key)

        latest = await fetch_latest_up_video_dynamic(uid, cookie)
        if latest is None:
            summary[str(uid)] = {"new": False, "action": "no_dynamic"}
            continue

        dynamic_id = latest["dynamic_id"]
        bv = latest["bv"]

        if stored_id is None:
            await r.set(redis_key, dynamic_id)
            await enqueue_ready_video(bv)
            summary[str(uid)] = {
                "new": True,
                "action": "first_seen",
                "bv": bv,
                "dynamic_id": dynamic_id,
            }
        elif stored_id == dynamic_id:
            summary[str(uid)] = {"new": False, "action": "no_change"}
        else:
            await r.set(redis_key, dynamic_id)
            await enqueue_ready_video(bv)
            summary[str(uid)] = {
                "new": True,
                "action": "updated",
                "bv": bv,
                "dynamic_id": dynamic_id,
                "prev_dynamic_id": stored_id,
            }

    return {"status": "ok", "result": summary}


# -----------------------------
# 路由：获取关注的所有 UP 主
# -----------------------------
@app.get("/my_followings")
async def my_followings():
    """获取当前账号关注的所有 UP 主列表。"""
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    data = await fetch_followings(cookie)
    return data


# -----------------------------
# 路由：Cookie 保活
# -----------------------------
@app.get("/keep_alive")
async def keep_alive():
    """Cookie 保活接口。

    定期调用此接口会访问一次需要登录的 B 站接口，帮助 Cookie 续期。
    """
    cookie = await load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    headers = {"Cookie": cookie, **BASE_HEADERS}
    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=API_CLIENT_TIMEOUT,
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
# 路由：健康检查
# -----------------------------
@app.get("/health")
async def health():
    """健康检查：Redis 连接、ffmpeg 可用性、登录状态、B 站连通性。"""
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
            "ffmpeg",
            "-version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        first_line = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
        result["ffmpeg"] = (
            first_line if proc.returncode == 0 else "error: ffmpeg exited non-zero"
        )
    except FileNotFoundError:
        result["ffmpeg"] = "error: ffmpeg not found in PATH"
    except Exception as exc:
        result["ffmpeg"] = f"error: {exc}"

    # 登录状态
    cookie = await load_cookie()
    result["logged_in"] = bool(cookie)

    # B 站接口连通性（不需要登录）
    try:
        async with httpx.AsyncClient(
            headers=BASE_HEADERS, timeout=httpx.Timeout(10.0)
        ) as client:
            resp = await client.get("https://api.bilibili.com/x/web-interface/nav")
        result["bilibili_api"] = (
            "ok" if resp.status_code == 200 else f"http {resp.status_code}"
        )
    except Exception as exc:
        result["bilibili_api"] = f"error: {exc}"

    overall = (
        "ok" if all(v == "ok" or v is True for v in result.values()) else "degraded"
    )
    result["overall"] = overall
    return result


# -----------------------------
# CLI 入口：启动 uvicorn 服务
# -----------------------------
def main() -> None:
    """启动 Bili-Auto API 服务。

    监听地址和端口通过环境变量 SERVICE_HOST / SERVICE_PORT 控制，
    默认 0.0.0.0:8000。
    """
    import os

    import uvicorn

    host = os.getenv("SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("SERVICE_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
