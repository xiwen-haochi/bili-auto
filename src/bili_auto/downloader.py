import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import httpx
import redis.asyncio as redis
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

REDIS_KEY = os.getenv("REDIS_KEY", "bili:downloaded")
COOKIE_REDIS_KEY = os.getenv("COOKIE_REDIS_KEY", "bili:auth:cookie")
VIDEO_REDIS_PREFIX = os.getenv("VIDEO_REDIS_PREFIX", "bili:video:")
DOWNLOAD_LOCK_KEY = os.getenv("DOWNLOAD_LOCK_KEY", "bili:download:lock")
DOWNLOAD_LOCK_TTL_SECONDS = int(os.getenv("DOWNLOAD_LOCK_TTL_SECONDS", "7200"))
VIDEO_DONE_TTL_SECONDS = int(os.getenv("VIDEO_DONE_TTL_SECONDS", "10800"))
MAX_DOWNLOADS_PER_RUN = int(os.getenv("MAX_DOWNLOADS_PER_RUN", "10"))
DOWNLOAD_INTERVAL_SECONDS = int(os.getenv("DOWNLOAD_INTERVAL_SECONDS", "3"))
CLIENT_TIMEOUT = httpx.Timeout(connect=10.0, read=None, write=30.0, pool=30.0)
MAX_DOWNLOAD_RETRIES = int(os.getenv("MAX_DOWNLOAD_RETRIES", "5"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads"))

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "downloader.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}

r = cast(Any, redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=True))


def now_iso() -> str:
    """统一输出 UTC 时间字符串，便于记录下载状态。"""
    return datetime.now(timezone.utc).isoformat()


def video_state_key(bvid: str) -> str:
    """拼出单个视频在 Redis 中的元数据 key。"""
    return f"{VIDEO_REDIS_PREFIX}{bvid}"


async def load_cookie() -> str | None:
    """从 Redis 读取当前登录 Cookie。"""
    return await r.get(COOKIE_REDIS_KEY)


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
) -> dict:
    """统一处理 JSON 接口请求，便于复用错误检查。"""
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_stream_url(stream: dict) -> str | None:
    """兼容 B 站字段大小写差异，提取流地址。"""
    return stream.get("baseUrl") or stream.get("base_url")


def sanitize_title(text: str) -> str:
    """标题/作者名转安全文件名：去除首尾符号，内部符号替换为下划线，合并连续下划线。"""
    cleaned = re.sub(r'[^\w\u4e00-\u9fff]', '_', text)
    cleaned = cleaned.strip('_')
    cleaned = re.sub(r'_+', '_', cleaned)
    return cleaned or "unknown"


def select_best_video_stream(videos: list[dict]) -> dict:
    """按清晰度 ID、分辨率和码率选择最高可用视频流。"""
    return max(
        videos,
        key=lambda item: (
            int(item.get("id", 0)),
            int(item.get("height", 0)),
            int(item.get("bandwidth", 0)),
        ),
    )


def select_best_audio_stream(audios: list[dict]) -> dict:
    """音频流按码率选择最高可用档位。"""
    return max(audios, key=lambda item: int(item.get("bandwidth", 0)))


async def download_file(client: httpx.AsyncClient, url: str, dest: Path) -> None:
    """异步下载单个媒体文件，支持断点续传，失败自动重试。"""
    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        downloaded = dest.stat().st_size if dest.exists() else 0
        extra_headers = {"Range": f"bytes={downloaded}-"} if downloaded > 0 else {}

        try:
            async with client.stream("GET", url, headers=extra_headers) as resp:
                if resp.status_code == 416:
                    # 服务端认为已完整，直接返回
                    return
                resp.raise_for_status()
                total = downloaded + int(resp.headers.get("content-length", 0))

                with (
                    dest.open("ab" if downloaded > 0 else "wb") as file_obj,
                    tqdm(
                        total=total,
                        initial=downloaded,
                        unit="B",
                        unit_scale=True,
                        desc=dest.name,
                    ) as progress_bar,
                ):
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        if chunk:
                            file_obj.write(chunk)
                            progress_bar.update(len(chunk))
            return  # 下载完成
        except httpx.TransportError as exc:
            # httpx.TransportError 是所有传输层错误的基类，包含：
            #   - NetworkError (ConnectError / ReadError / WriteError)
            #   - ProtocolError (RemoteProtocolError / LocalProtocolError)
            #   - TimeoutException (ConnectTimeout / ReadTimeout / WriteTimeout / PoolTimeout)
            # 大文件下载耗时较长，CDN / 中间代理可能在传输途中重置连接或触发超时，
            # 统一用基类捕获可确保所有情况都能触发断点续传重试。
            if attempt == MAX_DOWNLOAD_RETRIES:
                raise
            wait = 2 ** attempt
            logger.warning("下载中断（第 %d/%d 次），%d 秒后续传：%s", attempt, MAX_DOWNLOAD_RETRIES, wait, exc)
            await asyncio.sleep(wait)


async def merge_av(video_file: Path, audio_file: Path, output_file: Path) -> None:
    """使用异步子进程调用 ffmpeg 合并音视频。"""
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(video_file),
        "-i",
        str(audio_file),
        "-c",
        "copy",
        str(output_file),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.communicate()
    if process.returncode != 0:
        raise RuntimeError("ffmpeg 合并失败，请确认本机已安装 ffmpeg")


async def mark_video_status(bvid: str, mapping: dict[str, str]) -> None:
    """更新 Redis 中单个视频的下载状态。"""
    await r.hset(video_state_key(bvid), mapping=mapping)


async def download_bv(bvid: str, cookie: str) -> None:
    """下载指定 BV 视频，并自动选择当前可用的最高画质。"""
    logger.info("开始下载 BV: %s", bvid)

    headers = {"Cookie": cookie, **BASE_HEADERS}

    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=CLIENT_TIMEOUT,
    ) as client:
        view_resp = await fetch_json(
            client,
            "https://api.bilibili.com/x/web-interface/view",
            params={"bvid": bvid},
        )

        if view_resp.get("code") != 0:
            raise RuntimeError(f"获取视频信息失败：{view_resp}")

        view = view_resp["data"]
        title = view["title"]
        author = view["owner"]["name"]
        cid = view["pages"][0]["cid"]

        # 先请求高档清晰度，再从服务端返回的候选流中选最高一档。
        play_resp = await fetch_json(
            client,
            "https://api.bilibili.com/x/player/playurl",
            params={"bvid": bvid, "cid": cid, "qn": 127, "fnval": 16, "fourk": 1},
        )

        if play_resp.get("code") != 0:
            raise RuntimeError(f"获取播放地址失败：{play_resp}")

        dash = play_resp.get("data", {}).get("dash", {})
        videos = dash.get("video") or []
        audios = dash.get("audio") or []

        if not videos or not audios:
            raise RuntimeError(f"当前视频未返回 DASH 音视频流：{play_resp}")

        best_video = select_best_video_stream(videos)
        best_audio = select_best_audio_stream(audios)
        video_url = get_stream_url(best_video)
        audio_url = get_stream_url(best_audio)

        if not video_url or not audio_url:
            raise RuntimeError(f"无法解析音视频下载地址：{play_resp}")

        safe_author = sanitize_title(author)
        clean_title = sanitize_title(title)
        video_dir = DOWNLOAD_DIR / safe_author
        video_dir.mkdir(parents=True, exist_ok=True)

        video_file = video_dir / f"{bvid}_video.m4s"
        audio_file = video_dir / f"{bvid}_audio.m4s"
        output_file = video_dir / f"{clean_title}.mp4"

        await download_file(client, video_url, video_file)
        await download_file(client, audio_url, audio_file)

    await merge_av(video_file, audio_file, output_file)
    video_file.unlink(missing_ok=True)
    audio_file.unlink(missing_ok=True)
    logger.info("下载完成: %s", output_file)


async def acquire_download_lock() -> bool:
    """避免定时任务重叠执行下载队列。"""
    return bool(await r.set(DOWNLOAD_LOCK_KEY, now_iso(), ex=DOWNLOAD_LOCK_TTL_SECONDS, nx=True))


async def release_download_lock() -> None:
    """下载任务结束后释放全局下载锁。"""
    await r.delete(DOWNLOAD_LOCK_KEY)


async def process_ready_queue() -> None:
    """扫描 bili:video:* hash，找出 download=ready 的条目逐个下载，每次最多处理 MAX_DOWNLOADS_PER_RUN 个。"""
    cookie = await load_cookie()
    if not cookie:
        raise RuntimeError("未找到登录 Cookie，请先完成扫码登录")

    ready_bvs: list[str] = []
    async for key in r.scan_iter(f"{VIDEO_REDIS_PREFIX}*"):
        status = await r.hget(key, "download")
        if status == "ready":
            ready_bvs.append(key[len(VIDEO_REDIS_PREFIX):])
    ready_bvs.sort()

    if not ready_bvs:
        logger.info("ready 队列为空，无需下载")
        return

    batch = ready_bvs[:MAX_DOWNLOADS_PER_RUN]
    logger.info("本次共 %d 个待下载视频，最多处理 %d 个", len(ready_bvs), len(batch))

    for idx, bvid in enumerate(batch):
        await mark_video_status(
            bvid,
            {
                "bvid": bvid,
                "download": "downloading",
                "updated_at": now_iso(),
            },
        )

        try:
            await download_bv(bvid, cookie)
        except Exception as exc:
            await mark_video_status(
                bvid,
                {
                    "bvid": bvid,
                    "download": "failed",
                    "error": str(exc),
                    "updated_at": now_iso(),
                },
            )
            logger.error("%s 下载失败：%s", bvid, exc)
            if idx < len(batch) - 1:
                await asyncio.sleep(DOWNLOAD_INTERVAL_SECONDS)
            continue

        await r.sadd(REDIS_KEY, bvid)
        await mark_video_status(
            bvid,
            {
                "bvid": bvid,
                "download": "done",
                "error": "",
                "downloaded_at": now_iso(),
                "updated_at": now_iso(),
            },
        )
        await r.expire(video_state_key(bvid), VIDEO_DONE_TTL_SECONDS)

        if idx < len(batch) - 1:
            await asyncio.sleep(DOWNLOAD_INTERVAL_SECONDS)


async def async_main() -> None:
    """消费 ready 队列并更新 Redis 状态。可单独运行也可由 API 服务触发。"""
    locked = await acquire_download_lock()
    if not locked:
        logger.info("已有下载任务在执行，跳过本次运行")
        return

    try:
        await process_ready_queue()
    finally:
        await release_download_lock()


def main() -> None:
    """CLI 入口：运行下载队列消费脚本。"""

    async def _run() -> None:
        try:
            await async_main()
        finally:
            await r.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()