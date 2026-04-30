import asyncio
import os
import re
from datetime import datetime, timezone
from typing import Any, cast

import httpx
import redis.asyncio as redis
from tqdm import tqdm

REDIS_KEY = "bili:downloaded"
READY_REDIS_KEY = "bili:ready"
COOKIE_REDIS_KEY = "bili:auth:cookie"
VIDEO_REDIS_PREFIX = "bili:video:"
DOWNLOAD_LOCK_KEY = "bili:download:lock"
DOWNLOAD_LOCK_TTL_SECONDS = 7200
CLIENT_TIMEOUT = httpx.Timeout(30.0, connect=10.0, read=60.0)

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}

r = cast(Any, redis.Redis(host="localhost", port=6379, decode_responses=True))


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


async def download_file(client: httpx.AsyncClient, url: str, filename: str) -> None:
    """异步下载单个媒体文件，并保留进度条展示。"""
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))

        with (
            open(filename, "wb") as file_obj,
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                desc=os.path.basename(filename),
            ) as progress_bar,
        ):
            async for chunk in resp.aiter_bytes(chunk_size=8192):
                if chunk:
                    file_obj.write(chunk)
                    progress_bar.update(len(chunk))


async def merge_av(video_file: str, audio_file: str, output_file: str) -> None:
    """使用异步子进程调用 ffmpeg 合并音视频。"""
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        video_file,
        "-i",
        audio_file,
        "-c",
        "copy",
        output_file,
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
    print(f"[下载] BV: {bvid}")

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

        safe_title = re.sub(r'[\\/:*?"<>|]', "_", title)
        os.makedirs(safe_title, exist_ok=True)

        video_file = os.path.join(safe_title, "video.m4s")
        audio_file = os.path.join(safe_title, "audio.m4s")
        output_file = os.path.join(safe_title, safe_title + ".mp4")

        await download_file(client, video_url, video_file)
        await download_file(client, audio_url, audio_file)

    await merge_av(video_file, audio_file, output_file)
    print(f"[完成] {output_file}")


async def acquire_download_lock() -> bool:
    """避免定时任务重叠执行下载队列。"""
    return bool(await r.set(DOWNLOAD_LOCK_KEY, now_iso(), ex=DOWNLOAD_LOCK_TTL_SECONDS, nx=True))


async def release_download_lock() -> None:
    """下载任务结束后释放全局下载锁。"""
    await r.delete(DOWNLOAD_LOCK_KEY)


async def process_ready_queue() -> None:
    """扫描 Redis ready 集合，逐个下载，并更新 download 字段。"""
    cookie = await load_cookie()
    if not cookie:
        raise RuntimeError("未找到登录 Cookie，请先完成扫码登录")

    ready_bvs = sorted(await r.smembers(READY_REDIS_KEY))
    if not ready_bvs:
        print("[下载] ready 队列为空")
        return
    for bvid in ready_bvs:
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
            print(f"[错误] {bvid} 下载失败：{exc}")
            continue

        await r.sadd(REDIS_KEY, bvid)
        await r.srem(READY_REDIS_KEY, bvid)
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


async def async_main() -> None:
    """下载脚本入口：消费 ready 队列并更新 Redis 状态。"""
    locked = await acquire_download_lock()
    if not locked:
        print("[下载] 已有下载任务在执行，跳过本次运行")
        return

    try:
        await process_ready_queue()
    finally:
        await release_download_lock()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(async_main())