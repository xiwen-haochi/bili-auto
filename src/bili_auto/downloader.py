import asyncio
import logging
import os
import re
import sys
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
# DOWNLOAD_MODE：控制 async_main() 的执行方式。
#   bg   （默认）：启动后台 asyncio task 后立即返回，适合从 API 服务触发。
#   sync          ：原地等待全部下载完成再返回，适合 CLI 直接运行。
DOWNLOAD_MODE = os.getenv("DOWNLOAD_MODE", "bg")


def _parse_size(value: str) -> int:
    """解析人类可读的文件大小字符串，返回字节数。

    支持单位后缀（不区分大小写）：
      - g / gb  → GiB（× 1024³）
      - m / mb  → MiB（× 1024²）
      - k / kb  → KiB（× 1024）
      - 无后缀  → 字节

    示例：
      "1g"   → 1073741824
      "500m" → 524288000
      "2048" → 2048

    Args:
        value: 待解析的字符串，例如 "1g"、"500m"、"1073741824"。

    Returns:
        对应的字节数（int）。

    Raises:
        ValueError: 当字符串格式无法识别时抛出。
    """
    value = value.strip().lower()
    # 依次尝试 gb/g、mb/m、kb/k 后缀，最后回落到纯数字
    for suffix, factor in (
        ("gb", 1024**3),
        ("g", 1024**3),
        ("mb", 1024**2),
        ("m", 1024**2),
        ("kb", 1024),
        ("k", 1024),
    ):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * factor)
    return int(value)


# MAX_MP4_SIZE：合并后单个 mp4 的最大字节数；超出则自动分割为多段。
# 未设置时不分割。示例：MAX_MP4_SIZE=1g、MAX_MP4_SIZE=500m、MAX_MP4_SIZE=1073741824
_MAX_MP4_SIZE_STR = os.getenv("MAX_MP4_SIZE")
MAX_MP4_SIZE: int | None = _parse_size(_MAX_MP4_SIZE_STR) if _MAX_MP4_SIZE_STR else None

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # 显式指定 stdout，避免默认的 stderr 在部分终端/进程管理器中被屏蔽
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "downloader.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}

r = cast(
    Any,
    redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    ),
)


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
    cleaned = re.sub(r"[^\w\u4e00-\u9fff]", "_", text)
    cleaned = cleaned.strip("_")
    cleaned = re.sub(r"_+", "_", cleaned)
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

        extra_headers = {
            "Range": f"bytes={downloaded}-",
            "Connection": "close",  # ⭐ 强制短连接
            "User-Agent": "Mozilla/5.0",  # ⭐ 必须
            "Accept": "*/*",
            "Referer": "https://www.bilibili.com",
        }

        try:
            async with client.stream("GET", url, headers=extra_headers) as resp:
                if resp.status_code == 416:
                    return

                if resp.status_code in (429, 503):
                    await asyncio.sleep(5)
                    continue

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
                    async for chunk in resp.aiter_bytes(chunk_size=16384):  # ⭐ 更稳定
                        if chunk:
                            file_obj.write(chunk)
                            progress_bar.update(len(chunk))

            return

        except httpx.TransportError as exc:
            if attempt == MAX_DOWNLOAD_RETRIES:
                raise

            wait = min(2**attempt, 60)
            logger.warning(
                "下载中断（第 %d/%d 次），%d 秒后续传：%s",
                attempt,
                MAX_DOWNLOAD_RETRIES,
                wait,
                exc,
            )
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


async def _probe_duration(path: Path) -> float:
    """用 ffprobe 读取媒体文件的总时长（秒）。

    使用 ffprobe 的 `-show_entries format=duration` 输出纯数字，无需解析 JSON，
    比调用 ffmpeg 本身更轻量（类比 Python 中只 import 需要的模块）。

    Args:
        path: 待探测的媒体文件路径。

    Returns:
        浮点数秒数，例如 3723.45。

    Raises:
        RuntimeError: ffprobe 失败或输出无法解析时抛出。
    """
    process = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "quiet",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError("ffprobe 探测时长失败，请确认本机已安装 ffmpeg")
    try:
        return float(stdout.strip())
    except ValueError as exc:
        raise RuntimeError(f"ffprobe 返回无法解析的时长：{stdout!r}") from exc


async def split_mp4(source: Path, max_bytes: int) -> list[Path]:
    """将超大 mp4 按指定字节上限分割为多段，在关键帧处切割，无需重新编码。

    输出文件命名规则：在原文件名（不含扩展名）后追加三位 1 起始序号，例如：
      标题.mp4  →  标题_001.mp4、标题_002.mp4、标题_003.mp4

    实现原理：
      `-f segment -segment_size` 仅适用于 MPEG-TS 等流式容器，MP4 写入时需要
      seek 回头补全 moov atom，size-based 分割会导致 ffmpeg 报错。
      因此改为 `-segment_time`（按时长分割）：先用 ffprobe 读取总时长，再按
      文件大小比例折算每段秒数，效果与按字节限制等效。

    Args:
        source:    待分割的源 mp4 文件路径。
        max_bytes: 每段的字节上限（类比 Python 的 int，即纯字节数）。

    Returns:
        分割后生成的所有段文件路径列表，按序号升序排列。

    Raises:
        RuntimeError: ffprobe 或 ffmpeg 失败时抛出。
    """
    file_size = source.stat().st_size
    total_duration = await _probe_duration(source)

    # 按文件大小比例折算每段对应的时长（单位：秒）
    # 例如：2.5G 文件、限制 1G → 每段约 total_duration * (1G / 2.5G) 秒
    seconds_per_segment = (max_bytes / file_size) * total_duration

    stem = source.stem
    output_dir = source.parent
    # ffmpeg segment muxer 输出模式：标题_%03d.mp4，序号从 1 开始
    pattern = output_dir / f"{stem}_%03d.mp4"

    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(source),
        "-c",
        "copy",  # 不重新编码，直接复制流
        "-f",
        "segment",  # 使用 segment muxer 分段输出
        "-segment_time",
        f"{seconds_per_segment:.3f}",  # 每段时长（在关键帧对齐）
        "-reset_timestamps",
        "1",  # 每段时间戳从 0 开始，便于独立播放
        "-segment_start_number",
        "1",  # 序号从 001 开始而非 000
        str(pattern),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.communicate()
    if process.returncode != 0:
        raise RuntimeError("ffmpeg 分割失败，请确认本机已安装 ffmpeg")

    # 收集所有分段文件，按序号升序返回（类似 Python sorted()）
    parts = sorted(output_dir.glob(f"{stem}_???.mp4"))
    return parts


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

    # 如果设置了 MAX_MP4_SIZE 且合并后文件超出限制，则分割为多段
    if MAX_MP4_SIZE is not None and output_file.stat().st_size > MAX_MP4_SIZE:
        file_mb = output_file.stat().st_size / 1024 / 1024
        limit_mb = MAX_MP4_SIZE / 1024 / 1024
        logger.info(
            "文件 %.1f MB 超出上限 %.1f MB，开始分割：%s",
            file_mb,
            limit_mb,
            output_file.name,
        )
        parts = await split_mp4(output_file, MAX_MP4_SIZE)
        output_file.unlink(missing_ok=True)
        logger.info("分割完成，共 %d 段：%s", len(parts), [p.name for p in parts])
    else:
        logger.info("下载完成: %s", output_file)


async def acquire_download_lock() -> bool:
    """尝试获取下载锁，成功返回 True。

    锁初始值为 "0-0"，待 process_ready_queue 确定批次大小后由
    update_lock_progress 更新为 "{done}-{total}" 格式的进度字符串。
    使用 SET NX 保证同一时刻只有一个下载任务持有锁。
    """
    return bool(
        await r.set(DOWNLOAD_LOCK_KEY, "0-0", ex=DOWNLOAD_LOCK_TTL_SECONDS, nx=True)
    )


async def update_lock_progress(done: int, total: int) -> None:
    """将锁中的进度更新为 "{done}-{total}" 并刷新 TTL。

    每成功或失败完成一条视频后调用，外部可通过读取锁的值观察实时进度。
    例如：已完成 1 条、共 4 条时值为 "1-4"。

    Args:
        done:  已处理完毕（成功 + 失败）的视频数量。
        total: 本批次计划处理的总视频数量。
    """
    await r.set(DOWNLOAD_LOCK_KEY, f"{done}-{total}", ex=DOWNLOAD_LOCK_TTL_SECONDS)


async def release_download_lock() -> None:
    """下载任务结束后释放全局下载锁。"""
    await r.delete(DOWNLOAD_LOCK_KEY)


async def process_ready_queue() -> None:
    """扫描 bili:video:* hash，找出 download=ready 的条目逐个下载，每次最多处理 MAX_DOWNLOADS_PER_RUN 个。

    每处理完一条（无论成功或失败）都会调用 update_lock_progress 刷新锁中的进度，
    格式为 "{已完成}-{总数}"，例如 "1-4" 表示共 4 条已完成 1 条。
    """
    cookie = await load_cookie()
    if not cookie:
        raise RuntimeError("未找到登录 Cookie，请先完成扫码登录")

    ready_bvs: list[str] = []
    async for key in r.scan_iter(f"{VIDEO_REDIS_PREFIX}*"):
        status = await r.hget(key, "download")
        if status == "ready":
            ready_bvs.append(key[len(VIDEO_REDIS_PREFIX) :])
    ready_bvs.sort()

    if not ready_bvs:
        logger.info("ready 队列为空，无需下载")
        return

    batch = ready_bvs[:MAX_DOWNLOADS_PER_RUN]
    logger.info("本次共 %d 个待下载视频，最多处理 %d 个", len(ready_bvs), len(batch))

    # 批次确定后立即写入总数，进度从 0 开始
    await update_lock_progress(0, len(batch))

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

        # 无论成功还是失败，完成一条后更新锁中的进度
        await update_lock_progress(idx + 1, len(batch))

        if idx < len(batch) - 1:
            await asyncio.sleep(DOWNLOAD_INTERVAL_SECONDS)


async def async_main() -> None:
    """消费 ready 队列并更新 Redis 状态。可单独运行也可由 API 服务触发。

    执行模式由环境变量 DOWNLOAD_MODE 控制：
      bg   （默认）：将下载包装为后台 asyncio task，本函数立即返回，
                     适合 API handler 调用——调用方可以快速响应，下载在后台进行。
      sync          ：原地等待全部下载完成再返回，适合 CLI 直接运行。

    两种模式下锁均会记录进度，格式为 "{done}-{total}"，可通过读取
    Redis 键 DOWNLOAD_LOCK_KEY 查询当前下载进度。
    """
    locked = await acquire_download_lock()
    if not locked:
        logger.info("已有下载任务在执行，跳过本次运行")
        return

    async def _task() -> None:
        """实际下载逻辑，成功或异常后均释放锁。"""
        try:
            await process_ready_queue()
        finally:
            await release_download_lock()

    if DOWNLOAD_MODE == "sync":
        # 同步模式：等待全部下载完成后再返回
        logger.info("下载模式：sync（同步）")
        await _task()
    else:
        # 后台模式：创建 asyncio task 后立即返回，task 在事件循环中异步执行
        logger.info("下载模式：bg（后台）")
        asyncio.create_task(_task())


def main() -> None:
    """CLI 入口：运行下载队列消费脚本。"""

    async def _run() -> None:
        try:
            await async_main()
            # bg 模式下 async_main 立即返回但后台 task 仍在运行，
            # 需等待当前协程之外的所有 task 结束后再关闭 Redis 连接，
            # 否则事件循环退出时后台 task 会被强制取消。
            pending = [
                t for t in asyncio.all_tasks() if t is not asyncio.current_task()
            ]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            await r.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
