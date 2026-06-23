"""
视频下载核心模块。

负责从 B 站获取视频信息、下载音视频流、合并/分割文件，
以及消费 ready 队列。

可独立作为 CLI 运行（python -m bili_auto.downloader），
也可被 api.py 中的 /download 端点触发。
"""

import asyncio
import logging
import random
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import httpx
from tqdm import tqdm

from bili_auto.config import (
    BASE_HEADERS,
    DOWNLOAD_CLIENT_TIMEOUT,
    DOWNLOAD_DIR,
    DOWNLOAD_INTERVAL_JITTER,
    DOWNLOAD_INTERVAL_SECONDS,
    DOWNLOAD_LOCK_KEY,
    DOWNLOAD_MODE,
    LOG_DIR,
    MAX_DOWNLOAD_RETRIES,
    MAX_DOWNLOADS_PER_RUN,
    MAX_MP4_SIZE,
    MAX_PLAY_URL_RETRIES,
    PLAYURL_RATE_LIMIT_BACKOFF_BASE,
    PLAYURL_RATE_LIMIT_MAX_DELAY,
    VIDEO_DONE_TTL_SECONDS,
    VIDEO_REDIS_PREFIX,
)
from bili_auto.redis_client import (
    acquire_download_lock,
    add_video_downloaded,
    get_max_downloads_per_run,
    get_video_folder_name,
    load_cookie,
    mark_video_status,
    r,
    release_download_lock,
    update_lock_progress,
    video_state_key,
)
from bili_auto.utils import (
    fetch_json,
    get_stream_url,
    now_iso,
    sanitize_title,
    select_best_audio_stream,
    select_best_video_stream,
)

# -----------------------------
# 日志配置（下载专用，按天轮转）
# -----------------------------
_file_handler = TimedRotatingFileHandler(
    filename=str(LOG_DIR / "downloader.log"),
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
    utc=False,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        _file_handler,
    ],
)
logger = logging.getLogger(__name__)


# -----------------------------
# 媒体下载与处理
# -----------------------------
async def download_file(client: httpx.AsyncClient, url: str, dest: Path) -> None:
    """异步下载单个媒体文件，支持断点续传，失败自动重试。

    Args:
        client: 共享的 httpx AsyncClient 实例。
        url: 媒体文件下载地址。
        dest: 本地目标文件路径。

    Raises:
        httpx.TransportError: 重试次数耗尽后抛出。
    """
    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        downloaded = dest.stat().st_size if dest.exists() else 0

        extra_headers = {
            "Range": f"bytes={downloaded}-",
            "Connection": "close",
            "User-Agent": "Mozilla/5.0",
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
                    async for chunk in resp.aiter_bytes(chunk_size=16384):
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
    """使用异步子进程调用 ffmpeg 合并音视频。

    Args:
        video_file: 视频流文件路径。
        audio_file: 音频流文件路径。
        output_file: 合并后的 mp4 输出路径。

    Raises:
        RuntimeError: ffmpeg 执行失败时抛出。
    """
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


async def _merge_durl_segments(segment_files: list[Path], output_file: Path) -> None:
    """使用 ffmpeg concat 协议合并多段 durl mp4 片段。

    durl 格式的视频可能被 B 站切分为多段（通常每段约 6 分钟），
    下载后需用本函数合并为一个完整 mp4 文件，无需重新编码。

    Args:
        segment_files: 按播放顺序排列的分段文件路径列表。
        output_file: 合并后的 mp4 输出路径。

    Raises:
        RuntimeError: ffmpeg 执行失败时抛出。
    """
    concat_input = "|".join(str(p) for p in segment_files)
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        f"concat:{concat_input}",
        "-c",
        "copy",
        str(output_file),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.communicate()
    if process.returncode != 0:
        raise RuntimeError("ffmpeg 合并 durl 分段失败，请确认本机已安装 ffmpeg")


async def _probe_duration(path: Path) -> float:
    """用 ffprobe 读取媒体文件的总时长（秒）。

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

    Args:
        source:    待分割的源 mp4 文件路径。
        max_bytes: 每段的字节上限。

    Returns:
        分割后生成的所有段文件路径列表，按序号升序排列。

    Raises:
        RuntimeError: ffprobe 或 ffmpeg 失败时抛出。
    """
    file_size = source.stat().st_size
    total_duration = await _probe_duration(source)

    seconds_per_segment = (max_bytes / file_size) * total_duration

    stem = source.stem
    output_dir = source.parent
    pattern = output_dir / f"{stem}_%03d.mp4"

    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(source),
        "-c",
        "copy",
        "-f",
        "segment",
        "-segment_time",
        f"{seconds_per_segment:.3f}",
        "-reset_timestamps",
        "1",
        "-segment_start_number",
        "1",
        str(pattern),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.communicate()
    if process.returncode != 0:
        raise RuntimeError("ffmpeg 分割失败，请确认本机已安装 ffmpeg")

    parts = sorted(output_dir.glob(f"{stem}_???.mp4"))
    return parts


# -----------------------------
# playurl API 辅助函数：带风控退避重试
# -----------------------------
# B 站 playurl API 错误码 87008 表示当前请求触发频率风控，
# 需要等待一段时间后重试（指数退避）。
_PLAYURL_87008_RETRIES = 5  # 87008 风控专用重试次数


async def _fetch_playurl(
    client: httpx.AsyncClient,
    bvid: str,
    cid: int,
    params: dict | None = None,
) -> dict:
    """调用 B 站 playurl API，针对 87008 风控错误做指数退避重试。

    当返回 code=87008 时，自动等待递增时长后重试；
    其他非 0 错误码直接抛出 RuntimeError。

    Args:
        client: 共享的 httpx AsyncClient 实例。
        bvid: 视频 BV 号（仅用于日志）。
        cid: 视频分 P 的 cid。
        params: 额外的查询参数（如 fnval 等）。

    Returns:
        playurl API 的完整 JSON 响应。

    Raises:
        RuntimeError: 非 87008 错误或 87008 重试耗尽后抛出。
    """
    base_params = {"bvid": bvid, "cid": cid, "qn": 127, "fourk": 1}
    if params:
        base_params.update(params)

    for attempt in range(1, _PLAYURL_87008_RETRIES + 1):
        resp = await fetch_json(
            client,
            "https://api.bilibili.com/x/player/playurl",
            params=base_params,
        )

        code = resp.get("code", 0)
        if code == 0:
            return resp

        # 87008 风控限流：指数退避后重试
        if code == 87008:
            if attempt == _PLAYURL_87008_RETRIES:
                raise RuntimeError(
                    f"获取播放地址失败（87008 风控，已重试 {_PLAYURL_87008_RETRIES} 次）：{resp}"
                )
            delay = min(
                PLAYURL_RATE_LIMIT_BACKOFF_BASE * (2 ** (attempt - 1)),
                PLAYURL_RATE_LIMIT_MAX_DELAY,
            )
            # 加随机抖动，避免多线程/多进程同时重试的惊群效应
            jitter = random.uniform(0, delay * 0.3)
            total_delay = delay + jitter
            logger.warning(
                "BV %s playurl 触发风控 87008，第 %d/%d 次重试，等待 %.1f 秒",
                bvid,
                attempt,
                _PLAYURL_87008_RETRIES,
                total_delay,
            )
            await asyncio.sleep(total_delay)
            continue

        # 其他非 0 错误：不重试，直接抛
        raise RuntimeError(f"获取播放地址失败：{resp}")


# -----------------------------
# 单视频下载流程
# -----------------------------
async def download_bv(bvid: str, cookie: str) -> None:
    """下载指定 BV 视频，并自动选择当前可用的最高画质。

    Args:
        bvid: 视频 BV 号。
        cookie: 登录后的 Cookie 字符串。

    Raises:
        RuntimeError: 获取视频信息或播放地址失败时抛出。
    """
    logger.info("开始下载 BV: %s", bvid)

    headers = {"Cookie": cookie, **BASE_HEADERS}

    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=DOWNLOAD_CLIENT_TIMEOUT,
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

        folder_name = await get_video_folder_name(bvid)
        if folder_name:
            folder_name = f"fav-{sanitize_title(folder_name)}"

        play_resp = await _fetch_playurl(client, bvid, cid, params={"fnval": 16})

        safe_author = sanitize_title(author)
        clean_title = sanitize_title(title)

        if folder_name:
            video_dir = DOWNLOAD_DIR / folder_name
        else:
            video_dir = DOWNLOAD_DIR / safe_author

        video_dir.mkdir(parents=True, exist_ok=True)

        output_file = video_dir / f"{clean_title}.mp4"

        dash = play_resp.get("data", {}).get("dash", {})
        videos = dash.get("video") or []
        audios = dash.get("audio") or []

        # 用于标记当前视频使用的是 DASH 模式还是 durl 回退模式
        is_dash = False

        if videos and audios:
            # DASH 模式：分别下载视频流和音频流，之后用 ffmpeg 合并
            # 外层重试：download_file 内层重试耗尽后，重新请求 playurl API
            # 获取新的 CDN 地址再尝试，避免始终撞同一个不稳定的 CDN 节点
            is_dash = True

            for play_attempt in range(1, MAX_PLAY_URL_RETRIES + 1):
                best_video = select_best_video_stream(videos)
                best_audio = select_best_audio_stream(audios)
                video_url = get_stream_url(best_video)
                audio_url = get_stream_url(best_audio)

                if not video_url or not audio_url:
                    raise RuntimeError(f"无法解析 DASH 音视频下载地址：{play_resp}")

                video_file = video_dir / f"{bvid}_video.m4s"
                audio_file = video_dir / f"{bvid}_audio.m4s"

                try:
                    await download_file(client, video_url, video_file)
                    await download_file(client, audio_url, audio_file)
                    break  # 下载成功，退出外层重试循环
                except httpx.TransportError:
                    if play_attempt == MAX_PLAY_URL_RETRIES:
                        raise

                    logger.warning(
                        "BV %s DASH 下载失败（第 %d/%d 次），" "将重新获取播放地址",
                        bvid,
                        play_attempt,
                        MAX_PLAY_URL_RETRIES,
                    )

                    # 删除部分下载的文件
                    video_file.unlink(missing_ok=True)
                    audio_file.unlink(missing_ok=True)

                    # 重新请求 playurl API，获取新的 CDN 地址
                    play_resp = await _fetch_playurl(
                        client, bvid, cid, params={"fnval": 16}
                    )

                    dash = play_resp.get("data", {}).get("dash", {})
                    videos = dash.get("video") or []
                    audios = dash.get("audio") or []

                    if not videos or not audios:
                        raise RuntimeError(
                            "重新获取播放地址后仍无 DASH 流，"
                            "建议降级为 durl 模式手动处理"
                        )
        else:
            # durl 回退模式：部分视频无 DASH 流，使用传统直链 mp4
            # 注意：fnval=16 请求下非DASH视频的 durl 可能不完整（仅预览片段），
            # 需用 fnval=1 重新请求以获取完整分段
            logger.info("BV %s 无 DASH 流，切换到 durl（非DASH）格式下载", bvid)

            durl_play_resp = await _fetch_playurl(
                client, bvid, cid, params={"fnval": 1}
            )

            durl_list = durl_play_resp.get("data", {}).get("durl") or []
            if not durl_list:
                raise RuntimeError(f"当前视频未返回 DASH 音视频流，也无 durl 直链")

            if len(durl_list) == 1:
                # 单段 durl：直接下载即为完整 mp4
                # 同样增加外层重试，避免单个 CDN 节点不稳定
                for play_attempt in range(1, MAX_PLAY_URL_RETRIES + 1):
                    video_url = durl_list[0].get("url")
                    if not video_url:
                        raise RuntimeError("无法解析 durl 下载地址")
                    try:
                        await download_file(client, video_url, output_file)
                        break
                    except httpx.TransportError:
                        if play_attempt == MAX_PLAY_URL_RETRIES:
                            raise
                        logger.warning(
                            "BV %s durl 下载失败（第 %d/%d 次），" "将重新获取播放地址",
                            bvid,
                            play_attempt,
                            MAX_PLAY_URL_RETRIES,
                        )
                        output_file.unlink(missing_ok=True)
                        durl_play_resp = await _fetch_playurl(
                            client, bvid, cid, params={"fnval": 1}
                        )
                        durl_list = durl_play_resp.get("data", {}).get("durl") or []
                        if not durl_list:
                            raise RuntimeError("重新获取后无 durl 直链")
            else:
                # 多段 durl：逐段下载，然后用 ffmpeg concat 合并
                segment_files: list[Path] = []
                for i, segment in enumerate(durl_list, 1):
                    seg_url = segment.get("url")
                    if not seg_url:
                        logger.warning(
                            "durl 第 %d 段无有效 URL，跳过", segment.get("order", i)
                        )
                        continue
                    seg_file = video_dir / f"{bvid}_seg_{i:03d}.mp4"
                    await download_file(client, seg_url, seg_file)
                    segment_files.append(seg_file)

                if not segment_files:
                    raise RuntimeError("durl 分段均无有效下载地址")

                logger.info("BV %s durl 共 %d 段，开始合并", bvid, len(segment_files))
                await _merge_durl_segments(segment_files, output_file)
                for sf in segment_files:
                    sf.unlink(missing_ok=True)

    # DASH 模式需要在退出 client 上下文后合并音视频
    if is_dash:
        await merge_av(video_file, audio_file, output_file)
        video_file.unlink(missing_ok=True)
        audio_file.unlink(missing_ok=True)

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


# -----------------------------
# 队列消费与入口
# -----------------------------
async def process_ready_queue() -> None:
    """扫描 bili:video:* hash，找出 download=ready 的条目逐个下载。

    每次最多处理 MAX_DOWNLOADS_PER_RUN 个。
    每处理完一条（无论成功或失败）都会调用 update_lock_progress 刷新进度。
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

    # 动态读取单轮最大下载数：优先 Redis，Redis 无值时回退到 .env / 默认值
    max_per_run = await get_max_downloads_per_run()
    if max_per_run is None:
        max_per_run = MAX_DOWNLOADS_PER_RUN

    batch = ready_bvs[:max_per_run]
    logger.info("本次共 %d 个待下载视频，最多处理 %d 个", len(ready_bvs), len(batch))

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
            await add_video_downloaded(bvid)
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

        await update_lock_progress(idx + 1, len(batch))

        if idx < len(batch) - 1:
            # 在基础间隔上增加随机抖动，避免请求模式过于规律被 B 站风控识别
            jitter = random.uniform(0, DOWNLOAD_INTERVAL_JITTER)
            total_delay = DOWNLOAD_INTERVAL_SECONDS + jitter
            logger.debug("等待 %.1f 秒后处理下一个视频", total_delay)
            await asyncio.sleep(total_delay)


async def async_main() -> None:
    """消费 ready 队列并更新 Redis 状态。可单独运行也可由 API 服务触发。

    执行模式由环境变量 DOWNLOAD_MODE 控制：
      bg   （默认）：将下载包装为后台 asyncio task，本函数立即返回。
      sync          ：原地等待全部下载完成再返回。
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
        logger.info("下载模式：sync（同步）")
        await _task()
    else:
        logger.info("下载模式：bg（后台）")
        asyncio.create_task(_task())


def main() -> None:
    """CLI 入口：运行下载队列消费脚本。"""

    async def _run() -> None:
        try:
            await async_main()
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
