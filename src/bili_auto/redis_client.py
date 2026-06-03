"""
共享 Redis 客户端及数据操作模块。

提供单一 Redis 连接实例 `r` 以及所有与 Redis 交互的读写操作。
api.py 与 downloader.py 原先各自维护一个连接，现已统一为同一个实例。
"""

from secrets import token_hex
from typing import Any, cast

import redis.asyncio as redis

from bili_auto.config import (
    COOKIE_REDIS_KEY,
    DOWNLOAD_LOCK_KEY,
    DOWNLOAD_LOCK_TTL_SECONDS,
    LOGIN_KEY_TTL_SECONDS,
    LOGIN_REDIS_PREFIX,
    MAX_DURATION_SECONDS_KEY,
    REDIS_DB,
    REDIS_HOST,
    REDIS_KEY,
    REDIS_PASSWORD,
    REDIS_PORT,
    SCAN_FAV_LOCK_KEY,
    SCAN_FAV_LOCK_TTL_SECONDS,
    VIDEO_DONE_TTL_SECONDS,
    VIDEO_REDIS_PREFIX,
)

# -----------------------------
# 单一 Redis 连接实例
# -----------------------------
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


# -----------------------------
# Key 构造辅助函数
# -----------------------------
def login_state_key(qrcode_key: str) -> str:
    """拼出二维码登录状态在 Redis 中对应的 key。"""
    return f"{LOGIN_REDIS_PREFIX}{qrcode_key}"


def video_state_key(bvid: str) -> str:
    """拼出单个视频在 Redis 中的元数据 key。"""
    return f"{VIDEO_REDIS_PREFIX}{bvid}"


# -----------------------------
# 登录状态读写
# -----------------------------
async def save_login_state(qrcode_key: str, mapping: dict[str, str]) -> None:
    """将二维码状态写入 Redis，并刷新过期时间。"""
    key = login_state_key(qrcode_key)
    await r.hset(key, mapping=mapping)
    await r.expire(key, LOGIN_KEY_TTL_SECONDS)


async def load_login_state(qrcode_key: str) -> dict[str, str] | None:
    """读取 Redis 中的二维码状态，不存在时返回 None。"""
    data = await r.hgetall(login_state_key(qrcode_key))
    return data or None


# -----------------------------
# Cookie 读写
# -----------------------------
async def save_cookie(cookie: str) -> None:
    """将当前登录态仅保存到 Redis，不再落地本地文件。"""
    await r.set(COOKIE_REDIS_KEY, cookie)


async def load_cookie() -> str | None:
    """从 Redis 读取当前登录 Cookie。"""
    return await r.get(COOKIE_REDIS_KEY)


# -----------------------------
# 扫描锁操作
# -----------------------------
async def acquire_scan_lock() -> str | None:
    """尝试获取扫描锁，成功时返回本次锁令牌，失败时返回 None。"""
    lock_token = token_hex(16)
    locked = await r.set(
        SCAN_FAV_LOCK_KEY, lock_token, ex=SCAN_FAV_LOCK_TTL_SECONDS, nx=True
    )
    return lock_token if locked else None


async def release_scan_lock(lock_token: str) -> None:
    """只在当前请求仍持有锁时释放，避免误删其他请求的新锁。"""
    current_token = await r.get(SCAN_FAV_LOCK_KEY)
    if current_token == lock_token:
        await r.delete(SCAN_FAV_LOCK_KEY)


# -----------------------------
# 下载锁操作
# -----------------------------
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

    Args:
        done:  已处理完毕（成功 + 失败）的视频数量。
        total: 本批次计划处理的总视频数量。
    """
    await r.set(DOWNLOAD_LOCK_KEY, f"{done}-{total}", ex=DOWNLOAD_LOCK_TTL_SECONDS)


async def release_download_lock() -> None:
    """下载任务结束后释放全局下载锁。"""
    await r.delete(DOWNLOAD_LOCK_KEY)


# -----------------------------
# 视频下载状态读写
# -----------------------------
async def mark_video_status(bvid: str, mapping: dict[str, str]) -> None:
    """更新 Redis 中单个视频的下载状态。"""
    await r.hset(video_state_key(bvid), mapping=mapping)


async def enqueue_ready_video(
    bvid: str,
    folder_name: str | None = None,
) -> None:
    """把待下载视频写入 Redis hash，并初始化下载状态字段。"""
    from bili_auto.utils import now_iso

    now = now_iso()
    await r.hset(
        video_state_key(bvid),
        mapping={
            "bvid": bvid,
            "download": "ready",
            "created_at": now,
            "updated_at": now,
            "folder_name": folder_name or "",
        },
    )


async def is_video_downloaded(bvid: str) -> bool:
    """检查视频是否已下载完成（存在于 REDIS_KEY 集合中）。"""
    return bool(await r.sismember(REDIS_KEY, bvid))


async def add_video_downloaded(bvid: str) -> None:
    """标记视频为已下载。"""
    await r.sadd(REDIS_KEY, bvid)


async def get_video_download_status(bvid: str) -> str | None:
    """获取视频下载状态（ready / downloading / done / failed / None）。"""
    return await r.hget(video_state_key(bvid), "download")


async def get_video_folder_name(bvid: str) -> str | None:
    """获取视频关联的收藏夹名称。"""
    return await r.hget(video_state_key(bvid), "folder_name")


# -----------------------------
# 视频时长过滤阈值（仅对 up_video_dynamic_all 接口生效）
# -----------------------------
async def get_max_duration_seconds() -> int | None:
    """从 Redis 读取视频最大时长阈值（秒）。

    超过该时长的视频不会被入队。不存在则返回 None 表示不过滤。
    """
    raw = await r.get(MAX_DURATION_SECONDS_KEY)
    return int(raw) if raw is not None else None


async def set_max_duration_seconds(seconds: int | None) -> None:
    """设置（或删除）视频最大时长阈值。

    Args:
        seconds: 最大时长（秒），传入 0 或 None 时删除该 key 以关闭过滤。
    """
    if seconds and seconds > 0:
        await r.set(MAX_DURATION_SECONDS_KEY, str(seconds))
    else:
        await r.delete(MAX_DURATION_SECONDS_KEY)
