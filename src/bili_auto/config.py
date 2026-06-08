"""
统一配置管理模块。

将 api.py 与 downloader.py 中散落的环境变量读取收敛到一处，
所有模块通过 `from bili_auto.config import ...` 引用配置常量，
避免重复定义和不一致。
"""

import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Redis 连接参数
# -----------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# -----------------------------
# Redis Key 前缀 / 锁定 Key
# -----------------------------
REDIS_KEY = os.getenv("REDIS_KEY", "bili:downloaded")
COOKIE_REDIS_KEY = os.getenv("COOKIE_REDIS_KEY", "bili:auth:cookie")
LOGIN_REDIS_PREFIX = os.getenv("LOGIN_REDIS_PREFIX", "bili:login:")
VIDEO_REDIS_PREFIX = os.getenv("VIDEO_REDIS_PREFIX", "bili:video:")
SCAN_FAV_LOCK_KEY = os.getenv("SCAN_FAV_LOCK_KEY", "bili:scan_fav:lock")
DOWNLOAD_LOCK_KEY = os.getenv("DOWNLOAD_LOCK_KEY", "bili:download:lock")
UP_DYNAMIC_PREFIX = os.getenv("UP_DYNAMIC_PREFIX", "bili:up:dynamic:")

# 视频时长过滤阈值（单位：秒），超过该时长的视频不入队
MAX_DURATION_SECONDS_KEY = os.getenv(
    "MAX_DURATION_SECONDS_KEY", "bili:config:max_duration_seconds"
)

# -----------------------------
# 扫描收藏夹参数
# -----------------------------
SCAN_FAV_LOCK_TTL_SECONDS = int(os.getenv("SCAN_FAV_LOCK_TTL_SECONDS", "1800"))

# -----------------------------
# 登录参数
# -----------------------------
LOGIN_POLL_INTERVAL_SECONDS = int(os.getenv("LOGIN_POLL_INTERVAL_SECONDS", "10"))
LOGIN_MAX_POLLS = int(os.getenv("LOGIN_MAX_POLLS", "5"))
LOGIN_KEY_TTL_SECONDS = int(os.getenv("LOGIN_KEY_TTL_SECONDS", "600"))

# -----------------------------
# 下载参数
# -----------------------------
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads"))
DOWNLOAD_LOCK_TTL_SECONDS = int(os.getenv("DOWNLOAD_LOCK_TTL_SECONDS", "7200"))
VIDEO_DONE_TTL_SECONDS = int(os.getenv("VIDEO_DONE_TTL_SECONDS", "10800"))
MAX_DOWNLOADS_PER_RUN = int(os.getenv("MAX_DOWNLOADS_PER_RUN", "10"))
DOWNLOAD_INTERVAL_SECONDS = int(os.getenv("DOWNLOAD_INTERVAL_SECONDS", "3"))
MAX_DOWNLOAD_RETRIES = int(os.getenv("MAX_DOWNLOAD_RETRIES", "5"))

# 单视频级别重试次数：download_file 内部重试耗尽后，重新请求 playurl API
# 获取新的 CDN 地址再尝试下载
MAX_PLAY_URL_RETRIES = int(os.getenv("MAX_PLAY_URL_RETRIES", "3"))

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

    Args:
        value: 待解析的字符串，例如 "1g"、"500m"、"1073741824"。

    Returns:
        对应的字节数（int）。

    Raises:
        ValueError: 当字符串格式无法识别时抛出。
    """
    value = value.strip().lower()
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
_MAX_MP4_SIZE_STR = os.getenv("MAX_MP4_SIZE")
MAX_MP4_SIZE: int | None = _parse_size(_MAX_MP4_SIZE_STR) if _MAX_MP4_SIZE_STR else None

# -----------------------------
# HTTP 客户端超时参数
# -----------------------------
# API 交互用超时（登录、收藏夹扫描等），需要限制 read 时间避免 hang 住
API_CLIENT_TIMEOUT = httpx.Timeout(30.0, connect=10.0, read=60.0)

# 下载用超时，read 不设限制以支持大文件下载
DOWNLOAD_CLIENT_TIMEOUT = httpx.Timeout(connect=10.0, read=None, write=30.0, pool=30.0)

# -----------------------------
# API 鉴权
# -----------------------------
API_KEY = os.getenv("API_KEY", "")

# -----------------------------
# 日志目录
# -----------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# -----------------------------
# 基础 HTTP 请求头
# -----------------------------
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}

AUTH_HEADERS = {
    **BASE_HEADERS,
    "Origin": "https://www.bilibili.com",
}
