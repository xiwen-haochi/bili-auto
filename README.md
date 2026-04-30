# bili-auto

自动扫描 B 站收藏夹、将新视频入队，并以最高画质下载合并为 MP4 的异步工具。

> **免责声明**
>
> 本项目仅供个人学习与技术研究使用，请勿用于任何商业或违法用途。
>
> - 下载的视频版权归原作者及 B 站平台所有，请在 24 小时内删除，勿二次传播
> - 使用本工具须遵守 [哔哩哔哩用户协议](https://www.bilibili.com/blackboard/protocal.html) 及相关法律法规
> - 因使用本工具导致的账号封禁、法律责任等一切后果由使用者自行承担，与作者无关

## 功能

- **扫码登录**：浏览器打开页面即可扫码，Cookie 存入 Redis，无需本地文件
- **收藏夹扫描**：调用 `/scan_fav` 接口自动扫描所有（或指定）收藏夹，增量入队新视频
- **最高画质下载**：DASH 流优先选最高分辨率 + 最高码率，支持 4K
- **音视频合并**：ffmpeg 合并后自动删除临时 m4s 文件
- **目录结构**：`下载目录/作者名/标题.mp4`
- **Redis 状态追踪**：每个视频有独立 hash 记录下载状态（ready / downloading / done / failed），完成后 3 小时自动过期
- **下载限流**：每次最多处理 10 个视频，每个间隔 3 秒，防止封禁
- **并发保护**：扫描锁 + 下载锁，防止重复执行
- **日志**：终端 + 文件双输出（`logs/bili_auto.log`、`logs/downloader.log`）

## 依赖

- Python 3.12+
- Redis
- ffmpeg（需在系统 PATH 中）

## 安装

**从 PyPI 安装（推荐）：**

```bash
pip install bili-auto
# 或
uv add bili-auto
```

## 配置

复制示例文件并按需修改：

```bash
cp .env.example .env
```

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_HOST` | `localhost` | Redis 主机 |
| `REDIS_PORT` | `6379` | Redis 端口 |
| `REDIS_DB` | `0` | Redis 数据库编号 |
| `DOWNLOAD_DIR` | `./downloads` | 视频保存根目录 |
| `MAX_DOWNLOADS_PER_RUN` | `10` | 每次 downloader 最多下载数 |
| `DOWNLOAD_INTERVAL_SECONDS` | `3` | 视频下载间隔（秒） |
| `VIDEO_DONE_TTL_SECONDS` | `10800` | done 状态 hash 过期时间（秒） |
| `LOGIN_MAX_POLLS` | `5` | 登录轮询最大次数 |
| `LOGIN_POLL_INTERVAL_SECONDS` | `10` | 登录轮询间隔（秒） |

## 使用

### 1. 启动 API 服务

```bash
bili-auto
# 或（开发模式）
uvicorn bili_auto.api:app --host 0.0.0.0 --port 8000
```

### 2. 扫码登录

浏览器打开 `http://localhost:8000/login_qrcode`，用 B 站 App 扫码，Cookie 自动写入 Redis。

### 3. 扫描收藏夹

```bash
# 扫描全部收藏夹
curl http://localhost:8000/scan_fav

# 扫描指定收藏夹
curl "http://localhost:8000/scan_fav?folder_name=我的收藏"
```

### 4. 执行下载

```bash
bili-downloader
```

每次运行最多下载 `MAX_DOWNLOADS_PER_RUN` 个视频。也可通过 API 服务触发，无需启动独立进程：

```bash
# 触发下载（后台执行，立即返回）
curl -X POST http://localhost:8000/download

# 查询下载是否正在运行
curl http://localhost:8000/download/status
```

可配合 cron 定时执行：

```cron
# 每小时扫描一次收藏夹
0 * * * * curl -s http://localhost:8000/scan_fav

# 每小时触发一次下载（通过 API）
30 * * * * curl -s -X POST http://localhost:8000/download

# 或直接运行 CLI（二选一）
30 * * * * /path/to/.venv/bin/bili-downloader
```

### 5. Cookie 保活（可选）

```bash
curl http://localhost:8000/keep_alive
```

### 6. 健康检查

```bash
curl http://localhost:8000/health
```

返回示例：

```json
{
  "status": "ok",
  "redis": "ok",
  "ffmpeg": "ffmpeg version 7.1 Copyright (c) 2000-2024 the FFmpeg developers",
  "logged_in": true,
  "bilibili_api": "ok"
}
```

| 字段 | 说明 |
|---|---|
| `status` | 整体状态：`ok` 全部正常，`degraded` 有项异常 |
| `redis` | Redis 连通性（`ok` 或错误信息） |
| `ffmpeg` | ffmpeg 版本行（`ok` 状态下）或错误信息 |
| `logged_in` | 是否已有登录 Cookie |
| `bilibili_api` | B 站接口是否可达 |

## 项目结构

```
bili-auto/
├── src/
│   └── bili_auto/
│       ├── __init__.py
│       ├── api.py          # FastAPI 服务：登录、扫描、Cookie 管理
│       └── downloader.py   # 独立下载脚本：消费 Redis 队列
├── .env                    # 本地配置（不提交）
├── .env.example            # 配置示例
├── pyproject.toml
├── logs/                   # 日志目录（自动创建）
└── downloads/              # 视频下载目录（自动创建）
```

## Redis 数据结构

| Key | 类型 | 说明 |
|---|---|---|
| `bili:downloaded` | Set | 已完成下载的 BV 号 |
| `bili:auth:cookie` | String | 当前登录 Cookie |
| `bili:video:{bvid}` | Hash | 单视频状态（download: ready/downloading/done/failed） |
| `bili:login:{key}` | Hash | 二维码登录状态，10 分钟过期 |
| `bili:scan_fav:lock` | String | 扫描锁 |
| `bili:download:lock` | String | 下载锁 |
