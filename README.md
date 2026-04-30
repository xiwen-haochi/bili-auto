# bili-auto

自动扫描 B 站收藏夹、获取up最新动态、将新视频入队，并以最高画质下载合并为 MP4 的异步工具。

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
| `SERVICE_HOST` | `0.0.0.0` | 服务监听 IP |
| `SERVICE_PORT` | `8000` | 服务监听端口 |
| `REDIS_HOST` | `localhost` | Redis 主机 |
| `REDIS_PORT` | `6379` | Redis 端口 |
| `REDIS_DB` | `0` | Redis 数据库编号 |
| `REDIS_PASSWORD` | 无 | Redis 密码（不填则不鉴权） |
| `API_KEY` | 无 | API 鉴权密钥，所有请求须携带 `X-API-Key` header |
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
0 * * * * /home/ubuntu/auto-bili/scripts/scan_fav.sh

# 每 30 分钟检查一次 UP 主新视频
*/30 * * * * /home/ubuntu/auto-bili/scripts/check_up_new_video.sh

# 每小时触发一次下载
30 * * * * /home/ubuntu/auto-bili/scripts/download.sh

# 每 6 小时保活一次（内置 50% 随机概率）
0 */6 * * * /home/ubuntu/auto-bili/scripts/keep_alive.sh
```

### 5. UP 主动态监控

**检查多个 UP 主是否有新视频（推荐定时调用）：**

```bash
# 编辑脚本，填入要监控的 UID
nano scripts/check_up_new_video.sh

# 执行
./scripts/check_up_new_video.sh
```

或直接调用接口：

```bash
curl "http://localhost:8000/check_up_new_video?uids=123456&uids=789012" \
  -H "X-API-Key: YOUR_KEY"
```

逻辑：首次见到该 UID 时记录最新 `dynamic_id`；后续调用若 `dynamic_id` 变化则将新 BV 写入下载队列。

**获取指定 UP 主全部历史视频动态并入队：**

```bash
curl "http://localhost:8000/up_video_dynamic_all?uid=123456" \
  -H "X-API-Key: YOUR_KEY"
```

自动翻页获取全部视频动态，过滤已完成/已入队的 BV 后批量写入下载队列。

### 6. Cookie 保活（可选）

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

## systemd 服务（Linux）

创建服务文件：

```bash
sudo nano /etc/systemd/system/bili-auto.service
```

填入以下内容：

```ini
[Unit]
Description=bili-auto API Service
After=network.target redis.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/auto-bili
EnvironmentFile=/home/ubuntu/auto-bili/.env
ExecStart=/home/ubuntu/auto-bili/.venv/bin/bili-auto
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable bili-auto
sudo systemctl start bili-auto

# 查看状态
sudo systemctl status bili-auto

# 查看日志
sudo journalctl -u bili-auto -f
```

## 项目结构

```
bili-auto/
├── src/
│   └── bili_auto/
│       ├── __init__.py
│       ├── api.py          # FastAPI 服务：登录、扫描、Cookie 管理、动态监控
│       └── downloader.py   # 独立下载脚本：消费 Redis 队列
├── scripts/
│   ├── scan_fav.sh         # 扫描收藏夹（默认：测试收藏）
│   ├── download.sh         # 触发下载任务
│   ├── keep_alive.sh       # Cookie 保活（50% 随机概率）
│   └── check_up_new_video.sh  # UP 主新视频检测
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
| `bili:up:dynamic:{uid}` | String | UP 主最新视频动态 id_str，用于检测更新 |
