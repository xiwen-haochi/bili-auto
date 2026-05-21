# bili-auto

自动扫描 B 站收藏夹、获取 UP 主最新动态、将新视频入队，并以最高画质下载合并为 MP4 的异步工具。

> **免责声明**
>
> 本项目仅供个人学习与技术研究使用，请勿用于任何商业或违法用途。
>
> - 下载的视频版权归原作者及 B 站平台所有，请在 24 小时内删除，勿二次传播
> - 使用本工具须遵守 [哔哩哔哩用户协议](https://www.bilibili.com/blackboard/protocal.html) 及相关法律法规
> - 因使用本工具导致的账号封禁、法律责任等一切后果由使用者自行承担，与作者无关

## 功能

- **扫码登录**：浏览器打开页面即可扫码，Cookie 存入 Redis，无需本地文件
- **收藏夹扫描**：调用 `/scan_fav` 自动扫描指定收藏夹，增量入队新视频，同时自动删除已入队的收藏内容
- **最高画质下载**：DASH 流优先选最高分辨率 + 最高码率，支持 4K；支持断点续传、失败自动重试
- **音视频合并**：ffmpeg 合并后自动删除临时 m4s 文件；超大文件自动按大小分割
- **UP 主动态监控**：检查多个 UP 主是否有新视频，自动入队；支持获取 UP 主全部历史视频动态
- **关注列表**：获取当前账号关注的所有 UP 主
- **Redis 状态追踪**：每个视频有独立 hash 记录下载状态（ready / downloading / done / failed），完成后自动过期
- **下载限流**：每次最多处理配置数量视频，间隔可配，防止封禁
- **并发保护**：扫描锁 + 下载锁，下载锁可查询实时进度
- **Cookie 保活**：提供保活接口，可配合定时任务维持登录态
- **健康检查**：检查 Redis / ffmpeg / B 站接口 / 登录态
- **API 鉴权**：支持 X-API-Key 请求头鉴权
- **目录结构**：有收藏夹名时为 `下载目录/fav-收藏夹名/标题.mp4`，否则为 `下载目录/作者名/标题.mp4`

## 依赖

- Python 3.10+
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

完整环境变量参见 `.env.example`，核心配置如下：

| 变量                          | 默认值        | 说明                                               |
| ----------------------------- | ------------- | -------------------------------------------------- |
| `SERVICE_HOST`                | `0.0.0.0`     | 服务监听 IP                                        |
| `SERVICE_PORT`                | `8000`        | 服务监听端口                                       |
| `REDIS_HOST`                  | `localhost`   | Redis 主机                                         |
| `REDIS_PORT`                  | `6379`        | Redis 端口                                         |
| `REDIS_DB`                    | `0`           | Redis 数据库编号                                   |
| `REDIS_PASSWORD`              | 无            | Redis 密码（不填则不鉴权）                         |
| `API_KEY`                     | 无            | API 鉴权密钥，所有请求须携带 `X-API-Key` header    |
| `DOWNLOAD_DIR`                | `./downloads` | 视频保存根目录                                     |
| `MAX_DOWNLOADS_PER_RUN`       | `10`          | 每次下载最多处理视频数                             |
| `DOWNLOAD_INTERVAL_SECONDS`   | `3`           | 视频下载间隔（秒）                                 |
| `MAX_DOWNLOAD_RETRIES`        | `5`           | 单文件下载最大重试次数                             |
| `MAX_MP4_SIZE`                | 无            | 合并后 MP4 最大字节数，超出自动分割（如 1g, 500m） |
| `DOWNLOAD_MODE`               | `bg`          | 下载模式：`bg`（后台异步）或 `sync`（同步等待）    |
| `DOWNLOAD_LOCK_TTL_SECONDS`   | `7200`        | 下载锁过期时间（秒）                               |
| `LOGIN_MAX_POLLS`             | `5`           | 登录轮询最大次数                                   |
| `LOGIN_POLL_INTERVAL_SECONDS` | `10`          | 登录轮询间隔（秒）                                 |
| `LOGIN_KEY_TTL_SECONDS`       | `600`         | 登录状态 Redis key 过期时间（秒）                  |
| `SCAN_FAV_LOCK_TTL_SECONDS`   | `1800`        | 扫描锁过期时间（秒）                               |
| `VIDEO_DONE_TTL_SECONDS`      | `10800`       | done 状态 hash 过期时间（秒）                      |

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
curl "http://localhost:8000/scan_fav?folder_name=我的收藏" \
  -H "X-API-Key: YOUR_KEY"
```

扫描后将新增视频写入下载队列，同时从收藏夹中删除已入队的视频。

### 4. 查看收藏夹内容

```bash
curl "http://localhost:8000/fav_items?folder_name=我的收藏" \
  -H "X-API-Key: YOUR_KEY"
```

### 5. 执行下载

```bash
# CLI 方式
bili-downloader

# API 方式（后台执行，立即返回）
curl -X POST http://localhost:8000/download \
  -H "X-API-Key: YOUR_KEY"

# 查询下载进度
curl http://localhost:8000/download/status \
  -H "X-API-Key: YOUR_KEY"
```

每次运行最多下载 `MAX_DOWNLOADS_PER_RUN` 个视频。

### 6. UP 主动态监控

**检查多个 UP 主是否有新视频（推荐定时调用）：**

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

### 7. 获取关注列表

```bash
curl http://localhost:8000/my_followings \
  -H "X-API-Key: YOUR_KEY"
```

### 8. Cookie 保活

```bash
curl http://localhost:8000/keep_alive \
  -H "X-API-Key: YOUR_KEY"
```

### 9. 健康检查

```bash
curl http://localhost:8000/health
```

返回示例：

```json
{
  "overall": "ok",
  "redis": "ok",
  "ffmpeg": "ffmpeg version 7.1 Copyright (c) 2000-2024 the FFmpeg developers",
  "logged_in": true,
  "bilibili_api": "ok"
}
```

| 字段           | 说明                                         |
| -------------- | -------------------------------------------- |
| `overall`      | 整体状态：`ok` 全部正常，`degraded` 有项异常 |
| `redis`        | Redis 连通性（`ok` 或错误信息）              |
| `ffmpeg`       | ffmpeg 版本行（`ok` 状态下）或错误信息       |
| `logged_in`    | 是否已有登录 Cookie                          |
| `bilibili_api` | B 站接口是否可达                             |

### 10. 定时任务

配合 cron 定时执行：

```cron
# 每小时扫描一次收藏夹
0 * * * * curl -s "http://localhost:8000/scan_fav?folder_name=我的收藏" -H "X-API-Key: YOUR_KEY"

# 每 30 分钟检查一次 UP 主新视频
*/30 * * * * curl -s "http://localhost:8000/check_up_new_video?uids=123456&uids=789012" -H "X-API-Key: YOUR_KEY"

# 每小时触发一次下载
30 * * * * curl -s -X POST "http://localhost:8000/download" -H "X-API-Key: YOUR_KEY"

# 每 6 小时保活一次
0 */6 * * * curl -s "http://localhost:8000/keep_alive" -H "X-API-Key: YOUR_KEY"
```

## API 路由一览

| 方法 | 路由                    | 说明                       |
| ---- | ----------------------- | -------------------------- |
| GET  | `/`                     | 首页，跳转到二维码登录页   |
| GET  | `/login_qrcode`         | 扫码登录页面               |
| GET  | `/login_poll`           | 查询登录轮询状态           |
| GET  | `/scan_fav`             | 扫描收藏夹并入队           |
| GET  | `/fav_items`            | 查看收藏夹全部内容         |
| GET  | `/fav_delete`           | 从收藏夹删除指定视频       |
| POST | `/download`             | 触发下载队列消费           |
| GET  | `/download/status`      | 查询下载进度               |
| GET  | `/up_video_dynamic_all` | 获取 UP 主全部视频动态     |
| GET  | `/check_up_new_video`   | 检查多个 UP 主新视频       |
| GET  | `/my_followings`        | 获取关注列表               |
| GET  | `/keep_alive`           | Cookie 保活                |
| GET  | `/health`               | 健康检查                   |
| GET  | `/docs`                 | OpenAPI 文档（Swagger UI） |

## 项目结构

```
bili-auto/
├── src/
│   └── bili_auto/
│       ├── __init__.py         # 包初始化
│       ├── api.py              # FastAPI 应用入口，路由定义（仅编排）
│       ├── bilibili_api.py     # B 站 API 交互层（登录轮询、收藏扫描、动态获取）
│       ├── config.py           # 统一配置管理（环境变量加载）
│       ├── downloader.py       # 下载核心（断点续传、队列消费、ffmpeg 合并/分割）
│       ├── redis_client.py     # Redis 连接与数据读写操作
│       ├── templates.py        # 登录页面 HTML 模板
│       └── utils.py            # 通用工具函数（WBI 签名、文件名清洗、码流选择等）
├── .env.example                # 配置示例
├── pyproject.toml
├── logs/                       # 日志目录（自动创建）
└── downloads/                  # 视频下载目录（自动创建）
```

## Redis 数据结构

| Key                     | 类型   | 说明                                                  |
| ----------------------- | ------ | ----------------------------------------------------- |
| `bili:downloaded`       | Set    | 已完成下载的 BV 号                                    |
| `bili:auth:cookie`      | String | 当前登录 Cookie                                       |
| `bili:video:{bvid}`     | Hash   | 单视频状态（download: ready/downloading/done/failed） |
| `bili:login:{key}`      | Hash   | 二维码登录状态，10 分钟过期                           |
| `bili:scan_fav:lock`    | String | 扫描锁                                                |
| `bili:download:lock`    | String | 下载锁，值格式 `{done}-{total}` 记录进度              |
| `bili:up:dynamic:{uid}` | String | UP 主最新视频动态 id_str，用于检测更新                |

## OpenAPI 文档

启动服务后，访问 `http://localhost:8000/docs` 查看 Swagger UI 在线文档。

## TODO（计划功能）

- [ ] **下载通知**：下载完成/失败时通过 Webhook（企业微信、飞书、Telegram 等）推送通知
- [ ] **简易 Web UI**：一个轻量前端页面，展示队列状态、下载进度和历史记录
- [ ] **下载格式可选**：支持仅下载音频流并转为 MP3
- [ ] **自动重试失败任务**：定时扫描 `failed` 状态的视频并重新入队
- [ ] **带宽限制**：支持配置下载速度上限，避免占满带宽
- [ ] **Docker 部署**：提供 Dockerfile 和 docker-compose.yml，一键部署
- [ ] **多账号支持**：支持切换多个 B 站账号的 Cookie，按账号隔离下载目录
