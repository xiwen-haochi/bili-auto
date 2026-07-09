"""
B 站 API 交互层。

封装所有与 B 站服务端直接通信的业务逻辑：
  - 二维码登录后台轮询
  - 收藏夹扫描、删除
  - UP 主动态获取

不包含 FastAPI 路由定义（路由在 api.py），也不包含下载逻辑（下载在 downloader.py）。
"""

import asyncio
import logging

import httpx

from bili_auto.config import (
    API_CLIENT_TIMEOUT,
    AUTH_HEADERS,
    BASE_HEADERS,
    LOGIN_MAX_POLLS,
    LOGIN_POLL_INTERVAL_SECONDS,
    REDIS_KEY,
)
from bili_auto.redis_client import (
    enqueue_ready_video,
    get_video_download_status,
    is_video_downloaded,
    load_login_state,
    save_cookie,
    save_login_state,
)
from bili_auto.utils import (
    cookies_to_string,
    extract_bili_jct,
    fetch_json,
    get_wbi_key,
    now_iso,
    parse_duration_text,
    wbi_sign,
)

logger = logging.getLogger(__name__)


# -----------------------------
# 登录：后台轮询二维码状态
# -----------------------------
async def poll_login_status_task(qrcode_key: str) -> None:
    """后台轮询 B 站登录状态，成功后把 Cookie 存入 Redis。

    Args:
        qrcode_key: B 站二维码 key，用于轮询查询状态。
    """
    poll_url = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"

    for attempt in range(1, LOGIN_MAX_POLLS + 1):
        current_state = await load_login_state(qrcode_key)
        if not current_state:
            return

        if current_state.get("status") in {"success", "expired", "failed"}:
            return

        async with httpx.AsyncClient(
            headers=AUTH_HEADERS,
            follow_redirects=True,
            timeout=API_CLIENT_TIMEOUT,
        ) as client:
            try:
                data = await fetch_json(
                    client, poll_url, params={"qrcode_key": qrcode_key}
                )
            except Exception as exc:
                await save_login_state(
                    qrcode_key,
                    {
                        "status": "failed",
                        "message": f"轮询异常：{exc}",
                        "poll_count": str(attempt),
                        "updated_at": now_iso(),
                    },
                )
                return

        poll_code = data.get("data", {}).get("code")

        if poll_code in (-2, 86101):
            await save_login_state(
                qrcode_key,
                {
                    "status": "pending",
                    "message": "等待扫码",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
        elif poll_code in (-4, 86090):
            await save_login_state(
                qrcode_key,
                {
                    "status": "pending",
                    "message": "已扫码，等待手机确认",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
        elif poll_code == 0:
            login_url = data["data"]["url"]
            async with httpx.AsyncClient(
                headers=AUTH_HEADERS,
                follow_redirects=True,
                timeout=API_CLIENT_TIMEOUT,
            ) as client:
                await client.get(login_url)
                cookie_str = cookies_to_string(client.cookies)

            await save_cookie(cookie_str)
            await save_login_state(
                qrcode_key,
                {
                    "status": "success",
                    "message": "登录成功，Cookie 已写入 Redis",
                    "poll_count": str(attempt),
                    "cookie": cookie_str,
                    "updated_at": now_iso(),
                },
            )
            return
        elif poll_code in (-5, 86038):
            await save_login_state(
                qrcode_key,
                {
                    "status": "expired",
                    "message": "二维码已过期，请刷新页面重新获取",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
            return
        else:
            await save_login_state(
                qrcode_key,
                {
                    "status": "failed",
                    "message": f"未知登录状态：{poll_code}",
                    "poll_count": str(attempt),
                    "updated_at": now_iso(),
                },
            )
            return

        if attempt < LOGIN_MAX_POLLS:
            await asyncio.sleep(LOGIN_POLL_INTERVAL_SECONDS)

    await save_login_state(
        qrcode_key,
        {
            "status": "expired",
            "message": "轮询次数已达上限，请重新获取二维码",
            "poll_count": str(LOGIN_MAX_POLLS),
            "updated_at": now_iso(),
        },
    )


# -----------------------------
# 收藏夹扫描
# -----------------------------
async def scan_fav(
    cookie: str, folder_name: str | None = None, uid: str | None = None
) -> list[dict]:
    """扫描指定收藏夹中未下载的视频，返回新增的 BV 列表。

    Args:
        cookie: 登录后的 Cookie 字符串。
        folder_name: 要扫描的收藏夹名称，None 表示扫描所有收藏夹。
        uid: UP 主 ID，None 表示从 cookie 中提取。

    Returns:
        新增视频列表，每个元素为 {"bv", "rid", "media_id"}。
    """
    headers = {"Cookie": cookie, **BASE_HEADERS}

    async with httpx.AsyncClient(headers=headers, timeout=API_CLIENT_TIMEOUT) as client:
        wbi_key = await get_wbi_key(client)

        if uid is None:
            nav_resp = await fetch_json(
                client, "https://api.bilibili.com/x/web-interface/nav"
            )
            uid = nav_resp["data"]["mid"]

        fav_resp = await fetch_json(
            client,
            f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={uid}",
        )
        favs = fav_resp["data"]["list"]

        if folder_name:
            favs = [f for f in favs if f["title"] == folder_name]
            if not favs:
                logger.info("未找到名称为《%s》的收藏夹", folder_name)
                return []

        new_bvs = []
        seen = set()

        for fav in favs:
            media_id = fav["id"]
            pn = 1

            while True:
                params = {"media_id": media_id, "pn": pn, "ps": 20}
                params = wbi_sign(params, wbi_key)

                resp = await fetch_json(
                    client,
                    "https://api.bilibili.com/x/v3/fav/resource/list",
                    params=params,
                )

                medias = resp["data"]["medias"]
                if not medias:
                    break

                for m in medias:
                    bv = m["bv_id"]
                    rid = m["id"]
                    if bv and bv not in seen:
                        seen.add(bv)
                        if await is_video_downloaded(bv):
                            continue
                        download_status = await get_video_download_status(bv)
                        if download_status in ("ready", "downloading"):
                            continue
                        new_bvs.append({"bv": bv, "rid": rid, "media_id": media_id})

                pn += 1

    return new_bvs


# -----------------------------
# 收藏夹内容删除
# -----------------------------
async def delete_fav_item(
    client: httpx.AsyncClient, media_id: int, rid: int, bili_jct: str
) -> bool:
    """使用 /x/v3/fav/resource/deal 从指定收藏夹删除单个视频。

    Args:
        client: 共享的 httpx AsyncClient 实例。
        media_id: 收藏夹 ID。
        rid: 视频资源 ID（对应收藏里的那条资源）。
        bili_jct: CSRF token（从 cookie 中提取）。

    Returns:
        删除成功返回 True，失败返回 False。
    """
    url = "https://api.bilibili.com/x/v3/fav/resource/deal"

    data = {
        "rid": str(rid),
        "type": "2",
        "del_media_ids": str(media_id),
        "csrf": bili_jct,
        "platform": "web",
    }

    resp = await client.post(url, data=data)
    data_resp = resp.json()

    if data_resp.get("code") != 0:
        logger.error("删除收藏失败: %s", data_resp)
        return False

    return True


# -----------------------------
# 获取指定收藏夹全部内容
# -----------------------------
async def fetch_fav_all_items(
    cookie: str,
    folder_name: str | None = None,
    uid: str | None = None,
    media_id: int | None = None,
) -> list[dict]:
    """获取指定收藏夹的全部内容，返回 [{bv, rid, title}]。

    支持两种查找方式：
      - media_id: 直接传入收藏夹完整 ID，跳过名称搜索
      - folder_name: 按名称在自建 + 收藏的收藏夹中搜索

    Args:
        cookie: 登录后的 Cookie 字符串。
        folder_name: 收藏夹名称，media_id 提供时可为 None。
        uid: 目标用户 B 站 UID，None 表示从 cookie 中提取。
        media_id: 收藏夹完整 ID（优先使用，提供后跳过名称搜索）。

    Returns:
        视频列表，每个元素包含 bv、rid、title。
    """
    headers = {"Cookie": cookie, **BASE_HEADERS}

    async with httpx.AsyncClient(headers=headers, timeout=API_CLIENT_TIMEOUT) as client:
        wbi_key = await get_wbi_key(client)
        if uid is None:
            nav_resp = await fetch_json(
                client, "https://api.bilibili.com/x/web-interface/nav"
            )
            uid = nav_resp["data"]["mid"]

        # 如果提供了 media_id，直接使用；否则在自建 + 收藏的收藏夹中按名称查找
        if media_id is None:
            if not folder_name:
                return []

            # 并行搜索自建收藏夹和收藏的别人的收藏夹
            fav_resp = await fetch_json(
                client,
                f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={uid}",
            )
            self_favs = fav_resp["data"]["list"]

            collected_favs = await fetch_collected_fav_folders(uid, cookie)

            # 合并两类收藏夹，优先匹配自建的
            target = None
            for f in self_favs:
                if f["title"] == folder_name:
                    target = f
                    break
            if target is None:
                for f in collected_favs:
                    if f["title"] == folder_name:
                        target = f
                        break

            if not target:
                return []

            # 记录原创建者 mid：自建为当前用户，别人的为原创建者
            target_mid = target.get("mid", uid)
            media_id = target["id"]

            # 别人的收藏夹：collected 里的 id 不能直接用于 resource/list，
            # 需要去原创建者的 created/list-all 中拿到原始 media_id
            if target_mid and target_mid != int(uid):
                creator_fav_resp = await fetch_json(
                    client,
                    f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={target_mid}",
                )
                creator_favs = creator_fav_resp.get("data", {}).get("list") or []
                creator_target = None
                for f in creator_favs:
                    if f["title"] == target["title"]:
                        creator_target = f
                        break
                if creator_target:
                    media_id = creator_target["id"]
                    logger.info(
                        "别人的收藏夹《%s》：原创建者 media_id=%s",
                        target["title"],
                        media_id,
                    )
                else:
                    logger.warning(
                        "在原创建者 (mid=%s) 处未找到收藏夹《%s》",
                        target_mid,
                        target["title"],
                    )
                    return []

        else:
            # 直接传 media_id 时，先不传 up_mid，后续从响应中的 info.mid 推断
            target_mid = None

        # 翻页获取收藏夹全部内容
        results = []
        pn = 1
        resolved_mid = False  # 是否已补上 up_mid 参数

        while True:
            params = {"media_id": media_id, "pn": pn, "ps": 20}
            # 别人的收藏夹需要传 up_mid（原创建者 mid）
            if target_mid and target_mid != int(uid):
                params["up_mid"] = target_mid
            params = wbi_sign(params, wbi_key)

            resp = await fetch_json(
                client,
                "https://api.bilibili.com/x/v3/fav/resource/list",
                params=params,
            )

            if resp.get("code") != 0:
                logger.error("获取收藏夹内容失败 media_id=%s: %s", media_id, resp)
                break

            data_block = resp.get("data") or {}
            medias = data_block.get("medias") or []

            # medias 为 null 但 info 有数据：可能是别人的收藏夹
            # resource/list 不支持直接查别人的收藏夹，需要拿到原创建者那边的 media_id
            if not medias and not resolved_mid:
                info = data_block.get("info") or {}
                info_mid = info.get("mid", 0)
                info_title = info.get("title", "")
                if info_mid and info_mid != int(uid) and info_title:
                    logger.info(
                        "收藏夹 media_id=%s 为别人的（creator=%s title=%s），"
                        "去原创建者处获取 media_id",
                        media_id,
                        info_mid,
                        info_title,
                    )
                    # 去原创建者的 created/list-all 中按名称匹配拿到原始 media_id
                    creator_fav_resp = await fetch_json(
                        client,
                        f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={info_mid}",
                    )
                    creator_favs = creator_fav_resp.get("data", {}).get("list") or []
                    target = None
                    for f in creator_favs:
                        if f["title"] == info_title:
                            target = f
                            break
                    if target:
                        media_id = target["id"]
                        target_mid = info_mid
                        resolved_mid = True
                        logger.info("在原创建者处找到收藏夹，media_id=%s", media_id)
                        continue
                    else:
                        logger.warning(
                            "在原创建者 (mid=%s) 处未找到收藏夹《%s》",
                            info_mid,
                            info_title,
                        )
                        break
                else:
                    logger.info(
                        "收藏夹 media_id=%s 为空，完整 data: %s", media_id, data_block
                    )
                    break

            if not medias:
                break

            resolved_mid = True  # 正常拿到数据，后续翻页不再重试

            for m in medias:
                bv = m.get("bv_id") or m.get("bvid")
                rid = m.get("id")
                title = m.get("title")

                if bv:
                    results.append(
                        {
                            "bv": bv,
                            "rid": rid,
                            "title": title,
                        }
                    )

            has_more = resp.get("data", {}).get("has_more")
            if not has_more:
                break
            pn += 1

        return results


# -----------------------------
# 获取用户收藏的别人的收藏夹（追更/订阅）
# -----------------------------
async def fetch_collected_fav_folders(uid: int, cookie: str) -> list[dict]:
    """获取指定用户收藏的别人的收藏夹列表（追更/订阅的收藏夹）。

    Args:
        uid: 目标用户的 B 站 UID。
        cookie: 登录后的 Cookie 字符串。

    Returns:
        收藏夹列表，每个元素包含 id、fid、title、media_count、mid（创建者）、
        upper_name（创建者昵称）。
    """
    headers = {
        "Cookie": cookie,
        **BASE_HEADERS,
        "Origin": "https://space.bilibili.com",
        "Referer": f"https://space.bilibili.com/{uid}/favlist",
    }

    async with httpx.AsyncClient(headers=headers, timeout=API_CLIENT_TIMEOUT) as client:
        wbi_key = await get_wbi_key(client)

        # 分页获取全部收藏列表
        # platform=web 确保返回值包含用户收藏的视频合集
        all_folders = []
        pn = 1
        while True:
            params: dict = {"up_mid": uid, "pn": pn, "ps": 20, "platform": "web"}
            params = wbi_sign(params, wbi_key)

            resp = await fetch_json(
                client,
                "https://api.bilibili.com/x/v3/fav/folder/collected/list",
                params=params,
            )

            if resp.get("code") != 0:
                logger.error(
                    "获取用户 %s 收藏的收藏夹列表失败 (pn=%d): %s", uid, pn, resp
                )
                break

            data = resp.get("data")
            if not data:
                break
            folders = data.get("list") or []
            if not folders:
                break

            for f in folders:
                all_folders.append(
                    {
                        "id": f["id"],
                        "fid": f.get("fid", 0),
                        "title": f["title"],
                        "media_count": f.get("media_count", 0),
                        "mid": f.get("mid", 0),
                        "upper_name": f.get("upper", {}).get("name", ""),
                    }
                )

            has_more = data.get("has_more", False)
            if not has_more:
                break
            pn += 1

        return all_folders


# -----------------------------
# 获取用户空间的视频合集（season）+ 视频系列（series）
# -----------------------------
async def fetch_video_seasons_series(uid: int, cookie: str) -> list[dict]:
    """获取指定用户空间的视频合集和视频系列列表。

    B 站有两类组合内容：
      - season（合集）：立体叠放正方形图标，创作者中心管理
      - series（系列/频道）：平面叠放矩形图标，个人空间直接操作

    本函数通过 web-space/home/seasons_series 接口一次性获取两类数据。

    Args:
        uid: 目标用户的 B 站 UID。
        cookie: 登录后的 Cookie 字符串。

    Returns:
        合并后的列表，每个元素包含 id、title、resource_count、type（"season" 或 "series"）。
    """
    headers = {
        "Cookie": cookie,
        "Origin": "https://space.bilibili.com",
        "Referer": f"https://space.bilibili.com/{uid}",
        "User-Agent": "Mozilla/5.0",
    }

    async with httpx.AsyncClient(headers=headers, timeout=API_CLIENT_TIMEOUT) as client:
        # 注意：此接口官方文档标注 WBI 签名为"不必要"，但实测有时需要
        # web_location 参数属于 seasons_archives_list 接口而非此接口，传入会导致 -400
        wbi_key = await get_wbi_key(client)
        params: dict = {
            "mid": uid,
            "page_num": 1,
            "page_size": 50,
        }
        params = wbi_sign(params, wbi_key)

        resp = await fetch_json(
            client,
            "https://api.bilibili.com/x/polymer/web-space/home/seasons_series",
            params=params,
        )

        if resp.get("code") != 0:
            logger.error("获取用户 %s 视频合集/系列失败: %s", uid, resp)
            return []

        data = resp.get("data") or {}

        # 合集（season）列表
        seasons = data.get("seasons_list") or []
        # 系列/频道列表
        series = data.get("series_list") or []

        return [
            {
                "id": s["meta"]["season_id"],
                "title": s["meta"]["name"],
                "resource_count": s["meta"]["total"],
                "type": "season",
            }
            for s in seasons
        ] + [
            {
                "id": s["meta"]["series_id"],
                "title": s["meta"]["name"],
                "resource_count": s["meta"]["total"],
                "type": "series",
            }
            for s in series
        ]


# -----------------------------
# UP 主最新视频动态（仅查一次，不翻页）
# -----------------------------
async def fetch_latest_up_video_dynamic(uid: int, cookie: str) -> dict | None:
    """只检查 UP 主最新的视频动态（不翻页），返回最新视频动态或 None。

    Args:
        uid: UP 主的 B 站 UID。
        cookie: 登录后的 Cookie 字符串。

    Returns:
        最新视频动态 dict（dynamic_id、title、bv、cover、desc、pubtime），无视频动态返回 None。
    """
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://space.bilibili.com/{uid}/dynamic",
    }

    async with httpx.AsyncClient(headers=headers, timeout=10) as client:
        wbi_key = await get_wbi_key(client)

        params = {"host_mid": uid, "offset": ""}
        params = wbi_sign(params, wbi_key)

        resp = await client.get(
            "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space",
            params=params,
        )
        data = resp.json()

    if data.get("code") != 0:
        return None

    items = data["data"]["items"]

    for item in items:
        if item["type"] != "DYNAMIC_TYPE_AV":
            continue

        archive = item["modules"]["module_dynamic"]["major"]["archive"]

        return {
            "dynamic_id": item["id_str"],
            "title": archive["title"],
            "bv": archive["bvid"],
            "cover": archive["cover"],
            "desc": archive.get("desc", ""),
            "pubtime": item["modules"]["module_author"]["pub_ts"],
        }

    return None


# -----------------------------
# 获取关注列表
# -----------------------------
async def fetch_followings(cookie: str) -> list[dict]:
    """获取当前账号关注的所有 UP 主，返回 [{uid, name}]。

    Args:
        cookie: 登录后的 Cookie 字符串。

    Returns:
        UP 主列表，每个元素包含 uid、name。
    """
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://space.bilibili.com",
    }

    async with httpx.AsyncClient(headers=headers, timeout=10) as client:
        nav_resp = await fetch_json(
            client, "https://api.bilibili.com/x/web-interface/nav"
        )

        if nav_resp.get("code") != 0:
            logger.error("无法获取用户信息: %s", nav_resp)
            return []

        uid = nav_resp["data"]["mid"]

        page = 1
        results = []

        while True:
            resp = await client.get(
                "https://api.bilibili.com/x/relation/followings",
                params={"vmid": uid, "pn": page, "ps": 50, "order": "desc"},
            )
            data = resp.json()

            if data.get("code") != 0:
                logger.error("获取关注列表失败: %s", data)
                break

            list_data = data["data"].get("list") or []
            if not list_data:
                break

            for item in list_data:
                results.append(
                    {
                        "uid": item["mid"],
                        "name": item["uname"],
                    }
                )

            page += 1

        return results


# -----------------------------
# 根据收藏夹名称获取 media_id
# -----------------------------
async def get_media_id_by_name(
    client: httpx.AsyncClient, uid: int, folder_name: str
) -> int | None:
    """根据收藏夹名称获取 media_id。

    Args:
        client: 共享的 httpx AsyncClient 实例。
        uid: B 站用户 UID。
        folder_name: 收藏夹名称。

    Returns:
        收藏夹的 media_id，未找到则返回 None。
    """
    resp = await fetch_json(
        client,
        f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={uid}",
    )
    favs = resp["data"]["list"]

    for f in favs:
        if f["title"] == folder_name:
            return f["id"]

    return None


# -----------------------------
# 获取指定 UID 用户的所有公开收藏夹
# -----------------------------
async def fetch_user_fav_folders(uid: int, cookie: str) -> list[dict]:
    """获取指定用户的所有公开收藏夹列表。

    Args:
        uid: 目标用户的 B 站 UID。
        cookie: 登录后的 Cookie 字符串。

    Returns:
        收藏夹列表，每个元素包含 id、title、media_count。
    """
    headers = {
        "Cookie": cookie,
        **BASE_HEADERS,
        "Origin": "https://space.bilibili.com",
        "Referer": f"https://space.bilibili.com/{uid}/favlist",
    }

    async with httpx.AsyncClient(headers=headers, timeout=API_CLIENT_TIMEOUT) as client:
        wbi_key = await get_wbi_key(client)

        params: dict = {"up_mid": uid, "web_location": "333.1387"}
        params = wbi_sign(params, wbi_key)

        resp = await fetch_json(
            client,
            "https://api.bilibili.com/x/v3/fav/folder/created/list-all",
            params=params,
        )

        if resp.get("code") != 0:
            logger.error("获取用户 %s 收藏夹列表失败: %s", uid, resp)
            return []

        data = resp.get("data")
        if not data:
            return []
        folders = data.get("list") or []
        return [
            {
                "id": f["id"],
                "title": f["title"],
                "media_count": f.get("media_count", 0),
            }
            for f in folders
        ]


# -----------------------------
# UP 主动态获取
# -----------------------------
async def fetch_all_up_video_dynamic(
    uid: int, cookie: str, max_count: int = 0
) -> list[dict]:
    """自动翻页 + WBI 签名，获取指定 UP 主的全部视频动态（DYNAMIC_TYPE_AV）。

    Args:
        uid: UP 主的 B 站 UID。
        cookie: 登录后的 Cookie 字符串。
        max_count: 最多获取的视频动态数量，0 表示不限制。

    Returns:
        视频动态列表，每个元素包含 type、dynamic_id、title、bv、cover、desc、pubtime、duration。
    """
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://space.bilibili.com/{uid}/dynamic",
        "Origin": "https://www.bilibili.com",
    }

    offset = ""
    results = []

    async with httpx.AsyncClient(headers=headers, timeout=10) as client:
        wbi_key = await get_wbi_key(client)

        while True:
            params = {
                "host_mid": uid,
                "offset": offset,
            }
            params = wbi_sign(params, wbi_key)

            resp = await client.get(
                "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space",
                params=params,
            )
            data = resp.json()

            if data.get("code") != 0:
                logger.error("获取动态失败: %s", data)
                break

            items = data["data"]["items"]
            if not items:
                break

            for item in items:
                if item["type"] != "DYNAMIC_TYPE_AV":
                    continue

                archive = item["modules"]["module_dynamic"]["major"]["archive"]

                results.append(
                    {
                        "type": "video",
                        "dynamic_id": item["id_str"],
                        "title": archive["title"],
                        "bv": archive["bvid"],
                        "cover": archive["cover"],
                        "desc": archive.get("desc", ""),
                        "pubtime": item["modules"]["module_author"]["pub_ts"],
                        "duration": parse_duration_text(
                            archive.get("duration_text", "")
                        ),
                    }
                )

                # 达到数量上限时停止收集
                if max_count > 0 and len(results) >= max_count:
                    break

            # 达到数量上限或没有更多页时退出
            if max_count > 0 and len(results) >= max_count:
                break

            offset = data["data"]["offset"]
            if not offset:
                break

    return results
