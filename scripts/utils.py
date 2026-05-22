import httpx
import asyncio
import json
from pathlib import Path


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
    "Accept": "application/json, text/plain, */*",
}


def normalize_url(url: str) -> str:
    if url.startswith("//"):
        return "https:" + url
    if not url.startswith("http"):
        return "https://" + url
    return url


async def safe_json(resp: httpx.Response):
    try:
        return resp.json()
    except json.JSONDecodeError:
        return {
            "code": -999,
            "message": "not json",
            "raw": resp.text[:200],
        }


# ---------------------------------------------------------
# 下载头像
# ---------------------------------------------------------
async def download_face(url: str, uname: str):
    url = normalize_url(url)
    base_dir = Path(__file__).parent
    folder = Path(base_dir, "up_faces")
    folder.mkdir(exist_ok=True)

    ext = url.split("?")[0].split(".")[-1]
    filename = folder / f"{uname}.{ext}"

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url)
        filename.write_bytes(resp.content)

    return str(filename)


# ---------------------------------------------------------
# 搜索 UP 主
# ---------------------------------------------------------
async def search_up(keyword: str, exact: bool = False, download_avatar: bool = False):
    url = "https://api.bilibili.com/x/web-interface/search/all/v2"

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url, params={"keyword": keyword})
        data = await safe_json(resp)

    if data.get("code") != 0:
        return {"error": "search failed", "raw": data}

    result = data["data"].get("result") or []

    users = []
    for block in result:
        if block.get("result_type") == "bili_user":
            users = block.get("data", [])
            break

    if exact:
        users = [u for u in users if u["uname"] == keyword]

    users = users[:20]
    if len(users) != 1:
        download_avatar = False

    results = []
    for u in users:
        item = {
            "mid": u["mid"],
            "uname": u["uname"],
            "fans": u.get("fans", 0),
            "sign": u.get("usign", ""),
            "face": u.get("upic", ""),
        }

        if download_avatar:
            item["face_local"] = await download_face(u["upic"], u["uname"])

        results.append(item)

    return {
        "keyword": keyword,
        "exact": exact,
        "count": len(results),
        "items": results,
    }


# ---------------------------------------------------------
# 获取动态列表
# ---------------------------------------------------------
# async def fetch_dynamics(mid: int):
#     """获取 UP 主动态（新版接口，稳定可用）"""
#     url = "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space"
#     params = {"host_mid": mid, "offset": "", "features": "itemOpusStyle"}  # 关键参数

#     async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
#         resp = await client.get(url, params=params)
#         data = await safe_json(resp)

#     if data.get("code") != 0:
#         return []

#     return data["data"].get("items", [])


async def fetch_dynamics(mid: int):
    """获取 UP 主动态（新版接口 + Cookie）"""
    url = "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space"
    params = {"host_mid": mid, "offset": "", "features": "itemOpusStyle"}

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url, params=params)
        data = await safe_json(resp)

    if data.get("code") != 0:
        return []

    return data["data"].get("items", [])


# ---------------------------------------------------------
# 从动态中提取图片
# ---------------------------------------------------------


def extract_images_from_dynamic(card: dict):
    """从动态中提取所有图片 URL（支持新版 opus、多图、图文混排）"""
    modules = card.get("modules", {})
    pics = []

    # 1) 新版动态：opus（2024+）
    md = modules.get("module_dynamic", {})
    major = md.get("major", {})

    if major.get("type") == "MAJOR_TYPE_OPUS":
        opus = major.get("opus", {})
        content = opus.get("content", [])
        for block in content:
            if block.get("type") == "IMAGE":
                pics.append(block["original_url"])

    # 2) 图文动态（旧版）
    if major.get("type") == "MAJOR_TYPE_DRAW":
        draw = major.get("draw", {})
        for img in draw.get("items", []):
            pics.append(img["src"])

    # 3) 专栏动态（article）
    if major.get("type") == "MAJOR_TYPE_ARTICLE":
        article = major.get("article", {})
        for img in article.get("covers", []):
            pics.append(img)

    # 4) 图文混排（rich_text_nodes）
    desc = md.get("desc", {})
    for node in desc.get("rich_text_nodes", []):
        if node.get("type") == "RICH_TEXT_NODE_TYPE_IMAGE":
            pics.append(node.get("text", ""))

    # 去重
    pics = [p for p in pics if p]
    return list(dict.fromkeys(pics))


# ---------------------------------------------------------
# 下载单张图片
# ---------------------------------------------------------
async def download_image(url: str, save_path: Path):
    url = normalize_url(url)
    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url)
        save_path.write_bytes(resp.content)


# ---------------------------------------------------------
# 下载 UP 所有动态图片
# ---------------------------------------------------------
async def download_up_photos(mid: int):
    base_dir = Path(__file__).parent
    root = Path(base_dir, "up_photos", str(mid))
    root.mkdir(parents=True, exist_ok=True)

    cards = await fetch_dynamics(mid)
    print(cards)
    results = []

    for card in cards:
        dynamic_id = card.get("id_str") or card.get("id")
        images = extract_images_from_dynamic(card)

        if not images:
            continue

        dyn_folder = root / str(dynamic_id)
        dyn_folder.mkdir(exist_ok=True)

        saved_files = []
        for idx, img_url in enumerate(images, start=1):
            ext = img_url.split("?")[0].split(".")[-1]
            filename = dyn_folder / f"{idx}.{ext}"
            await download_image(img_url, filename)
            saved_files.append(str(filename))

        results.append(
            {
                "dynamic_id": dynamic_id,
                "count": len(saved_files),
                "files": saved_files,
            }
        )

    return results


# ---------------------------------------------------------
# 获取 UP 主信息（新增 download_photos 参数）
# ---------------------------------------------------------
async def up_info(mid: int, download_photos: bool = False):
    url = "https://api.bilibili.com/x/web-interface/card"

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url, params={"mid": mid})
        data = await safe_json(resp)

    if data.get("code") != 0:
        return {"error": "fetch failed", "raw": data}

    card = data["data"]["card"]
    stats = data["data"]["follower"]

    result = {
        "mid": mid,
        "name": card["name"],
        "sex": card["sex"],
        "face": card["face"],
        "sign": card["sign"],
        "fans": stats,
        "level": card["level_info"]["current_level"],
    }

    if download_photos:
        result["photos"] = await download_up_photos(mid)

    return result


# ---------------------------------------------------------
# 测试入口
# ---------------------------------------------------------
async def main():
    # 搜索示例
    # print(await search_up("张靓颖", download_avatar=True))

    # 获取 UP 信息 + 下载动态图片（下载动态目前还没成功）
    info = await up_info(123, download_photos=True)
    print(info)


asyncio.run(main())
