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


async def download_face(url: str, uname: str):
    """下载 UP 主头像"""
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


async def search_up(keyword: str, exact: bool = False, download_avatar: bool = False):
    """
    搜索 UP 主（模糊搜索），可选精准匹配 & 下载头像
    """
    url = "https://api.bilibili.com/x/web-interface/search/all/v2"

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url, params={"keyword": keyword})
        data = await safe_json(resp)

    if data.get("code") != 0:
        return {"error": "search failed", "raw": data}

    result = data["data"].get("result") or []

    # 找到 bili_user 分组
    users = []
    for block in result:
        if block.get("result_type") == "bili_user":
            users = block.get("data", [])
            break

    # 精准匹配
    if exact:
        users = [u for u in users if u["uname"] == keyword]

    # 只取前 20 个
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

        # 下载头像
        if download_avatar:
            item["face_local"] = await download_face(u["upic"], u["uname"])

        results.append(item)

    return {
        "keyword": keyword,
        "exact": exact,
        "count": len(results),
        "items": results,
    }


async def up_info(mid: int):
    """
    获取指定 UP 主的详细信息
    """
    url = "https://api.bilibili.com/x/web-interface/card"

    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url, params={"mid": mid})
        data = await safe_json(resp)

    if data.get("code") != 0:
        return {"error": "fetch failed", "raw": data}

    card = data["data"]["card"]
    stats = data["data"]["follower"]

    return {
        "mid": mid,
        "name": card["name"],
        "sex": card["sex"],
        "face": card["face"],
        "sign": card["sign"],
        "fans": stats,
        "level": card["level_info"]["current_level"],
    }


async def main():
    print(await search_up("张靓颖", download_avatar=True))
    # print(await up_info(14547903))
    # time.sleep(1)
    # print(await up_info(384542128))


asyncio.run(main())
