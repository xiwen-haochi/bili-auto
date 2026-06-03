import httpx
import asyncio
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 将 src 目录加入 Python 路径，以便导入 bili_auto 模块（复用 WBI 签名等工具函数）
_src_dir = Path(__file__).resolve().parent.parent / "src"
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

from bili_auto.utils import get_wbi_key, wbi_sign
from bili_auto.redis_client import load_cookie


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
# 下载单张图片
# ---------------------------------------------------------
async def download_image(url: str, save_path: Path):
    url = normalize_url(url)
    async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
        resp = await client.get(url)
        save_path.write_bytes(resp.content)


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
        pass

    return result


# ---------------------------------------------------------
# 下载 UP 主所有图片动态（DYNAMIC_TYPE_DRAW）
# ---------------------------------------------------------
async def download_up_all_photos(
    mid: int,
    cookie: str = "",
    max_dynamics: int = 0,
    since_date: str = "",
    save_root: str = "",
) -> dict:
    """下载指定 UP 主所有图片动态中的图片到本地。

    通过 B 站空间动态接口（web-dynamic/v1/feed/space）自动翻页获取该
    UP 主的动态，筛选出 DYNAMIC_TYPE_DRAW（图片动态）类型，
    提取图片 URL 并逐张下载。使用 WBI 签名以通过 B 站反爬校验。

    保存路径结构：
         {save_root}/{uname}_photos/{动态ID前8位}-{日期}-{序号}.{ext}
     例：
         ./downloads/up_photos/某某_photos/99586658-20241104-001.png

    Args:
        mid: UP 主的 B 站 UID。
        cookie: 登录后的 Cookie 字符串（可选），登录后可获取更高请求频率。
        max_dynamics: 最多阅读多少条动态，0 表示不限制，阅读所有。
        since_date: 起始日期（含），格式 "YYYY-MM-DD"，只下载该日期及之后的动态，
                     空字符串表示不限制。由于动态按时间倒序返回，
                     遇到早于该日期的动态即停止翻页。
        save_root: 图片保存根目录，留空默认保存到 ./downloads/up_photos/。

    Returns:
        dict: 下载统计结果。
            - mid:             UP 主 UID
            - uname:           UP 主名称
            - total_dynamics:  图片动态总数
            - total_images:    图片文件总数
            - downloaded:      成功下载数
            - failed:          下载失败数
            - save_dir:        图片保存目录绝对路径
    """
    if not cookie:
        cookie = await load_cookie()

    # 解析 since_date 为 UTC+8 午夜时间戳
    since_ts = 0
    if since_date:
        tz_cn = timezone(timedelta(hours=8))
        since_dt = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=tz_cn)
        since_ts = int(since_dt.timestamp())
        print(f"仅获取 {since_date}(含) 之后的动态（时间戳 >= {since_ts}）")

    # 构建请求头，有 cookie 时带上鉴权
    req_headers = dict(HEADERS)
    if cookie:
        req_headers["Cookie"] = cookie
    req_headers["Referer"] = f"https://space.bilibili.com/{mid}/dynamic"

    # 确定保存根目录
    if save_root:
        base_dir = Path(save_root)
    else:
        base_dir = Path(__file__).resolve().parent.parent / "downloads" / "up_photos"

    total_dynamics = 0  # 已处理的图片动态数
    total_read = 0  # 已阅读的动态总数（含非图片类型）
    total_images = 0
    downloaded = 0
    failed = 0
    uname = ""

    async with httpx.AsyncClient(headers=req_headers, timeout=30) as client:
        # 获取 WBI 签名密钥（B 站反爬参数）
        wbi_key = await get_wbi_key(client)

        # 获取 UP 主名称，用于命名保存目录
        card_resp = await client.get(
            "https://api.bilibili.com/x/web-interface/card",
            params={"mid": mid},
        )
        card_data = await safe_json(card_resp)
        if card_data.get("code") == 0:
            uname = card_data["data"]["card"]["name"]

        # 构造保存目录：{root}/{up名称}_photos/
        save_dir = (
            base_dir / f"{uname}_photos" if uname else base_dir / f"uid_{mid}_photos"
        )
        save_dir.mkdir(parents=True, exist_ok=True)
        print(f"UP 主: {uname}(UID:{mid})，保存目录: {save_dir}")

        # 翻页获取所有动态
        offset = ""
        page_count = 0
        stopped_by_date = False

        while True:
            page_count += 1

            # max_dynamics 到达上限时停止
            if max_dynamics > 0 and total_dynamics >= max_dynamics:
                print(f"已达到 max_dynamics={max_dynamics} 条图片动态上限，停止翻页")
                break

            # WBI 签名：每次翻页都需要重新签名（wts 时间戳会变）
            params = {"host_mid": mid, "offset": offset}
            params = wbi_sign(params, wbi_key)

            resp = await client.get(
                "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space",
                params=params,
            )
            data = await safe_json(resp)

            if data.get("code") != 0:
                print(f"获取动态失败: {data}")
                break

            items = data["data"]["items"]
            if not items:
                break

            print(f"第 {page_count} 页，共 {len(items)} 条动态")

            # 遍历当前页动态，筛选图片类型
            for item in items:
                total_read += 1

                # 提取发布时间戳，用于 since_date 判断
                pub_ts = int(item["modules"]["module_author"]["pub_ts"])

                # since_date 判断：动态按时间倒序，早于起始日期的直接停止全部翻页
                if since_ts > 0 and pub_ts < since_ts:
                    print(
                        f"动态时间 {datetime.fromtimestamp(pub_ts, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d')}"
                        f" 早于起始日期 {since_date}，停止翻页"
                    )
                    stopped_by_date = True
                    break

                if item["type"] != "DYNAMIC_TYPE_DRAW":
                    continue

                dynamic_id = item["id_str"]

                # DYNAMIC_TYPE_DRAW 的图片列表在 major.draw.items 中
                try:
                    draw_data = item["modules"]["module_dynamic"]["major"]["draw"]
                    print(f"err:获取图片动态数据draw_data失败")
                except TypeError:
                    continue
                images = draw_data.get("items", [])
                if not images:
                    continue

                # 满足条件：图片动态计数 +1
                total_dynamics += 1

                # 构造文件名前缀：取动态ID前8位
                name_prefix = dynamic_id[:8]

                # 将发布时间戳转为 YYYYMMDD 格式，方便按时间排序
                date_str = datetime.fromtimestamp(
                    pub_ts, tz=timezone(timedelta(hours=8))
                ).strftime("%Y%m%d")

                for idx, img in enumerate(images, 1):
                    total_images += 1
                    # B 站图片 URL 可能携带 @xxx 缩略图修饰符（如 @104w_104h_1c），去除后获取原图
                    raw_url = img["src"]
                    raw_url = re.sub(r"@[^./?&]+", "", raw_url)
                    img_url = normalize_url(raw_url)

                    # 从 URL 中提取文件扩展名，无效时默认 jpg
                    ext = img_url.split("?")[0].split(".")[-1].lower()
                    if ext not in ("jpg", "jpeg", "png", "gif", "webp", "bmp"):
                        ext = "jpg"

                    # 文件名格式：{前缀}-{日期}-{序号}.{ext}，所有图片平铺在同一目录下
                    filename = save_dir / f"{name_prefix}-{date_str}-{idx:03d}.{ext}"

                    # 跳过已存在的文件（支持断点续传）
                    if filename.exists():
                        downloaded += 1
                        print(f"  [跳过] {filename.name} 已存在")
                        continue

                    try:
                        await download_image(img_url, filename)
                        downloaded += 1
                        print(f"  [{downloaded}/{total_images}] {filename.name}")
                    except Exception as exc:
                        failed += 1
                        print(f"  [失败] {filename.name}: {exc}")

                # max_dynamics 到达上限时停止本页遍历
                if max_dynamics > 0 and total_dynamics >= max_dynamics:
                    break

                # 动态间短暂延迟，避免请求过快被封
                await asyncio.sleep(0.5)

            # 因 since_date 提前退出，跳出翻页循环
            if stopped_by_date:
                break

            # 翻页：offset 为空表示已到最后一页
            offset = data["data"].get("offset", "")
            if not offset:
                break

            # 页间延迟
            await asyncio.sleep(1)

    result = {
        "mid": mid,
        "uname": uname,
        "total_dynamics": total_dynamics,
        "total_images": total_images,
        "downloaded": downloaded,
        "failed": failed,
        "save_dir": str(save_dir),
    }

    print(f"\n=== 下载完成 ===")
    print(f"UP 主: {uname}(UID:{mid})")
    print(f"图片动态: {total_dynamics} 条, 图片: {total_images} 张")
    print(f"成功: {downloaded}, 失败: {failed}")
    if since_date:
        print(f"时间范围: {since_date}(含) 之后")
    print(f"保存目录: {save_dir}")

    return result


# ---------------------------------------------------------
# 测试入口
# ---------------------------------------------------------
async def main():

    # 搜索示例
    print(await search_up("吱吱", download_avatar=False))
    #       348710707

    # 获取 UP 信息
    # info = await up_info(1889545341)
    # print(info)

    # 20260522 使用过
    # 下载 UP 主所有图片动态（基本用法）
    # result = await download_up_all_photos(mid=1889545341)
    # print(result)

    # 仅阅读最近 50 条动态中的图片动态
    # result = await download_up_all_photos(mid=1889545341, max_dynamics=50)
    # print(result)

    # 只获取 2026-04-22 及之后的图片动态（不含更早的动态）
    # result = await download_up_all_photos(mid=1889545341, since_date="2026-04-22")
    # print(result)

    # 组合使用：最近 50 条 + 日期限制
    # result = await download_up_all_photos(
    #     mid=1889545341, max_dynamics=50, since_date="2026-04-22"
    # )
    # print(result)

    # print("请在 main() 中取消注释需要的示例并传入正确的 mid 参数运行")


asyncio.run(main())
