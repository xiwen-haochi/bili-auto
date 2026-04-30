import os
import re
import json
import subprocess
from io import BytesIO

import base64
import requests
import redis
import qrcode
from tqdm import tqdm
from fastapi import FastAPI

# -----------------------------
# 配置
# -----------------------------
COOKIE_FILE = "bili_cookie.txt"
REDIS_KEY = "bili:downloaded"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
app = FastAPI()

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com",
}


# -----------------------------
# 工具函数
# -----------------------------
def save_cookie(cookie: str):
    with open(COOKIE_FILE, "w") as f:
        f.write(cookie)


def load_cookie():
    if not os.path.exists(COOKIE_FILE):
        return None
    return open(COOKIE_FILE).read().strip()


def extract_bvid(url: str):
    if "b23.tv" in url:
        resp = requests.get(url, headers=BASE_HEADERS, allow_redirects=True)
        url = resp.url
    m = re.search(r"BV([a-zA-Z0-9]{10})", url)
    return "BV" + m.group(1) if m else None


# -----------------------------
# 登录：获取二维码
# -----------------------------
@app.get("/login_qrcode")
def login_qrcode():
    """
    返回：
    - qrcode_key：用于轮询登录状态
    - qrcode_base64：前端直接 <img :src="qrcode_base64">
    """
    session = requests.Session()

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bilibili.com",
        "Origin": "https://www.bilibili.com",
    }

    api = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
    resp = session.get(api, headers=headers).json()

    if resp.get("code") != 0:
        return {"error": "获取二维码失败", "raw": resp}

    qrcode_url = resp["data"]["url"]
    qrcode_key = resp["data"]["qrcode_key"]

    # 用 qrcode 库本地生成二维码图片
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_L)
    qr.add_data(qrcode_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buffer = BytesIO()
    img.save(buffer, format="PNG")
    img_base64 = base64.b64encode(buffer.getvalue()).decode()

    return {
        "qrcode_key": qrcode_key,
        "qrcode_base64": "data:image/png;base64," + img_base64,
    }


# -----------------------------
# 登录：轮询二维码状态
# -----------------------------
@app.get("/login_poll")
def login_poll(qrcode_key: str):
    url = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bilibili.com",
        "Origin": "https://www.bilibili.com",
    }

    resp = requests.get(url, params={"qrcode_key": qrcode_key}, headers=headers)
    try:
        data = resp.json()
    except Exception:
        return {"error": "B站返回的不是JSON", "raw": resp.text}

    code = data.get("data", {}).get("code")

    # -2: 未扫码；-4: 已扫码未确认
    if code in (-2, -4):
        return {"status": "pending", "data": data}

    # 0: 登录成功
    if code == 0:
        login_url = data["data"]["url"]
        session = requests.Session()
        session.get(login_url, headers=headers)

        cookie_str = "; ".join([f"{k}={v}" for k, v in session.cookies.items()])
        save_cookie(cookie_str)

        return {"status": "success", "cookie": cookie_str}

    return {"status": "unknown", "data": data}


# -----------------------------
# 下载相关
# -----------------------------
def download_file(url: str, filename: str, cookie: str):
    headers = BASE_HEADERS.copy()
    headers["Cookie"] = cookie

    resp = requests.get(url, headers=headers, stream=True)
    total = int(resp.headers.get("content-length", 0))

    with (
        open(filename, "wb") as f,
        tqdm(
            total=total, unit="B", unit_scale=True, desc=os.path.basename(filename)
        ) as bar,
    ):
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))


def merge_av(video_file: str, audio_file: str, output_file: str):
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        video_file,
        "-i",
        audio_file,
        "-c",
        "copy",
        output_file,
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def download_bv(bvid: str, cookie: str):
    print(f"[下载] BV: {bvid}")

    headers = {"Cookie": cookie, **BASE_HEADERS}

    # 获取视频信息
    view_resp = requests.get(
        "https://api.bilibili.com/x/web-interface/view",
        params={"bvid": bvid},
        headers=headers,
    ).json()

    if view_resp.get("code") != 0:
        print("[错误] 获取视频信息失败：", view_resp)
        return

    view = view_resp["data"]
    title = view["title"]
    cid = view["pages"][0]["cid"]

    # 获取播放地址
    play_resp = requests.get(
        "https://api.bilibili.com/x/player/playurl",
        params={"bvid": bvid, "cid": cid, "qn": 80, "fnval": 16},
        headers=headers,
    ).json()

    if play_resp.get("code") != 0:
        print("[错误] 获取播放地址失败：", play_resp)
        return

    play = play_resp["data"]
    video_url = play["dash"]["video"][0]["baseUrl"]
    audio_url = play["dash"]["audio"][0]["baseUrl"]

    safe_title = re.sub(r'[\\/:*?"<>|]', "_", title)
    os.makedirs(safe_title, exist_ok=True)

    video_file = os.path.join(safe_title, "video.m4s")
    audio_file = os.path.join(safe_title, "audio.m4s")
    output_file = os.path.join(safe_title, safe_title + ".mp4")

    download_file(video_url, video_file, cookie)
    download_file(audio_url, audio_file, cookie)

    merge_av(video_file, audio_file, output_file)

    print(f"[完成] {output_file}")


# -----------------------------
# 扫描收藏夹
# -----------------------------
def scan_fav(cookie: str):
    headers = {"Cookie": cookie, **BASE_HEADERS}

    # 获取用户 UID
    nav_resp = requests.get(
        "https://api.bilibili.com/x/web-interface/nav", headers=headers
    ).json()
    if nav_resp.get("code") != 0:
        print("[错误] 获取用户信息失败：", nav_resp)
        return []

    uid = nav_resp["data"]["mid"]

    # 获取收藏夹列表
    fav_resp = requests.get(
        f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={uid}",
        headers=headers,
    ).json()

    if fav_resp.get("code") != 0:
        print("[错误] 获取收藏夹列表失败：", fav_resp)
        return []

    favs = fav_resp["data"]["list"]

    new_bvs = []

    for fav in favs:
        media_id = fav["id"]
        pn = 1

        while True:
            url = "https://api.bilibili.com/x/v3/fav/resource/list"
            resp = requests.get(
                url,
                params={"media_id": media_id, "pn": pn, "ps": 20},
                headers=headers,
            ).json()

            if resp.get("code") != 0:
                print("[错误] 获取收藏夹内容失败：", resp)
                break

            medias = resp["data"]["medias"]
            if not medias:
                break

            for m in medias:
                bv = m["bv_id"]
                if not r.sismember(REDIS_KEY, bv):
                    new_bvs.append(bv)

            pn += 1

    return new_bvs


@app.get("/scan_fav")
def scan_fav_api():
    cookie = load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    new_bvs = scan_fav(cookie)

    downloaded = []
    for bv in new_bvs:
        download_bv(bv, cookie)
        r.sadd(REDIS_KEY, bv)
        downloaded.append(bv)
        break  # 先测试一个，确认没问题再批量下载

    return {"downloaded": downloaded}


# -----------------------------
# Cookie 保活接口（定期调用）
# -----------------------------
@app.get("/keep_alive")
def keep_alive():
    """
    你可以用定时任务定期请求这个接口，
    它会访问一次需要登录的接口，帮助 Cookie 续期。
    """
    cookie = load_cookie()
    if not cookie:
        return {"error": "not logged in"}

    headers = {"Cookie": cookie, **BASE_HEADERS}
    resp = requests.get("https://api.bilibili.com/x/web-interface/nav", headers=headers)

    try:
        data = resp.json()
    except Exception:
        return {"status": "failed", "raw": resp.text[:200]}

    if data.get("code") == 0:
        return {"status": "ok", "uname": data["data"]["uname"]}
    else:
        return {"status": "failed", "data": data}


# -----------------------------
# 启动
# -----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
