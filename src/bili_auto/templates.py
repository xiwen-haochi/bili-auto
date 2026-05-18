"""
HTML 页面模板模块。

将原先内联在 api.py 中的前端页面抽离到此，
方便后续维护和独立测试。
"""

import json


def build_login_page(qrcode_key: str, qrcode_data_url: str) -> str:
    """返回一个可直接扫码的登录页面，内嵌二维码图片和前端轮询逻辑。

    Args:
        qrcode_key: B 站返回的二维码 key，用于轮询查询登录状态。
        qrcode_data_url: build_qrcode_data_url() 生成的 base64 图片 data URL。

    Returns:
        完整的 HTML 字符串。
    """
    # 延迟导入避免循环引用（config.py 不依赖 templates.py）
    from bili_auto.config import LOGIN_MAX_POLLS

    qrcode_key_json = json.dumps(qrcode_key)
    return f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>B 站扫码登录</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: linear-gradient(135deg, #f7efe6 0%, #d8ecff 100%);
      font-family: "PingFang SC", "Hiragino Sans GB", sans-serif;
      color: #1f2937;
    }}
    .panel {{
      width: min(92vw, 420px);
      padding: 28px;
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 20px 50px rgba(15, 23, 42, 0.15);
      text-align: center;
      backdrop-filter: blur(12px);
    }}
    img {{
      width: min(72vw, 280px);
      height: min(72vw, 280px);
      border-radius: 16px;
      background: #fff;
      padding: 12px;
      box-sizing: border-box;
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 26px;
    }}
    p {{
      margin: 8px 0;
      line-height: 1.6;
    }}
    .meta {{
      color: #4b5563;
      font-size: 14px;
      word-break: break-all;
    }}
    .status {{
      margin-top: 18px;
      padding: 14px;
      border-radius: 14px;
      background: #f8fafc;
    }}
  </style>
</head>
<body>
  <main class=\"panel\">
    <h1>B 站扫码登录</h1>
    <p>请直接使用 B 站 App 扫码，服务端会在后台每 10 秒轮询一次登录状态。</p>
    <img src=\"{qrcode_data_url}\" alt=\"B 站登录二维码\" />
    <div class=\"status\">
      <p id=\"status\">状态：等待扫码</p>
      <p id=\"message\">说明：二维码已生成，后台轮询已启动。</p>
      <p id=\"poll-count\" class=\"meta\">轮询次数：0 / {LOGIN_MAX_POLLS}</p>
      <p class=\"meta\">二维码 Key：{qrcode_key}</p>
    </div>
  </main>
  <script>
    const qrcodeKey = {qrcode_key_json};
    const statusEl = document.getElementById("status");
    const messageEl = document.getElementById("message");
    const pollCountEl = document.getElementById("poll-count");

    async function refreshLoginStatus() {{
      try {{
        const response = await fetch(`/login_poll?qrcode_key=${{encodeURIComponent(qrcodeKey)}}`, {{ cache: "no-store" }});
        const data = await response.json();

        statusEl.textContent = `状态：${{data.status || "unknown"}}`;
        messageEl.textContent = `说明：${{data.message || "暂无状态说明"}}`;
        pollCountEl.textContent = `轮询次数：${{data.poll_count || 0}} / {LOGIN_MAX_POLLS}`;

        if (["success", "expired", "failed", "not_found"].includes(data.status)) {{
          return;
        }}
      }} catch (error) {{
        messageEl.textContent = `说明：状态查询失败，${{error}}`;
      }}

      setTimeout(refreshLoginStatus, 2000);
    }}

    refreshLoginStatus();
  </script>
</body>
</html>
"""
