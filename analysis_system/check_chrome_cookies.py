#!/usr/bin/env python3
"""检查浏览器中的 Cookie。"""
import asyncio
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MEDIA_CRAWLER_ROOT = PROJECT_ROOT / "MediaCrawler-main"

if not MEDIA_CRAWLER_ROOT.exists():
    print("未找到 MediaCrawler-main 目录，当前仓库默认以公开数据分析模式运行。")
    raise SystemExit(0)

sys.path.insert(0, str(MEDIA_CRAWLER_ROOT))

from tools import utils
from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        except Exception:
            print("[错误] Chrome 未运行或未开启调试模式。")
            print("[提示] 可以先运行: python login_cdp.py")
            return

        contexts = browser.contexts
        if not contexts:
            print("[错误] 没有浏览器上下文。")
            return

        context = contexts[0]
        cookies = await context.cookies()
        xhs_cookies = [cookie for cookie in cookies if "xiaohongshu.com" in cookie.get("domain", "")]

        print(f"找到 {len(xhs_cookies)} 个小红书 Cookie:")
        print("-" * 50)

        for cookie in xhs_cookies:
            print(f"  {cookie['name']}: {cookie['value'][:30]}...")

        print("-" * 50)

        _, cookie_dict = utils.convert_cookies(xhs_cookies)
        web_session = cookie_dict.get("web_session", "")
        print(f"\nweb_session: {web_session}")
        print(f"长度: {len(web_session)}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
