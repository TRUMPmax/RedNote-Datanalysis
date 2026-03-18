#!/usr/bin/env python3
"""检查浏览器中的Cookie"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "MediaCrawler-main"))

from tools import utils
from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        # 连接到正在运行的Chrome (CDP模式)
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        except:
            print("[错误] Chrome未运行或未开启调试模式")
            print("[提示] 可以先运行: python login_cdp.py")
            return
        
        contexts = browser.contexts
        if not contexts:
            print("[错误] 没有浏览器上下文")
            return
        
        # 获取第一个上下文
        context = contexts[0]
        cookies = await context.cookies()
        
        # 筛选小红书Cookie
        xhs_cookies = [c for c in cookies if 'xiaohongshu.com' in c.get('domain', '')]
        
        print(f"找到 {len(xhs_cookies)} 个小红书Cookie:")
        print("-" * 50)
        
        for c in xhs_cookies:
            print(f"  {c['name']}: {c['value'][:30]}...")
        
        print("-" * 50)
        
        # 检查web_session
        _, cookie_dict = utils.convert_cookies(xhs_cookies)
        web_session = cookie_dict.get("web_session", "")
        print(f"\nweb_session: {web_session}")
        print(f"长度: {len(web_session)}")
        
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
