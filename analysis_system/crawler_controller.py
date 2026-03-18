# 爬虫控制器模块 - 简化版
import os
import sys
import json
import subprocess
import threading
import time
import base64
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio

# 路径配置
CRAWLER_DIR = Path(__file__).parent.parent / "MediaCrawler-main"
DATA_DIR = CRAWLER_DIR / "data" / "xhs"
LOGIN_QR_FILE = Path(__file__).parent / "static" / "qrcode.png"

class CrawlerController:
    """爬虫进程控制器"""
    
    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.status = "idle"
        self.start_time: Optional[float] = None
        self.log_lines = []
        self._log_thread: Optional[threading.Thread] = None
    
    def start(self, keywords: str, login_type: str = "cookie", 
              crawl_type: str = "search", max_count: int = 50) -> Dict[str, Any]:
        """启动爬虫"""
        if self.process and self.status == "running":
            return {"success": False, "message": "爬虫已在运行中"}
        
        try:
            # 构建命令 - 使用 Cookie 登录需要预先配置
            cmd = [
                sys.executable,
                str(CRAWLER_DIR / "main.py"),
                "--platform", "xhs",
                "--lt", login_type,
                "--type", crawl_type,
                "--keywords", keywords,
                "--max_count", str(max_count),
                "--save_data_option", "sqlite"
            ]
            
            self.process = subprocess.Popen(
                cmd,
                cwd=str(CRAWLER_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                bufsize=1
            )
            
            self.status = "running"
            self.start_time = time.time()
            self.log_lines = []
            
            self._log_thread = threading.Thread(target=self._read_log, daemon=True)
            self._log_thread.start()
            
            return {"success": True, "message": "爬虫已启动", "pid": self.process.pid}
            
        except Exception as e:
            self.status = "error"
            return {"success": False, "message": str(e)}
    
    def _read_log(self):
        if not self.process:
            return
        
        for line in self.process.stdout:
            if line.strip():
                self.log_lines.append(line.strip())
                if len(self.log_lines) > 1000:
                    self.log_lines = self.log_lines[-1000:]
        
        if self.process:
            self.process.wait()
            self.status = "stopped"
    
    def stop(self) -> Dict[str, Any]:
        if not self.process:
            return {"success": False, "message": "没有运行中的爬虫"}
        
        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.status = "stopped"
            return {"success": True, "message": "爬虫已停止"}
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        if self.process:
            poll = self.process.poll()
            if poll is not None:
                self.status = "stopped"
        
        notes_count = 0
        comments_count = 0
        
        try:
            if DATA_DIR.exists():
                for f in DATA_DIR.glob("*.csv"):
                    try:
                        content = f.read_text(encoding='utf-8', errors='ignore')
                        lines = content.strip().split('\n')
                        if "note_id" in content:
                            notes_count += len(lines) - 1
                        elif "comment_id" in content:
                            comments_count += len(lines) - 1
                    except:
                        pass
        except Exception:
            pass
        
        runtime = int(time.time() - self.start_time) if self.start_time else 0
        
        return {
            "status": self.status,
            "runtime": runtime,
            "notes_collected": notes_count,
            "comments_collected": comments_count,
            "recent_logs": self.log_lines[-20:] if self.log_lines else []
        }
    
    def get_logs(self, lines: int = 50) -> list:
        return self.log_lines[-lines:]


class LoginController:
    """登录控制器 - 启动独立浏览器获取二维码"""
    
    def __init__(self):
        self.status = "idle"
        self.process = None
        self._monitor_thread = None
        self._login_success = False
    
    def start_qrcode_login(self) -> Dict[str, Any]:
        """启动二维码登录 - 使用简化脚本"""
        if self.process:
            return {"success": False, "message": "登录流程已在进行中"}
        
        # 创建登录脚本
        login_script = CRAWLER_DIR / "login_qr.py"
        login_script.write_text('''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import sys
import os
from pathlib import Path

# 添加项目路径
crawler_dir = Path(__file__).parent
sys.path.insert(0, str(crawler_dir))

from tools.crawler_util import find_login_qrcode
from playwright.async_api import async_playwright
import base64

async def main():
    qrcode_base64 = None
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled'])
        context = await browser.new_context(
            viewport={"width": 800, "height": 700},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        
        # 访问小红书
        print("正在打开小红书...", flush=True)
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="networkidle")
        
        # 等待并点击登录按钮
        try:
            login_btn = page.locator("xpath=//div[@class='login-container']//img[@class='qrcode-img']")
            await login_btn.wait_for(timeout=5000)
            print("QRCODE_READY", flush=True)
        except:
            # 可能已显示登录框
            try:
                await page.wait_for_selector("img.qrcode-img", timeout=5000)
                print("QRCODE_READY", flush=True)
            except:
                print("需要手动点击登录", flush=True)
                await asyncio.sleep(2)
        
        # 获取二维码
        try:
            qrcode = await find_login_qrcode(page, "img.qrcode-img")
            if qrcode:
                print(f"QRCODE:{qrcode}", flush=True)
        except Exception as e:
            print(f"获取二维码失败: {e}", flush=True)
        
        # 等待登录成功
        print("请使用小红书APP扫码登录... (等待120秒)", flush=True)
        
        try:
            await page.wait_for_function("""() => {
                const cookies = document.cookie;
                return cookies.includes('webid') || cookies.includes('a1');
            }""", timeout=120000)
            
            # 获取登录后的 cookies
            cookies = await context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            print(f"LOGIN_SUCCESS:{cookie_str}", flush=True)
            
        except Exception as e:
            print(f"LOGIN_FAILED:{e}", flush=True)
        
        await asyncio.sleep(2)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
''', encoding='utf-8')
        
        try:
            self.process = subprocess.Popen(
                [sys.executable, str(login_script)],
                cwd=str(CRAWLER_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8'
            )
            
            self.status = "waiting"
            
            # 启动监控线程
            self._monitor_thread = threading.Thread(target=self._monitor_login, daemon=True)
            self._monitor_thread.start()
            
            return {"success": True, "message": "已启动登录窗口，请查看弹出的浏览器窗口"}
            
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def _monitor_login(self):
        """监控登录状态"""
        if not self.process:
            return
        
        for line in self.process.stdout:
            if "QRCODE_READY" in line:
                self.status = "waiting"
            elif line.startswith("LOGIN_SUCCESS:"):
                self.status = "success"
                self._login_success = True
                # 保存 cookie
                cookie_str = line.split("LOGIN_SUCCESS:")[1].strip()
                self._save_cookies(cookie_str)
                break
            elif "LOGIN_FAILED" in line:
                self.status = "failed"
                break
    
    def _save_cookies(self, cookie_str: str):
        """保存登录凭证"""
        config_file = CRAWLER_DIR / "config" / "base_config.py"
        try:
            content = config_file.read_text(encoding='utf-8')
            
            # 检查是否已有 COOKIES 配置
            import re
            if re.search(r'^COOKIES\s*=', content, re.MULTILINE):
                content = re.sub(
                    r'^COOKIES\s*=.*$',
                    f'COOKIES = """{cookie_str}"""',
                    content,
                    flags=re.MULTILINE
                )
            else:
                # 在 LOGIN_TYPE 后面添加
                content = re.sub(
                    r'(^LOGIN_TYPE\s*=.*$)',
                    r'\1\nCOOKIES = """{}"""'.format(cookie_str),
                    content,
                    flags=re.MULTILINE
                )
            
            config_file.write_text(content, encoding='utf-8')
            print("Cookies saved!")
        except Exception as e:
            print(f"Failed to save cookies: {e}")
    
    def check_login_status(self) -> Dict[str, Any]:
        if not self.process:
            return {"status": "idle", "message": "未启动登录"}
        
        poll = self.process.poll()
        
        if poll is not None and self.status == "waiting":
            self.status = "failed"
        
        return {"status": self.status}
    
    def stop(self):
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=3)
            except:
                self.process.kill()
            self.process = None
        self.status = "idle"


# 全局控制器
crawler_controller = CrawlerController()
login_controller = LoginController()
