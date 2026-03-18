# -*- coding: utf-8 -*-
"""
Web Dashboard 服务器 - Flask + Chart.js 的数据分析可视化系统
"""
import os
import sys
import json
import ast
import re
import subprocess
import time as time_module
from pathlib import Path
from typing import List, Dict, Any, Optional

# 确保可以导入本地模块
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.dirname(os.path.abspath(__file__))
CRAWLER_DIR = os.path.join(BASE_DIR, "MediaCrawler-main")
sys.path.insert(0, BASE_DIR)
if os.path.exists(CRAWLER_DIR):
    sys.path.insert(0, CRAWLER_DIR)

from flask import Flask, jsonify, request

# 导入分析模块
sys.path.insert(0, APP_DIR)
from analyzer.data_loader import DataLoader
from analyzer.stats_analyzer import StatsAnalyzer
from analyzer.text_analyzer import TextAnalyzer
from analyzer.trend_analyzer import TrendAnalyzer
from utils.public_data_importer import import_public_dataset, list_public_datasets

app = Flask(__name__, static_folder=None)
app.config['JSON_AS_ASCII'] = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# 全局数据加载器
loader = DataLoader()

# 路径配置
CRAWLER_DIR_P = Path(CRAWLER_DIR)
CRAWLER_AVAILABLE = (CRAWLER_DIR_P / "main.py").exists()
LOCAL_DATA_DIR = Path(APP_DIR) / "data" / "xhs"
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_RUNTIME_CONFIG = Path(APP_DIR) / "data" / "runtime_config.json"
DATA_DIR = (CRAWLER_DIR_P / "data" / "xhs") if CRAWLER_AVAILABLE else LOCAL_DATA_DIR
CONFIG_FILE = CRAWLER_DIR_P / "config" / "base_config.py"


def read_config_text() -> str:
    if not CONFIG_FILE.exists():
        return ""
    return CONFIG_FILE.read_text(encoding="utf-8")


def write_config_text(content: str) -> None:
    if CONFIG_FILE.parent.exists():
        CONFIG_FILE.write_text(content, encoding="utf-8")


def get_default_runtime_config() -> Dict[str, Any]:
    return {
        "PLATFORM": "xhs",
        "KEYWORDS": "公开样本",
        "LOGIN_TYPE": "cookie",
        "CRAWLER_TYPE": "search",
        "SAVE_DATA_OPTION": "csv",
        "CRAWLER_MAX_NOTES_COUNT": 50,
        "ENABLE_GET_COMMENTS": False,
        "ENABLE_CDP_MODE": False,
        "COOKIES": "",
    }


def load_local_runtime_config() -> Dict[str, Any]:
    runtime_config = get_default_runtime_config()
    if LOCAL_RUNTIME_CONFIG.exists():
        try:
            payload = json.loads(LOCAL_RUNTIME_CONFIG.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                runtime_config.update(payload)
        except Exception:
            pass
    return runtime_config


def write_local_runtime_config(payload: Dict[str, Any]) -> None:
    LOCAL_RUNTIME_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_RUNTIME_CONFIG.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def parse_cookie_string(cookie_text: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    if not cookie_text:
        return cookies
    for item in cookie_text.split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        name, value = item.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name:
            cookies[name] = value
    return cookies


def build_cookie_string(cookie_text: str = "", web_session: str = "") -> str:
    cookies = parse_cookie_string(cookie_text)
    web_session = (web_session or "").strip()
    if web_session:
        cookies["web_session"] = web_session
    return "; ".join(f"{key}={value}" for key, value in cookies.items())


def mask_secret(value: str, prefix: int = 6, suffix: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= prefix + suffix:
        return value
    return f"{value[:prefix]}...{value[-suffix:]}"


def extract_config_var(name: str, content: Optional[str] = None, default: Any = "") -> Any:
    if content is None:
        if CONFIG_FILE.exists():
            content = read_config_text()
        else:
            return load_local_runtime_config().get(name, default)
    if not content and not CONFIG_FILE.exists():
        return load_local_runtime_config().get(name, default)
    pattern = rf"^{name}\s*=\s*(.+?)(?=^[A-Z_][A-Z0-9_]*\s*=|^from\s+\.)"
    match = re.search(pattern, content + "\nfrom .__end__ import sentinel", re.MULTILINE | re.DOTALL)
    if not match:
        return default
    raw_value = match.group(1).strip()
    try:
        return ast.literal_eval(raw_value)
    except Exception:
        return raw_value.strip('"').strip("'")


def update_config_var(content: str, name: str, value: Any) -> str:
    serialized = json.dumps(value, ensure_ascii=False) if isinstance(value, str) else repr(value)
    replacement = f"{name} = {serialized}\n"
    pattern = rf"^{name}\s*=\s*(.+?)(?=^[A-Z_][A-Z0-9_]*\s*=|^from\s+\.)"
    if re.search(pattern, content + "\nfrom .__end__ import sentinel", re.MULTILINE | re.DOTALL):
        updated = re.sub(
            pattern,
            replacement,
            content + "\nfrom .__end__ import sentinel",
            count=1,
            flags=re.MULTILINE | re.DOTALL,
        )
        return updated.rsplit("\nfrom .__end__ import sentinel", 1)[0]
    return content.rstrip() + "\n" + replacement


def get_cookie_summary(cookie_text: str) -> Dict[str, Any]:
    cookie_dict = parse_cookie_string(cookie_text)
    return {
        "has_cookies": bool(cookie_dict),
        "cookie_count": len(cookie_dict),
        "web_session_masked": mask_secret(cookie_dict.get("web_session", "")),
    }


def save_runtime_config(
    *,
    keywords: Optional[str] = None,
    login_type: Optional[str] = None,
    crawl_type: Optional[str] = None,
    max_count: Optional[int] = None,
    save_data_option: Optional[str] = None,
    cookies: Optional[str] = None,
) -> Dict[str, Any]:
    if CONFIG_FILE.exists():
        content = read_config_text()
        if keywords is not None:
            content = update_config_var(content, "KEYWORDS", keywords)
        if login_type is not None:
            content = update_config_var(content, "LOGIN_TYPE", login_type)
        if crawl_type is not None:
            content = update_config_var(content, "CRAWLER_TYPE", crawl_type)
        if max_count is not None:
            content = update_config_var(content, "CRAWLER_MAX_NOTES_COUNT", int(max_count))
        if save_data_option is not None:
            content = update_config_var(content, "SAVE_DATA_OPTION", save_data_option)
        if cookies is not None:
            content = update_config_var(content, "COOKIES", cookies)
        write_config_text(content)
        snapshot = {
            "PLATFORM": extract_config_var("PLATFORM", content, "xhs"),
            "KEYWORDS": extract_config_var("KEYWORDS", content, ""),
            "LOGIN_TYPE": extract_config_var("LOGIN_TYPE", content, ""),
            "CRAWLER_TYPE": extract_config_var("CRAWLER_TYPE", content, ""),
            "CRAWLER_MAX_NOTES_COUNT": extract_config_var("CRAWLER_MAX_NOTES_COUNT", content, 0),
            "SAVE_DATA_OPTION": extract_config_var("SAVE_DATA_OPTION", content, "csv"),
            "ENABLE_GET_COMMENTS": extract_config_var("ENABLE_GET_COMMENTS", content, False),
            "ENABLE_CDP_MODE": extract_config_var("ENABLE_CDP_MODE", content, False),
            "COOKIES": str(extract_config_var("COOKIES", content, "")),
        }
    else:
        snapshot = load_local_runtime_config()
        if keywords is not None:
            snapshot["KEYWORDS"] = keywords
        if login_type is not None:
            snapshot["LOGIN_TYPE"] = login_type
        if crawl_type is not None:
            snapshot["CRAWLER_TYPE"] = crawl_type
        if max_count is not None:
            snapshot["CRAWLER_MAX_NOTES_COUNT"] = int(max_count)
        if save_data_option is not None:
            snapshot["SAVE_DATA_OPTION"] = save_data_option
        if cookies is not None:
            snapshot["COOKIES"] = cookies
        write_local_runtime_config(snapshot)

    write_local_runtime_config(snapshot)
    return {
        "PLATFORM": snapshot.get("PLATFORM", "xhs"),
        "KEYWORDS": snapshot.get("KEYWORDS", ""),
        "LOGIN_TYPE": snapshot.get("LOGIN_TYPE", ""),
        "CRAWLER_TYPE": snapshot.get("CRAWLER_TYPE", ""),
        "CRAWLER_MAX_NOTES_COUNT": snapshot.get("CRAWLER_MAX_NOTES_COUNT", 0),
        "SAVE_DATA_OPTION": snapshot.get("SAVE_DATA_OPTION", "csv"),
        "ENABLE_GET_COMMENTS": snapshot.get("ENABLE_GET_COMMENTS", False),
        "ENABLE_CDP_MODE": snapshot.get("ENABLE_CDP_MODE", False),
        **get_cookie_summary(str(snapshot.get("COOKIES", ""))),
    }

# 爬虫控制器
class CrawlerController:
    def __init__(self):
        self.process = None
        self.status = "idle"
        self.start_time = None
        self.log_lines = []
        self._read_thread = None
        self._default_login_type = "qrcode"  # 默认使用二维码登录
        self.last_save_data_option = "csv"
    
    def _read_output(self):
        """后台线程持续读取爬虫输出"""
        import threading
        def read_loop():
            if self.process and self.process.stdout:
                for line in iter(self.process.stdout.readline, ''):
                    if line:
                        self.log_lines.append(line.strip())
                        # 保持最近500行
                        if len(self.log_lines) > 500:
                            self.log_lines = self.log_lines[-500:]
                    if self.process.poll() is not None:
                        break
        self._read_thread = threading.Thread(target=read_loop, daemon=True)
        self._read_thread.start()
    
    def start(
        self,
        keywords,
        login_type="cookie",
        crawl_type="search",
        max_count=50,
        save_data_option="csv",
        cookies="",
        web_session="",
    ):
        if not CRAWLER_AVAILABLE:
            return {"success": False, "message": "当前工作区未检测到 MediaCrawler-main，已切换为公开数据分析模式。"}
        if self.process and self.status == "running":
            return {"success": False, "message": "爬虫已在运行中"}
        
        try:
            cookie_string = build_cookie_string(cookies, web_session)
            current_cookie_string = str(extract_config_var("COOKIES", default=""))
            effective_cookie_string = cookie_string or current_cookie_string

            if login_type == "cookie" and len(effective_cookie_string) < 20:
                return {"success": False, "message": "Cookie 登录已选择，但当前未提供有效 Cookie。请先粘贴 Cookie 或使用扫码登录。"}

            if save_data_option not in {"csv", "json", "sqlite", "db"}:
                save_data_option = "csv"

            config_snapshot = save_runtime_config(
                keywords=keywords,
                login_type=login_type,
                crawl_type=crawl_type,
                max_count=max_count,
                save_data_option=save_data_option,
                cookies=effective_cookie_string if effective_cookie_string else None,
            )
            self.last_save_data_option = str(config_snapshot.get("SAVE_DATA_OPTION") or save_data_option)
            
            print(
                f"[爬虫] 启动参数: keywords={keywords}, login_type={login_type}, "
                f"crawl_type={crawl_type}, save_data_option={self.last_save_data_option}",
                flush=True,
            )
            
            cmd = [
                sys.executable,
                str(CRAWLER_DIR_P / "main.py"),
                "--platform", "xhs",
                "--lt", login_type,
                "--type", crawl_type,
                "--keywords", keywords,
                "--max_count", str(max_count),
                "--save_data_option", self.last_save_data_option,
            ]
            if effective_cookie_string:
                cmd.extend(["--cookies", effective_cookie_string])
            
            self.process = subprocess.Popen(
                cmd,
                cwd=str(CRAWLER_DIR_P),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='gbk',
                errors='replace',
                bufsize=1
            )
            
            self.status = "running"
            self.start_time = time_module.time()
            self.log_lines = []
            
            # 启动后台线程读取输出
            self._read_output()
            
            return {"success": True, "message": "爬虫已启动", "pid": self.process.pid}
            
        except Exception as e:
            self.status = "error"
            return {"success": False, "message": str(e)}
    
    def stop(self):
        if not self.process:
            return {"success": False, "message": "没有运行中的爬虫"}
        
        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except:
                self.process.kill()
            self.status = "stopped"
            return {"success": True, "message": "爬虫已停止"}
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def get_status(self):
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
        except:
            pass
        
        runtime = int(time_module.time() - self.start_time) if self.start_time else 0
        
        return {
            "status": self.status,
            "runtime": runtime,
            "notes_collected": notes_count,
            "comments_collected": comments_count,
            "save_data_option": self.last_save_data_option,
            "recent_logs": self.log_lines[-20:] if self.log_lines else [],
            "crawler_available": CRAWLER_AVAILABLE,
        }
    
    def get_logs(self, lines=50):
        return self.log_lines[-lines:]


class LoginController:
    def __init__(self):
        self.status = "idle"
        self.process = None
        self._read_thread = None
        self.cookies = None
    
    def _read_output(self):
        """后台线程读取登录脚本输出"""
        import threading
        def read_loop():
            if self.process and self.process.stdout:
                for line in iter(self.process.stdout.readline, ''):
                    if line:
                        line = line.strip()
                        print(f"[登录脚本] {line}", flush=True)
                        if line == "QRCODE_READY":
                            self.status = "waiting"
                        elif line.startswith("LOGIN_SUCCESS:"):
                            self.status = "success"
                            self.cookies = line.split(":", 1)[1]
                            # 保存 cookies 到配置文件
                            self._save_cookies(self.cookies)
                        elif line.startswith("LOGIN_FAILED:"):
                            self.status = "failed"
                    if self.process.poll() is not None:
                        break
        self._read_thread = threading.Thread(target=read_loop, daemon=True)
        self._read_thread.start()
    
    def _save_cookies(self, cookies):
        """保存 cookies 到配置文件"""
        try:
            save_runtime_config(cookies=cookies, login_type="cookie")
            print(f"[登录] Cookies 已保存到配置文件", flush=True)
        except Exception as e:
            print(f"[登录] 保存Cookies失败: {e}", flush=True)
    
    def start_qrcode_login(self, force=False):
        if not CRAWLER_AVAILABLE:
            return {"success": False, "message": "当前工作区未接入扫码登录所需的爬虫后端。"}
        # 如果已有进程在运行，先强制停止
        if self.process:
            if force:
                self.stop()
            else:
                return {"success": False, "message": "登录流程已在进行中", "can_force": True}
        
        login_script = CRAWLER_DIR_P / "login_qr.py"
        login_script.write_text('''#!/usr/bin/env python3
import asyncio
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from tools.crawler_util import find_login_qrcode
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={"width": 800, "height": 700})
        page = await context.new_page()
        print("正在打开小红书...", flush=True)
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="networkidle")
        print("QRCODE_READY", flush=True)
        try:
            qrcode = await find_login_qrcode(page, "img.qrcode-img")
            if qrcode:
                print(f"QRCODE:{qrcode}", flush=True)
        except Exception as e:
            print(f"获取二维码失败: {e}", flush=True)
        print("请扫码登录... (等待120秒)", flush=True)
        try:
            await page.wait_for_function("""() => document.cookie.includes('webid')""", timeout=120000)
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
                cwd=str(CRAWLER_DIR_P),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='gbk',
                errors='replace'
            )
            self.status = "starting"
            self.cookies = None
            # 启动后台线程读取输出
            self._read_output()
            return {"success": True, "message": "已启动登录窗口，请扫码登录"}
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def check_login_status(self):
        if not CRAWLER_AVAILABLE:
            return {"status": "disabled", "message": "crawler backend unavailable", "cookies": None}
        if not self.process:
            return {"status": "idle", "message": "未启动登录", "cookies": None}
        
        poll = self.process.poll()
        # 如果进程已结束但状态仍是 waiting/starting，说明登录失败
        if poll is not None and self.status in ["waiting", "starting"]:
            self.status = "failed"
        
        result = {"status": self.status}
        if self.cookies:
            result["cookies"] = self.cookies
        return result
    
    def stop(self):
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=3)
            except:
                self.process.kill()
            self.process = None
        self.status = "idle"
        self.cookies = None


crawler_controller = CrawlerController()
login_controller = LoginController()


# API 接口
@app.route("/api/summary")
def api_summary():
    summary = loader.get_data_summary()
    return jsonify(summary)


@app.route("/api/keywords")
def api_keywords():
    keywords = loader.list_available_keywords()
    return jsonify({"keywords": keywords})


@app.route("/api/public_datasets")
def api_public_datasets():
    return jsonify({"datasets": list_public_datasets(), "crawler_available": CRAWLER_AVAILABLE})


@app.route("/api/public_datasets/import", methods=["POST"])
def api_public_dataset_import():
    data = request.json or {}
    dataset_id = (data.get("dataset_id") or "").strip()
    keyword_label = (data.get("keyword_label") or "").strip()
    replace_existing = bool(data.get("replace_existing", False))

    if not dataset_id:
        return jsonify({"success": False, "message": "缺少 dataset_id"})

    try:
        result = import_public_dataset(
            dataset_id,
            keyword_label=keyword_label,
            replace_existing=replace_existing,
        )
        save_runtime_config(
            keywords=result["keyword_label"],
            save_data_option="csv",
            crawl_type="search",
        )
        return jsonify(
            {
                "success": True,
                "result": result,
                "keywords": loader.list_available_keywords(),
                "summary": loader.get_data_summary(),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/stats")
def api_stats():
    keyword = request.args.get("keyword")
    notes = loader.auto_load_notes(keyword)
    comments = loader.auto_load_comments(keyword)
    if not notes:
        return jsonify({"error": "暂无数据", "note_count": 0})
    analyzer = StatsAnalyzer(notes, comments)
    return jsonify(analyzer.generate_full_report())


@app.route("/api/text")
def api_text():
    keyword = request.args.get("keyword")
    stop_words_file = os.path.join(CRAWLER_DIR, "docs", "hit_stopwords.txt")
    text_analyzer = TextAnalyzer(stop_words_file)
    notes = loader.auto_load_notes(keyword)
    comments = loader.auto_load_comments(keyword)
    
    if not notes and not comments:
        return jsonify({"error": "暂无数据"})
    
    result = {}
    if notes:
        result["note_keywords"] = text_analyzer.extract_keywords_from_notes(notes, top_n=50)
        result["note_sentiment"] = text_analyzer.analyze_notes_sentiment(notes)
        result["hashtags"] = text_analyzer.extract_hashtags(notes)
        result["text_length"] = text_analyzer.get_text_length_stats(notes)
    if comments:
        result["comment_keywords"] = text_analyzer.extract_keywords_from_comments(comments, top_n=50)
        result["comment_sentiment"] = text_analyzer.analyze_comments_sentiment(comments)
    return jsonify(result)


@app.route("/api/trend")
def api_trend():
    keyword = request.args.get("keyword")
    notes = loader.auto_load_notes(keyword)
    comments = loader.auto_load_comments(keyword)
    
    if not notes:
        return jsonify({"error": "暂无数据"})
    
    trend_analyzer = TrendAnalyzer(notes, comments)
    return jsonify(trend_analyzer.generate_trend_report())


@app.route("/api/top_notes")
def api_top_notes():
    keyword = request.args.get("keyword")
    metric = request.args.get("metric", "liked_count")
    top_n = int(request.args.get("top_n", 20))
    notes = loader.auto_load_notes(keyword)
    if not notes:
        return jsonify({"error": "暂无数据", "data": []})
    analyzer = StatsAnalyzer(notes)
    return jsonify({"data": analyzer.get_top_notes(by=metric, top_n=top_n)})


@app.route("/api/notes")
def api_notes():
    keyword = request.args.get("keyword")
    notes = loader.auto_load_notes(keyword)
    if not notes:
        return jsonify({"data": []})
    analyzer = StatsAnalyzer(notes)
    return jsonify({"data": analyzer.get_notes_table_data()})


@app.route("/api/crawl_config")
def api_crawl_config():
    conf = {
        "CRAWLER_AVAILABLE": CRAWLER_AVAILABLE,
        "PUBLIC_DATA_MODE": not CRAWLER_AVAILABLE,
    }
    try:
        content = read_config_text()
        cookies = str(extract_config_var("COOKIES", content, ""))
        conf["PLATFORM"] = extract_config_var("PLATFORM", content, "xhs")
        conf["KEYWORDS"] = extract_config_var("KEYWORDS", content, "公开样本")
        conf["LOGIN_TYPE"] = extract_config_var("LOGIN_TYPE", content, "cookie")
        conf["CRAWLER_TYPE"] = extract_config_var("CRAWLER_TYPE", content, "search")
        conf["SAVE_DATA_OPTION"] = extract_config_var("SAVE_DATA_OPTION", content, "csv")
        conf["CRAWLER_MAX_NOTES_COUNT"] = extract_config_var("CRAWLER_MAX_NOTES_COUNT", content, 0)
        conf["ENABLE_GET_COMMENTS"] = str(extract_config_var("ENABLE_GET_COMMENTS", content, False))
        conf["ENABLE_CDP_MODE"] = str(extract_config_var("ENABLE_CDP_MODE", content, False))
        conf["HAS_COOKIES"] = bool(cookies)
        conf["COOKIE_LENGTH"] = len(cookies)
        conf.update(get_cookie_summary(cookies))
    except Exception as e:
        conf["error"] = str(e)
    return jsonify(conf)


# 爬虫控制 API
@app.route("/api/crawler/start", methods=["POST"])
def crawler_start():
    data = request.json or {}
    keywords = data.get("keywords", "")
    login_type = data.get("login_type", "cookie")
    crawl_type = data.get("crawl_type", "search")
    max_count = int(data.get("max_count", 50))
    save_data_option = data.get("save_data_option", "csv")
    cookies = data.get("cookies", "")
    web_session = data.get("web_session", "")
    
    if not keywords:
        return jsonify({"success": False, "message": "请输入关键词"})
    
    result = crawler_controller.start(
        keywords,
        login_type,
        crawl_type,
        max_count,
        save_data_option,
        cookies,
        web_session,
    )
    return jsonify(result)


@app.route("/api/crawler/stop", methods=["POST"])
def crawler_stop():
    result = crawler_controller.stop()
    return jsonify(result)


@app.route("/api/crawler/status")
def crawler_status():
    return jsonify(crawler_controller.get_status())


@app.route("/api/crawler/logs")
def crawler_logs():
    lines = int(request.args.get("lines", 50))
    return jsonify({"logs": crawler_controller.get_logs(lines)})


@app.route("/api/login/qrcode", methods=["POST"])
def login_qrcode():
    data = request.json or {}
    force = data.get("force", False)
    result = login_controller.start_qrcode_login(force=force)
    return jsonify(result)


@app.route("/api/login/status")
def login_status():
    return jsonify(login_controller.check_login_status())


@app.route("/api/login/stop", methods=["POST"])
def login_stop():
    login_controller.stop()
    return jsonify({"success": True, "message": "已停止登录流程"})


@app.route("/api/config/save", methods=["POST"])
def save_config():
    data = request.json or {}
    
    try:
        cookies = build_cookie_string(data.get("cookies", ""), data.get("web_session", ""))
        saved = save_runtime_config(
            keywords=data.get("keywords"),
            login_type=data.get("login_type"),
            crawl_type=data.get("crawl_type"),
            max_count=int(data["max_count"]) if data.get("max_count") not in (None, "") else None,
            save_data_option=data.get("save_data_option"),
            cookies=cookies if cookies else None,
        )
        return jsonify({"success": True, "config": saved})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/")
def index():
    with open(os.path.join(os.path.dirname(__file__), "dashboard", "index.html"), "r", encoding="utf-8") as f:
        html = f.read()
    return html


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"\n{'='*55}")
    print("  [小红书数据采集与分析系统 已启动]")
    print(f"  访问地址: http://127.0.0.1:{port}")
    print(f"  数据目录: {loader.data_root}")
    print(f"{'='*55}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
