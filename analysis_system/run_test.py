# -*- coding: utf-8 -*-
import subprocess
import time
import requests
import sys
import os

# 启动服务器
print("Starting server...")
p = subprocess.Popen(
    [sys.executable, "app.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    cwd=os.path.dirname(os.path.abspath(__file__)) or "."
)

# 等待启动
time.sleep(4)

try:
    # 测试各个 API
    tests = [
        ("/api/crawl_config", "GET", None),
        ("/api/crawler/status", "GET", None),
    ]
    
    for path, method, body in tests:
        try:
            url = f"http://127.0.0.1:8080{path}"
            if method == "GET":
                r = requests.get(url, timeout=5)
            else:
                r = requests.post(url, json=body, timeout=5)
            print(f"{path}: {r.status_code} - {r.text[:100]}")
        except Exception as e:
            print(f"{path}: ERROR - {e}")
    
    # 测试启动爬虫
    print("\nTrying to start crawler...")
    try:
        r = requests.post("http://127.0.0.1:8080/api/crawler/start", 
                        json={"keywords": "测试", "login_type": "cookie", "max_count": 10}, 
                        timeout=5)
        print(f"/api/crawler/start: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        print(f"/api/crawler/start: ERROR - {e}")
    
finally:
    p.terminate()
    p.wait()
    print("\nDone!")
