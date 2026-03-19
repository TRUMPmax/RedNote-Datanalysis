# -*- coding: utf-8 -*-
"""检查 SQLite 数据结构。"""
from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "MediaCrawler-main" / "schema" / "sqlite_tables.db"


if not DB_PATH.exists():
    print(f"未找到 SQLite 数据库: {DB_PATH.relative_to(PROJECT_ROOT)}")
    print("当前项目已经切换为 CrawlerData 驱动的分析链路，这个脚本只在需要查看 MediaCrawler 的 SQLite 结构时使用。")
    raise SystemExit(0)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM xhs_note")
note_count = cur.fetchone()[0]
print(f"xhs_note 数量: {note_count}")

cur.execute("SELECT COUNT(*) FROM xhs_note_comment")
comment_count = cur.fetchone()[0]
print(f"xhs_note_comment 数量: {comment_count}")

cur.execute("PRAGMA table_info(xhs_note)")
print("\nxhs_note 表结构:")
for col in cur.fetchall():
    print(f"  {col[1]} {col[2]}")

conn.close()
