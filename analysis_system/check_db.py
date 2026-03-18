# -*- coding: utf-8 -*-
"""检查 SQLite 数据库中的笔记数量。"""
from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "MediaCrawler-main" / "schema" / "sqlite_tables.db"


if not DB_PATH.exists():
    print(f"未找到 SQLite 数据库: {DB_PATH.relative_to(PROJECT_ROOT)}")
    print("当前仓库默认可直接使用 analysis_system/data/xhs 下的 CSV 样本数据。")
    raise SystemExit(0)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    cursor.execute("SELECT COUNT(*) FROM xhs_note")
    count = cursor.fetchone()[0]
    print(f"笔记总数: {count}")
except Exception as exc:
    print(f"查询笔记失败: {exc}")

try:
    cursor.execute("SELECT COUNT(*) FROM xhs_note_comment")
    count = cursor.fetchone()[0]
    print(f"评论总数: {count}")
except Exception as exc:
    print(f"查询评论失败: {exc}")

try:
    cursor.execute("SELECT title, liked_count, collected_count FROM xhs_note ORDER BY id DESC LIMIT 5")
    notes = cursor.fetchall()
    print("\n最新笔记:")
    for note in notes:
        title = note[0][:30] if note[0] else "无标题"
        print(f"  - {title} | 点赞:{note[1]} 收藏:{note[2]}")
except Exception as exc:
    print(f"查询最新笔记失败: {exc}")

conn.close()
