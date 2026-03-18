# -*- coding: utf-8 -*-
"""检查数据库中的笔记数量"""
import sqlite3
import os

db_path = r"e:\PyProject\RedNote_Learning\MediaCrawler-main\schema\sqlite_tables.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 检查笔记数量
try:
    cursor.execute("SELECT COUNT(*) FROM xhs_note")
    count = cursor.fetchone()[0]
    print(f"笔记总数: {count}")
except Exception as e:
    print(f"查询笔记失败: {e}")

# 检查评论数量
try:
    cursor.execute("SELECT COUNT(*) FROM xhs_note_comment")
    count = cursor.fetchone()[0]
    print(f"评论总数: {count}")
except Exception as e:
    print(f"查询评论失败: {e}")

# 获取最新笔记
try:
    cursor.execute("SELECT title, liked_count, collected_count FROM xhs_note ORDER BY id DESC LIMIT 5")
    notes = cursor.fetchall()
    print("\n最新笔记:")
    for n in notes:
        print(f"  - {n[0][:30] if n[0] else '无标题'} | 点赞:{n[1]} 收藏:{n[2]}")
except Exception as e:
    print(f"查询最新笔记失败: {e}")

conn.close()
