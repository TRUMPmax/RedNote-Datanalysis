# -*- coding: utf-8 -*-
"""检查数据问题"""
import sqlite3

db_path = r"e:\PyProject\RedNote_Learning\MediaCrawler-main\schema\sqlite_tables.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM xhs_note")
note_count = cur.fetchone()[0]
print(f"xhs_note 数量: {note_count}")

cur.execute("SELECT COUNT(*) FROM xhs_note_comment")  
comment_count = cur.fetchone()[0]
print(f"xhs_note_comment 数量: {comment_count}")

# 查看表结构
cur.execute("PRAGMA table_info(xhs_note)")
print("\nxhs_note 表结构:")
for col in cur.fetchall():
    print(f"  {col[1]} {col[2]}")

conn.close()
