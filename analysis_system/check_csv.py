# -*- coding: utf-8 -*-
"""检查CSV数据"""
import os
import glob

data_dir = r"e:\PyProject\RedNote_Learning\MediaCrawler-main\data\xhs"
csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
print(f"CSV文件数量: {len(csv_files)}")
for f in csv_files:
    fname = os.path.basename(f)
    try:
        with open(f, "r", encoding="utf-8", errors="ignore") as fp:
            lines = fp.readlines()
        print(f"  {fname}: {len(lines)} 行")
    except Exception as e:
        print(f"  {fname}: 读取失败 - {e}")
