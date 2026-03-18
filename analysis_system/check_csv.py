# -*- coding: utf-8 -*-
"""检查 CSV 数据。"""
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR_CANDIDATES = [
    PROJECT_ROOT / "analysis_system" / "data" / "xhs",
    PROJECT_ROOT / "MediaCrawler-main" / "data" / "xhs",
]

data_dir = next((path for path in DATA_DIR_CANDIDATES if path.exists()), None)
if data_dir is None:
    print("未找到可用的 CSV 数据目录。")
    raise SystemExit(0)

csv_files = sorted(data_dir.glob("*.csv"))
print(f"数据目录: {data_dir.relative_to(PROJECT_ROOT)}")
print(f"CSV 文件数量: {len(csv_files)}")

for file_path in csv_files:
    try:
        with file_path.open("r", encoding="utf-8-sig", errors="ignore") as file_obj:
            lines = file_obj.readlines()
        print(f"  {file_path.name}: {len(lines)} 行")
    except Exception as exc:
        print(f"  {file_path.name}: 读取失败 - {exc}")
