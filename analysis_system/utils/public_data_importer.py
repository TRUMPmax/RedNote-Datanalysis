from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ANALYSIS_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_ROOT = ANALYSIS_ROOT / "data" / "xhs"
PUBLIC_DATASET_ROOT = PROJECT_ROOT / "public_datasets"

NOTE_FIELDNAMES = [
    "note_id",
    "title",
    "desc",
    "type",
    "note_url",
    "cover_url",
    "time",
    "ip_location",
    "nickname",
    "user_id",
    "author_homepage",
    "source_keyword",
    "tag_list",
    "liked_count",
    "collected_count",
    "comment_count",
    "share_count",
    "interaction_count",
    "follower_count",
    "current_follower_count",
    "note_category",
    "author_type",
    "account_tier",
    "estimated_read_count",
    "estimated_quote",
    "source_dataset",
    "source_url",
]

PUBLIC_DATASETS: Dict[str, Dict[str, Any]] = {
    "xiaohongshu_analysis_all": {
        "id": "xiaohongshu_analysis_all",
        "name": "XiaohongshuAnalysis / all.xlsx",
        "description": "GitHub 公开样本，包含 1048 条帖子级指标，适合做榜单、趋势和地域分析。",
        "source_url": "https://github.com/laiaccc/XiaohongshuAnalysis",
        "dataset_url": "https://github.com/laiaccc/XiaohongshuAnalysis/blob/main/all.xlsx",
        "license": "No license specified",
        "recommended": True,
        "import_supported": True,
        "default_keyword": "公开样本",
        "local_path": str(PUBLIC_DATASET_ROOT / "XiaohongshuAnalysis" / "all.xlsx"),
    },
    "xiaohongshu_train_text_hf": {
        "id": "xiaohongshu_train_text_hf",
        "name": "roseking/xiaohongshu-train_data",
        "description": "Hugging Face 文本数据，适合做对话或文本实验，但不适合当前这套互动趋势面板。",
        "source_url": "https://huggingface.co/datasets/roseking/xiaohongshu-train_data",
        "dataset_url": "https://huggingface.co/datasets/roseking/xiaohongshu-train_data",
        "license": "Apache-2.0",
        "recommended": False,
        "import_supported": False,
        "default_keyword": "文本训练样本",
        "local_path": "",
    },
}


def _safe_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _safe_int(value: Any) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    text = str(value).strip().replace(",", "")
    if not text or text.lower() == "nan":
        return 0
    multiplier = 1
    if text.endswith("万"):
        multiplier = 10000
        text = text[:-1]
    elif text.endswith("w") or text.endswith("W"):
        multiplier = 10000
        text = text[:-1]
    elif text.endswith("千"):
        multiplier = 1000
        text = text[:-1]
    elif text.endswith("k") or text.endswith("K"):
        multiplier = 1000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except Exception:
        return 0


def _parse_timestamp(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return str(int(dt.timestamp()))


def _slugify(value: str) -> str:
    value = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", value.strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dataset"


def _extract_note_id(note_url: str, fallback_seed: str) -> str:
    match = re.search(r"/item/([^/?#]+)", note_url or "")
    if match:
        return match.group(1)
    return hashlib.md5(fallback_seed.encode("utf-8")).hexdigest()[:24]


def _extract_user_id(profile_url: str, account_id: str) -> str:
    match = re.search(r"/profile/([^/?#]+)", profile_url or "")
    if match:
        return match.group(1)
    return account_id.strip()


def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for item in items:
        item = item.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _parse_list_like(value: Any) -> List[str]:
    text = _safe_text(value)
    if not text:
        return []

    hashtags = [item.strip() for item in re.findall(r"#([^,\]#]+)", text)]
    if hashtags:
        return _dedupe_keep_order(hashtags)

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return _dedupe_keep_order(_safe_text(item).lstrip("#") for item in parsed)

    parts = re.split(r"[,，/]", text.strip("[]"))
    return _dedupe_keep_order(_safe_text(item).lstrip("#") for item in parts)


def _merge_tags(*values: Any) -> str:
    tags: List[str] = []
    for value in values:
        tags.extend(_parse_list_like(value))
    return ",".join(_dedupe_keep_order(tags))


def _map_note_type(raw_value: Any) -> str:
    text = _safe_text(raw_value).lower()
    if "视频" in text or "video" in text:
        return "video"
    return "normal"


def list_public_datasets() -> List[Dict[str, Any]]:
    datasets: List[Dict[str, Any]] = []
    for meta in PUBLIC_DATASETS.values():
        row = dict(meta)
        local_path = Path(meta["local_path"]) if meta.get("local_path") else None
        row["local_available"] = bool(local_path and local_path.exists())
        datasets.append(row)
    return datasets


def _normalize_xiaohongshu_analysis_row(row: pd.Series, index: int, keyword_label: str) -> Dict[str, Any]:
    title = _safe_text(row.get("笔记标题"))
    desc = _safe_text(row.get("笔记内容"))
    note_url = _safe_text(row.get("笔记链接"))
    profile_url = _safe_text(row.get("账号主页链接"))
    account_id = _safe_text(row.get("账号小红书号"))
    fallback_seed = f"{keyword_label}-{index}-{title}-{note_url}"
    note_id = _extract_note_id(note_url, fallback_seed)
    user_id = _extract_user_id(profile_url, account_id)
    tags = _merge_tags(
        row.get("话题"),
        row.get("笔记内容标签"),
        row.get("命中关键词"),
        row.get("种草品牌"),
        row.get("商业合作品牌"),
    )

    return {
        "note_id": note_id,
        "title": title,
        "desc": desc,
        "type": _map_note_type(row.get("笔记类别")),
        "note_url": note_url,
        "cover_url": _safe_text(row.get("笔记封面")),
        "time": _parse_timestamp(row.get("发布时间")),
        "ip_location": _safe_text(row.get("发文属地")),
        "nickname": _safe_text(row.get("账号昵称")),
        "user_id": user_id,
        "author_homepage": profile_url,
        "source_keyword": keyword_label,
        "tag_list": tags,
        "liked_count": _safe_int(row.get("点赞")),
        "collected_count": _safe_int(row.get("收藏")),
        "comment_count": _safe_int(row.get("评论")),
        "share_count": _safe_int(row.get("分享")),
        "interaction_count": _safe_int(row.get("互动量")),
        "follower_count": _safe_int(row.get("发文时粉丝数")),
        "current_follower_count": _safe_int(row.get("当前粉丝数")),
        "note_category": _safe_text(row.get("笔记分类")),
        "author_type": _safe_text(row.get("作者类别")),
        "account_tier": _safe_text(row.get("账号属性")),
        "estimated_read_count": _safe_int(row.get("预估阅读")),
        "estimated_quote": _safe_int(row.get("预估投放报价")),
        "source_dataset": "xiaohongshu_analysis_all",
        "source_url": note_url,
    }


def import_public_dataset(
    dataset_id: str,
    *,
    keyword_label: str = "",
    replace_existing: bool = False,
) -> Dict[str, Any]:
    if dataset_id not in PUBLIC_DATASETS:
        raise ValueError(f"Unknown dataset id: {dataset_id}")

    meta = PUBLIC_DATASETS[dataset_id]
    if not meta.get("import_supported"):
        raise ValueError("This public dataset is not suitable for the current visualization pipeline.")

    source_path = Path(meta["local_path"])
    if not source_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {source_path}")

    keyword_label = (keyword_label or meta.get("default_keyword") or meta["name"]).strip()
    LOCAL_DATA_ROOT.mkdir(parents=True, exist_ok=True)

    if replace_existing:
        for old_file in LOCAL_DATA_ROOT.glob(f"{dataset_id}_*_contents.csv"):
            old_file.unlink()

    output_path = LOCAL_DATA_ROOT / f"{dataset_id}_{_slugify(keyword_label)}_contents.csv"

    df = pd.read_excel(source_path)
    rows: List[Dict[str, Any]] = []
    for index, row in df.iterrows():
        normalized = _normalize_xiaohongshu_analysis_row(row, index, keyword_label)
        if not normalized["title"] and not normalized["desc"]:
            continue
        rows.append(normalized)

    with output_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=NOTE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    return {
        "dataset_id": dataset_id,
        "dataset_name": meta["name"],
        "keyword_label": keyword_label,
        "notes_count": len(rows),
        "comments_count": 0,
        "output_path": str(output_path),
        "source_path": str(source_path),
        "source_url": meta["source_url"],
        "license": meta["license"],
    }


def save_public_runtime_snapshot(runtime_file: Path, payload: Dict[str, Any]) -> None:
    runtime_file.parent.mkdir(parents=True, exist_ok=True)
    runtime_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
