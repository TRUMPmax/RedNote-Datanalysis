from __future__ import annotations

from collections import Counter, defaultdict
from statistics import median
from typing import Any, Dict, List, Tuple

from .common import clean_text, safe_int


CHINA_REGION_ALIASES = {
    "北京": ["北京", "北京市"],
    "天津": ["天津", "天津市"],
    "上海": ["上海", "上海市"],
    "重庆": ["重庆", "重庆市"],
    "河北": ["河北", "河北省"],
    "山西": ["山西", "山西省"],
    "内蒙古": ["内蒙古", "内蒙古自治区"],
    "辽宁": ["辽宁", "辽宁省"],
    "吉林": ["吉林", "吉林省"],
    "黑龙江": ["黑龙江", "黑龙江省"],
    "江苏": ["江苏", "江苏省"],
    "浙江": ["浙江", "浙江省"],
    "安徽": ["安徽", "安徽省"],
    "福建": ["福建", "福建省"],
    "江西": ["江西", "江西省"],
    "山东": ["山东", "山东省"],
    "河南": ["河南", "河南省"],
    "湖北": ["湖北", "湖北省"],
    "湖南": ["湖南", "湖南省"],
    "广东": ["广东", "广东省"],
    "广西": ["广西", "广西壮族自治区"],
    "海南": ["海南", "海南省"],
    "四川": ["四川", "四川省"],
    "贵州": ["贵州", "贵州省"],
    "云南": ["云南", "云南省"],
    "西藏": ["西藏", "西藏自治区"],
    "陕西": ["陕西", "陕西省"],
    "甘肃": ["甘肃", "甘肃省"],
    "青海": ["青海", "青海省"],
    "宁夏": ["宁夏", "宁夏回族自治区"],
    "新疆": ["新疆", "新疆维吾尔自治区"],
    "台湾": ["台湾", "台湾省"],
    "香港": ["香港", "中国香港", "香港特别行政区"],
    "澳门": ["澳门", "中国澳门", "澳门特别行政区"],
}


def _author_key(note: Dict[str, Any]) -> str:
    for field in ("user_id", "nickname", "note_id"):
        value = clean_text(note.get(field))
        if value:
            return value
    return ""


def _metric_stats(values: List[int]) -> Dict[str, Any]:
    if not values:
        return {"total": 0, "avg": 0, "max": 0, "min": 0, "median": 0}
    return {
        "total": sum(values),
        "avg": round(sum(values) / len(values), 2),
        "max": max(values),
        "min": min(values),
        "median": median(values),
    }


def _normalize_region(location: Any) -> Tuple[str, str]:
    text = clean_text(location)
    if not text:
        return "未知", "unknown"

    for canonical, aliases in CHINA_REGION_ALIASES.items():
        if any(alias in text for alias in aliases):
            return canonical, "china"

    if text in {"中国", "中国大陆", "内地"}:
        return "其他国内", "china"
    return text, "overseas"


class StatsAnalyzer:
    def __init__(self, notes: List[Dict[str, Any]], comments: List[Dict[str, Any]] | None = None):
        self.notes = notes or []
        self.comments = comments or []

    def note_count(self) -> int:
        return len(self.notes)

    def comment_count(self) -> int:
        if self.comments:
            return len(self.comments)
        return sum(safe_int(note.get("comment_count")) for note in self.notes)

    def comment_record_count(self) -> int:
        return len(self.comments)

    def has_comment_details(self) -> bool:
        return bool(self.comments)

    def author_count(self) -> int:
        return len({_author_key(note) for note in self.notes if _author_key(note)})

    def get_interaction_stats(self) -> Dict[str, Any]:
        return {
            "liked": _metric_stats([safe_int(note.get("liked_count")) for note in self.notes]),
            "collected": _metric_stats([safe_int(note.get("collected_count")) for note in self.notes]),
            "comment": _metric_stats([safe_int(note.get("comment_count")) for note in self.notes]),
            "share": _metric_stats([safe_int(note.get("share_count")) for note in self.notes]),
            "total_interactions": sum(safe_int(note.get("interaction_count")) for note in self.notes),
        }

    def get_top_notes(self, by: str = "interaction_count", top_n: int = 10) -> List[Dict[str, Any]]:
        allowed = {
            "liked_count",
            "collected_count",
            "comment_count",
            "share_count",
            "interaction_count",
            "time",
        }
        metric = by if by in allowed else "interaction_count"
        sorted_notes = sorted(self.notes, key=lambda note: safe_int(note.get(metric)), reverse=True)
        results: List[Dict[str, Any]] = []
        for note in sorted_notes[:top_n]:
            results.append(
                {
                    "note_id": note.get("note_id", ""),
                    "title": note.get("title") or note.get("desc", ""),
                    "nickname": note.get("nickname", ""),
                    "liked_count": safe_int(note.get("liked_count")),
                    "collected_count": safe_int(note.get("collected_count")),
                    "comment_count": safe_int(note.get("comment_count")),
                    "share_count": safe_int(note.get("share_count")),
                    "interaction_count": safe_int(note.get("interaction_count")),
                    "ip_location": note.get("ip_location", ""),
                    "publish_date": note.get("publish_date", ""),
                    "note_url": note.get("note_url", ""),
                    "duplicate_count": safe_int(note.get("duplicate_count")),
                }
            )
        return results

    def get_notes_table_data(self) -> List[Dict[str, Any]]:
        return [
            {
                "note_id": note.get("note_id", ""),
                "title": note.get("title") or note.get("desc", ""),
                "nickname": note.get("nickname", ""),
                "liked_count": safe_int(note.get("liked_count")),
                "collected_count": safe_int(note.get("collected_count")),
                "comment_count": safe_int(note.get("comment_count")),
                "share_count": safe_int(note.get("share_count")),
                "interaction_count": safe_int(note.get("interaction_count")),
                "ip_location": note.get("ip_location", ""),
                "publish_date": note.get("publish_date", ""),
                "tag_count": safe_int(note.get("tag_count")),
                "duplicate_count": safe_int(note.get("duplicate_count")),
                "note_url": note.get("note_url", ""),
            }
            for note in self.notes
        ]

    def get_location_distribution(self, top_n: int = 20) -> List[Tuple[str, int]]:
        counter = Counter(clean_text(note.get("ip_location")) for note in self.notes if clean_text(note.get("ip_location")))
        return counter.most_common(top_n)

    def get_time_distribution(self) -> Dict[str, int]:
        hourly: Dict[str, int] = defaultdict(int)
        for note in self.notes:
            publish_datetime = clean_text(note.get("publish_datetime"))
            if len(publish_datetime) >= 13:
                hour = publish_datetime[11:13]
                hourly[hour] += 1
        return dict(sorted(hourly.items()))

    def get_daily_distribution(self) -> Dict[str, int]:
        daily: Dict[str, int] = defaultdict(int)
        for note in self.notes:
            publish_date = clean_text(note.get("publish_date"))
            if publish_date:
                daily[publish_date] += 1
        return dict(sorted(daily.items()))

    def get_author_leaderboard(self, top_n: int = 15) -> List[Dict[str, Any]]:
        author_map: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "total_interactions": 0, "total_likes": 0, "nickname": ""}
        )
        for note in self.notes:
            author_id = _author_key(note)
            if not author_id:
                continue
            author_map[author_id]["count"] += 1
            author_map[author_id]["total_interactions"] += safe_int(note.get("interaction_count"))
            author_map[author_id]["total_likes"] += safe_int(note.get("liked_count"))
            author_map[author_id]["nickname"] = note.get("nickname") or author_id

        ranked = sorted(
            author_map.items(),
            key=lambda item: (item[1]["count"], item[1]["total_interactions"], item[1]["total_likes"]),
            reverse=True,
        )
        return [
            {
                "user_id": author_id,
                "nickname": info["nickname"],
                "note_count": info["count"],
                "total_interactions": info["total_interactions"],
                "total_likes": info["total_likes"],
                "avg_likes": round(info["total_likes"] / info["count"], 2) if info["count"] else 0,
            }
            for author_id, info in ranked[:top_n]
        ]

    def get_tag_distribution(self, top_n: int = 30) -> List[Tuple[str, int]]:
        counter = Counter()
        for note in self.notes:
            for tag in note.get("tag_list", []) or []:
                tag_text = clean_text(tag)
                if tag_text:
                    counter[tag_text] += 1
        return counter.most_common(top_n)

    def get_keyword_stats(self) -> Dict[str, Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for note in self.notes:
            groups[clean_text(note.get("source_keyword")) or "CrawlerData"].append(note)

        result: Dict[str, Dict[str, Any]] = {}
        for keyword, notes in groups.items():
            likes = [safe_int(note.get("liked_count")) for note in notes]
            interactions = [safe_int(note.get("interaction_count")) for note in notes]
            result[keyword] = {
                "note_count": len(notes),
                "total_likes": sum(likes),
                "avg_likes": round(sum(likes) / len(likes), 2) if likes else 0,
                "total_interactions": sum(interactions),
            }
        return result

    def get_type_distribution(self) -> Dict[str, int]:
        counter = Counter(clean_text(note.get("type")) or "normal" for note in self.notes)
        return dict(counter)

    def get_duplicate_summary(self) -> Dict[str, Any]:
        duplicate_notes = [safe_int(note.get("duplicate_count")) for note in self.notes if safe_int(note.get("duplicate_count")) > 0]
        duplicate_comments = [safe_int(comment.get("duplicate_count")) for comment in self.comments if safe_int(comment.get("duplicate_count")) > 0]
        return {
            "notes_with_duplicates": len(duplicate_notes),
            "duplicate_note_rows_merged": sum(duplicate_notes),
            "comments_with_duplicates": len(duplicate_comments),
            "duplicate_comment_rows_merged": sum(duplicate_comments),
        }

    def _region_snapshot(self, name: str, notes: List[Dict[str, Any]]) -> Dict[str, Any]:
        authors = {_author_key(note) for note in notes if _author_key(note)}
        total_interactions = sum(safe_int(note.get("interaction_count")) for note in notes)
        return {
            "name": name,
            "note_count": len(notes),
            "author_count": len(authors),
            "total_interactions": total_interactions,
            "avg_interactions": round(total_interactions / len(notes), 2) if notes else 0,
            "avg_likes": round(
                sum(safe_int(note.get("liked_count")) for note in notes) / len(notes),
                2,
            )
            if notes
            else 0,
        }

    def get_geo_report(self) -> Dict[str, Any]:
        china_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        overseas_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        unknown_notes: List[Dict[str, Any]] = []

        for note in self.notes:
            region_name, region_type = _normalize_region(note.get("ip_location"))
            if region_type == "china":
                china_buckets[region_name].append(note)
            elif region_type == "overseas":
                overseas_buckets[region_name].append(note)
            else:
                unknown_notes.append(note)

        china_stats = sorted(
            [self._region_snapshot(name, items) for name, items in china_buckets.items()],
            key=lambda item: (item["author_count"], item["note_count"], item["total_interactions"]),
            reverse=True,
        )
        overseas_stats = sorted(
            [self._region_snapshot(name, items) for name, items in overseas_buckets.items()],
            key=lambda item: (item["author_count"], item["note_count"], item["total_interactions"]),
            reverse=True,
        )
        return {
            "china_region_stats": china_stats,
            "overseas_distribution": overseas_stats,
            "unknown_region": self._region_snapshot("未知", unknown_notes) if unknown_notes else None,
        }

    def get_comment_stats(self) -> Dict[str, Any]:
        likes = [safe_int(comment.get("like_count")) for comment in self.comments]
        return {
            "total": len(self.comments),
            "avg_likes": round(sum(likes) / len(likes), 2) if likes else 0,
            "max_likes": max(likes) if likes else 0,
            "comments_with_replies": sum(1 for comment in self.comments if safe_int(comment.get("sub_comment_count")) > 0),
        }

    def get_comment_location_distribution(self, top_n: int = 15) -> List[Tuple[str, int]]:
        counter = Counter(
            clean_text(comment.get("ip_location"))
            for comment in self.comments
            if clean_text(comment.get("ip_location"))
        )
        return counter.most_common(top_n)

    def get_active_commenters(self, top_n: int = 10) -> List[Dict[str, Any]]:
        user_map: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "nickname": ""})
        for comment in self.comments:
            user_id = clean_text(comment.get("user_id"))
            if not user_id:
                continue
            user_map[user_id]["count"] += 1
            user_map[user_id]["nickname"] = comment.get("nickname") or user_id

        ranked = sorted(user_map.items(), key=lambda item: item[1]["count"], reverse=True)
        return [
            {
                "user_id": user_id,
                "nickname": info["nickname"],
                "comment_count": info["count"],
            }
            for user_id, info in ranked[:top_n]
        ]

    def generate_full_report(self) -> Dict[str, Any]:
        return {
            "overview": {
                "note_count": self.note_count(),
                "comment_count": self.comment_count(),
                "comment_record_count": self.comment_record_count(),
                "author_count": self.author_count(),
                "has_comment_details": self.has_comment_details(),
            },
            "interaction_stats": self.get_interaction_stats(),
            "top_notes": self.get_top_notes(top_n=10),
            "location_distribution": self.get_location_distribution(),
            "time_distribution": self.get_time_distribution(),
            "daily_distribution": self.get_daily_distribution(),
            "author_leaderboard": self.get_author_leaderboard(),
            "tag_distribution": self.get_tag_distribution(),
            "keyword_stats": self.get_keyword_stats(),
            "type_distribution": self.get_type_distribution(),
            "duplicate_summary": self.get_duplicate_summary(),
            "geo_report": self.get_geo_report(),
            "comment_stats": self.get_comment_stats(),
            "comment_location": self.get_comment_location_distribution(),
            "active_commenters": self.get_active_commenters(),
        }
