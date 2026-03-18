# -*- coding: utf-8 -*-
"""
统计分析器：笔记互动、作者、地区分布等。
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple


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
    "香港": ["香港", "香港特别行政区", "中国香港"],
    "澳门": ["澳门", "澳门特别行政区", "中国澳门"],
}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        text = str(value).strip().replace(",", "")
        if not text or text.lower() == "nan":
            return 0
        if text.endswith("万"):
            return int(float(text[:-1]) * 10000)
        if text.endswith("千"):
            return int(float(text[:-1]) * 1000)
        if text.endswith("w") or text.endswith("W"):
            return int(float(text[:-1]) * 10000)
        if text.endswith("k") or text.endswith("K"):
            return int(float(text[:-1]) * 1000)
        return int(float(text))
    except Exception:
        return 0


def _author_key(note: Dict[str, Any]) -> str:
    for field in ("user_id", "author_homepage", "nickname"):
        value = _safe_text(note.get(field))
        if value:
            return value
    return _safe_text(note.get("note_id"))


def _normalize_region(location: Any) -> Tuple[str, str]:
    text = _safe_text(location)
    if not text:
        return "未知", "unknown"

    for canonical, aliases in CHINA_REGION_ALIASES.items():
        if any(alias in text for alias in aliases):
            return canonical, "china"

    if text in {"中国", "中国大陆", "内地"}:
        return "其他国内", "china"

    return "境外", "overseas"


class StatsAnalyzer:
    """小红书笔记与评论的统计分析。"""

    def __init__(self, notes: List[Dict], comments: List[Dict] = None):
        self.notes = notes or []
        self.comments = comments or []

    # 基础统计
    def note_count(self) -> int:
        return len(self.notes)

    def comment_count(self) -> int:
        if self.comments:
            return len(self.comments)
        return sum(_safe_int(note.get("comment_count")) for note in self.notes)

    def comment_record_count(self) -> int:
        return len(self.comments)

    def has_comment_details(self) -> bool:
        return bool(self.comments)

    def author_count(self) -> int:
        return len({_author_key(note) for note in self.notes if _author_key(note)})

    def get_interaction_stats(self) -> Dict[str, Any]:
        if not self.notes:
            return {}

        likes = [_safe_int(note.get("liked_count")) for note in self.notes]
        collects = [_safe_int(note.get("collected_count")) for note in self.notes]
        comments = [_safe_int(note.get("comment_count")) for note in self.notes]
        shares = [_safe_int(note.get("share_count")) for note in self.notes]

        def stats(values: List[int]) -> Dict[str, Any]:
            if not values:
                return {}
            total = sum(values)
            avg = total / len(values)
            sorted_values = sorted(values)
            count = len(sorted_values)
            median = (
                sorted_values[count // 2]
                if count % 2
                else (sorted_values[count // 2 - 1] + sorted_values[count // 2]) / 2
            )
            return {
                "total": total,
                "avg": round(avg, 1),
                "max": max(values),
                "min": min(values),
                "median": median,
            }

        return {
            "liked": stats(likes),
            "collected": stats(collects),
            "comment": stats(comments),
            "share": stats(shares),
            "total_interactions": sum(likes) + sum(collects) + sum(comments) + sum(shares),
        }

    def get_top_notes(self, by: str = "liked_count", top_n: int = 10) -> List[Dict]:
        sorted_notes = sorted(self.notes, key=lambda note: _safe_int(note.get(by, 0)), reverse=True)
        result: List[Dict[str, Any]] = []
        for note in sorted_notes[:top_n]:
            result.append(
                {
                    "note_id": note.get("note_id", ""),
                    "title": (note.get("title") or note.get("desc", ""))[:60],
                    "liked_count": _safe_int(note.get("liked_count")),
                    "collected_count": _safe_int(note.get("collected_count")),
                    "comment_count": _safe_int(note.get("comment_count")),
                    "share_count": _safe_int(note.get("share_count")),
                    "nickname": note.get("nickname", ""),
                    "note_url": note.get("note_url", ""),
                    "ip_location": note.get("ip_location", ""),
                    "source_keyword": note.get("source_keyword", ""),
                    "type": note.get("type", ""),
                }
            )
        return result

    def get_notes_table_data(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for note in self.notes:
            rows.append(
                {
                    "note_id": note.get("note_id", ""),
                    "title": note.get("title") or note.get("desc", ""),
                    "liked_count": _safe_int(note.get("liked_count")),
                    "collected_count": _safe_int(note.get("collected_count")),
                    "comment_count": _safe_int(note.get("comment_count")),
                    "share_count": _safe_int(note.get("share_count")),
                    "nickname": note.get("nickname", ""),
                    "note_url": note.get("note_url", ""),
                    "ip_location": note.get("ip_location", ""),
                    "source_keyword": note.get("source_keyword", ""),
                    "type": note.get("type", ""),
                    "time": note.get("time", ""),
                }
            )
        return rows

    def get_location_distribution(self, top_n: int = 20) -> List[Tuple[str, int]]:
        locations = [_safe_text(note.get("ip_location")) for note in self.notes if _safe_text(note.get("ip_location"))]
        return Counter(locations).most_common(top_n)

    def get_time_distribution(self) -> Dict[str, int]:
        hourly = defaultdict(int)
        for note in self.notes:
            timestamp = note.get("time")
            if not timestamp:
                continue
            try:
                timestamp = int(timestamp)
                if timestamp > 1e12:
                    timestamp //= 1000
                hourly[datetime.fromtimestamp(timestamp).hour] += 1
            except Exception:
                pass
        return dict(sorted(hourly.items()))

    def get_daily_distribution(self) -> Dict[str, int]:
        daily = defaultdict(int)
        for note in self.notes:
            timestamp = note.get("time")
            if not timestamp:
                continue
            try:
                timestamp = int(timestamp)
                if timestamp > 1e12:
                    timestamp //= 1000
                daily[datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")] += 1
            except Exception:
                pass
        return dict(sorted(daily.items()))

    def get_author_leaderboard(self, top_n: int = 15) -> List[Dict]:
        author_map: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_likes": 0, "nickname": ""})
        for note in self.notes:
            uid = _author_key(note)
            if not uid:
                continue
            author_map[uid]["count"] += 1
            author_map[uid]["total_likes"] += _safe_int(note.get("liked_count"))
            author_map[uid]["nickname"] = note.get("nickname", uid)

        ranked = sorted(author_map.items(), key=lambda item: item[1]["count"], reverse=True)
        return [
            {
                "user_id": uid,
                "nickname": info["nickname"],
                "note_count": info["count"],
                "total_likes": info["total_likes"],
                "avg_likes": round(info["total_likes"] / info["count"], 1) if info["count"] else 0,
            }
            for uid, info in ranked[:top_n]
        ]

    def get_tag_distribution(self, top_n: int = 30) -> List[Tuple[str, int]]:
        tag_counter = Counter()
        for note in self.notes:
            tags = [tag.strip() for tag in _safe_text(note.get("tag_list")).split(",") if tag.strip()]
            tag_counter.update(tags)
        return tag_counter.most_common(top_n)

    def get_keyword_stats(self) -> Dict[str, Dict]:
        keyword_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for note in self.notes:
            keyword_groups[_safe_text(note.get("source_keyword")) or "未知"].append(note)

        result: Dict[str, Dict[str, Any]] = {}
        for keyword, notes in keyword_groups.items():
            likes = [_safe_int(note.get("liked_count")) for note in notes]
            result[keyword] = {
                "count": len(notes),
                "total_likes": sum(likes),
                "avg_likes": round(sum(likes) / len(likes), 1) if likes else 0,
                "max_likes": max(likes) if likes else 0,
            }
        return result

    # 地域聚合
    def _build_region_stats(self, name: str, notes: List[Dict[str, Any]]) -> Dict[str, Any]:
        author_keys = {_author_key(note) for note in notes if _author_key(note)}
        total_likes = sum(_safe_int(note.get("liked_count")) for note in notes)
        total_collects = sum(_safe_int(note.get("collected_count")) for note in notes)
        total_comments = sum(_safe_int(note.get("comment_count")) for note in notes)
        total_shares = sum(_safe_int(note.get("share_count")) for note in notes)
        total_interactions = total_likes + total_collects + total_comments + total_shares
        raw_locations = Counter(_safe_text(note.get("ip_location")) for note in notes if _safe_text(note.get("ip_location")))

        return {
            "name": name,
            "note_count": len(notes),
            "author_count": len(author_keys),
            "total_likes": total_likes,
            "avg_likes": round(total_likes / len(notes), 1) if notes else 0,
            "total_collects": total_collects,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "total_interactions": total_interactions,
            "avg_interactions": round(total_interactions / len(notes), 1) if notes else 0,
            "avg_notes_per_author": round(len(notes) / len(author_keys), 2) if author_keys else 0,
            "raw_locations": [loc for loc, _ in raw_locations.most_common(3)],
        }

    def get_geo_report(self) -> Dict[str, Any]:
        china_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        overseas_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        unknown_notes: List[Dict[str, Any]] = []

        for note in self.notes:
            normalized_name, region_type = _normalize_region(note.get("ip_location"))
            raw_location = _safe_text(note.get("ip_location")) or "未知"
            if region_type == "china":
                china_buckets[normalized_name].append(note)
            elif region_type == "overseas":
                overseas_buckets[raw_location].append(note)
            else:
                unknown_notes.append(note)

        china_stats = [self._build_region_stats(name, bucket) for name, bucket in china_buckets.items()]
        china_stats.sort(key=lambda item: item["author_count"], reverse=True)

        overseas_stats = [self._build_region_stats(name, bucket) for name, bucket in overseas_buckets.items()]
        overseas_stats.sort(key=lambda item: item["author_count"], reverse=True)

        top_region = china_stats[0] if china_stats else None
        domestic_author_keys = {
            _author_key(note)
            for bucket in china_buckets.values()
            for note in bucket
            if _author_key(note)
        }
        overseas_author_keys = {
            _author_key(note)
            for bucket in overseas_buckets.values()
            for note in bucket
            if _author_key(note)
        }

        return {
            "china_region_stats": china_stats,
            "overseas_distribution": overseas_stats,
            "unknown_region": self._build_region_stats("未知", unknown_notes) if unknown_notes else None,
            "summary": {
                "domestic_region_count": len(china_stats),
                "domestic_author_count": len(domestic_author_keys),
                "domestic_note_count": sum(item["note_count"] for item in china_stats),
                "overseas_region_count": len(overseas_stats),
                "overseas_author_count": len(overseas_author_keys),
                "overseas_note_count": sum(item["note_count"] for item in overseas_stats),
                "top_region_name": top_region["name"] if top_region else "",
                "top_region_author_count": top_region["author_count"] if top_region else 0,
            },
        }

    # 评论统计
    def get_comment_stats(self) -> Dict[str, Any]:
        if not self.comments:
            return {}
        likes = [_safe_int(comment.get("like_count")) for comment in self.comments]
        return {
            "total": len(self.comments),
            "avg_likes": round(sum(likes) / len(likes), 2) if likes else 0,
            "max_likes": max(likes) if likes else 0,
            "has_sub_comments": sum(1 for comment in self.comments if _safe_int(comment.get("sub_comment_count")) > 0),
        }

    def get_comment_location_distribution(self, top_n: int = 15) -> List[Tuple[str, int]]:
        locations = [_safe_text(comment.get("ip_location")) for comment in self.comments if _safe_text(comment.get("ip_location"))]
        return Counter(locations).most_common(top_n)

    def get_active_commenters(self, top_n: int = 10) -> List[Dict]:
        user_map: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "nickname": ""})
        for comment in self.comments:
            uid = _safe_text(comment.get("user_id"))
            if not uid:
                continue
            user_map[uid]["count"] += 1
            user_map[uid]["nickname"] = comment.get("nickname", uid)
        ranked = sorted(user_map.items(), key=lambda item: item[1]["count"], reverse=True)
        return [
            {"user_id": uid, "nickname": info["nickname"], "comment_count": info["count"]}
            for uid, info in ranked[:top_n]
        ]

    def generate_full_report(self) -> Dict[str, Any]:
        return {
            "overview": {
                "note_count": self.note_count(),
                "comment_count": self.comment_count(),
                "author_count": self.author_count(),
                "comment_record_count": self.comment_record_count(),
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
            "geo_report": self.get_geo_report(),
            "comment_stats": self.get_comment_stats(),
            "comment_location": self.get_comment_location_distribution(),
            "active_commenters": self.get_active_commenters(),
        }
