from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List

from .common import clean_text, safe_int


class TrendAnalyzer:
    def __init__(self, notes: List[Dict[str, Any]], comments: List[Dict[str, Any]] | None = None):
        self.notes = notes or []
        self.comments = comments or []

    def _group_note_metric(self, key_field: str, metric_field: str | None = None) -> Dict[str, float]:
        totals: Dict[str, float] = defaultdict(float)
        counts: Dict[str, int] = defaultdict(int)

        for note in self.notes:
            key = clean_text(note.get(key_field))
            if not key:
                continue
            if metric_field:
                totals[key] += safe_int(note.get(metric_field))
                counts[key] += 1
            else:
                totals[key] += 1

        if not metric_field:
            return dict(sorted((key, int(value)) for key, value in totals.items()))

        return {
            key: round(totals[key] / counts[key], 2)
            for key in sorted(totals.keys())
            if counts[key]
        }

    def get_publish_trend(self, granularity: str = "day") -> Dict[str, int]:
        if granularity == "month":
            monthly: Dict[str, int] = defaultdict(int)
            for note in self.notes:
                date_text = clean_text(note.get("publish_date"))
                if len(date_text) >= 7:
                    monthly[date_text[:7]] += 1
            return dict(sorted(monthly.items()))

        if granularity == "week":
            weekly: Dict[str, int] = defaultdict(int)
            for note in self.notes:
                date_text = clean_text(note.get("publish_date"))
                if len(date_text) >= 10:
                    year = date_text[:4]
                    weekly[f"{year}-W{self._week_number(date_text):02d}"] += 1
            return dict(sorted(weekly.items()))

        return self._group_note_metric("publish_date")

    def get_interaction_trend(self, metric: str = "liked_count") -> Dict[str, float]:
        return self._group_note_metric("publish_date", metric_field=metric)

    def get_hot_topics_by_period(self, top_n: int = 10) -> List[Dict[str, Any]]:
        counter = Counter()
        for note in self.notes:
            for tag in note.get("tag_list", []) or []:
                tag_text = clean_text(tag)
                if tag_text:
                    counter[tag_text] += 1
        return [{"tag": tag, "count": count} for tag, count in counter.most_common(top_n)]

    def detect_viral_notes(self, threshold_ratio: float = 5.0) -> List[Dict[str, Any]]:
        likes = [safe_int(note.get("liked_count")) for note in self.notes]
        if not likes:
            return []

        avg_likes = sum(likes) / len(likes)
        threshold = avg_likes * threshold_ratio
        viral_notes: List[Dict[str, Any]] = []
        for note in self.notes:
            liked_count = safe_int(note.get("liked_count"))
            if liked_count < threshold:
                continue
            viral_notes.append(
                {
                    "note_id": note.get("note_id", ""),
                    "title": note.get("title") or note.get("desc", ""),
                    "nickname": note.get("nickname", ""),
                    "liked_count": liked_count,
                    "avg_likes": round(avg_likes, 2),
                    "ratio": round(liked_count / max(avg_likes, 1), 2),
                    "publish_date": note.get("publish_date", ""),
                    "note_url": note.get("note_url", ""),
                }
            )
        viral_notes.sort(key=lambda item: item["liked_count"], reverse=True)
        return viral_notes

    def get_comment_time_trend(self) -> Dict[str, int]:
        trend: Dict[str, int] = defaultdict(int)
        for comment in self.comments:
            date_text = clean_text(comment.get("create_date"))
            if date_text:
                trend[date_text] += 1
        return dict(sorted(trend.items()))

    def get_growth_rate(self) -> Dict[str, float]:
        daily = self.get_publish_trend("day")
        dates = sorted(daily.keys())
        growth: Dict[str, float] = {}
        for index in range(1, len(dates)):
            previous = daily[dates[index - 1]]
            current = daily[dates[index]]
            if previous <= 0:
                growth[dates[index]] = 0
            else:
                growth[dates[index]] = round((current - previous) / previous * 100, 2)
        return growth

    def get_top_publish_days(self, top_n: int = 10) -> List[Dict[str, Any]]:
        daily = self.get_publish_trend("day")
        ranked = sorted(daily.items(), key=lambda item: item[1], reverse=True)
        return [{"date": date, "count": count} for date, count in ranked[:top_n]]

    def generate_trend_report(self) -> Dict[str, Any]:
        return {
            "publish_trend_daily": self.get_publish_trend("day"),
            "publish_trend_weekly": self.get_publish_trend("week"),
            "publish_trend_monthly": self.get_publish_trend("month"),
            "likes_trend": self.get_interaction_trend("liked_count"),
            "interaction_trend": self.get_interaction_trend("interaction_count"),
            "comment_time_trend": self.get_comment_time_trend(),
            "viral_notes": self.detect_viral_notes(),
            "recent_hot_topics": self.get_hot_topics_by_period(),
            "growth_rate": self.get_growth_rate(),
            "top_publish_days": self.get_top_publish_days(),
        }

    def _week_number(self, date_text: str) -> int:
        from datetime import datetime

        iso_value = datetime.strptime(date_text, "%Y-%m-%d").isocalendar()
        if hasattr(iso_value, "week"):
            return int(iso_value.week)
        return int(iso_value[1])
