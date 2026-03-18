# -*- coding: utf-8 -*-
"""
趋势分析器 - 时间序列趋势、关键词热度变化
"""
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
import re


def _parse_ts(ts) -> datetime:
    """解析时间戳"""
    try:
        ts = int(ts)
        if ts > 1e12:
            ts = ts // 1000
        return datetime.fromtimestamp(ts)
    except:
        return None


class TrendAnalyzer:
    """趋势分析：时间序列下的内容热度与话题演变"""

    def __init__(self, notes: List[Dict], comments: List[Dict] = None):
        self.notes = notes
        self.comments = comments or []

    def get_publish_trend(self, granularity: str = "day") -> Dict[str, int]:
        """笔记发布量趋势（day/week/month）"""
        trend = defaultdict(int)
        for n in self.notes:
            dt = _parse_ts(n.get("time"))
            if not dt:
                continue
            if granularity == "day":
                key = dt.strftime("%Y-%m-%d")
            elif granularity == "week":
                # ISO year-week
                key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            elif granularity == "month":
                key = dt.strftime("%Y-%m")
            else:
                key = dt.strftime("%Y-%m-%d")
            trend[key] += 1
        return dict(sorted(trend.items()))

    def get_interaction_trend(self, metric: str = "liked_count", granularity: str = "day") -> Dict[str, float]:
        """某指标的时间趋势（均值）"""
        trend_sum = defaultdict(float)
        trend_count = defaultdict(int)
        for n in self.notes:
            dt = _parse_ts(n.get("time"))
            if not dt:
                continue
            if granularity == "day":
                key = dt.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            else:
                key = dt.strftime("%Y-%m")
            try:
                val_str = str(n.get(metric, "0") or "0")
                val_str = val_str.replace(",", "")
                if "万" in val_str:
                    val = float(val_str.replace("万", "")) * 10000
                else:
                    val = float(val_str)
            except:
                val = 0
            trend_sum[key] += val
            trend_count[key] += 1
        result = {
            k: round(trend_sum[k] / trend_count[k], 1)
            for k in sorted(trend_sum.keys())
        }
        return result

    def get_keyword_trend(self, keywords: List[str], granularity: str = "day") -> Dict[str, Dict[str, int]]:
        """多关键词在时间上的热度对比（出现次数）"""
        trend = defaultdict(lambda: defaultdict(int))
        for n in self.notes:
            dt = _parse_ts(n.get("time"))
            if not dt:
                continue
            if granularity == "day":
                key = dt.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            else:
                key = dt.strftime("%Y-%m")
            text = f"{n.get('title', '')} {n.get('desc', '')}"
            for kw in keywords:
                if kw in text:
                    trend[kw][key] += 1
        # 转为正常 dict
        return {kw: dict(sorted(v.items())) for kw, v in trend.items()}

    def get_hot_topics_by_period(self, top_n: int = 5, days: int = 7) -> List[Dict]:
        """最近 N 天的热门话题"""
        cutoff = datetime.now() - timedelta(days=days)
        recent_notes = []
        for n in self.notes:
            dt = _parse_ts(n.get("time"))
            if dt and dt >= cutoff:
                recent_notes.append(n)

        # 统计话题标签
        from collections import Counter
        tag_counter = Counter()
        for n in recent_notes:
            tag_str = n.get("tag_list", "") or ""
            tags = [t.strip() for t in tag_str.split(",") if t.strip()]
            tag_counter.update(tags)

        return [
            {"tag": tag, "count": cnt}
            for tag, cnt in tag_counter.most_common(top_n)
        ]

    def detect_viral_notes(self, threshold_ratio: float = 5.0) -> List[Dict]:
        """检测爆款笔记（点赞量超过平均值 threshold_ratio 倍）"""
        if not self.notes:
            return []

        likes = []
        for n in self.notes:
            try:
                v = str(n.get("liked_count", "0") or "0")
                v = v.replace(",", "")
                if "万" in v:
                    likes.append(float(v.replace("万", "")) * 10000)
                else:
                    likes.append(float(v))
            except:
                likes.append(0)

        if not likes:
            return []
        avg_likes = sum(likes) / len(likes)
        threshold = avg_likes * threshold_ratio

        viral = []
        for i, n in enumerate(self.notes):
            if likes[i] >= threshold:
                viral.append({
                    "note_id": n.get("note_id"),
                    "title": (n.get("title") or n.get("desc", ""))[:60],
                    "liked_count": int(likes[i]),
                    "avg_likes": round(avg_likes, 1),
                    "ratio": round(likes[i] / max(avg_likes, 1), 1),
                    "nickname": n.get("nickname", ""),
                    "note_url": n.get("note_url", "")
                })
        return sorted(viral, key=lambda x: x["liked_count"], reverse=True)

    def get_comment_time_trend(self, granularity: str = "day") -> Dict[str, int]:
        """评论发布时间趋势"""
        trend = defaultdict(int)
        for c in self.comments:
            dt = _parse_ts(c.get("create_time"))
            if not dt:
                continue
            if granularity == "day":
                key = dt.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            else:
                key = dt.strftime("%Y-%m")
            trend[key] += 1
        return dict(sorted(trend.items()))

    def get_growth_rate(self, granularity: str = "day") -> Dict[str, float]:
        """笔记发布量环比增长率"""
        trend = self.get_publish_trend(granularity)
        if len(trend) < 2:
            return {}
        keys = sorted(trend.keys())
        growth = {}
        for i in range(1, len(keys)):
            prev = trend[keys[i - 1]]
            curr = trend[keys[i]]
            if prev > 0:
                rate = round((curr - prev) / prev * 100, 1)
            else:
                rate = 0
            growth[keys[i]] = rate
        return growth

    def generate_trend_report(self) -> Dict[str, Any]:
        return {
            "publish_trend_daily": self.get_publish_trend("day"),
            "publish_trend_weekly": self.get_publish_trend("week"),
            "publish_trend_monthly": self.get_publish_trend("month"),
            "likes_trend": self.get_interaction_trend("liked_count"),
            "comment_time_trend": self.get_comment_time_trend("day"),
            "viral_notes": self.detect_viral_notes(),
            "recent_hot_topics": self.get_hot_topics_by_period(top_n=10, days=30),
            "growth_rate": self.get_growth_rate("day"),
        }
