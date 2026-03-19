from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .common import clamp, clean_text, compact_text, round_float, safe_float, safe_int
from .text_analyzer import NEGATIVE_WORDS, POSITIVE_WORDS


RISK_KEYWORDS = {
    "广告",
    "营销",
    "恰饭",
    "翻车",
    "假货",
    "骗人",
    "骗局",
    "问题",
    "无语",
    "失望",
    "后悔",
    "夸张",
    "贵",
    "不值",
    "焦虑",
    "担心",
    "难受",
}

REGRESSION_FEATURES = [
    ("title_length", "标题长度"),
    ("desc_length", "正文长度"),
    ("tag_count", "标签数"),
    ("image_count", "配图数"),
    ("is_video", "视频内容"),
    ("note_sentiment_health", "文本情绪健康度"),
]

CORRELATION_FIELDS = [
    ("content_score", "内容质量分"),
    ("engagement_score", "传播表现分"),
    ("quality_score", "综合质量分"),
    ("liked_count", "点赞"),
    ("collected_count", "收藏"),
    ("comment_count", "评论"),
    ("share_count", "分享"),
    ("interaction_count", "总互动"),
    ("desc_length", "正文长度"),
    ("tag_count", "标签数"),
    ("image_count", "配图数"),
]


def _minmax_normalizer(values: Iterable[float]) -> Tuple[float, float]:
    values = list(values)
    if not values:
        return 0.0, 1.0
    low = min(values)
    high = max(values)
    if math.isclose(low, high):
        return low, low + 1.0
    return low, high


def _normalize(value: float, bounds: Tuple[float, float]) -> float:
    low, high = bounds
    return clamp((value - low) / (high - low))


def _pearson(x_values: Sequence[float], y_values: Sequence[float]) -> float:
    if not x_values or not y_values or len(x_values) != len(y_values):
        return 0.0
    count = len(x_values)
    mean_x = sum(x_values) / count
    mean_y = sum(y_values) / count
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in x_values))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in y_values))
    if denom_x == 0 or denom_y == 0:
        return 0.0
    return numerator / (denom_x * denom_y)


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


class MiningAnalyzer:
    def __init__(self, notes: List[Dict[str, Any]], comments: List[Dict[str, Any]]):
        self.notes = notes or []
        self.comments = comments or []
        self.comment_profiles = self._build_comment_profiles()
        self.comments_by_note: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for comment in self.comment_profiles:
            self.comments_by_note[comment["note_id"]].append(comment)
        self.note_profiles = self._build_note_profiles()
        self.note_profile_map = {note["note_id"]: note for note in self.note_profiles}
        self.cluster_report = self._build_cluster_report()
        self._apply_cluster_labels()

    def _keyword_hits(self, text: str, words: Iterable[str]) -> List[str]:
        content = compact_text(text).lower()
        if not content:
            return []
        hits = [word for word in words if word.lower() in content]
        return sorted(set(hits))

    def _sentiment_snapshot(self, text: str) -> Dict[str, Any]:
        positive_hits = self._keyword_hits(text, POSITIVE_WORDS)
        negative_hits = self._keyword_hits(text, NEGATIVE_WORDS)
        positive_count = len(positive_hits)
        negative_count = len(negative_hits)
        score = 0.0
        if positive_count or negative_count:
            score = (positive_count - negative_count) / max(positive_count + negative_count, 1)

        if score > 0:
            label = "positive"
        elif score < 0:
            label = "negative"
        else:
            label = "neutral"

        return {
            "label": label,
            "score": round_float(score, 4),
            "positive_hits": positive_hits,
            "negative_hits": negative_hits,
        }

    def _build_comment_profiles(self) -> List[Dict[str, Any]]:
        profiles: List[Dict[str, Any]] = []
        for comment in self.comments:
            content = compact_text(comment.get("content"))
            sentiment = self._sentiment_snapshot(content)
            risk_hits = self._keyword_hits(content, RISK_KEYWORDS)
            profiles.append(
                {
                    "comment_id": clean_text(comment.get("comment_id")),
                    "note_id": clean_text(comment.get("note_id")),
                    "content": content,
                    "nickname": compact_text(comment.get("nickname")),
                    "like_count": safe_int(comment.get("like_count")),
                    "sub_comment_count": safe_int(comment.get("sub_comment_count")),
                    "create_date": clean_text(comment.get("create_date")),
                    "ip_location": compact_text(comment.get("ip_location")),
                    "sentiment_label": sentiment["label"],
                    "sentiment_score": sentiment["score"],
                    "risk_hits": risk_hits,
                    "risk_hit_count": len(risk_hits),
                }
            )
        return profiles

    def _build_note_profiles(self) -> List[Dict[str, Any]]:
        draft_profiles: List[Dict[str, Any]] = []
        for note in self.notes:
            title = compact_text(note.get("title"))
            desc = compact_text(note.get("desc"))
            text_sentiment = self._sentiment_snapshot(f"{title} {desc}")
            note_comments = self.comments_by_note.get(clean_text(note.get("note_id")), [])
            comment_counter = Counter(item["sentiment_label"] for item in note_comments)
            risk_hits = Counter(hit for item in note_comments for hit in item["risk_hits"])
            negative_ratio = (
                comment_counter.get("negative", 0) / len(note_comments)
                if note_comments
                else 0.0
            )
            controversy_raw = (
                math.log1p(len(note_comments))
                * (1 + negative_ratio)
                * (1 - abs(comment_counter.get("positive", 0) - comment_counter.get("negative", 0)) / max(len(note_comments), 1))
            )

            draft_profiles.append(
                {
                    "note_id": clean_text(note.get("note_id")),
                    "title": title or desc[:36] or "(无标题)",
                    "nickname": compact_text(note.get("nickname")),
                    "publish_date": clean_text(note.get("publish_date")),
                    "type": clean_text(note.get("type")) or "normal",
                    "ip_location": compact_text(note.get("ip_location")),
                    "note_url": clean_text(note.get("note_url")),
                    "liked_count": safe_int(note.get("liked_count")),
                    "collected_count": safe_int(note.get("collected_count")),
                    "comment_count": safe_int(note.get("comment_count")),
                    "share_count": safe_int(note.get("share_count")),
                    "interaction_count": safe_int(note.get("interaction_count")),
                    "title_length": len(title),
                    "desc_length": len(desc),
                    "tag_count": len(note.get("tag_list", []) or []),
                    "image_count": safe_int(note.get("image_count")),
                    "tag_list": list(note.get("tag_list", []) or []),
                    "is_video": 1 if clean_text(note.get("type")) == "video" else 0,
                    "note_sentiment_label": text_sentiment["label"],
                    "note_sentiment_score": text_sentiment["score"],
                    "note_sentiment_health": round_float((text_sentiment["score"] + 1) / 2, 4),
                    "comment_actual_count": len(note_comments),
                    "comment_positive_count": comment_counter.get("positive", 0),
                    "comment_negative_count": comment_counter.get("negative", 0),
                    "comment_neutral_count": comment_counter.get("neutral", 0),
                    "comment_negative_ratio": round_float(negative_ratio, 4),
                    "comment_avg_like": round_float(_mean(item["like_count"] for item in note_comments), 2),
                    "risk_keyword_count": sum(risk_hits.values()),
                    "risk_keywords": [item for item, _ in risk_hits.most_common(5)],
                    "controversy_raw": round_float(controversy_raw, 4),
                    "content_score": 0.0,
                    "engagement_score": 0.0,
                    "quality_score": 0.0,
                    "discussion_score": 0.0,
                    "quality_tier": "",
                    "cluster_id": -1,
                    "cluster_name": "",
                }
            )

        bounds = {
            "liked_count": _minmax_normalizer(item["liked_count"] for item in draft_profiles),
            "collected_count": _minmax_normalizer(item["collected_count"] for item in draft_profiles),
            "comment_count": _minmax_normalizer(item["comment_count"] for item in draft_profiles),
            "share_count": _minmax_normalizer(item["share_count"] for item in draft_profiles),
            "title_length": _minmax_normalizer(item["title_length"] for item in draft_profiles),
            "desc_length": _minmax_normalizer(item["desc_length"] for item in draft_profiles),
            "tag_count": _minmax_normalizer(item["tag_count"] for item in draft_profiles),
            "image_count": _minmax_normalizer(item["image_count"] for item in draft_profiles),
            "comment_actual_count": _minmax_normalizer(item["comment_actual_count"] for item in draft_profiles),
            "comment_avg_like": _minmax_normalizer(item["comment_avg_like"] for item in draft_profiles),
            "controversy_raw": _minmax_normalizer(item["controversy_raw"] for item in draft_profiles),
            "comment_negative_ratio": _minmax_normalizer(item["comment_negative_ratio"] for item in draft_profiles),
        }

        for profile in draft_profiles:
            content_score = 100 * (
                0.24 * _normalize(profile["desc_length"], bounds["desc_length"])
                + 0.18 * _normalize(profile["tag_count"], bounds["tag_count"])
                + 0.18 * _normalize(profile["image_count"], bounds["image_count"])
                + 0.14 * _normalize(profile["title_length"], bounds["title_length"])
                + 0.10 * profile["is_video"]
                + 0.16 * profile["note_sentiment_health"]
            )
            engagement_score = 100 * (
                0.40 * _normalize(profile["liked_count"], bounds["liked_count"])
                + 0.24 * _normalize(profile["collected_count"], bounds["collected_count"])
                + 0.22 * _normalize(profile["comment_count"], bounds["comment_count"])
                + 0.14 * _normalize(profile["share_count"], bounds["share_count"])
            )
            discussion_score = 100 * (
                0.40 * _normalize(profile["comment_actual_count"], bounds["comment_actual_count"])
                + 0.25 * _normalize(profile["comment_avg_like"], bounds["comment_avg_like"])
                + 0.20 * _normalize(profile["controversy_raw"], bounds["controversy_raw"])
                + 0.15 * _normalize(profile["comment_negative_ratio"], bounds["comment_negative_ratio"])
            )
            quality_score = 0.42 * content_score + 0.58 * engagement_score
            profile["content_score"] = round_float(content_score, 2)
            profile["engagement_score"] = round_float(engagement_score, 2)
            profile["discussion_score"] = round_float(discussion_score, 2)
            profile["quality_score"] = round_float(quality_score, 2)
            profile["quality_tier"] = self._quality_tier(profile["quality_score"])

        draft_profiles.sort(
            key=lambda item: (
                item["quality_score"],
                item["interaction_count"],
                item["liked_count"],
            ),
            reverse=True,
        )
        return draft_profiles

    def _quality_tier(self, score: float) -> str:
        if score >= 82:
            return "S"
        if score >= 68:
            return "A"
        if score >= 54:
            return "B"
        if score >= 40:
            return "C"
        return "D"

    def _cluster_vectors(self) -> Tuple[List[List[float]], List[Dict[str, Any]]]:
        if not self.note_profiles:
            return [], []
        feature_names = [
            "content_score",
            "engagement_score",
            "discussion_score",
            "comment_negative_ratio",
            "tag_count",
        ]
        bounds = {name: _minmax_normalizer(item[name] for item in self.note_profiles) for name in feature_names}
        vectors: List[List[float]] = []
        for item in self.note_profiles:
            vectors.append([_normalize(safe_float(item[name]), bounds[name]) for name in feature_names])
        return vectors, self.note_profiles

    def _distance(self, left: Sequence[float], right: Sequence[float]) -> float:
        return math.sqrt(sum((lv - rv) ** 2 for lv, rv in zip(left, right)))

    def _kmeans(self, vectors: List[List[float]], k: int = 4, iterations: int = 32) -> Tuple[List[int], List[List[float]]]:
        if not vectors:
            return [], []

        k = max(1, min(k, len(vectors)))
        ranked_index = sorted(range(len(vectors)), key=lambda idx: sum(vectors[idx]))
        seed_positions = sorted(set([0, len(ranked_index) // 3, (len(ranked_index) * 2) // 3, len(ranked_index) - 1]))[:k]
        centers = [vectors[ranked_index[pos]][:] for pos in seed_positions]
        while len(centers) < k:
            centers.append(vectors[len(centers) % len(vectors)][:])

        assignments = [0 for _ in vectors]
        for _ in range(iterations):
            changed = False
            for index, vector in enumerate(vectors):
                best_cluster = min(range(k), key=lambda cid: self._distance(vector, centers[cid]))
                if assignments[index] != best_cluster:
                    assignments[index] = best_cluster
                    changed = True

            grouped: Dict[int, List[List[float]]] = defaultdict(list)
            for index, cluster_id in enumerate(assignments):
                grouped[cluster_id].append(vectors[index])

            for cluster_id in range(k):
                if not grouped[cluster_id]:
                    continue
                centers[cluster_id] = [
                    sum(point[axis] for point in grouped[cluster_id]) / len(grouped[cluster_id])
                    for axis in range(len(vectors[0]))
                ]
            if not changed:
                break

        return assignments, centers

    def _cluster_label(self, summary: Dict[str, Any], overall: Dict[str, float]) -> str:
        if summary["avg_discussion_score"] > overall["discussion_score"] * 1.15:
            return "高讨论热度型"
        if summary["avg_engagement_score"] > overall["engagement_score"] * 1.12:
            return "高传播爆发型"
        if summary["avg_content_score"] > overall["content_score"] * 1.10 and summary["avg_quality_score"] >= overall["quality_score"]:
            return "高质量种草型"
        return "稳态日常型"

    def _build_cluster_report(self) -> Dict[str, Any]:
        vectors, profiles = self._cluster_vectors()
        assignments, _ = self._kmeans(vectors, k=4)
        if not profiles:
            return {"summary": [], "scatter": [], "radar": {"indicators": [], "series": []}}

        grouped_profiles: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for index, profile in enumerate(profiles):
            cluster_id = assignments[index]
            profile["cluster_id"] = cluster_id
            grouped_profiles[cluster_id].append(profile)

        overall = {
            "quality_score": _mean(item["quality_score"] for item in profiles),
            "engagement_score": _mean(item["engagement_score"] for item in profiles),
            "content_score": _mean(item["content_score"] for item in profiles),
            "discussion_score": _mean(item["discussion_score"] for item in profiles),
        }

        summary: List[Dict[str, Any]] = []
        scatter: List[Dict[str, Any]] = []
        radar_series: List[Dict[str, Any]] = []
        indicators = [
            {"name": "综合质量", "max": 100},
            {"name": "内容质量", "max": 100},
            {"name": "传播表现", "max": 100},
            {"name": "讨论热度", "max": 100},
            {"name": "负面占比", "max": 100},
        ]

        for cluster_id, items in sorted(grouped_profiles.items()):
            avg_quality = _mean(item["quality_score"] for item in items)
            avg_content = _mean(item["content_score"] for item in items)
            avg_engagement = _mean(item["engagement_score"] for item in items)
            avg_discussion = _mean(item["discussion_score"] for item in items)
            avg_negative = _mean(item["comment_negative_ratio"] * 100 for item in items)
            top_tags = Counter(tag for item in items for tag in item["tag_list"]).most_common(5)
            sample_notes = sorted(items, key=lambda item: item["quality_score"], reverse=True)[:3]
            cluster_summary = {
                "cluster_id": cluster_id,
                "cluster_name": "",
                "note_count": len(items),
                "avg_quality_score": round_float(avg_quality, 2),
                "avg_content_score": round_float(avg_content, 2),
                "avg_engagement_score": round_float(avg_engagement, 2),
                "avg_discussion_score": round_float(avg_discussion, 2),
                "avg_negative_ratio": round_float(avg_negative, 2),
                "top_tags": [tag for tag, _ in top_tags],
                "sample_notes": [
                    {
                        "note_id": item["note_id"],
                        "title": item["title"],
                        "nickname": item["nickname"],
                        "quality_score": item["quality_score"],
                    }
                    for item in sample_notes
                ],
            }
            cluster_summary["cluster_name"] = self._cluster_label(cluster_summary, overall)
            summary.append(cluster_summary)

            radar_series.append(
                {
                    "cluster_id": cluster_id,
                    "name": cluster_summary["cluster_name"],
                    "value": [
                        cluster_summary["avg_quality_score"],
                        cluster_summary["avg_content_score"],
                        cluster_summary["avg_engagement_score"],
                        cluster_summary["avg_discussion_score"],
                        cluster_summary["avg_negative_ratio"],
                    ],
                }
            )

            for item in items:
                scatter.append(
                    {
                        "name": item["title"],
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_summary["cluster_name"],
                        "value": [
                            item["quality_score"],
                            item["engagement_score"],
                            item["discussion_score"],
                        ],
                        "note_id": item["note_id"],
                        "nickname": item["nickname"],
                    }
                )

        duplicate_counter = Counter(item["cluster_name"] for item in summary)
        seen_counter = Counter()
        for item in summary:
            name = item["cluster_name"]
            if duplicate_counter[name] > 1:
                seen_counter[name] += 1
                item["cluster_name"] = f"{name}{seen_counter[name]}"

        summary_map = {item["cluster_id"]: item for item in summary}
        for series in radar_series:
            cluster_info = summary_map.get(series["cluster_id"])
            if cluster_info:
                series["name"] = cluster_info["cluster_name"]
            series.pop("cluster_id", None)
        for point in scatter:
            cluster_info = summary_map.get(point["cluster_id"])
            if cluster_info:
                point["cluster_name"] = cluster_info["cluster_name"]

        summary_map = {item["cluster_id"]: item for item in summary}
        for item in profiles:
            cluster_info = summary_map.get(item["cluster_id"])
            item["cluster_name"] = cluster_info["cluster_name"] if cluster_info else ""

        return {
            "summary": summary,
            "scatter": scatter,
            "radar": {
                "indicators": indicators,
                "series": radar_series,
            },
        }

    def _apply_cluster_labels(self) -> None:
        summary_map = {item["cluster_id"]: item["cluster_name"] for item in self.cluster_report["summary"]}
        for item in self.note_profiles:
            item["cluster_name"] = summary_map.get(item["cluster_id"], "")

    def _top_authors(self) -> List[Dict[str, Any]]:
        author_map: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"note_count": 0, "quality_total": 0.0, "interaction_total": 0.0, "nickname": ""}
        )
        for item in self.note_profiles:
            key = item["nickname"] or item["note_id"]
            author_map[key]["note_count"] += 1
            author_map[key]["quality_total"] += item["quality_score"]
            author_map[key]["interaction_total"] += item["interaction_count"]
            author_map[key]["nickname"] = item["nickname"] or key

        ranked = sorted(
            author_map.items(),
            key=lambda row: (row[1]["interaction_total"], row[1]["note_count"]),
            reverse=True,
        )[:10]
        return [
            {
                "nickname": info["nickname"],
                "note_count": info["note_count"],
                "avg_quality_score": round_float(info["quality_total"] / info["note_count"], 2),
                "interaction_total": round_float(info["interaction_total"], 2),
            }
            for _, info in ranked
        ]

    def _build_overview_report(self) -> Dict[str, Any]:
        province_counter: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0, "interactions": 0.0})
        for note in self.note_profiles:
            province = clean_text(note.get("ip_location")) or "未知"
            province_counter[province]["count"] += 1
            province_counter[province]["interactions"] += note["interaction_count"]

        province_rank = sorted(
            (
                {
                    "name": name,
                    "note_count": int(info["count"]),
                    "interaction_count": round_float(info["interactions"], 2),
                }
                for name, info in province_counter.items()
            ),
            key=lambda item: (item["interaction_count"], item["note_count"]),
            reverse=True,
        )[:10]

        type_counter = Counter("视频" if clean_text(item["type"]) == "video" else "图文" for item in self.note_profiles)
        daily_counter = Counter(item["publish_date"] for item in self.note_profiles if item["publish_date"])

        return {
            "kpis": {
                "note_count": len(self.note_profiles),
                "comment_count": len(self.comment_profiles),
                "author_count": len({item["nickname"] or item["note_id"] for item in self.note_profiles}),
                "avg_quality_score": round_float(_mean(item["quality_score"] for item in self.note_profiles), 2),
                "avg_engagement_score": round_float(_mean(item["engagement_score"] for item in self.note_profiles), 2),
                "total_interactions": sum(item["interaction_count"] for item in self.note_profiles),
            },
            "publish_trend": [
                {"date": date, "note_count": daily_counter[date]}
                for date in sorted(daily_counter.keys())[-30:]
            ],
            "province_rank": province_rank,
            "type_distribution": [
                {"name": name, "value": value}
                for name, value in type_counter.items()
            ],
            "top_notes": [
                {
                    "note_id": item["note_id"],
                    "title": item["title"],
                    "nickname": item["nickname"],
                    "quality_score": item["quality_score"],
                    "interaction_count": item["interaction_count"],
                    "publish_date": item["publish_date"],
                }
                for item in self.note_profiles[:10]
            ],
            "top_authors": self._top_authors(),
        }

    def _build_opinion_report(self) -> Dict[str, Any]:
        timeline: Dict[str, Dict[str, int]] = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0})
        risk_counter = Counter()
        for comment in self.comment_profiles:
            date_key = comment["create_date"] or "未知"
            timeline[date_key][comment["sentiment_label"]] += 1
            risk_counter.update(comment["risk_hits"])

        controversial_notes = sorted(
            self.note_profiles,
            key=lambda item: (item["discussion_score"], item["comment_negative_ratio"], item["comment_actual_count"]),
            reverse=True,
        )[:12]
        negative_comments = sorted(
            (comment for comment in self.comment_profiles if comment["sentiment_label"] == "negative"),
            key=lambda item: (item["like_count"], item["sub_comment_count"]),
            reverse=True,
        )[:15]

        note_sentiment_counter = Counter(item["note_sentiment_label"] for item in self.note_profiles)
        comment_sentiment_counter = Counter(item["sentiment_label"] for item in self.comment_profiles)

        return {
            "note_sentiment": {
                "positive": note_sentiment_counter.get("positive", 0),
                "neutral": note_sentiment_counter.get("neutral", 0),
                "negative": note_sentiment_counter.get("negative", 0),
            },
            "comment_sentiment": {
                "positive": comment_sentiment_counter.get("positive", 0),
                "neutral": comment_sentiment_counter.get("neutral", 0),
                "negative": comment_sentiment_counter.get("negative", 0),
            },
            "timeline": [
                {
                    "date": date,
                    "positive": values["positive"],
                    "neutral": values["neutral"],
                    "negative": values["negative"],
                }
                for date, values in sorted(timeline.items())[-30:]
            ],
            "risk_keywords": [
                {"name": keyword, "value": value}
                for keyword, value in risk_counter.most_common(12)
            ],
            "controversial_notes": [
                {
                    "note_id": item["note_id"],
                    "title": item["title"],
                    "nickname": item["nickname"],
                    "discussion_score": item["discussion_score"],
                    "negative_ratio": round_float(item["comment_negative_ratio"] * 100, 2),
                    "comment_actual_count": item["comment_actual_count"],
                    "risk_keywords": item["risk_keywords"],
                }
                for item in controversial_notes
            ],
            "negative_comments": [
                {
                    "comment_id": item["comment_id"],
                    "note_id": item["note_id"],
                    "content": item["content"],
                    "nickname": item["nickname"],
                    "like_count": item["like_count"],
                    "create_date": item["create_date"],
                }
                for item in negative_comments
            ],
        }

    def _build_content_report(self) -> Dict[str, Any]:
        quality_distribution = Counter(item["quality_tier"] for item in self.note_profiles)
        scatter = [
            {
                "name": item["title"],
                "value": [item["desc_length"], item["interaction_count"], item["quality_score"]],
                "note_id": item["note_id"],
                "nickname": item["nickname"],
            }
            for item in self.note_profiles
        ]
        return {
            "quality_distribution": [
                {"name": tier, "value": quality_distribution.get(tier, 0)}
                for tier in ["S", "A", "B", "C", "D"]
            ],
            "quality_top_notes": [
                {
                    "note_id": item["note_id"],
                    "title": item["title"],
                    "nickname": item["nickname"],
                    "quality_score": item["quality_score"],
                    "content_score": item["content_score"],
                    "engagement_score": item["engagement_score"],
                    "quality_tier": item["quality_tier"],
                }
                for item in self.note_profiles[:12]
            ],
            "score_breakdown": {
                "content_score": round_float(_mean(item["content_score"] for item in self.note_profiles), 2),
                "engagement_score": round_float(_mean(item["engagement_score"] for item in self.note_profiles), 2),
                "discussion_score": round_float(_mean(item["discussion_score"] for item in self.note_profiles), 2),
                "quality_score": round_float(_mean(item["quality_score"] for item in self.note_profiles), 2),
            },
            "content_vs_engagement": scatter,
        }

    def _standardize_features(self, features: List[List[float]]) -> Tuple[List[List[float]], List[Tuple[float, float]]]:
        if not features:
            return [], []
        columns = len(features[0])
        means = []
        stds = []
        for axis in range(columns):
            values = [row[axis] for row in features]
            mean_value = _mean(values)
            variance = _mean((value - mean_value) ** 2 for value in values)
            std_value = math.sqrt(variance) or 1.0
            means.append(mean_value)
            stds.append(std_value)
        standardized = [
            [(value - means[axis]) / stds[axis] for axis, value in enumerate(row)]
            for row in features
        ]
        return standardized, list(zip(means, stds))

    def _train_interaction_model(self) -> Dict[str, Any]:
        if not self.note_profiles:
            return {"r2": 0.0, "features": []}

        features = [
            [safe_float(note[field]) for field, _ in REGRESSION_FEATURES]
            for note in self.note_profiles
        ]
        targets = [math.log1p(max(note["interaction_count"], 0)) for note in self.note_profiles]
        x_values, _ = self._standardize_features(features)
        y_mean = _mean(targets)
        y_std = math.sqrt(_mean((value - y_mean) ** 2 for value in targets)) or 1.0
        y_values = [(value - y_mean) / y_std for value in targets]

        weights = [0.0 for _ in REGRESSION_FEATURES]
        bias = 0.0
        learning_rate = 0.06
        sample_count = len(x_values)

        for _ in range(1800):
            pred_values = [sum(weight * value for weight, value in zip(weights, row)) + bias for row in x_values]
            errors = [pred - actual for pred, actual in zip(pred_values, y_values)]
            gradient_w = [
                2 * sum(error * row[index] for error, row in zip(errors, x_values)) / sample_count
                for index in range(len(weights))
            ]
            gradient_b = 2 * sum(errors) / sample_count
            weights = [weight - learning_rate * gradient for weight, gradient in zip(weights, gradient_w)]
            bias -= learning_rate * gradient_b

        predictions = [sum(weight * value for weight, value in zip(weights, row)) + bias for row in x_values]
        ss_res = sum((actual - pred) ** 2 for actual, pred in zip(y_values, predictions))
        ss_tot = sum((actual - _mean(y_values)) ** 2 for actual in y_values) or 1.0
        r2 = 1 - ss_res / ss_tot
        total_importance = sum(abs(weight) for weight in weights) or 1.0

        return {
            "r2": round_float(r2, 4),
            "features": [
                {
                    "feature": field,
                    "label": label,
                    "coefficient": round_float(weight, 4),
                    "importance": round_float(abs(weight) / total_importance * 100, 2),
                }
                for (field, label), weight in sorted(
                    zip(REGRESSION_FEATURES, weights),
                    key=lambda row: abs(row[1]),
                    reverse=True,
                )
            ],
        }

    def _build_relationship_report(self) -> Dict[str, Any]:
        labels = [item[1] for item in CORRELATION_FIELDS]
        value_map = {
            field: [safe_float(note[field]) for note in self.note_profiles]
            for field, _ in CORRELATION_FIELDS
        }
        matrix: List[List[float]] = []
        strong_pairs: List[Dict[str, Any]] = []
        for row_index, (left_field, left_label) in enumerate(CORRELATION_FIELDS):
            for col_index, (right_field, right_label) in enumerate(CORRELATION_FIELDS):
                corr = round_float(_pearson(value_map[left_field], value_map[right_field]), 4)
                matrix.append([row_index, col_index, corr])
                if col_index > row_index:
                    strong_pairs.append(
                        {
                            "left": left_label,
                            "right": right_label,
                            "value": corr,
                        }
                    )

        strong_pairs.sort(key=lambda item: abs(item["value"]), reverse=True)
        regression = self._train_interaction_model()
        return {
            "correlation": {
                "labels": labels,
                "matrix": matrix,
            },
            "strong_pairs": strong_pairs[:8],
            "engagement_scatter": [
                {
                    "name": item["title"],
                    "value": [
                        item["liked_count"],
                        item["comment_count"],
                        item["collected_count"],
                        item["quality_score"],
                    ],
                    "quality_tier": item["quality_tier"],
                    "note_id": item["note_id"],
                }
                for item in self.note_profiles
            ],
            "model": regression,
        }

    def build_page_report(self) -> Dict[str, Any]:
        return {
            "overview": self._build_overview_report(),
            "opinion": self._build_opinion_report(),
            "content": self._build_content_report(),
            "relationship": self._build_relationship_report(),
            "clusters": self.cluster_report,
        }
