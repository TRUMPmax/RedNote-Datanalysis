from __future__ import annotations

import threading
from typing import Any, Dict, List

from analyzer.data_loader import DataLoader
from analyzer.mining_analyzer import MiningAnalyzer
from analyzer.stats_analyzer import StatsAnalyzer
from analyzer.text_analyzer import TextAnalyzer
from analyzer.trend_analyzer import TrendAnalyzer


class AnalysisService:
    def __init__(self, loader: DataLoader):
        self.loader = loader
        self.text_analyzer = TextAnalyzer()
        self._lock = threading.Lock()
        self._bundle: Dict[str, Any] | None = None
        self._version = ""
        self._note_map: Dict[str, Dict[str, Any]] = {}
        self._comment_map: Dict[str, Dict[str, Any]] = {}

    def get_bundle(self, force: bool = False) -> Dict[str, Any]:
        raw_bundle = self.loader.refresh(force=force)
        version = raw_bundle["report"]["generated_at"]

        with self._lock:
            if force or self._bundle is None or version != self._version:
                notes = raw_bundle["notes"]
                comments = raw_bundle["comments"]

                stats = StatsAnalyzer(notes, comments).generate_full_report()
                trend = TrendAnalyzer(notes, comments).generate_trend_report()
                text = {
                    "note_keywords": self.text_analyzer.extract_keywords_from_notes(notes, top_n=20),
                    "comment_keywords": self.text_analyzer.extract_keywords_from_comments(comments, top_n=20),
                    "note_sentiment": self.text_analyzer.analyze_notes_sentiment(notes),
                    "comment_sentiment": self.text_analyzer.analyze_comments_sentiment(comments),
                    "hashtags": self.text_analyzer.extract_hashtags(notes),
                    "text_length": self.text_analyzer.get_text_length_stats(notes),
                }
                mining = MiningAnalyzer(notes, comments)
                page_report = mining.build_page_report()

                self._note_map = {
                    item["note_id"]: item
                    for item in mining.note_profiles
                }
                self._comment_map = {
                    item["comment_id"]: item
                    for item in mining.comment_profiles
                }

                self._bundle = {
                    "project": {
                        "name": "小红书数据采集与分析平台",
                        "generated_at": raw_bundle["report"]["generated_at"],
                    },
                    "summary": self.loader.get_data_summary(),
                    "assets": raw_bundle["report"],
                    "stats": stats,
                    "trend": trend,
                    "text": text,
                    "analysis": page_report,
                }
                self._version = version

            return self._bundle

    def enrich_notes(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for row in rows:
            enriched = dict(row)
            extra = self._note_map.get(row.get("note_id", ""), {})
            for field in (
                "quality_score",
                "content_score",
                "engagement_score",
                "discussion_score",
                "quality_tier",
                "cluster_name",
                "comment_negative_ratio",
            ):
                if field in extra:
                    enriched[field] = extra[field]
            results.append(enriched)
        return results

    def enrich_comments(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for row in rows:
            enriched = dict(row)
            extra = self._comment_map.get(row.get("comment_id", ""), {})
            for field in ("sentiment_label", "sentiment_score", "risk_hit_count"):
                if field in extra:
                    enriched[field] = extra[field]
            results.append(enriched)
        return results
