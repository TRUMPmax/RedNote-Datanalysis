from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, Response, jsonify, request
from flask import cli as flask_cli


APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from analysis_service import AnalysisService
from analyzer.common import clean_text, safe_int
from analyzer.data_loader import DataLoader


app = Flask(__name__, static_folder=None)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
app.json.ensure_ascii = False

loader = DataLoader()
service = AnalysisService(loader)


@app.after_request
def apply_utf8_headers(response):
    if response.mimetype == "application/json":
        response.headers["Content-Type"] = "application/json; charset=utf-8"
    elif response.mimetype == "text/html":
        response.headers["Content-Type"] = "text/html; charset=utf-8"
    return response


def _limit_param(default: int = 100, max_value: int = 1000) -> int:
    value = safe_int(request.args.get("limit", default))
    if value <= 0:
        return default
    return min(value, max_value)


def _sort_order_desc() -> bool:
    return clean_text(request.args.get("order", "desc")).lower() != "asc"


def _filtered_notes() -> List[Dict[str, Any]]:
    service.get_bundle()
    notes = loader.get_notes()
    query = clean_text(request.args.get("q")).lower()
    author = clean_text(request.args.get("author")).lower()
    note_id = clean_text(request.args.get("note_id"))
    min_likes = safe_int(request.args.get("min_likes"))
    sort_by = clean_text(request.args.get("sort_by")) or "quality_score"
    allowed_sort_fields = {
        "liked_count",
        "collected_count",
        "comment_count",
        "share_count",
        "interaction_count",
        "quality_score",
        "content_score",
        "engagement_score",
        "discussion_score",
        "time",
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "quality_score"

    rows = service.enrich_notes(notes)
    filtered: List[Dict[str, Any]] = []
    for note in rows:
        if note_id and clean_text(note.get("note_id")) != note_id:
            continue
        if min_likes and safe_int(note.get("liked_count")) < min_likes:
            continue
        if query:
            haystack = " ".join(
                [
                    clean_text(note.get("title")),
                    clean_text(note.get("desc")),
                    clean_text(note.get("nickname")),
                    clean_text(note.get("ip_location")),
                    clean_text(note.get("cluster_name")),
                ]
            ).lower()
            if query not in haystack:
                continue
        if author and author not in clean_text(note.get("nickname")).lower():
            continue
        filtered.append(note)

    filtered.sort(key=lambda item: safe_int(item.get(sort_by)) if sort_by.endswith("_count") else float(item.get(sort_by, 0) or 0), reverse=_sort_order_desc())
    return filtered[: _limit_param()]


def _filtered_comments() -> List[Dict[str, Any]]:
    service.get_bundle()
    comments = loader.get_comments()
    query = clean_text(request.args.get("q")).lower()
    note_id = clean_text(request.args.get("note_id"))
    min_likes = safe_int(request.args.get("min_likes"))
    sentiment = clean_text(request.args.get("sentiment")).lower()
    sort_by = clean_text(request.args.get("sort_by")) or "like_count"
    allowed_sort_fields = {"like_count", "create_time", "sub_comment_count", "risk_hit_count"}
    if sort_by not in allowed_sort_fields:
        sort_by = "like_count"

    rows = service.enrich_comments(comments)
    filtered: List[Dict[str, Any]] = []
    for comment in rows:
        if note_id and clean_text(comment.get("note_id")) != note_id:
            continue
        if min_likes and safe_int(comment.get("like_count")) < min_likes:
            continue
        if sentiment and clean_text(comment.get("sentiment_label")).lower() != sentiment:
            continue
        if query:
            haystack = " ".join(
                [
                    clean_text(comment.get("content")),
                    clean_text(comment.get("nickname")),
                    clean_text(comment.get("ip_location")),
                ]
            ).lower()
            if query not in haystack:
                continue
        filtered.append(comment)

    filtered.sort(key=lambda item: safe_int(item.get(sort_by)), reverse=_sort_order_desc())
    return filtered[: _limit_param(default=120, max_value=2000)]


@app.get("/api/health")
def api_health():
    bundle = service.get_bundle()
    return jsonify(
        {
            "status": "ok",
            "project": bundle["project"]["name"],
            "generated_at": bundle["project"]["generated_at"],
            "raw_root": bundle["summary"]["raw_root"],
            "processed_root": bundle["summary"]["processed_root"],
        }
    )


@app.get("/api/summary")
def api_summary():
    return jsonify(service.get_bundle()["summary"])


@app.get("/api/quality")
def api_quality():
    return jsonify(service.get_bundle()["assets"])


@app.get("/api/stats")
def api_stats():
    return jsonify(service.get_bundle()["stats"])


@app.get("/api/text")
def api_text():
    return jsonify(service.get_bundle()["text"])


@app.get("/api/trend")
def api_trend():
    return jsonify(service.get_bundle()["trend"])


@app.get("/api/dashboard")
def api_dashboard():
    return jsonify(service.get_bundle())


@app.get("/api/overview")
def api_overview():
    return jsonify(service.get_bundle()["analysis"]["overview"])


@app.get("/api/opinion")
def api_opinion():
    return jsonify(service.get_bundle()["analysis"]["opinion"])


@app.get("/api/content")
def api_content():
    return jsonify(service.get_bundle()["analysis"]["content"])


@app.get("/api/relationship")
def api_relationship():
    return jsonify(service.get_bundle()["analysis"]["relationship"])


@app.get("/api/clusters")
def api_clusters():
    return jsonify(service.get_bundle()["analysis"]["clusters"])


@app.get("/api/notes")
def api_notes():
    rows = _filtered_notes()
    return jsonify({"count": len(rows), "data": rows})


@app.get("/api/comments")
def api_comments():
    rows = _filtered_comments()
    return jsonify({"count": len(rows), "data": rows})


@app.post("/api/rebuild")
def api_rebuild():
    loader.refresh(force=True)
    bundle = service.get_bundle(force=True)
    return jsonify(
        {
            "success": True,
            "message": "CrawlerData 预处理已重新执行，分析缓存已刷新。",
            "generated_at": bundle["project"]["generated_at"],
        }
    )


@app.get("/")
def index():
    html = (APP_DIR / "dashboard" / "index.html").read_text(encoding="utf-8")
    return Response(html, content_type="text/html; charset=utf-8")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    bundle = service.get_bundle()
    def _safe_console_print(message: str) -> None:
        try:
            print(message)
        except OSError:
            pass

    flask_cli.show_server_banner = lambda *args, **kwargs: None
    _safe_console_print("=" * 60)
    _safe_console_print(bundle["project"]["name"])
    _safe_console_print(f"访问地址: http://127.0.0.1:{port}")
    _safe_console_print(f"原始数据目录: {bundle['summary']['raw_root']}")
    _safe_console_print(f"清洗结果目录: {bundle['summary']['processed_root']}")
    _safe_console_print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False)
