from __future__ import annotations

import json
import sys

from app import app


def _safe_print(value: object) -> None:
    text = str(value)
    encoding = sys.stdout.encoding or "utf-8"
    print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def main() -> None:
    endpoints = [
        "/",
        "/static/vendor/echarts.min.js",
        "/api/health",
        "/api/dashboard",
        "/api/overview",
        "/api/opinion",
        "/api/content",
        "/api/relationship",
        "/api/clusters",
        "/api/summary",
        "/api/quality",
        "/api/notes?limit=3&sort_by=quality_score",
        "/api/comments?limit=3&sentiment=negative",
    ]

    with app.test_client() as client:
        for endpoint in endpoints:
            response = client.get(endpoint)
            _safe_print(f"{endpoint}: {response.status_code}")
            body = response.get_json(silent=True)
            if isinstance(body, dict):
                payload = json.dumps(body, ensure_ascii=False)
                _safe_print(payload[:300])
            else:
                _safe_print((response.get_data(as_text=True) or "")[:300])
            _safe_print("-" * 60)


if __name__ == "__main__":
    main()
