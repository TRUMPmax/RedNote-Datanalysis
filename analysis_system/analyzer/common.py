from __future__ import annotations

import ast
import json
import math
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, List


NULLISH_VALUES = {"", "nan", "none", "null", "nat"}
SHANGHAI_TZ = timezone(timedelta(hours=8))


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = (
        text.replace("\ufeff", "")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\x00", "")
        .strip()
    )
    return "" if text.lower() in NULLISH_VALUES else text


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip()


def safe_decimal(value: Any) -> Decimal:
    text = clean_text(value).replace(",", "")
    if not text:
        return Decimal("0")

    multiplier = Decimal("1")
    suffix = text[-1]
    if suffix in {"万", "w", "W"}:
        multiplier = Decimal("10000")
        text = text[:-1]
    elif suffix in {"千", "k", "K"}:
        multiplier = Decimal("1000")
        text = text[:-1]

    try:
        return Decimal(text) * multiplier
    except (InvalidOperation, ValueError):
        return Decimal("0")


def safe_int(value: Any) -> int:
    try:
        return int(safe_decimal(value))
    except (InvalidOperation, ValueError, OverflowError):
        return 0


def safe_float(value: Any) -> float:
    try:
        return float(safe_decimal(value))
    except (InvalidOperation, ValueError, OverflowError):
        return 0.0


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def round_float(value: float, digits: int = 2) -> float:
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return round(value, digits)


def parse_timestamp(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0

    try:
        number = safe_decimal(text)
    except (InvalidOperation, ValueError):
        return 0

    if number <= 0:
        return 0

    raw = int(number)
    if raw >= 10**12:
        return raw // 1000
    if raw >= 10**9:
        return raw
    if 20000 <= raw <= 80000:
        excel_base = datetime(1899, 12, 30, tzinfo=timezone.utc)
        dt = excel_base + timedelta(days=raw)
        return int(dt.timestamp())
    return 0


def timestamp_to_date_text(value: Any) -> str:
    ts = parse_timestamp(value) if not isinstance(value, int) else value
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=SHANGHAI_TZ).strftime("%Y-%m-%d")


def timestamp_to_datetime_text(value: Any) -> str:
    ts = parse_timestamp(value) if not isinstance(value, int) else value
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")


def dedupe_keep_order(items: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for item in items:
        text = compact_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def parse_list_like(value: Any) -> List[str]:
    text = clean_text(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return dedupe_keep_order(str(item) for item in parsed)

    parts: List[str] = []
    for piece in re.split(r"[\n|]+", text):
        parts.extend(re.split(r"\s*,\s*|\s*，\s*", piece))
    return dedupe_keep_order(parts)


def merge_unique_values(*groups: Iterable[str]) -> List[str]:
    flattened: List[str] = []
    for group in groups:
        for item in group:
            flattened.append(item)
    return dedupe_keep_order(flattened)


def extract_hashtags(text: Any) -> List[str]:
    content = clean_text(text).replace("[话题]", "")
    if not content:
        return []
    hashtags = re.findall(r"#([^#\[\]\s]+)", content)
    return dedupe_keep_order(hashtags)
