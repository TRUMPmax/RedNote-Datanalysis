from __future__ import annotations

import csv
import hashlib
import json
import threading
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .common import (
    clean_text,
    compact_text,
    dedupe_keep_order,
    extract_hashtags,
    merge_unique_values,
    parse_list_like,
    parse_timestamp,
    safe_int,
    timestamp_to_date_text,
    timestamp_to_datetime_text,
)


NOTE_EXPORT_FIELDS = [
    "note_id",
    "type",
    "title",
    "desc",
    "video_url",
    "time",
    "publish_date",
    "publish_datetime",
    "last_update_time",
    "user_id",
    "nickname",
    "avatar",
    "liked_count",
    "collected_count",
    "comment_count",
    "share_count",
    "interaction_count",
    "ip_location",
    "image_count",
    "image_list",
    "tag_count",
    "tag_list",
    "last_modify_ts",
    "note_url",
    "source_keyword",
    "xsec_token",
    "source_files",
    "duplicate_count",
    "field_completeness",
]

COMMENT_EXPORT_FIELDS = [
    "comment_id",
    "note_id",
    "create_time",
    "create_date",
    "create_datetime",
    "ip_location",
    "content",
    "content_length",
    "user_id",
    "nickname",
    "avatar",
    "sub_comment_count",
    "picture_count",
    "pictures",
    "parent_comment_id",
    "last_modify_ts",
    "like_count",
    "source_files",
    "duplicate_count",
    "field_completeness",
]

NOTE_SOURCE_FIELDS = {
    "note_id",
    "type",
    "title",
    "desc",
    "video_url",
    "time",
    "last_update_time",
    "user_id",
    "nickname",
    "avatar",
    "liked_count",
    "collected_count",
    "comment_count",
    "share_count",
    "ip_location",
    "image_list",
    "tag_list",
    "last_modify_ts",
    "note_url",
    "source_keyword",
    "xsec_token",
}

COMMENT_SOURCE_FIELDS = {
    "comment_id",
    "create_time",
    "ip_location",
    "note_id",
    "content",
    "user_id",
    "nickname",
    "avatar",
    "sub_comment_count",
    "pictures",
    "parent_comment_id",
    "last_modify_ts",
    "like_count",
}


class DataLoader:
    """Load, preprocess, deduplicate, and cache CrawlerData artifacts."""

    def __init__(self, raw_root: str | None = None, processed_root: str | None = None):
        project_root = Path(__file__).resolve().parents[2]
        self.project_root = project_root
        self.raw_root = Path(raw_root) if raw_root else project_root / "CrawlerData"
        self.processed_root = Path(processed_root) if processed_root else project_root / "analysis_system" / "data" / "processed"
        self.processed_root.mkdir(parents=True, exist_ok=True)

        self.notes_file = self.processed_root / "notes_cleaned.csv"
        self.comments_file = self.processed_root / "comments_cleaned.csv"
        self.report_file = self.processed_root / "preprocess_report.json"

        self._bundle: Dict[str, Any] | None = None
        self._lock = threading.Lock()
        self._encoding_cache: Dict[str, str] = {}

    def refresh(self, force: bool = False) -> Dict[str, Any]:
        with self._lock:
            if self._bundle is None or force:
                self._bundle = self._build_bundle()
            return self._bundle

    def get_notes(self, force: bool = False) -> List[Dict[str, Any]]:
        return list(self.refresh(force)["notes"])

    def get_comments(self, force: bool = False) -> List[Dict[str, Any]]:
        return list(self.refresh(force)["comments"])

    def get_processing_report(self, force: bool = False) -> Dict[str, Any]:
        return deepcopy(self.refresh(force)["report"])

    def get_data_summary(self, force: bool = False) -> Dict[str, Any]:
        report = self.get_processing_report(force)
        return {
            "raw_root": report["raw_root"],
            "processed_root": report["processed_root"],
            "generated_at": report["generated_at"],
            "source_file_count": report["source_file_count"],
            "schema_group_count": len(report["schema_groups"]),
            "warnings": report["warnings"],
            "artifacts": report["artifacts"],
            "note_summary": report["notes"],
            "comment_summary": report["comments"],
            "date_range": report["date_range"],
        }

    def list_available_keywords(self) -> List[str]:
        keywords = {note.get("source_keyword", "") for note in self.get_notes() if note.get("source_keyword")}
        return sorted(keywords)

    def _build_bundle(self) -> Dict[str, Any]:
        if not self.raw_root.exists():
            raise FileNotFoundError(f"CrawlerData directory not found: {self.raw_root}")

        csv_files = sorted(self.raw_root.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {self.raw_root}")

        schema_groups = self._discover_schema_groups(csv_files)
        source_files: List[Dict[str, Any]] = []
        warnings: List[str] = []

        all_note_rows: List[Dict[str, Any]] = []
        all_comment_rows: List[Dict[str, Any]] = []
        raw_note_rows = 0
        raw_comment_rows = 0

        schema_report: List[Dict[str, Any]] = []
        for kind, header, files in schema_groups:
            merged_group_rows: List[Dict[str, Any]] = []
            for file_path in files:
                rows = self._read_raw_rows(file_path)
                merged_group_rows.extend(rows)
                source_files.append(
                    {
                        "name": file_path.name,
                        "kind": kind,
                        "row_count": len(rows),
                    }
                )

            extra_columns = sorted(set(header) - (NOTE_SOURCE_FIELDS if kind == "notes" else COMMENT_SOURCE_FIELDS))
            if extra_columns:
                warnings.append(
                    f"{kind} 模式中发现额外字段: {', '.join(extra_columns)}"
                )

            if kind == "notes":
                normalized_rows = [
                    self._normalize_note_row(row)
                    for row in merged_group_rows
                ]
                normalized_rows = [row for row in normalized_rows if self._is_valid_note(row)]
                raw_note_rows += len(normalized_rows)
                all_note_rows.extend(normalized_rows)
            else:
                normalized_rows = [
                    self._normalize_comment_row(row)
                    for row in merged_group_rows
                ]
                normalized_rows = [row for row in normalized_rows if self._is_valid_comment(row)]
                raw_comment_rows += len(normalized_rows)
                all_comment_rows.extend(normalized_rows)

            schema_report.append(
                {
                    "kind": kind,
                    "schema_id": self._schema_id(header),
                    "column_count": len(header),
                    "columns": list(header),
                    "extra_columns": extra_columns,
                    "file_count": len(files),
                    "raw_row_count": len(merged_group_rows),
                    "normalized_row_count": len(normalized_rows),
                    "files": [item.name for item in files],
                }
            )

        notes, note_duplicate_rows = self._dedupe_notes(all_note_rows)
        note_ids = {note["note_id"] for note in notes}
        comments, comment_duplicate_rows, orphan_comments = self._dedupe_comments(all_comment_rows, note_ids)

        note_title_missing = sum(1 for note in notes if not note.get("title"))
        note_location_missing = sum(1 for note in notes if not note.get("ip_location"))
        note_desc_missing = sum(1 for note in notes if not note.get("desc"))
        comment_location_missing = sum(1 for comment in comments if not comment.get("ip_location"))

        note_dates = [note["publish_date"] for note in notes if note.get("publish_date")]
        comment_dates = [comment["create_date"] for comment in comments if comment.get("create_date")]

        artifacts = {
            "notes_csv": str(self.notes_file),
            "comments_csv": str(self.comments_file),
            "report_json": str(self.report_file),
        }

        report = {
            "generated_at": timestamp_to_datetime_text(parse_timestamp(self._now_timestamp())),
            "raw_root": str(self.raw_root),
            "processed_root": str(self.processed_root),
            "source_file_count": len(csv_files),
            "source_files": sorted(source_files, key=lambda item: (item["kind"], item["name"])),
            "schema_groups": schema_report,
            "warnings": warnings,
            "notes": {
                "raw_rows": raw_note_rows,
                "clean_rows": len(notes),
                "duplicate_rows_removed": note_duplicate_rows,
                "missing_title_count": note_title_missing,
                "missing_desc_count": note_desc_missing,
                "missing_location_count": note_location_missing,
                "unique_author_count": len({note["user_id"] or note["nickname"] for note in notes if note["user_id"] or note["nickname"]}),
            },
            "comments": {
                "raw_rows": raw_comment_rows,
                "clean_rows": len(comments),
                "duplicate_rows_removed": comment_duplicate_rows,
                "orphan_rows_removed": orphan_comments,
                "missing_location_count": comment_location_missing,
                "notes_covered": len({comment["note_id"] for comment in comments if comment["note_id"]}),
            },
            "date_range": {
                "notes": {
                    "start": min(note_dates) if note_dates else "",
                    "end": max(note_dates) if note_dates else "",
                },
                "comments": {
                    "start": min(comment_dates) if comment_dates else "",
                    "end": max(comment_dates) if comment_dates else "",
                },
            },
            "artifacts": artifacts,
        }

        self._write_cleaned_csv(self.notes_file, NOTE_EXPORT_FIELDS, notes)
        self._write_cleaned_csv(self.comments_file, COMMENT_EXPORT_FIELDS, comments)
        self.report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        return {"notes": notes, "comments": comments, "report": report}

    def _discover_schema_groups(self, files: Iterable[Path]) -> List[Tuple[str, Tuple[str, ...], List[Path]]]:
        grouped: Dict[Tuple[str, ...], List[Path]] = defaultdict(list)
        for path in files:
            header = tuple(self._read_header(path))
            grouped[header].append(path)

        result: List[Tuple[str, Tuple[str, ...], List[Path]]] = []
        for header, header_files in grouped.items():
            kind = self._classify_schema(header)
            if not kind:
                continue
            result.append((kind, header, sorted(header_files)))

        result.sort(key=lambda item: (item[0], item[2][0].name))
        return result

    def _classify_schema(self, header: Iterable[str]) -> str:
        columns = set(header)
        if {"note_id", "title", "desc"}.issubset(columns):
            return "notes"
        if {"comment_id", "note_id", "content"}.issubset(columns):
            return "comments"
        return ""

    def _read_header(self, file_path: Path) -> List[str]:
        with file_path.open("r", encoding=self._resolve_encoding(file_path), errors="replace", newline="") as file_obj:
            reader = csv.reader(file_obj)
            return [clean_text(cell) for cell in next(reader, [])]

    def _read_raw_rows(self, file_path: Path) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        with file_path.open("r", encoding=self._resolve_encoding(file_path), errors="replace", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            for row in reader:
                payload = {clean_text(key): value for key, value in (row or {}).items()}
                if self._looks_like_inline_header_row(payload):
                    continue
                payload["_source_file"] = file_path.name
                rows.append(payload)
        return rows

    def _resolve_encoding(self, file_path: Path) -> str:
        cache_key = str(file_path)
        if cache_key in self._encoding_cache:
            return self._encoding_cache[cache_key]

        raw = file_path.read_bytes()
        for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
            try:
                raw.decode(encoding)
                self._encoding_cache[cache_key] = encoding
                return encoding
            except UnicodeDecodeError:
                continue

        self._encoding_cache[cache_key] = "utf-8-sig"
        return "utf-8-sig"

    def _normalize_note_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        title = compact_text(row.get("title"))
        desc = compact_text(row.get("desc"))
        note_id = clean_text(row.get("note_id")) or self._build_note_fingerprint(row)
        image_list = parse_list_like(row.get("image_list"))
        tag_list = merge_unique_values(
            parse_list_like(row.get("tag_list")),
            extract_hashtags(title),
            extract_hashtags(desc),
        )
        publish_time = parse_timestamp(row.get("time"))
        last_update_time = max(parse_timestamp(row.get("last_update_time")), publish_time)
        last_modify_ts = max(parse_timestamp(row.get("last_modify_ts")), last_update_time)
        liked_count = safe_int(row.get("liked_count"))
        collected_count = safe_int(row.get("collected_count"))
        comment_count = safe_int(row.get("comment_count"))
        share_count = safe_int(row.get("share_count"))

        normalized = {
            "note_id": note_id,
            "type": "video" if clean_text(row.get("type")).lower() == "video" or clean_text(row.get("video_url")) else "normal",
            "title": title,
            "desc": desc,
            "video_url": clean_text(row.get("video_url")),
            "time": publish_time,
            "publish_date": timestamp_to_date_text(publish_time),
            "publish_datetime": timestamp_to_datetime_text(publish_time),
            "last_update_time": last_update_time,
            "user_id": clean_text(row.get("user_id")),
            "nickname": compact_text(row.get("nickname")),
            "avatar": clean_text(row.get("avatar")),
            "liked_count": liked_count,
            "collected_count": collected_count,
            "comment_count": comment_count,
            "share_count": share_count,
            "interaction_count": liked_count + collected_count + comment_count + share_count,
            "ip_location": compact_text(row.get("ip_location")),
            "image_count": len(image_list),
            "image_list": image_list,
            "tag_count": len(tag_list),
            "tag_list": tag_list,
            "last_modify_ts": last_modify_ts,
            "note_url": clean_text(row.get("note_url")),
            "source_keyword": clean_text(row.get("source_keyword")) or "CrawlerData",
            "xsec_token": clean_text(row.get("xsec_token")),
            "source_files": [clean_text(row.get("_source_file"))],
            "duplicate_count": 0,
            "field_completeness": self._count_non_empty(
                title,
                desc,
                row.get("video_url"),
                row.get("user_id"),
                row.get("nickname"),
                row.get("note_url"),
                row.get("avatar"),
                row.get("ip_location"),
            ),
        }
        return normalized

    def _normalize_comment_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        create_time = parse_timestamp(row.get("create_time"))
        pictures = parse_list_like(row.get("pictures"))
        content = compact_text(row.get("content"))
        normalized = {
            "comment_id": clean_text(row.get("comment_id")) or self._build_comment_fingerprint(row),
            "note_id": clean_text(row.get("note_id")),
            "create_time": create_time,
            "create_date": timestamp_to_date_text(create_time),
            "create_datetime": timestamp_to_datetime_text(create_time),
            "ip_location": compact_text(row.get("ip_location")),
            "content": content,
            "content_length": len(content),
            "user_id": clean_text(row.get("user_id")),
            "nickname": compact_text(row.get("nickname")),
            "avatar": clean_text(row.get("avatar")),
            "sub_comment_count": safe_int(row.get("sub_comment_count")),
            "picture_count": len(pictures),
            "pictures": pictures,
            "parent_comment_id": clean_text(row.get("parent_comment_id")) or "0",
            "last_modify_ts": max(parse_timestamp(row.get("last_modify_ts")), create_time),
            "like_count": safe_int(row.get("like_count")),
            "source_files": [clean_text(row.get("_source_file"))],
            "duplicate_count": 0,
            "field_completeness": self._count_non_empty(
                row.get("note_id"),
                content,
                row.get("user_id"),
                row.get("nickname"),
                row.get("avatar"),
                row.get("ip_location"),
            ),
        }
        return normalized

    def _dedupe_notes(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            buckets[row["note_id"]].append(row)

        merged_rows: List[Dict[str, Any]] = []
        duplicate_rows_removed = 0
        for note_id, items in buckets.items():
            merged_rows.append(self._merge_note_rows(note_id, items))
            duplicate_rows_removed += len(items) - 1

        merged_rows.sort(
            key=lambda row: (
                row.get("time", 0),
                row.get("interaction_count", 0),
                row.get("liked_count", 0),
                row.get("note_id", ""),
            ),
            reverse=True,
        )
        return merged_rows, duplicate_rows_removed

    def _dedupe_comments(
        self,
        rows: List[Dict[str, Any]],
        valid_note_ids: set[str],
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        orphan_rows = 0
        for row in rows:
            if row["note_id"] not in valid_note_ids:
                orphan_rows += 1
                continue
            buckets[row["comment_id"]].append(row)

        merged_rows: List[Dict[str, Any]] = []
        duplicate_rows_removed = 0
        for comment_id, items in buckets.items():
            merged_rows.append(self._merge_comment_rows(comment_id, items))
            duplicate_rows_removed += len(items) - 1

        merged_rows.sort(
            key=lambda row: (
                row.get("create_time", 0),
                row.get("like_count", 0),
                row.get("comment_id", ""),
            ),
            reverse=True,
        )
        return merged_rows, duplicate_rows_removed, orphan_rows

    def _merge_note_rows(self, note_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        ranked_rows = sorted(rows, key=self._note_rank, reverse=True)
        merged = deepcopy(ranked_rows[0])

        merged["title"] = self._best_text(rows, "title")
        merged["desc"] = self._best_text(rows, "desc")
        merged["video_url"] = self._best_text(rows, "video_url")
        merged["user_id"] = self._best_text(rows, "user_id")
        merged["nickname"] = self._best_text(rows, "nickname")
        merged["avatar"] = self._best_text(rows, "avatar")
        merged["ip_location"] = self._best_text(rows, "ip_location")
        merged["note_url"] = self._best_text(rows, "note_url")
        merged["xsec_token"] = self._best_text(rows, "xsec_token")
        merged["source_keyword"] = self._best_text(rows, "source_keyword") or "CrawlerData"
        merged["type"] = "video" if any(item.get("type") == "video" for item in rows) else "normal"

        merged["time"] = self._pick_timestamp(rows, "time", mode="min")
        merged["publish_date"] = timestamp_to_date_text(merged["time"])
        merged["publish_datetime"] = timestamp_to_datetime_text(merged["time"])
        merged["last_update_time"] = self._pick_timestamp(rows, "last_update_time", mode="max")
        merged["last_modify_ts"] = self._pick_timestamp(rows, "last_modify_ts", mode="max")

        for field in ("liked_count", "collected_count", "comment_count", "share_count"):
            merged[field] = max(item.get(field, 0) for item in rows)

        merged["interaction_count"] = (
            merged["liked_count"]
            + merged["collected_count"]
            + merged["comment_count"]
            + merged["share_count"]
        )
        merged["image_list"] = merge_unique_values(*(item.get("image_list", []) for item in rows))
        merged["image_count"] = len(merged["image_list"])
        merged["tag_list"] = merge_unique_values(*(item.get("tag_list", []) for item in rows))
        merged["tag_count"] = len(merged["tag_list"])
        merged["source_files"] = dedupe_keep_order(
            source_file
            for item in rows
            for source_file in item.get("source_files", [])
        )
        merged["duplicate_count"] = len(rows) - 1
        merged["field_completeness"] = max(item.get("field_completeness", 0) for item in rows)
        merged["note_id"] = note_id
        return merged

    def _merge_comment_rows(self, comment_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        ranked_rows = sorted(rows, key=self._comment_rank, reverse=True)
        merged = deepcopy(ranked_rows[0])

        merged["note_id"] = self._best_text(rows, "note_id")
        merged["ip_location"] = self._best_text(rows, "ip_location")
        merged["content"] = self._best_text(rows, "content")
        merged["content_length"] = len(merged["content"])
        merged["user_id"] = self._best_text(rows, "user_id")
        merged["nickname"] = self._best_text(rows, "nickname")
        merged["avatar"] = self._best_text(rows, "avatar")
        merged["parent_comment_id"] = self._best_text(rows, "parent_comment_id") or "0"

        merged["create_time"] = self._pick_timestamp(rows, "create_time", mode="min")
        merged["create_date"] = timestamp_to_date_text(merged["create_time"])
        merged["create_datetime"] = timestamp_to_datetime_text(merged["create_time"])
        merged["last_modify_ts"] = self._pick_timestamp(rows, "last_modify_ts", mode="max")
        merged["like_count"] = max(item.get("like_count", 0) for item in rows)
        merged["sub_comment_count"] = max(item.get("sub_comment_count", 0) for item in rows)
        merged["pictures"] = merge_unique_values(*(item.get("pictures", []) for item in rows))
        merged["picture_count"] = len(merged["pictures"])
        merged["source_files"] = dedupe_keep_order(
            source_file
            for item in rows
            for source_file in item.get("source_files", [])
        )
        merged["duplicate_count"] = len(rows) - 1
        merged["field_completeness"] = max(item.get("field_completeness", 0) for item in rows)
        merged["comment_id"] = comment_id
        return merged

    def _best_text(self, rows: List[Dict[str, Any]], field: str) -> str:
        values = [compact_text(item.get(field)) for item in rows]
        values = [value for value in values if value]
        if not values:
            return ""
        return max(values, key=lambda value: (len(value), value))

    def _pick_timestamp(self, rows: List[Dict[str, Any]], field: str, mode: str) -> int:
        values = [int(item.get(field, 0)) for item in rows if item.get(field)]
        if not values:
            return 0
        return min(values) if mode == "min" else max(values)

    def _note_rank(self, row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        return (
            row.get("field_completeness", 0),
            row.get("last_modify_ts", 0),
            row.get("interaction_count", 0),
            row.get("time", 0),
        )

    def _comment_rank(self, row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        return (
            row.get("field_completeness", 0),
            row.get("last_modify_ts", 0),
            row.get("like_count", 0),
            row.get("create_time", 0),
        )

    def _write_cleaned_csv(self, output_path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
        with output_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(self._serialize_row(row, fieldnames))

    def _serialize_row(self, row: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for field in fieldnames:
            value = row.get(field, "")
            if isinstance(value, list):
                if field in {"tag_list", "source_files"}:
                    payload[field] = ",".join(value)
                else:
                    payload[field] = json.dumps(value, ensure_ascii=False)
            else:
                payload[field] = value
        return payload

    def _count_non_empty(self, *values: Any) -> int:
        return sum(1 for value in values if clean_text(value))

    def _build_note_fingerprint(self, row: Dict[str, Any]) -> str:
        seed = "|".join(
            [
                compact_text(row.get("title")),
                compact_text(row.get("desc")),
                compact_text(row.get("note_url")),
                compact_text(row.get("user_id")),
            ]
        )
        return hashlib.md5(seed.encode("utf-8")).hexdigest()[:24]

    def _build_comment_fingerprint(self, row: Dict[str, Any]) -> str:
        seed = "|".join(
            [
                compact_text(row.get("note_id")),
                compact_text(row.get("content")),
                compact_text(row.get("user_id")),
                compact_text(row.get("parent_comment_id")),
            ]
        )
        return hashlib.md5(seed.encode("utf-8")).hexdigest()[:24]

    def _is_valid_note(self, row: Dict[str, Any]) -> bool:
        if self._looks_like_inline_header_row(row):
            return False
        note_id = clean_text(row.get("note_id")).lower()
        if not note_id or note_id in {"note_id", "comment_id"}:
            return False
        return bool(row.get("title") or row.get("desc") or row.get("note_url"))

    def _is_valid_comment(self, row: Dict[str, Any]) -> bool:
        if self._looks_like_inline_header_row(row):
            return False
        comment_id = clean_text(row.get("comment_id")).lower()
        note_id = clean_text(row.get("note_id")).lower()
        if not comment_id or comment_id in {"comment_id", "note_id"}:
            return False
        if not note_id or note_id in {"note_id", "comment_id"}:
            return False
        return bool(row.get("content"))

    def _looks_like_inline_header_row(self, row: Dict[str, Any]) -> bool:
        matches = 0
        for key, value in row.items():
            key_text = clean_text(key).lower()
            if not key_text or key_text.startswith("_"):
                continue
            value_text = clean_text(value).lower()
            if value_text and value_text == key_text:
                matches += 1
        return matches >= 4

    def _schema_id(self, header: Iterable[str]) -> str:
        joined = "|".join(header)
        return hashlib.md5(joined.encode("utf-8")).hexdigest()[:12]

    def _now_timestamp(self) -> int:
        from time import time

        return int(time())
