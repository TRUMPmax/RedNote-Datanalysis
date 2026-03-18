# -*- coding: utf-8 -*-
"""
Data loader for CSV / JSON / SQLite sources.
"""
import csv
import glob
import json
import os
import re
import sqlite3
from typing import Dict, List


class DataLoader:
    """Unified data access for crawler output and local public datasets."""

    def __init__(self, data_root: str = None):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        crawler_data_root = os.path.join(project_root, "MediaCrawler-main", "data", "xhs")
        local_data_root = os.path.join(project_root, "analysis_system", "data", "xhs")
        os.makedirs(local_data_root, exist_ok=True)

        local_has_data = bool(glob.glob(os.path.join(local_data_root, "*.csv"))) or bool(
            glob.glob(os.path.join(local_data_root, "json", "*.json"))
        )
        crawler_has_data = bool(glob.glob(os.path.join(crawler_data_root, "*.csv"))) or bool(
            glob.glob(os.path.join(crawler_data_root, "json", "*.json"))
        )

        if data_root is None:
            if local_has_data or not os.path.exists(crawler_data_root):
                data_root = local_data_root
            elif crawler_has_data:
                data_root = crawler_data_root
            else:
                data_root = local_data_root

        self.data_root = data_root
        self.sqlite_db = os.path.join(project_root, "MediaCrawler-main", "schema", "sqlite_tables.db")
        self.config_file = os.path.join(project_root, "MediaCrawler-main", "config", "base_config.py")
        self.local_runtime_config = os.path.join(project_root, "analysis_system", "data", "runtime_config.json")

    # CSV
    def load_notes_from_csv(self, keyword: str = None) -> List[Dict]:
        pattern = os.path.join(self.data_root, "*_contents_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            pattern = os.path.join(self.data_root, "*.csv")
            files = [f for f in sorted(glob.glob(pattern)) if "contents" in f or "notes" in f]
        return self._load_csv_files(files, keyword)

    def load_comments_from_csv(self, keyword: str = None) -> List[Dict]:
        pattern = os.path.join(self.data_root, "*_comments_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            pattern = os.path.join(self.data_root, "*.csv")
            files = [f for f in sorted(glob.glob(pattern)) if "comments" in f]
        rows = self._load_csv_files(files)
        if not keyword:
            return rows
        note_ids = {item.get("note_id") for item in self.load_notes_from_csv(keyword) if item.get("note_id")}
        if not note_ids:
            return []
        return [row for row in rows if row.get("note_id") in note_ids]

    def _load_csv_files(self, files: List[str], keyword: str = None) -> List[Dict]:
        rows = []
        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8-sig", errors="replace") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        if keyword and row.get("source_keyword", "") != keyword:
                            continue
                        rows.append(dict(row))
            except Exception as exc:
                print(f"[DataLoader] failed to read {fp}: {exc}")
        return rows

    # JSON
    def load_notes_from_json(self, keyword: str = None) -> List[Dict]:
        json_dir = os.path.join(self.data_root, "json")
        pattern = os.path.join(json_dir, "*_contents_*.json")
        files = sorted(glob.glob(pattern))
        return self._load_json_files(files, keyword)

    def load_comments_from_json(self, keyword: str = None) -> List[Dict]:
        json_dir = os.path.join(self.data_root, "json")
        pattern = os.path.join(json_dir, "*_comments_*.json")
        files = sorted(glob.glob(pattern))
        rows = self._load_json_files(files)
        if not keyword:
            return rows
        note_ids = {item.get("note_id") for item in self.load_notes_from_json(keyword) if item.get("note_id")}
        if not note_ids:
            return []
        return [row for row in rows if row.get("note_id") in note_ids]

    def _load_json_files(self, files: List[str], keyword: str = None) -> List[Dict]:
        rows = []
        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8") as file_obj:
                    data = json.load(file_obj)
                if isinstance(data, list):
                    for item in data:
                        if keyword and item.get("source_keyword", "") != keyword:
                            continue
                        rows.append(item)
            except Exception as exc:
                print(f"[DataLoader] failed to read {fp}: {exc}")
        return rows

    # SQLite
    def load_notes_from_sqlite(self, keyword: str = None, limit: int = 5000) -> List[Dict]:
        if not os.path.exists(self.sqlite_db):
            return []
        try:
            conn = sqlite3.connect(self.sqlite_db)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if keyword:
                cur.execute("SELECT * FROM xhs_note WHERE source_keyword=? LIMIT ?", (keyword, limit))
            else:
                cur.execute("SELECT * FROM xhs_note LIMIT ?", (limit,))
            rows = [dict(row) for row in cur.fetchall()]
            conn.close()
            return rows
        except Exception as exc:
            print(f"[DataLoader] failed to read notes from SQLite: {exc}")
            return []

    def load_comments_from_sqlite(self, note_id: str = None, limit: int = 10000, keyword: str = None) -> List[Dict]:
        if not os.path.exists(self.sqlite_db):
            return []
        try:
            conn = sqlite3.connect(self.sqlite_db)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if keyword:
                cur.execute(
                    """
                    SELECT c.*
                    FROM xhs_note_comment c
                    INNER JOIN xhs_note n ON n.note_id = c.note_id
                    WHERE n.source_keyword = ?
                    LIMIT ?
                    """,
                    (keyword, limit),
                )
            elif note_id:
                cur.execute("SELECT * FROM xhs_note_comment WHERE note_id=? LIMIT ?", (note_id, limit))
            else:
                cur.execute("SELECT * FROM xhs_note_comment LIMIT ?", (limit,))
            rows = [dict(row) for row in cur.fetchall()]
            conn.close()
            return rows
        except Exception as exc:
            print(f"[DataLoader] failed to read comments from SQLite: {exc}")
            return []

    def load_creators_from_sqlite(self) -> List[Dict]:
        if not os.path.exists(self.sqlite_db):
            return []
        try:
            conn = sqlite3.connect(self.sqlite_db)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM xhs_creator")
            rows = [dict(row) for row in cur.fetchall()]
            conn.close()
            return rows
        except Exception as exc:
            print(f"[DataLoader] failed to read creators from SQLite: {exc}")
            return []

    # Auto source selection
    def get_preferred_source(self) -> str:
        if os.path.exists(self.local_runtime_config):
            try:
                with open(self.local_runtime_config, "r", encoding="utf-8") as file_obj:
                    payload = json.load(file_obj)
                value = str(payload.get("SAVE_DATA_OPTION", "")).lower()
                if value == "db":
                    return "sqlite"
                if value in {"sqlite", "json", "csv"}:
                    return value
            except Exception:
                pass

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r", encoding="utf-8") as file_obj:
                    content = file_obj.read()
                match = re.search(r'^SAVE_DATA_OPTION\s*=\s*["\']?([A-Za-z0-9_]+)["\']?', content, re.MULTILINE)
                value = match.group(1).lower() if match else "csv"
                if value == "db":
                    return "sqlite"
                if value in {"sqlite", "json", "csv"}:
                    return value
            except Exception:
                pass

        return "csv"

    def auto_load_notes(self, keyword: str = None) -> List[Dict]:
        preferred = self.get_preferred_source()
        source_order = {
            "sqlite": ["sqlite", "json", "csv"],
            "json": ["json", "csv", "sqlite"],
            "csv": ["csv", "json", "sqlite"],
        }.get(preferred, ["csv", "json", "sqlite"])

        for source in source_order:
            if source == "sqlite":
                rows = self.load_notes_from_sqlite(keyword)
            elif source == "json":
                rows = self.load_notes_from_json(keyword)
            else:
                rows = self.load_notes_from_csv(keyword)
            if rows:
                print(f"[DataLoader] loaded {len(rows)} notes from {source.upper()}")
                return rows

        print("[DataLoader] no note data found")
        return []

    def auto_load_comments(self, keyword: str = None) -> List[Dict]:
        preferred = self.get_preferred_source()
        source_order = {
            "sqlite": ["sqlite", "json", "csv"],
            "json": ["json", "csv", "sqlite"],
            "csv": ["csv", "json", "sqlite"],
        }.get(preferred, ["csv", "json", "sqlite"])

        for source in source_order:
            if source == "sqlite":
                rows = self.load_comments_from_sqlite(keyword=keyword)
            elif source == "json":
                rows = self.load_comments_from_json(keyword)
            else:
                rows = self.load_comments_from_csv(keyword)
            if rows:
                print(f"[DataLoader] loaded {len(rows)} comments from {source.upper()}")
                return rows

        print("[DataLoader] no comment data found")
        return []

    def list_available_keywords(self) -> List[str]:
        keywords = set()

        for fp in glob.glob(os.path.join(self.data_root, "*.csv")):
            notes = self._load_csv_files([fp])
            for note in notes[:20]:
                keyword = note.get("source_keyword", "")
                if keyword:
                    keywords.add(keyword)

        if os.path.exists(self.sqlite_db):
            try:
                conn = sqlite3.connect(self.sqlite_db)
                cur = conn.cursor()
                cur.execute("SELECT DISTINCT source_keyword FROM xhs_note WHERE source_keyword IS NOT NULL")
                for row in cur.fetchall():
                    if row[0]:
                        keywords.add(row[0])
                conn.close()
            except Exception:
                pass

        return sorted(keywords)

    def get_data_summary(self) -> Dict:
        summary = {
            "data_root": self.data_root,
            "sqlite_db": self.sqlite_db,
            "sqlite_exists": os.path.exists(self.sqlite_db),
            "csv_files": len(glob.glob(os.path.join(self.data_root, "*.csv"))),
            "json_files": len(glob.glob(os.path.join(self.data_root, "json", "*.json"))),
            "keywords": self.list_available_keywords(),
        }

        if summary["sqlite_exists"]:
            try:
                conn = sqlite3.connect(self.sqlite_db)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM xhs_note")
                summary["note_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM xhs_note_comment")
                summary["comment_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM xhs_creator")
                summary["creator_count"] = cur.fetchone()[0]
                conn.close()
            except Exception:
                summary["note_count"] = 0
                summary["comment_count"] = 0
                summary["creator_count"] = 0
        else:
            summary["note_count"] = len(self.load_notes_from_csv())
            summary["comment_count"] = len(self.load_comments_from_csv())
            summary["creator_count"] = 0

        return summary
