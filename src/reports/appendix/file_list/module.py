"""Appendix File List Module.

Displays a list of files with tag and match filters.
Uses a compact two-row layout to fit more content per page.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixFileListModule(BaseAppendixModule):
    """Appendix module for listing files with tag and match filters."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_file_list",
            name="File List",
            description="Lists files with tag and reference list match filters",
            category="Appendix",
            icon="📁",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Matches",
                filter_type=FilterType.MULTI_SELECT,
                help_text="Filter by one or more reference list matches",
                required=False,
            ),
            FilterField(
                key="filter_mode",
                label="Filter Mode",
                filter_type=FilterType.DROPDOWN,
                default="or",
                options=[
                    ("or", "OR - Any tag or any match"),
                    ("and", "AND - Must have tag AND match"),
                ],
                help_text="OR: Files with any selected tag or match. AND: Files must have a selected tag AND a selected match.",
                required=False,
            ),
            FilterField(
                key="include_deleted",
                label="Include Deleted Files",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Include files marked as deleted",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'file_list'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "match_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT reference_list_name
                    FROM file_list_matches
                    ORDER BY reference_list_name
                    """
                )
                for (list_name,) in cursor.fetchall():
                    options.append((list_name, list_name))
            except Exception:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})

        tag_filter = config.get("tag_filter") or []
        match_filter = config.get("match_filter") or []
        filter_mode = config.get("filter_mode", "or")
        include_deleted = bool(config.get("include_deleted", True))
        date_format = config.get("_date_format", "eu")

        query, params = self._build_query(
            evidence_id, tag_filter, match_filter, filter_mode, include_deleted
        )

        files: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                file_id = row["id"]
                if file_id in seen_ids:
                    continue
                seen_ids.add(file_id)
                files.append(
                    {
                        "id": file_id,
                        "file_path": row["file_path"],
                        "file_name": row["file_name"],
                        "extension": row["extension"] or "",
                        "size_bytes": row["size_bytes"],
                        "created_ts": self._format_ts(row["created_ts"], date_format),
                        "modified_ts": self._format_ts(row["modified_ts"], date_format),
                        "accessed_ts": self._format_ts(row["accessed_ts"], date_format),
                        "deleted": bool(row["deleted"]),
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading files: {exc}</div>'

        # Sort by file path
        files.sort(key=lambda x: x["file_path"].lower())

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        env.filters["format_size"] = self._format_size
        template = env.get_template("template.html")

        return template.render(
            files=files,
            total_count=len(files),
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        match_filter: List[str],
        filter_mode: str = "or",
        include_deleted: bool = True,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["f.evidence_id = ?"]

        if not include_deleted:
            conditions.append("(f.deleted = 0 OR f.deleted IS NULL)")

        tag_condition = None
        match_condition = None

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = f.id
                      AND ta.artifact_type = 'file_list'
                      AND ta.evidence_id = f.evidence_id
                      AND t.name IN ({placeholders})
                )
                """

        if match_filter:
            placeholders = ", ".join(["?"] * len(match_filter))
            match_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM file_list_matches m
                    WHERE m.file_list_id = f.id
                      AND m.evidence_id = f.evidence_id
                      AND m.reference_list_name IN ({placeholders})
                )
                """

        # Apply filter logic based on mode
        if filter_mode == "and" and tag_filter and match_filter:
            # AND mode: must have a selected tag AND a selected match
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(f"({match_condition})")
            params.extend(match_filter)
        elif tag_condition or match_condition:
            # OR mode (default): any selected tag OR any selected match
            or_parts = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if match_condition:
                or_parts.append(match_condition)
                params.extend(match_filter)
            if or_parts:
                conditions.append(f"({' OR '.join(or_parts)})")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT
                f.id,
                f.file_path,
                f.file_name,
                f.extension,
                f.size_bytes,
                f.created_ts,
                f.modified_ts,
                f.accessed_ts,
                f.deleted
            FROM file_list f
            WHERE {where_clause}
            ORDER BY f.file_path
        """

        return query, params

    def _format_size(self, size_bytes: Optional[int]) -> str:
        """Format file size in human-readable form."""
        if size_bytes is None:
            return "-"
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def _format_ts(self, ts: Optional[str], date_format: str) -> str:
        """Format a timestamp string according to selected date format."""
        if not ts:
            return "-"
        return format_datetime(ts, date_format, include_time=True, include_seconds=False)
