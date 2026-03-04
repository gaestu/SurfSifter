"""Appendix Web Storage Module.

Displays a full list of web storage (localStorage and sessionStorage) key-value pairs
grouped by origin. Supports multi-select filtering by tags on stored sites (Sites Overview).
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...paths import get_module_template_dir


class AppendixWebStorageModule(BaseAppendixModule):
    """Appendix module for listing web storage entries grouped by origin."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_web_storage",
            name="Web Storage List",
            description="Lists web storage key-value pairs grouped by origin with site tag filters",
            category="Appendix",
            icon="🗄️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="tag_filter",
                label="Site Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by tags on stored sites (Sites Overview)",
                required=False,
            ),
            FilterField(
                key="storage_type",
                label="Storage Type",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=[
                    ("all", "All"),
                    ("local", "Local Storage only"),
                    ("session", "Session Storage only"),
                ],
                help_text="Filter by storage type",
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
                    WHERE ta.artifact_type = 'stored_site'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
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
        storage_type = config.get("storage_type", "all")

        # Fetch storage entries
        entries = self._get_storage_entries(db_conn, evidence_id, tag_filter, storage_type)

        # Group by origin
        grouped = self._group_by_origin(entries)

        # Count totals
        total_entries = len(entries)
        total_origins = len(grouped)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            grouped=grouped,
            total_entries=total_entries,
            total_origins=total_origins,
            t=translations,
            locale=locale,
        )

    def _get_storage_entries(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        tag_filter: List[str],
        storage_type: str,
    ) -> List[Dict[str, Any]]:
        """Fetch storage entries from local_storage and/or session_storage.

        Args:
            db_conn: SQLite connection
            evidence_id: Evidence ID
            tag_filter: List of tag names to filter stored_sites by (empty = all)
            storage_type: 'all', 'local', or 'session'

        Returns:
            List of entry dicts with key, value, type, origin
        """
        db_conn.row_factory = sqlite3.Row

        # Get origins from tagged stored_sites (or all if no filter)
        tagged_origins: Optional[set[str]] = None
        if tag_filter:
            tagged_origins = set()
            placeholders = ", ".join(["?"] * len(tag_filter))
            query = f"""
                SELECT DISTINCT ss.origin
                FROM stored_sites ss
                WHERE ss.evidence_id = ?
                  AND EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = ss.id
                      AND ta.artifact_type = 'stored_site'
                      AND ta.evidence_id = ss.evidence_id
                      AND t.name IN ({placeholders})
                  )
            """
            try:
                cursor = db_conn.execute(query, [evidence_id] + tag_filter)
                for row in cursor.fetchall():
                    tagged_origins.add(row["origin"])
            except Exception:
                pass

            # If no tagged sites found, return empty
            if not tagged_origins:
                return []

        entries: List[Dict[str, Any]] = []

        tables_to_query: List[tuple[str, str]] = []
        if storage_type in ("all", "local"):
            tables_to_query.append(("local_storage", "Local"))
        if storage_type in ("all", "session"):
            tables_to_query.append(("session_storage", "Session"))

        for table_name, type_label in tables_to_query:
            params: List[Any] = [evidence_id]
            conditions = ["s.evidence_id = ?"]

            # Filter by tagged origins if applicable
            if tagged_origins is not None:
                placeholders = ", ".join(["?"] * len(tagged_origins))
                conditions.append(f"s.origin IN ({placeholders})")
                params.extend(tagged_origins)

            where_clause = " AND ".join(conditions)

            query = f"""
                SELECT s.origin, s.key, s.value
                FROM {table_name} s
                WHERE {where_clause}
                ORDER BY s.origin, s.key
            """

            try:
                cursor = db_conn.execute(query, params)
                for row in cursor.fetchall():
                    entries.append({
                        "origin": row["origin"] or "",
                        "key": row["key"] or "",
                        "value": row["value"] or "",
                        "type": type_label,
                    })
            except Exception:
                pass

        # Sort by origin, then key
        entries.sort(key=lambda x: (x["origin"].lower(), x["key"].lower()))

        return entries

    def _group_by_origin(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Group entries by origin.

        Args:
            entries: List of entry dicts

        Returns:
            List of dicts with 'origin', 'entries', and 'count' keys
        """
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for entry in entries:
            origin = entry["origin"] or "(no origin)"
            groups[origin].append(entry)

        result = []
        for origin in sorted(groups.keys(), key=str.lower):
            result.append({
                "origin": origin,
                "entries": groups[origin],
                "count": len(groups[origin]),
            })

        return result
