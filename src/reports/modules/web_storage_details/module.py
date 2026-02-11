"""Web Storage Details Report Module.

Displays web storage (localStorage and sessionStorage) key-value pairs
grouped by site/origin with filtering by tags.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class WebStorageDetailsModule(BaseReportModule):
    """Module for displaying web storage details by site in reports."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="web_storage_details",
            name="Web Storage Details",
            description="Displays web storage (localStorage/sessionStorage) key-value pairs grouped by site",
            category="Browser",
            icon="🗄️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tag selection and display options."""
        return [
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display module title",
                required=False,
            ),
            FilterField(
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display explanatory description for non-technical readers",
                required=False,
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
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
            FilterField(
                key="max_entries_per_site",
                label="Max Entries per Site",
                filter_type=FilterType.DROPDOWN,
                default="50",
                options=[
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    ("all", "All"),
                ],
                help_text="Maximum number of storage entries to show per site",
                required=False,
            ),
            FilterField(
                key="truncate_values",
                label="Truncate Long Values",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Truncate values longer than 100 characters",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the content",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for tag filter.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
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
        """Render the web storage details as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})

        # Extract config values
        show_title = bool(config.get("show_title", True))
        show_description = bool(config.get("show_description", True))
        tag_filter = config.get("tag_filter") or []
        storage_type = config.get("storage_type", "all")
        max_entries = config.get("max_entries_per_site", "50")
        truncate_values = bool(config.get("truncate_values", True))
        show_filter_info = bool(config.get("show_filter_info", False))

        # Convert max_entries to int (or None for "all")
        max_entries_int: Optional[int] = None
        if max_entries != "all":
            try:
                max_entries_int = int(max_entries)
            except ValueError:
                max_entries_int = 50

        # Get tagged sites
        sites_data = self._get_sites_with_storage(
            db_conn,
            evidence_id,
            tag_filter,
            storage_type,
            max_entries_int,
            truncate_values,
        )

        # Build filter description
        filter_parts = []
        if tag_filter:
            filter_parts.append(f"Tags: {', '.join(tag_filter)}")
        if storage_type != "all":
            type_label = "Local Storage" if storage_type == "local" else "Session Storage"
            filter_parts.append(f"Type: {type_label}")
        filter_description = "; ".join(filter_parts) if filter_parts else translations.get("filter_all_urls", "All")

        # Count totals
        total_sites = len(sites_data)
        total_entries = sum(len(site["entries"]) for site in sites_data)

        # Get stored site label from translations
        stored_site_label = translations.get("web_storage_stored_site", "Stored Site")

        # Load template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            sites=sites_data,
            total_sites=total_sites,
            total_entries=total_entries,
            show_title=show_title,
            show_description=show_description,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            stored_site_label=stored_site_label,
            t=translations,
            locale=locale,
        )

    def _get_sites_with_storage(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        tag_filter: List[str],
        storage_type: str,
        max_entries: Optional[int],
        truncate_values: bool,
    ) -> List[Dict[str, Any]]:
        """Get stored sites with their storage entries.

        Args:
            db_conn: SQLite connection
            evidence_id: Evidence ID
            tag_filter: List of tag names to filter by
            storage_type: 'all', 'local', or 'session'
            max_entries: Maximum entries per site (None for all)
            truncate_values: Whether to truncate long values

        Returns:
            List of site dicts with their storage entries
        """
        db_conn.row_factory = sqlite3.Row

        # Build query for stored_sites with tag filtering
        params: List[Any] = [evidence_id]
        conditions = ["ss.evidence_id = ?"]

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = ss.id
                      AND ta.artifact_type = 'stored_site'
                      AND ta.evidence_id = ss.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        query = f"""
            SELECT ss.id, ss.origin, ss.local_storage_count, ss.session_storage_count,
                   ss.indexeddb_count, ss.total_keys, ss.browsers, ss.tags
            FROM stored_sites ss
            WHERE {' AND '.join(conditions)}
            ORDER BY ss.total_keys DESC
        """

        try:
            cursor = db_conn.execute(query, params)
            sites_rows = cursor.fetchall()
        except Exception:
            return []

        sites_data: List[Dict[str, Any]] = []

        for site_row in sites_rows:
            origin = site_row["origin"]
            site_data: Dict[str, Any] = {
                "origin": origin,
                "local_storage_count": site_row["local_storage_count"] or 0,
                "session_storage_count": site_row["session_storage_count"] or 0,
                "tags": site_row["tags"] or "",
                "entries": [],
            }

            # Fetch storage entries for this origin
            entries = []

            # Fetch local storage entries
            if storage_type in ("all", "local"):
                local_entries = self._get_storage_entries(
                    db_conn,
                    evidence_id,
                    origin,
                    "local_storage",
                    "Local",
                    max_entries,
                    truncate_values,
                )
                entries.extend(local_entries)

            # Fetch session storage entries
            if storage_type in ("all", "session"):
                remaining = None
                if max_entries is not None:
                    remaining = max_entries - len(entries)
                    if remaining <= 0:
                        remaining = None  # Already at limit from local storage

                if remaining is None or remaining > 0:
                    session_entries = self._get_storage_entries(
                        db_conn,
                        evidence_id,
                        origin,
                        "session_storage",
                        "Session",
                        remaining,
                        truncate_values,
                    )
                    entries.extend(session_entries)

            # Apply max_entries limit if needed
            if max_entries is not None and len(entries) > max_entries:
                entries = entries[:max_entries]

            site_data["entries"] = entries
            site_data["total_shown"] = len(entries)

            # Only include sites that have entries matching the filter
            if entries:
                sites_data.append(site_data)

        return sites_data

    def _get_storage_entries(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        origin: str,
        table_name: str,
        storage_type_label: str,
        limit: Optional[int],
        truncate_values: bool,
    ) -> List[Dict[str, Any]]:
        """Get storage entries from a specific storage table.

        Args:
            db_conn: SQLite connection
            evidence_id: Evidence ID
            origin: Site origin
            table_name: 'local_storage' or 'session_storage'
            storage_type_label: 'Local' or 'Session'
            limit: Maximum number of entries
            truncate_values: Whether to truncate long values

        Returns:
            List of entry dicts
        """
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        query = f"""
            SELECT key, value, value_type
            FROM {table_name}
            WHERE evidence_id = ? AND origin = ?
            ORDER BY key
            {limit_clause}
        """

        try:
            cursor = db_conn.execute(query, (evidence_id, origin))
            rows = cursor.fetchall()
        except Exception:
            return []

        entries = []
        for row in rows:
            value = row["value"] or ""
            if truncate_values and len(value) > 100:
                value = value[:100] + "..."

            entries.append({
                "key": row["key"] or "",
                "value": value,
                "type": storage_type_label,
            })

        return entries
