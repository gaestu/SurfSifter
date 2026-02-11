"""Tagged File List Report Module.

Displays a table of files from the file_list with filtering by tags and matches.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader

from ...dates import format_datetime
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class TaggedFileListModule(BaseReportModule):
    """Module for displaying tagged files as a table in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    ANY_MATCH = "any_match"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="tagged_file_list",
            name="Tagged File List",
            description="Displays files from file list with tag and match filters",
            category="Files",
            icon="📁",
        )

    # Special limit value
    UNLIMITED = "unlimited"

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags, matches, deleted, and sort."""
        return [
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a title above the file list",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default=self.UNLIMITED,
                options=[
                    ("5", "5"),
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of files to show",
                required=False,
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_TAG, "Any Tag"),
                ],
                help_text="Filter by tag (specific tags loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Matches",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_MATCH, "Any Match"),
                ],
                help_text="Filter by reference list match (lists loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="include_deleted",
                label="Include Deleted Files",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Include files marked as deleted",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="modified_desc",
                options=[
                    ("modified_desc", "Modified (Newest First)"),
                    ("modified_asc", "Modified (Oldest First)"),
                    ("name_asc", "File Name (A-Z)"),
                    ("name_desc", "File Name (Z-A)"),
                    ("path_asc", "Path (A-Z)"),
                    ("path_desc", "Path (Z-A)"),
                ],
                help_text="Sort order for the file list",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the file list",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag and match filters.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "tag_filter":
            # Get all tags used in file_list
            options = [
                (self.ALL, "All"),
                (self.ANY_TAG, "Any Tag"),
            ]
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

        elif key == "match_filter":
            # Get all reference lists with matches
            options = [
                (self.ALL, "All"),
                (self.ANY_MATCH, "Any Match"),
            ]
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
        """Render the tagged file list as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        from jinja2 import Environment, FileSystemLoader

        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Extract config values
        show_title = config.get("show_title", True)
        limit = config.get("limit", self.UNLIMITED)
        tag_filter = config.get("tag_filter", self.ALL)
        match_filter = config.get("match_filter", self.ALL)
        include_deleted = config.get("include_deleted", False)
        sort_by = config.get("sort_by", "modified_desc")
        show_filter_info = config.get("show_filter_info", False)

        # Build query
        query, params = self._build_query(
            evidence_id, tag_filter, match_filter, include_deleted, sort_by
        )

        # Execute query
        files = []
        total_count = 0
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            # Apply limit
            if limit != self.UNLIMITED:
                limit_int = int(limit)
                all_rows = all_rows[:limit_int]

            for row in all_rows:
                files.append(
                    {
                        "path": row["file_path"],
                        "name": row["file_name"],
                        "modified": self._format_datetime(row["modified_ts"], date_format),
                        "deleted": bool(row["deleted"]) if row["deleted"] else False,
                    }
                )
        except Exception as e:
            # Return error HTML
            return f'<div class="module-error">Error loading file list: {e}</div>'

        # Determine if list is truncated
        shown_count = len(files)
        is_truncated = shown_count < total_count

        # Build filter description
        filter_desc = self._build_filter_description(
            tag_filter, match_filter, include_deleted, sort_by, translations
        )

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            files=files,
            filter_description=filter_desc,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            show_filter_info=show_filter_info,
            show_title=show_title,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: str,
        match_filter: str,
        include_deleted: bool,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query based on filters.

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = ["f.evidence_id = ?"]

        # Handle tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = f.id AND ta.artifact_type = 'file_list'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = f.id AND ta.artifact_type = 'file_list'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        # Handle match filter
        if match_filter == self.ANY_MATCH:
            joins.append(
                """
                INNER JOIN file_list_matches m ON m.file_list_id = f.id
                """
            )
        elif match_filter != self.ALL:
            joins.append(
                """
                INNER JOIN file_list_matches m ON m.file_list_id = f.id
                """
            )
            conditions.append("m.reference_list_name = ?")
            params.append(match_filter)

        # Handle deleted filter
        if not include_deleted:
            conditions.append("(f.deleted = 0 OR f.deleted IS NULL)")

        # Build ORDER BY
        order_map = {
            "modified_desc": "f.modified_ts DESC NULLS LAST",
            "modified_asc": "f.modified_ts ASC NULLS FIRST",
            "name_asc": "f.file_name ASC",
            "name_desc": "f.file_name DESC",
            "path_asc": "f.file_path ASC",
            "path_desc": "f.file_path DESC",
        }
        order_by = order_map.get(sort_by, "f.modified_ts DESC NULLS LAST")

        # Combine query parts
        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT f.id, f.file_path, f.file_name, f.modified_ts, f.deleted
            FROM file_list f
            {join_clause}
            WHERE {where_clause}
            ORDER BY {order_by}
        """

        return query, params

    def _format_datetime(self, ts: str | None, date_format: str) -> str:
        """Format timestamp using selected date format.

        Args:
            ts: ISO timestamp string or None

        Returns:
            Formatted date string or empty string
        """
        if not ts:
            return ""
        return format_datetime(ts, date_format, include_time=True, include_seconds=False)

    def _build_filter_description(
        self,
        tag_filter: str,
        match_filter: str,
        include_deleted: bool,
        sort_by: str,
        t: Dict[str, str] | None = None,
    ) -> str:
        """Build human-readable filter description.

        Args:
            tag_filter: Tag filter value
            match_filter: Match filter value
            include_deleted: Whether deleted files included
            sort_by: Sort order key

        Returns:
            Description string
        """
        parts = []
        t = t or {}

        # Tag description
        if tag_filter == self.ALL:
            pass  # No filter applied
        elif tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        else:
            parts.append(
                t.get("filter_tagged", 'tagged "{tag}"').format(tag=tag_filter)
            )

        # Match description
        if match_filter == self.ALL:
            pass
        elif match_filter == self.ANY_MATCH:
            parts.append(t.get("filter_any_match", "with any match"))
        else:
            parts.append(
                t.get("filter_matching", 'matching "{match}"').format(match=match_filter)
            )

        # Deleted description
        if include_deleted:
            parts.append(t.get("filter_including_deleted", "including deleted"))

        # Sort description
        sort_labels = {
            "modified_desc": t.get("sort_newest_first", "newest first"),
            "modified_asc": t.get("sort_oldest_first", "oldest first"),
            "name_asc": t.get("sort_name_az", "name A-Z"),
            "name_desc": t.get("sort_name_za", "name Z-A"),
            "path_asc": t.get("sort_path_az", "path A-Z"),
            "path_desc": t.get("sort_path_za", "path Z-A"),
        }
        parts.append(
            t.get("filter_sorted_by", "sorted by {sort}").format(
                sort=sort_labels.get(sort_by, sort_by)
            )
        )

        return ", ".join(parts) if parts else t.get("filter_all_files", "all files")

    def format_config_summary(self, config: dict[str, Any]) -> str:
        """Format configuration for display in section card.

        Args:
            config: Module configuration

        Returns:
            Short summary string
        """
        parts = []

        limit = config.get("limit", self.UNLIMITED)
        if limit != self.UNLIMITED:
            parts.append(f"Limit: {limit}")

        tag = config.get("tag_filter", self.ALL)
        if tag == self.ANY_TAG:
            parts.append("Any Tag")
        elif tag != self.ALL:
            parts.append(f"Tag: {tag}")

        match = config.get("match_filter", self.ALL)
        if match == self.ANY_MATCH:
            parts.append("Any Match")
        elif match != self.ALL:
            parts.append(f"Match: {match}")

        if config.get("include_deleted"):
            parts.append("+Deleted")

        return ", ".join(parts) if parts else "All files"
