"""Bookmarks Report Module.

Displays a table of bookmarks with filtering by tags.
"""

from __future__ import annotations

import sqlite3
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


class BookmarksModule(BaseReportModule):
    """Module for displaying bookmarks as a table in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="bookmarks",
            name="Bookmarks",
            description="Displays browser bookmarks with tag filters",
            category="Browser",
            icon="🔖",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags, limit, and sort."""
        return [
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional heading displayed above the bookmarks table (leave empty to hide)",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default=self.UNLIMITED,
                options=[
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of bookmarks to show",
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
                key="show_folder",
                label="Show Folder",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the folder path column",
                required=False,
            ),
            FilterField(
                key="show_browser",
                label="Show Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the browser column",
                required=False,
            ),
            FilterField(
                key="show_date_added",
                label="Show Date Added",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the date added column",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="date_added_desc",
                options=[
                    ("date_added_desc", "Date Added (Newest First)"),
                    ("date_added_asc", "Date Added (Oldest First)"),
                    ("title_asc", "Title (A-Z)"),
                    ("title_desc", "Title (Z-A)"),
                    ("folder_asc", "Folder (A-Z)"),
                    ("folder_desc", "Folder (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                    ("browser_desc", "Browser (Z-A)"),
                ],
                help_text="Sort order for the bookmarks list",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the bookmarks list",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag filter.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "tag_filter":
            # Get all tags used on bookmarks
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
                    WHERE ta.artifact_type = 'bookmark'
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
        """Render the bookmarks list as HTML.

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
        date_format = config.get("_date_format", "eu")

        # Extract config values
        section_title = config.get("section_title", "")
        limit = config.get("limit", self.UNLIMITED)
        tag_filter = config.get("tag_filter", self.ALL)
        show_folder = config.get("show_folder", True)
        show_browser = config.get("show_browser", True)
        show_date_added = config.get("show_date_added", True)
        sort_by = config.get("sort_by", "date_added_desc")
        show_filter_info = config.get("show_filter_info", False)

        # Build query
        query, params = self._build_query(evidence_id, tag_filter, sort_by)

        # Execute query
        bookmarks = []
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
                bookmarks.append(
                    {
                        "title": row["title"] or row["url"],
                        "url": row["url"],
                        "folder": row["folder_path"] or "",
                        "browser": row["browser"] or "",
                        "date_added": self._format_datetime(row["date_added_utc"], date_format),
                    }
                )
        except Exception as e:
            # Return error HTML
            return f'<div class="module-error">Error loading bookmarks: {e}</div>'

        # Determine if list is truncated
        shown_count = len(bookmarks)
        is_truncated = shown_count < total_count

        # Build filter description
        filter_desc = self._build_filter_description(tag_filter, sort_by, translations)

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            bookmarks=bookmarks,
            section_title=section_title,
            show_folder=show_folder,
            show_browser=show_browser,
            show_date_added=show_date_added,
            filter_description=filter_desc,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            show_filter_info=show_filter_info,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query based on filters.

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = ["b.evidence_id = ?", "b.bookmark_type = 'url'"]

        # Handle tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = b.id AND ta.artifact_type = 'bookmark'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = b.id AND ta.artifact_type = 'bookmark'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        # Build ORDER BY
        order_map = {
            "date_added_desc": "b.date_added_utc DESC NULLS LAST",
            "date_added_asc": "b.date_added_utc ASC NULLS FIRST",
            "title_asc": "COALESCE(b.title, b.url) ASC",
            "title_desc": "COALESCE(b.title, b.url) DESC",
            "folder_asc": "b.folder_path ASC NULLS LAST",
            "folder_desc": "b.folder_path DESC NULLS FIRST",
            "browser_asc": "b.browser ASC",
            "browser_desc": "b.browser DESC",
        }
        order_by = order_map.get(sort_by, "b.date_added_utc DESC NULLS LAST")

        # Combine query parts
        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT b.id, b.title, b.url, b.folder_path, b.browser, b.date_added_utc
            FROM bookmarks b
            {join_clause}
            WHERE {where_clause}
            ORDER BY {order_by}
        """

        return query, params

    def _build_filter_description(
        self, tag_filter: str, sort_by: str, t: Dict[str, str]
    ) -> str:
        """Build human-readable filter description.

        Args:
            tag_filter: Tag filter value
            sort_by: Sort option value
            t: Translation dictionary

        Returns:
            Filter description string
        """
        parts = []

        # Tag filter
        if tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        elif tag_filter != self.ALL:
            template = t.get("filter_tagged", 'tagged "{tag}"')
            parts.append(template.replace("{tag}", tag_filter))
        else:
            parts.append(t.get("filter_all_tags", "all tags"))

        # Sort option
        sort_labels = {
            "date_added_desc": t.get("sort_newest_first", "newest first"),
            "date_added_asc": t.get("sort_oldest_first", "oldest first"),
            "title_asc": t.get("sort_name_az", "name A-Z"),
            "title_desc": t.get("sort_name_za", "name Z-A"),
            "folder_asc": t.get("sort_path_az", "folder A-Z"),
            "folder_desc": t.get("sort_path_za", "folder Z-A"),
            "browser_asc": t.get("sort_name_az", "browser A-Z"),
            "browser_desc": t.get("sort_name_za", "browser Z-A"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        sort_template = t.get("filter_sorted_by", "sorted by {sort}")
        parts.append(sort_template.replace("{sort}", sort_label))

        return ", ".join(parts)

    def _format_datetime(self, value: str | None, date_format: str) -> str:
        """Format datetime string for display.

        Args:
            value: ISO datetime string or None
            date_format: 'eu' or 'us' format

        Returns:
            Formatted date string or empty string
        """
        if not value:
            return ""
        return format_datetime(value, date_format)
