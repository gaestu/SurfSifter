"""Browser History Report Module.

Displays a table of browser history entries with filtering by tags.
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


class BrowserHistoryModule(BaseReportModule):
    """Module for displaying browser history entries as a table in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="browser_history",
            name="Browser History",
            description="Displays browser history entries with tag filters",
            category="Browser",
            icon="🕐",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for title, description, tags, limit, and sort."""
        return [
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional heading displayed above the history table (leave empty to hide)",
                required=False,
            ),
            FilterField(
                key="section_description",
                label="Section Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional description displayed below the title (leave empty to use default or hide)",
                required=False,
            ),
            FilterField(
                key="show_default_description",
                label="Show Default Description",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the default description explaining what browser history is",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default="100",
                options=[
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    ("250", "250"),
                    ("500", "500"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of history entries to show",
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
                key="show_browser",
                label="Show Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the browser column",
                required=False,
            ),
            FilterField(
                key="show_profile",
                label="Show Profile",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the browser profile column",
                required=False,
            ),
            FilterField(
                key="show_visit_count",
                label="Show Visit Count",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the visit count column",
                required=False,
            ),
            FilterField(
                key="show_transition_type",
                label="Show Transition Type",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show how the page was accessed (typed, link, etc.)",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="visit_time_desc",
                options=[
                    ("visit_time_desc", "Visit Time (Newest First)"),
                    ("visit_time_asc", "Visit Time (Oldest First)"),
                    ("title_asc", "Title (A-Z)"),
                    ("title_desc", "Title (Z-A)"),
                    ("url_asc", "URL (A-Z)"),
                    ("url_desc", "URL (Z-A)"),
                    ("visit_count_desc", "Visit Count (Most First)"),
                    ("visit_count_asc", "Visit Count (Least First)"),
                    ("browser_asc", "Browser (A-Z)"),
                    ("browser_desc", "Browser (Z-A)"),
                ],
                help_text="Sort order for the history list",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the history list",
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
            # Get all tags used on browser history
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
                    WHERE ta.artifact_type = 'history'
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
        """Render the browser history list as HTML.

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
        section_description = config.get("section_description", "")
        show_default_description = config.get("show_default_description", False)
        limit = config.get("limit", "100")
        tag_filter = config.get("tag_filter", self.ALL)
        show_browser = config.get("show_browser", True)
        show_profile = config.get("show_profile", False)
        show_visit_count = config.get("show_visit_count", False)
        show_transition_type = config.get("show_transition_type", False)
        sort_by = config.get("sort_by", "visit_time_desc")
        show_filter_info = config.get("show_filter_info", False)

        # Build query
        query, params = self._build_query(evidence_id, tag_filter, sort_by)

        # Execute query
        history_entries = []
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
                history_entries.append(
                    {
                        "title": row["title"] or row["url"],
                        "url": row["url"],
                        "visit_time": self._format_datetime(row["ts_utc"], date_format),
                        "browser": row["browser"] or "",
                        "profile": row["profile"] or "",
                        "visit_count": row["visit_count"] or 0,
                        "transition_type": row["transition_type_name"] or "",
                    }
                )
        except Exception as e:
            # Return error HTML
            return f'<div class="module-error">Error loading browser history: {e}</div>'

        # Determine if list is truncated
        shown_count = len(history_entries)
        is_truncated = shown_count < total_count

        # Build filter description
        filter_desc = self._build_filter_description(tag_filter, sort_by, translations)

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            history_entries=history_entries,
            section_title=section_title,
            section_description=section_description,
            show_default_description=show_default_description,
            show_browser=show_browser,
            show_profile=show_profile,
            show_visit_count=show_visit_count,
            show_transition_type=show_transition_type,
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
        conditions: list[str] = ["h.evidence_id = ?"]

        # Handle tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = h.id AND ta.artifact_type = 'history'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = h.id AND ta.artifact_type = 'history'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        # Build ORDER BY
        order_map = {
            "visit_time_desc": "h.ts_utc DESC NULLS LAST",
            "visit_time_asc": "h.ts_utc ASC NULLS FIRST",
            "title_asc": "COALESCE(h.title, h.url) ASC",
            "title_desc": "COALESCE(h.title, h.url) DESC",
            "url_asc": "h.url ASC",
            "url_desc": "h.url DESC",
            "visit_count_desc": "h.visit_count DESC NULLS LAST",
            "visit_count_asc": "h.visit_count ASC NULLS FIRST",
            "browser_asc": "h.browser ASC NULLS LAST",
            "browser_desc": "h.browser DESC NULLS FIRST",
        }
        order_by = order_map.get(sort_by, "h.ts_utc DESC NULLS LAST")

        # Combine query parts
        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT h.id, h.title, h.url, h.ts_utc, h.browser, h.profile,
                   h.visit_count, h.transition_type_name
            FROM browser_history h
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
            "visit_time_desc": t.get("sort_newest_first", "newest first"),
            "visit_time_asc": t.get("sort_oldest_first", "oldest first"),
            "title_asc": t.get("sort_name_az", "name A-Z"),
            "title_desc": t.get("sort_name_za", "name Z-A"),
            "url_asc": t.get("sort_url_az", "URL A-Z"),
            "url_desc": t.get("sort_url_za", "URL Z-A"),
            "visit_count_desc": t.get("sort_most_frequent_first", "most visits first"),
            "visit_count_asc": t.get("sort_least_frequent_first", "least visits first"),
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
