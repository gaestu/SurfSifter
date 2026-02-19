"""Browser Downloads Report Module.

Displays browser download history entries with tag filtering.
"""

from __future__ import annotations

import sqlite3
from urllib.parse import unquote, urlparse
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


class BrowserDownloadsModule(BaseReportModule):
    """Module for displaying browser downloads as a table in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="browser_downloads",
            name="Browser Downloads",
            description="Displays browser download history with tag filters",
            category="Browser",
            icon="📥",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags and display options."""
        return [
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional heading displayed above the downloads table (leave empty to hide)",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default="100",
                options=[
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    ("250", "250"),
                    ("500", "500"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of downloads to show",
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
                key="show_state",
                label="Show State",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the download state column",
                required=False,
            ),
            FilterField(
                key="show_size",
                label="Show Size",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the download size column",
                required=False,
            ),
            FilterField(
                key="show_end_time",
                label="Show End Time",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the end time column",
                required=False,
            ),
            FilterField(
                key="shorten_urls",
                label="Shorten URLs",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Truncate long URLs to fit on one line with ellipsis",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="start_time_desc",
                options=[
                    ("start_time_desc", "Start Time (Newest First)"),
                    ("start_time_asc", "Start Time (Oldest First)"),
                    ("end_time_desc", "End Time (Newest First)"),
                    ("end_time_asc", "End Time (Oldest First)"),
                    ("filename_asc", "Filename (A-Z)"),
                    ("filename_desc", "Filename (Z-A)"),
                    ("url_asc", "URL (A-Z)"),
                    ("url_desc", "URL (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                    ("browser_desc", "Browser (Z-A)"),
                    ("state_asc", "State (A-Z)"),
                    ("state_desc", "State (Z-A)"),
                    ("size_desc", "Size (Largest First)"),
                    ("size_asc", "Size (Smallest First)"),
                ],
                help_text="Sort order for the downloads list",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the downloads list",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag filter."""
        if key == "tag_filter":
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
                    WHERE ta.artifact_type = 'browser_download'
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
        """Render the browser downloads list as HTML."""
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        section_title = config.get("section_title", "")
        limit = config.get("limit", "100")
        tag_filter = config.get("tag_filter", self.ALL)
        show_browser = bool(config.get("show_browser", True))
        show_state = bool(config.get("show_state", True))
        show_size = bool(config.get("show_size", True))
        show_end_time = bool(config.get("show_end_time", True))
        shorten_urls = bool(config.get("shorten_urls", False))
        sort_by = config.get("sort_by", "start_time_desc")
        show_filter_info = bool(config.get("show_filter_info", False))

        query, params = self._build_query(evidence_id, tag_filter, sort_by)

        downloads: List[Dict[str, str]] = []
        total_count = 0
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            if limit != self.UNLIMITED:
                try:
                    all_rows = all_rows[: int(limit)]
                except (TypeError, ValueError):
                    all_rows = all_rows[:100]

            for row in all_rows:
                size_bytes = row["total_bytes"] or row["received_bytes"] or 0
                downloads.append(
                    {
                        "filename": self._resolve_filename(
                            row["filename"],
                            row["target_path"],
                            row["url"],
                        ),
                        "url": row["url"] or "",
                        "browser": (row["browser"] or "").capitalize(),
                        "state": self._format_state(row["state"]),
                        "size": self._format_size(size_bytes),
                        "start_time": self._format_datetime(
                            row["start_time_utc"], date_format
                        ),
                        "end_time": self._format_datetime(
                            row["end_time_utc"], date_format
                        ),
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading browser downloads: {exc}</div>'

        shown_count = len(downloads)
        is_truncated = shown_count < total_count
        filter_desc = self._build_filter_description(tag_filter, sort_by, translations)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            downloads=downloads,
            section_title=section_title,
            show_browser=show_browser,
            show_state=show_state,
            show_size=show_size,
            show_end_time=show_end_time,
            shorten_urls=shorten_urls,
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
        """Build SQL query based on filters."""
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = ["d.evidence_id = ?"]

        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = d.id AND ta.artifact_type = 'browser_download'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = d.id AND ta.artifact_type = 'browser_download'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        order_map = {
            "start_time_desc": "COALESCE(d.start_time_utc, '') DESC",
            "start_time_asc": "COALESCE(d.start_time_utc, '') ASC",
            "end_time_desc": "COALESCE(d.end_time_utc, '') DESC",
            "end_time_asc": "COALESCE(d.end_time_utc, '') ASC",
            "filename_asc": "COALESCE(d.filename, '') ASC",
            "filename_desc": "COALESCE(d.filename, '') DESC",
            "url_asc": "COALESCE(d.url, '') ASC",
            "url_desc": "COALESCE(d.url, '') DESC",
            "browser_asc": "COALESCE(d.browser, '') ASC",
            "browser_desc": "COALESCE(d.browser, '') DESC",
            "state_asc": "COALESCE(d.state, '') ASC",
            "state_desc": "COALESCE(d.state, '') DESC",
            "size_desc": "COALESCE(d.total_bytes, d.received_bytes, 0) DESC",
            "size_asc": "COALESCE(d.total_bytes, d.received_bytes, 0) ASC",
        }
        order_by = order_map.get(sort_by, "COALESCE(d.start_time_utc, '') DESC")

        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT
                d.id,
                d.filename,
                d.target_path,
                d.url,
                d.browser,
                d.state,
                d.total_bytes,
                d.received_bytes,
                d.start_time_utc,
                d.end_time_utc
            FROM browser_downloads d
            {join_clause}
            WHERE {where_clause}
            ORDER BY {order_by}
        """

        return query, params

    def _build_filter_description(
        self,
        tag_filter: str,
        sort_by: str,
        t: Dict[str, str],
    ) -> str:
        """Build human-readable filter description."""
        parts: list[str] = []

        if tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        elif tag_filter != self.ALL:
            template = t.get("filter_tagged", 'tagged "{tag}"')
            parts.append(template.replace("{tag}", tag_filter))
        else:
            parts.append(t.get("filter_all_tags", "all tags"))

        sort_labels = {
            "start_time_desc": t.get("sort_newest_first", "newest first"),
            "start_time_asc": t.get("sort_oldest_first", "oldest first"),
            "end_time_desc": t.get("sort_newest_first", "newest first"),
            "end_time_asc": t.get("sort_oldest_first", "oldest first"),
            "filename_asc": t.get("sort_name_az", "name A-Z"),
            "filename_desc": t.get("sort_name_za", "name Z-A"),
            "url_asc": t.get("sort_url_az", "URL A-Z"),
            "url_desc": t.get("sort_url_za", "URL Z-A"),
            "browser_asc": t.get("sort_name_az", "browser A-Z"),
            "browser_desc": t.get("sort_name_za", "browser Z-A"),
            "state_asc": t.get("sort_name_az", "state A-Z"),
            "state_desc": t.get("sort_name_za", "state Z-A"),
            "size_desc": t.get("sort_largest_first", "largest first"),
            "size_asc": t.get("sort_smallest_first", "smallest first"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        sort_template = t.get("filter_sorted_by", "sorted by {sort}")
        parts.append(sort_template.replace("{sort}", sort_label))

        return ", ".join(parts)

    def _format_datetime(self, value: str | None, date_format: str) -> str:
        """Format datetime string for display."""
        if not value:
            return ""
        return format_datetime(value, date_format, include_time=True, include_seconds=True)

    @staticmethod
    def _format_state(value: str | None) -> str:
        """Format a raw state value into readable text."""
        if not value:
            return ""
        return value.replace("_", " ").title()

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format size in bytes as a readable string."""
        if size_bytes <= 0:
            return ""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        if size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def _resolve_filename(
        self,
        filename: str | None,
        target_path: str | None,
        url: str | None,
    ) -> str:
        """Resolve best display filename from filename, path, or URL."""
        if filename:
            return filename

        if target_path:
            normalized = target_path.replace("\\", "/").rstrip("/")
            if normalized:
                tail = normalized.rsplit("/", 1)[-1]
                if tail:
                    return tail

        if url:
            parsed = urlparse(url)
            if parsed.path:
                tail = unquote(parsed.path).rstrip("/").rsplit("/", 1)[-1]
                if tail:
                    return tail

        return "Unknown"
