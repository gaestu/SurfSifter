"""Jump Lists Report Module.

Displays a table of Windows Jump List entries with filtering by tags.
Shows application name, path, title, access/creation times, pin status,
and optionally URL and jump list path in a second detail line.
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


class JumpListsModule(BaseReportModule):
    """Module for displaying Jump List entries as a table in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="jump_lists",
            name="Jump Lists",
            description="Displays Windows Jump List entries with tag filters",
            category="System",
            icon="📋",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags, limit, and sort."""
        return [
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional heading displayed above the table (leave empty to hide)",
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
                help_text="Maximum number of entries to show",
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
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="access_time_desc",
                options=[
                    ("access_time_desc", "Access Time (Newest First)"),
                    ("access_time_asc", "Access Time (Oldest First)"),
                    ("creation_time_desc", "Creation Time (Newest First)"),
                    ("creation_time_asc", "Creation Time (Oldest First)"),
                    ("app_asc", "Application (A-Z)"),
                    ("app_desc", "Application (Z-A)"),
                ],
                help_text="Sort order for the jump list entries",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the table",
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
                    WHERE ta.artifact_type = 'jump_list'
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
        """Render the jump list entries as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        section_title = config.get("section_title", "")
        limit = config.get("limit", self.UNLIMITED)
        tag_filter = config.get("tag_filter", self.ALL)
        sort_by = config.get("sort_by", "access_time_desc")
        show_filter_info = config.get("show_filter_info", False)

        query, params = self._build_query(evidence_id, tag_filter, sort_by)

        entries: list[dict[str, Any]] = []
        total_count = 0
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            if limit != self.UNLIMITED:
                limit_int = int(limit)
                all_rows = all_rows[:limit_int]

            for row in all_rows:
                entries.append(
                    {
                        "application": row["appid"] or "",
                        "target_path": row["target_path"] or "",
                        "title": row["title"] or "",
                        "access_time": self._format_dt(
                            row["lnk_access_time"], date_format
                        ),
                        "creation_time": self._format_dt(
                            row["lnk_creation_time"], date_format
                        ),
                        "pin_status": row["pin_status"] or "",
                        "url": row["url"] or "",
                        "jumplist_path": row["jumplist_path"] or "",
                    }
                )
        except Exception as e:
            return f'<div class="module-error">Error loading jump list entries: {e}</div>'
        finally:
            db_conn.row_factory = orig_row_factory

        shown_count = len(entries)
        is_truncated = shown_count < total_count

        filter_desc = self._build_filter_description(
            tag_filter, sort_by, translations
        )

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            section_title=section_title,
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
        conditions: list[str] = ["j.evidence_id = ?"]

        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = j.id AND ta.artifact_type = 'jump_list'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = j.id AND ta.artifact_type = 'jump_list'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        order_map = {
            "access_time_desc": "j.lnk_access_time DESC NULLS LAST",
            "access_time_asc": "j.lnk_access_time ASC NULLS FIRST",
            "creation_time_desc": "j.lnk_creation_time DESC NULLS LAST",
            "creation_time_asc": "j.lnk_creation_time ASC NULLS FIRST",
            "app_asc": "j.appid ASC",
            "app_desc": "j.appid DESC",
        }
        order_by = order_map.get(sort_by, "j.lnk_access_time DESC NULLS LAST")

        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT j.id, j.appid, j.target_path, j.title,
                   j.lnk_access_time, j.lnk_creation_time, j.pin_status,
                   j.url, j.jumplist_path
            FROM jump_list_entries j
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

        if tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        elif tag_filter != self.ALL:
            template = t.get("filter_tagged", 'tagged "{tag}"')
            parts.append(template.replace("{tag}", tag_filter))
        else:
            parts.append(t.get("filter_all_tags", "all tags"))

        sort_labels = {
            "access_time_desc": t.get("sort_newest_first", "newest first"),
            "access_time_asc": t.get("sort_oldest_first", "oldest first"),
            "creation_time_desc": t.get("sort_newest_first", "newest first"),
            "creation_time_asc": t.get("sort_oldest_first", "oldest first"),
            "app_asc": t.get("sort_name_az", "name A-Z"),
            "app_desc": t.get("sort_name_za", "name Z-A"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        sort_template = t.get("filter_sorted_by", "sorted by {sort}")
        parts.append(sort_template.replace("{sort}", sort_label))

        return ", ".join(parts)

    def _format_dt(self, value: str | None, date_format: str) -> str:
        """Format datetime string for display."""
        if not value:
            return ""
        return format_datetime(value, date_format)
