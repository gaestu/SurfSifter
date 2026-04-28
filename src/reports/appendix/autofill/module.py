"""Appendix Autofill Module.

Displays a compact list of autofill form-data entries for use as
appendix reference material. Supports multi-select tag filtering,
optional browser filtering, partial field-name matching, and
optional grouping by browser.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...modules.base import sanitize_display_value
from ...paths import get_module_template_dir


class AppendixAutofillModule(BaseAppendixModule):
    """Appendix module for listing autofill form-data entries."""

    ALL_BROWSERS = "all"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_autofill",
            name="Autofill",
            description=(
                "Lists autofill form-data entries with optional grouping "
                "by browser, tag and browser filters"
            ),
            category="Appendix",
            icon="📝",
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
                key="browser_filter",
                label="Browser",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL_BROWSERS,
                options=[
                    (self.ALL_BROWSERS, "All Browsers"),
                ],
                help_text="Filter by browser (browsers loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="field_filter",
                label="Field Name Contains",
                filter_type=FilterType.TEXT,
                placeholder="e.g. email, address, name",
                help_text="Filter by field name (partial match)",
                required=False,
            ),
            FilterField(
                key="group_by_browser",
                label="Group by Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Group autofill entries by browser",
                required=False,
            ),
            FilterField(
                key="show_browser",
                label="Show Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Browser column",
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
                key="show_value",
                label="Show Value",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Value column",
                required=False,
            ),
            FilterField(
                key="show_count",
                label="Show Use Count",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Use Count column",
                required=False,
            ),
            FilterField(
                key="show_first_used",
                label="Show First Used",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the First Used (created) date column",
                required=False,
            ),
            FilterField(
                key="show_last_used",
                label="Show Last Used",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Last Used date column",
                required=False,
            ),
            FilterField(
                key="hide_placeholders",
                label="Hide Browser Placeholders",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text=(
                    "Replace known browser dummy values "
                    "(e.g. edge_default_dummy_password_value) with an em-dash"
                ),
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="last_used_desc",
                options=[
                    ("last_used_desc", "Last Used (Newest First)"),
                    ("last_used_asc", "Last Used (Oldest First)"),
                    ("created_desc", "Created (Newest First)"),
                    ("created_asc", "Created (Oldest First)"),
                    ("count_desc", "Use Count (Most First)"),
                    ("count_asc", "Use Count (Least First)"),
                    ("name_asc", "Field Name (A-Z)"),
                    ("name_desc", "Field Name (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                ],
                help_text="Sort order for the autofill list",
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
                    WHERE ta.artifact_type = 'autofill'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "browser_filter":
            options: List[tuple] = [(self.ALL_BROWSERS, "All Browsers")]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT browser
                    FROM autofill
                    WHERE browser IS NOT NULL AND browser != ''
                    ORDER BY browser
                    """
                )
                for (browser,) in cursor.fetchall():
                    options.append((browser, browser.capitalize()))
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
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        tag_filter: List[str] = list(config.get("tag_filter") or [])
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        field_filter = (config.get("field_filter") or "").strip()
        group_by_browser = bool(config.get("group_by_browser", True))
        show_browser = bool(config.get("show_browser", True))
        show_profile = bool(config.get("show_profile", False))
        show_value = bool(config.get("show_value", True))
        show_count = bool(config.get("show_count", True))
        show_first_used = bool(config.get("show_first_used", True))
        show_last_used = bool(config.get("show_last_used", True))
        hide_placeholders = bool(config.get("hide_placeholders", True))
        sort_by = config.get("sort_by", "last_used_desc")

        query, params = self._build_query(
            evidence_id, tag_filter, browser_filter, field_filter, sort_by
        )

        entries: List[Dict[str, Any]] = []
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                raw_value = row["value"] or ""
                entries.append(
                    {
                        "name": row["name"] or "",
                        "value": sanitize_display_value(
                            raw_value, hide_placeholders
                        ),
                        "browser": (row["browser"] or "").capitalize(),
                        "browser_raw": row["browser"] or "",
                        "profile": row["profile"] or "",
                        "count": row["count"] or 0,
                        "first_used": self._format_dt(
                            row["date_created_utc"], date_format
                        ),
                        "last_used": self._format_dt(
                            row["date_last_used_utc"], date_format
                        ),
                    }
                )
        except Exception as exc:
            return (
                f'<div class="module-error">'
                f"Error loading autofill data: {exc}</div>"
            )
        finally:
            db_conn.row_factory = orig_row_factory

        # Compute number of columns on the main row for grouped header colspan
        col_count = 1  # Name (always shown)
        if show_value:
            col_count += 1
        if not group_by_browser and show_browser:
            col_count += 1
        if show_profile:
            col_count += 1
        if show_count:
            col_count += 1
        if show_first_used:
            col_count += 1
        if show_last_used:
            col_count += 1

        grouped = None
        if group_by_browser:
            grouped = self._group_by_browser(entries)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            grouped=grouped,
            group_by_browser=group_by_browser,
            show_browser=show_browser,
            show_profile=show_profile,
            show_value=show_value,
            show_count=show_count,
            show_first_used=show_first_used,
            show_last_used=show_last_used,
            col_count=col_count,
            total_count=len(entries),
            t=translations,
            locale=locale,
        )

    def _format_dt(self, value: Optional[str], date_format: str) -> str:
        if not value:
            return ""
        try:
            return format_datetime(value, date_format)
        except Exception:
            return value

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        browser_filter: str,
        field_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["a.evidence_id = ?"]

        if browser_filter and browser_filter != self.ALL_BROWSERS:
            conditions.append("a.browser = ?")
            params.append(browser_filter)

        if field_filter:
            conditions.append("a.name LIKE ?")
            params.append(f"%{field_filter}%")

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = a.id
                      AND ta.artifact_type = 'autofill'
                      AND ta.evidence_id = a.evidence_id
                      AND t.name IN ({placeholders})
                )
                """
            )
            params.extend(tag_filter)

        order_clause = self._get_order_clause(sort_by)
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT a.id, a.name, a.value, a.browser, a.profile,
                   a.count, a.date_created_utc, a.date_last_used_utc
            FROM autofill a
            WHERE {where_clause}
            {order_clause}
            LIMIT 5000
        """
        return query, params

    @staticmethod
    def _get_order_clause(sort_by: str) -> str:
        order_map = {
            "last_used_desc": "ORDER BY a.date_last_used_utc DESC NULLS LAST",
            "last_used_asc": "ORDER BY a.date_last_used_utc ASC NULLS LAST",
            "created_desc": "ORDER BY a.date_created_utc DESC NULLS LAST",
            "created_asc": "ORDER BY a.date_created_utc ASC NULLS LAST",
            "count_desc": "ORDER BY a.count DESC NULLS LAST",
            "count_asc": "ORDER BY a.count ASC NULLS LAST",
            "name_asc": "ORDER BY a.name ASC",
            "name_desc": "ORDER BY a.name DESC",
            "browser_asc": "ORDER BY a.browser ASC, a.name ASC",
        }
        return order_map.get(
            sort_by, "ORDER BY a.date_last_used_utc DESC NULLS LAST"
        )

    def _group_by_browser(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            key = entry["browser"] or "(unknown)"
            groups[key].append(entry)

        result: List[Dict[str, Any]] = []
        for browser in sorted(groups.keys(), key=str.lower):
            result.append({"browser": browser, "entries": groups[browser]})
        return result
