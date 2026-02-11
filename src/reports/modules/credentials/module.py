"""Credentials Report Module.

Displays a table of credentials (saved login data) with filtering by browser
and tags. Supports multi-select tag filtering with checkboxes.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ...dates import format_datetime
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class CredentialsModule(BaseReportModule):
    """Module for displaying saved credentials in reports."""

    # Special filter values
    ALL_BROWSERS = "all"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="credentials",
            name="Credentials",
            description="Displays saved login credentials with browser and tag filters",
            category="Browser",
            icon="🔑",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for browser, tags, and display options."""
        return [
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
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
                required=False,
            ),
            FilterField(
                key="show_profile",
                label="Show Profile Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the browser profile column",
                required=False,
            ),
            FilterField(
                key="show_password",
                label="Show Password Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display whether password is stored",
                required=False,
            ),
            FilterField(
                key="show_dates",
                label="Show Date Columns",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display Created and Last Used date columns",
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
                    ("origin_asc", "Origin URL (A-Z)"),
                    ("origin_desc", "Origin URL (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                ],
                help_text="Sort order for the credentials list",
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
    ) -> Optional[List[tuple]]:
        """Load dynamic options for browser and tag filters.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "browser_filter":
            options: List[tuple] = [
                (self.ALL_BROWSERS, "All Browsers"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT browser
                    FROM credentials
                    WHERE browser IS NOT NULL AND browser != ''
                    ORDER BY browser
                    """
                )
                for (browser,) in cursor.fetchall():
                    options.append((browser, browser.capitalize()))
            except Exception:
                pass
            return options

        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'credential'
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
        """Render the credentials table as HTML.

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
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        tag_filter = config.get("tag_filter") or []
        show_profile = bool(config.get("show_profile", False))
        show_password = bool(config.get("show_password", True))
        show_dates = bool(config.get("show_dates", True))
        sort_by = config.get("sort_by", "last_used_desc")
        show_filter_info = bool(config.get("show_filter_info", False))

        # Build and execute query
        query, params = self._build_query(
            evidence_id, browser_filter, tag_filter, sort_by
        )

        credentials: List[Dict[str, Any]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                cred = {
                    "origin_url": row["origin_url"] or "",
                    "username_element": row["username_element"] or "",
                    "username_value": row["username_value"] or "",
                    "browser": (row["browser"] or "").capitalize(),
                    "profile": row["profile"] or "",
                    "has_password": bool(row["has_password"]),
                    "date_created": format_datetime(
                        row["date_created_utc"], date_format
                    ) if row["date_created_utc"] else "",
                    "date_last_used": format_datetime(
                        row["date_last_used_utc"], date_format
                    ) if row["date_last_used_utc"] else "",
                }
                credentials.append(cred)
        except Exception as exc:
            return f'<div class="module-error">Error loading credentials: {exc}</div>'

        # Build filter description
        filter_parts = []
        if browser_filter != self.ALL_BROWSERS:
            filter_parts.append(f"Browser: {browser_filter.capitalize()}")
        if tag_filter:
            filter_parts.append(f"Tags: {', '.join(tag_filter)}")
        filter_description = "; ".join(filter_parts) if filter_parts else "All"

        # Load template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            credentials=credentials,
            total_count=len(credentials),
            show_profile=show_profile,
            show_password=show_password,
            show_dates=show_dates,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        browser_filter: str,
        tag_filter: List[str],
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query for credentials with filters.

        Args:
            evidence_id: Evidence ID
            browser_filter: Browser name filter
            tag_filter: List of tag names to filter by
            sort_by: Sort order

        Returns:
            Tuple of (query_string, params_list)
        """
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["c.evidence_id = ?"]

        # Browser filter
        if browser_filter != self.ALL_BROWSERS:
            conditions.append("c.browser = ?")
            params.append(browser_filter)

        # Tag filter (multi-select OR logic)
        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = c.id
                      AND ta.artifact_type = 'credential'
                      AND ta.evidence_id = c.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        # Build ORDER BY clause
        order_clause = self._get_order_clause(sort_by)

        query = f"""
            SELECT
                c.origin_url,
                c.username_element,
                c.username_value,
                c.browser,
                c.profile,
                CASE WHEN c.password_value_encrypted IS NOT NULL
                     AND length(c.password_value_encrypted) > 0
                     THEN 1 ELSE 0 END as has_password,
                c.date_created_utc,
                c.date_last_used_utc
            FROM credentials c
            WHERE {' AND '.join(conditions)}
            {order_clause}
            LIMIT 5000
        """

        return query, params

    def _get_order_clause(self, sort_by: str) -> str:
        """Get ORDER BY clause for sort option."""
        order_map = {
            "last_used_desc": "ORDER BY c.date_last_used_utc DESC NULLS LAST",
            "last_used_asc": "ORDER BY c.date_last_used_utc ASC NULLS LAST",
            "created_desc": "ORDER BY c.date_created_utc DESC NULLS LAST",
            "created_asc": "ORDER BY c.date_created_utc ASC NULLS LAST",
            "origin_asc": "ORDER BY c.origin_url ASC",
            "origin_desc": "ORDER BY c.origin_url DESC",
            "browser_asc": "ORDER BY c.browser ASC, c.origin_url ASC",
        }
        return order_map.get(sort_by, "ORDER BY c.date_last_used_utc DESC NULLS LAST")
