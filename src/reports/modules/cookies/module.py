"""Cookies Report Module.

Displays a table of tagged cookies with multi-select tag filtering,
optional browser filter, and configurable column visibility.
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


class CookiesModule(BaseReportModule):
    """Module for displaying tagged cookies in reports."""

    # Special filter values
    ALL_BROWSERS = "all"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="cookies",
            name="Cookies",
            description="Displays tagged cookies with browser and tag filters",
            category="Browser",
            icon="🍪",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields following standard field order."""
        return [
            # ── 1. Title group ──────────────────────────────────────
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a title at the top of this section",
            ),
            FilterField(
                key="custom_title",
                label="Custom Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom title (leave empty for default)",
            ),
            # ── 2. Description group ────────────────────────────────
            FilterField(
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a short description below the title",
            ),
            FilterField(
                key="custom_description",
                label="Custom Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom description (leave empty for default)",
            ),
            # ── 3. Data filters ─────────────────────────────────────
            FilterField(
                key="browser_filter",
                label="Browser",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL_BROWSERS,
                options=[
                    (self.ALL_BROWSERS, "All Browsers"),
                ],
                help_text="Filter by browser (browsers loaded dynamically)",
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
            ),
            # ── 4. Display options ──────────────────────────────────
            FilterField(
                key="show_browser",
                label="Show Browser Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the browser column",
            ),
            FilterField(
                key="show_profile",
                label="Show Profile Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the browser profile column",
            ),
            FilterField(
                key="show_value",
                label="Show Value Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the cookie value column",
            ),
            FilterField(
                key="show_path",
                label="Show Path Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the cookie path column",
            ),
            FilterField(
                key="show_expires",
                label="Show Expires Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the expiration date column",
            ),
            FilterField(
                key="show_secure",
                label="Show Secure Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the Secure flag column",
            ),
            FilterField(
                key="show_httponly",
                label="Show HttpOnly Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the HttpOnly flag column",
            ),
            FilterField(
                key="show_samesite",
                label="Show SameSite Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the SameSite attribute column",
            ),
            FilterField(
                key="show_created",
                label="Show Created Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the creation date column",
            ),
            FilterField(
                key="show_last_access",
                label="Show Last Access Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the last access date column",
            ),
            # ── 5. Sort & limit ─────────────────────────────────────
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="last_access_desc",
                options=[
                    ("last_access_desc", "Last Access (Newest First)"),
                    ("last_access_asc", "Last Access (Oldest First)"),
                    ("created_desc", "Created (Newest First)"),
                    ("created_asc", "Created (Oldest First)"),
                    ("domain_asc", "Domain (A-Z)"),
                    ("domain_desc", "Domain (Z-A)"),
                    ("name_asc", "Name (A-Z)"),
                    ("name_desc", "Name (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                ],
                help_text="Sort order for the cookies list",
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default="500",
                options=[
                    ("50", "50"),
                    ("100", "100"),
                    ("250", "250"),
                    ("500", "500"),
                    ("1000", "1000"),
                    ("unlimited", "Unlimited"),
                ],
                help_text="Maximum number of cookies to show",
            ),
            # ── 6. Footer ──────────────────────────────────────────
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the table",
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for browser and tag filters."""
        if key == "browser_filter":
            options: List[tuple] = [
                (self.ALL_BROWSERS, "All Browsers"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT browser
                    FROM cookies
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
                    WHERE ta.artifact_type = 'cookie'
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
        """Render the cookies table as HTML."""
        # ── Internal config keys ────────────────────────────────────
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")
        t = translations

        # ── Standard fields ─────────────────────────────────────────
        show_title = config.get("show_title", True)
        custom_title = config.get("custom_title", "")
        title_text = custom_title or t.get("cookies_title", self.metadata.name)

        show_description = config.get("show_description", True)
        custom_description = config.get("custom_description", "")
        description_text = custom_description or t.get(
            "cookies_description", self.metadata.description
        )

        show_filter_info = config.get("show_filter_info", False)

        # ── Data filters ────────────────────────────────────────────
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        tag_filter = config.get("tag_filter") or []
        sort_by = config.get("sort_by", "last_access_desc")
        limit_str = config.get("limit", "500")

        # ── Display options ─────────────────────────────────────────
        show_browser = bool(config.get("show_browser", True))
        show_profile = bool(config.get("show_profile", False))
        show_value = bool(config.get("show_value", False))
        show_path = bool(config.get("show_path", False))
        show_expires = bool(config.get("show_expires", True))
        show_secure = bool(config.get("show_secure", True))
        show_httponly = bool(config.get("show_httponly", True))
        show_samesite = bool(config.get("show_samesite", False))
        show_created = bool(config.get("show_created", False))
        show_last_access = bool(config.get("show_last_access", True))

        # ── Build and execute query ─────────────────────────────────
        limit_val = None if limit_str == "unlimited" else int(limit_str)
        query, params = self._build_query(
            evidence_id, browser_filter, tag_filter, sort_by, limit_val
        )

        cookies: List[Dict[str, Any]] = []
        total_count = 0
        try:
            db_conn.row_factory = sqlite3.Row
            # Get total count first
            count_query, count_params = self._build_count_query(
                evidence_id, browser_filter, tag_filter
            )
            total_count = db_conn.execute(count_query, count_params).fetchone()[0]

            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                cookie = {
                    "name": row["name"] or "",
                    "domain": row["domain"] or "",
                    "value": row["value"] or "",
                    "path": row["path"] or "",
                    "browser": (row["browser"] or "").capitalize(),
                    "profile": row["profile"] or "",
                    "is_secure": bool(row["is_secure"]),
                    "is_httponly": bool(row["is_httponly"]),
                    "samesite": row["samesite"] or "",
                    "expires_utc": format_datetime(
                        row["expires_utc"], date_format
                    ) if row["expires_utc"] else "",
                    "creation_utc": format_datetime(
                        row["creation_utc"], date_format
                    ) if row["creation_utc"] else "",
                    "last_access_utc": format_datetime(
                        row["last_access_utc"], date_format
                    ) if row["last_access_utc"] else "",
                }
                cookies.append(cookie)
        except Exception as exc:
            return f'<div class="module-error">Error loading cookies: {exc}</div>'

        # ── Truncation ──────────────────────────────────────────────
        shown_count = len(cookies)
        is_truncated = shown_count < total_count

        # ── Filter description ──────────────────────────────────────
        filter_description = self._build_filter_description(
            browser_filter, tag_filter, sort_by, t
        )

        # ── Render template ─────────────────────────────────────────
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            # Standard
            show_title=show_title,
            title_text=title_text,
            show_description=show_description,
            description_text=description_text,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            # Module-specific
            cookies=cookies,
            show_browser=show_browser,
            show_profile=show_profile,
            show_value=show_value,
            show_path=show_path,
            show_expires=show_expires,
            show_secure=show_secure,
            show_httponly=show_httponly,
            show_samesite=show_samesite,
            show_created=show_created,
            show_last_access=show_last_access,
            # Translation / locale
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        browser_filter: str,
        tag_filter: List[str],
        sort_by: str,
        limit: Optional[int],
    ) -> tuple[str, list[Any]]:
        """Build SQL query for cookies with filters."""
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
                      AND ta.artifact_type = 'cookie'
                      AND ta.evidence_id = c.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        order_clause = self._get_order_clause(sort_by)
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        query = f"""
            SELECT
                c.name,
                c.domain,
                c.value,
                c.path,
                c.browser,
                c.profile,
                c.is_secure,
                c.is_httponly,
                c.samesite,
                c.expires_utc,
                c.creation_utc,
                c.last_access_utc
            FROM cookies c
            WHERE {' AND '.join(conditions)}
            {order_clause}
            {limit_clause}
        """

        return query, params

    def _build_count_query(
        self,
        evidence_id: int,
        browser_filter: str,
        tag_filter: List[str],
    ) -> tuple[str, list[Any]]:
        """Build count query with same filters (for truncation info)."""
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["c.evidence_id = ?"]

        if browser_filter != self.ALL_BROWSERS:
            conditions.append("c.browser = ?")
            params.append(browser_filter)

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = c.id
                      AND ta.artifact_type = 'cookie'
                      AND ta.evidence_id = c.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        query = f"""
            SELECT COUNT(*)
            FROM cookies c
            WHERE {' AND '.join(conditions)}
        """

        return query, params

    def _get_order_clause(self, sort_by: str) -> str:
        """Get ORDER BY clause for sort option."""
        order_map = {
            "last_access_desc": "ORDER BY c.last_access_utc DESC NULLS LAST",
            "last_access_asc": "ORDER BY c.last_access_utc ASC NULLS LAST",
            "created_desc": "ORDER BY c.creation_utc DESC NULLS LAST",
            "created_asc": "ORDER BY c.creation_utc ASC NULLS LAST",
            "domain_asc": "ORDER BY c.domain ASC, c.name ASC",
            "domain_desc": "ORDER BY c.domain DESC, c.name ASC",
            "name_asc": "ORDER BY c.name ASC, c.domain ASC",
            "name_desc": "ORDER BY c.name DESC, c.domain ASC",
            "browser_asc": "ORDER BY c.browser ASC, c.domain ASC",
        }
        return order_map.get(sort_by, "ORDER BY c.last_access_utc DESC NULLS LAST")

    def _build_filter_description(
        self,
        browser_filter: str,
        tag_filter: List[str],
        sort_by: str,
        t: Dict[str, str],
    ) -> str:
        """Build human-readable filter description for the footer."""
        parts = []

        if browser_filter != self.ALL_BROWSERS:
            parts.append(f"Browser: {browser_filter.capitalize()}")

        if tag_filter:
            parts.append(f"Tags: {', '.join(tag_filter)}")

        sort_labels = {
            "last_access_desc": t.get("sort_newest_first", "newest first"),
            "last_access_asc": t.get("sort_oldest_first", "oldest first"),
            "created_desc": t.get("sort_created_newest", "created newest"),
            "created_asc": t.get("sort_created_oldest", "created oldest"),
            "domain_asc": t.get("sort_domain_az", "domain A-Z"),
            "domain_desc": t.get("sort_domain_za", "domain Z-A"),
            "name_asc": t.get("sort_name_az", "name A-Z"),
            "name_desc": t.get("sort_name_za", "name Z-A"),
            "browser_asc": t.get("sort_browser_az", "browser A-Z"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        parts.append(
            t.get("filter_sorted_by", "sorted by {sort}").replace("{sort}", sort_label)
        )

        return ", ".join(parts) if parts else "All"
