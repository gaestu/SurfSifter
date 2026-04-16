"""Site Permissions Report Module.

Displays tagged site permission entries with multi-tag filtering.
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


class SitePermissionsModule(BaseReportModule):
    """Module for displaying tagged site permissions in reports."""

    UNLIMITED = "unlimited"
    ALL_BROWSERS = "all"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="site_permissions",
            name="Site Permissions",
            description="Displays tagged site permission entries (notifications, geolocation, camera, etc.)",
            category="Browser",
            icon="🔐",
        )

    def get_filter_fields(self) -> List[FilterField]:
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
                default=False,
                help_text="Show a brief explanation of site permissions",
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
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
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
            ),
            FilterField(
                key="permission_type_filter",
                label="Permission Type",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=[
                    ("all", "All Types"),
                ],
                help_text="Filter by permission type (types loaded dynamically)",
            ),
            FilterField(
                key="permission_value_filter",
                label="Permission Value",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=[
                    ("all", "All Values"),
                ],
                help_text="Filter by permission value (ALLOW, DENY, etc.)",
            ),
            # ── 4. Display options ──────────────────────────────────
            FilterField(
                key="show_browser",
                label="Show Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the browser column",
            ),
            FilterField(
                key="show_profile",
                label="Show Profile",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the browser profile column",
            ),
            FilterField(
                key="show_granted_at",
                label="Show Granted At",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the granted-at timestamp column",
            ),
            FilterField(
                key="show_expires_at",
                label="Show Expires At",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the expiration timestamp column",
            ),
            # ── 5. Sort & limit ─────────────────────────────────────
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="origin_asc",
                options=[
                    ("origin_asc", "Origin (A-Z)"),
                    ("origin_desc", "Origin (Z-A)"),
                    ("type_asc", "Permission Type (A-Z)"),
                    ("type_desc", "Permission Type (Z-A)"),
                    ("granted_desc", "Granted At (Newest First)"),
                    ("granted_asc", "Granted At (Oldest First)"),
                ],
                help_text="Sort order for the permissions list",
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
                help_text="Maximum number of entries to show",
            ),
            # ── 6. Footer ──────────────────────────────────────────
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the list",
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'site_permission'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "browser_filter":
            options = [(self.ALL_BROWSERS, "All Browsers")]
            try:
                cursor = db_conn.execute(
                    "SELECT DISTINCT browser FROM site_permissions ORDER BY browser"
                )
                for (browser,) in cursor.fetchall():
                    if browser:
                        options.append((browser, browser))
            except Exception:
                pass
            return options

        if key == "permission_type_filter":
            options = [("all", "All Types")]
            try:
                cursor = db_conn.execute(
                    "SELECT DISTINCT permission_type FROM site_permissions ORDER BY permission_type"
                )
                for (ptype,) in cursor.fetchall():
                    if ptype:
                        options.append((ptype, ptype))
            except Exception:
                pass
            return options

        if key == "permission_value_filter":
            options = [("all", "All Values")]
            try:
                cursor = db_conn.execute(
                    "SELECT DISTINCT permission_value FROM site_permissions ORDER BY permission_value"
                )
                for (pval,) in cursor.fetchall():
                    if pval:
                        options.append((pval, pval))
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
        t = translations

        # Standard fields
        show_title = config.get("show_title", True)
        custom_title = config.get("custom_title", "")
        title_text = custom_title or t.get("site_permissions_title", self.metadata.name)

        show_description = config.get("show_description", False)
        custom_description = config.get("custom_description", "")
        description_text = custom_description or t.get(
            "site_permissions_description",
            "Site permissions control what capabilities websites are granted, "
            "such as notifications, geolocation, camera, and microphone access.",
        )

        show_filter_info = config.get("show_filter_info", False)

        # Data filters
        tag_filter: List[str] = config.get("tag_filter") or []
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        permission_type_filter = config.get("permission_type_filter", "all")
        permission_value_filter = config.get("permission_value_filter", "all")

        # Display options
        show_browser = config.get("show_browser", True)
        show_profile = config.get("show_profile", False)
        show_granted_at = config.get("show_granted_at", True)
        show_expires_at = config.get("show_expires_at", False)

        # Sort & limit
        sort_by = config.get("sort_by", "origin_asc")
        limit = config.get("limit", "100")

        # Build and execute query
        query, params = self._build_query(
            evidence_id, tag_filter, browser_filter,
            permission_type_filter, permission_value_filter, sort_by,
        )

        permissions: List[Dict[str, Any]] = []
        total_count = 0
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            if limit != self.UNLIMITED:
                all_rows = all_rows[: int(limit)]

            for row in all_rows:
                permissions.append(
                    {
                        "origin": row["origin"] or "",
                        "permission_type": row["permission_type"] or "",
                        "permission_value": row["permission_value"] or "",
                        "browser": row["browser"] or "",
                        "profile": row["profile"] or "",
                        "granted_at": self._format_dt(row["granted_at_utc"], date_format),
                        "expires_at": self._format_dt(row["expires_at_utc"], date_format),
                    }
                )
        except Exception as e:
            return f'<div class="module-error">Error loading site permissions: {e}</div>'

        shown_count = len(permissions)
        is_truncated = shown_count < total_count

        filter_desc = self._build_filter_description(
            tag_filter, browser_filter, permission_type_filter,
            permission_value_filter, sort_by, t,
        )

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            permissions=permissions,
            show_title=show_title,
            title_text=title_text,
            show_description=show_description,
            description_text=description_text,
            show_browser=show_browser,
            show_profile=show_profile,
            show_granted_at=show_granted_at,
            show_expires_at=show_expires_at,
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
        tag_filter: List[str],
        browser_filter: str,
        permission_type_filter: str,
        permission_value_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["sp.evidence_id = ?"]

        # Tag filter (multi-select OR logic)
        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = sp.id
                      AND ta.artifact_type = 'site_permission'
                      AND ta.evidence_id = sp.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        if browser_filter != self.ALL_BROWSERS:
            conditions.append("sp.browser = ?")
            params.append(browser_filter)

        if permission_type_filter != "all":
            conditions.append("sp.permission_type = ?")
            params.append(permission_type_filter)

        if permission_value_filter != "all":
            conditions.append("sp.permission_value = ?")
            params.append(permission_value_filter)

        order_map = {
            "origin_asc": "sp.origin ASC",
            "origin_desc": "sp.origin DESC",
            "type_asc": "sp.permission_type ASC, sp.origin ASC",
            "type_desc": "sp.permission_type DESC, sp.origin ASC",
            "granted_desc": "sp.granted_at_utc DESC NULLS LAST",
            "granted_asc": "sp.granted_at_utc ASC NULLS FIRST",
        }
        order_by = order_map.get(sort_by, "sp.origin ASC")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                sp.id, sp.origin, sp.permission_type, sp.permission_value,
                sp.browser, sp.profile, sp.granted_at_utc, sp.expires_at_utc
            FROM site_permissions sp
            WHERE {where_clause}
            ORDER BY {order_by}
        """

        return query, params

    def _format_dt(self, value: Optional[str], date_format: str) -> str:
        if not value:
            return ""
        try:
            return format_datetime(value, date_format)
        except Exception:
            return value

    def _build_filter_description(
        self,
        tag_filter: List[str],
        browser_filter: str,
        permission_type_filter: str,
        permission_value_filter: str,
        sort_by: str,
        t: Dict[str, str],
    ) -> str:
        parts: List[str] = []

        if tag_filter:
            parts.append(f"Tags: {', '.join(tag_filter)}")

        if browser_filter != self.ALL_BROWSERS:
            parts.append(f"Browser: {browser_filter}")

        if permission_type_filter != "all":
            parts.append(f"Type: {permission_type_filter}")

        if permission_value_filter != "all":
            parts.append(f"Value: {permission_value_filter}")

        sort_labels = {
            "origin_asc": "Origin A-Z",
            "origin_desc": "Origin Z-A",
            "type_asc": "Type A-Z",
            "type_desc": "Type Z-A",
            "granted_desc": "Newest first",
            "granted_asc": "Oldest first",
        }
        parts.append(f"Sorted by: {sort_labels.get(sort_by, sort_by)}")

        return ", ".join(parts)
