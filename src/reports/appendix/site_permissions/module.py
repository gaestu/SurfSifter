"""Appendix Site Permissions Module.

Displays a compact list of site permission entries (notifications,
geolocation, camera, microphone, etc.) for use as appendix reference
material. Supports multi-select tag filtering, browser / permission
type / permission value filtering, and optional grouping by origin
or browser.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixSitePermissionsModule(BaseAppendixModule):
    """Appendix module for listing site permission entries."""

    ALL_BROWSERS = "all"
    ALL_TYPES = "all"
    ALL_VALUES = "all"

    GROUP_NONE = "none"
    GROUP_ORIGIN = "origin"
    GROUP_BROWSER = "browser"
    GROUP_TYPE = "permission_type"

    MODE_OR = "or"
    MODE_AND = "and"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_site_permissions",
            name="Site Permissions",
            description=(
                "Lists site permission entries with optional grouping "
                "and tag/browser/type/value filters"
            ),
            category="Appendix",
            icon="🔐",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            # ── Data filters ───────────────────────────────────────
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
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
                key="permission_type_filter",
                label="Permission Type",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL_TYPES,
                options=[
                    (self.ALL_TYPES, "All Types"),
                ],
                help_text="Filter by permission type (types loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="permission_value_filter",
                label="Permission Value",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL_VALUES,
                options=[
                    (self.ALL_VALUES, "All Values"),
                ],
                help_text="Filter by permission value (ALLOW, DENY, etc.)",
                required=False,
            ),
            # ── Grouping ───────────────────────────────────────────
            FilterField(
                key="group_by",
                label="Group By",
                filter_type=FilterType.DROPDOWN,
                default=self.GROUP_ORIGIN,
                options=[
                    (self.GROUP_NONE, "No grouping"),
                    (self.GROUP_ORIGIN, "Origin"),
                    (self.GROUP_BROWSER, "Browser"),
                    (self.GROUP_TYPE, "Permission Type"),
                ],
                help_text="Group entries by the selected field",
                required=False,
            ),
            # ── Display options ────────────────────────────────────
            FilterField(
                key="show_origin",
                label="Show Origin",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Origin column",
                required=False,
            ),
            FilterField(
                key="show_type",
                label="Show Permission Type",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Permission Type column",
                required=False,
            ),
            FilterField(
                key="show_value",
                label="Show Permission Value",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Permission Value column",
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
                key="show_granted_at",
                label="Show Granted At",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the granted-at timestamp column",
                required=False,
            ),
            FilterField(
                key="show_expires_at",
                label="Show Expires At",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the expiration timestamp column",
                required=False,
            ),
            # ── Sort ───────────────────────────────────────────────
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
            options: List[tuple] = [(self.ALL_BROWSERS, "All Browsers")]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT browser
                    FROM site_permissions
                    WHERE browser IS NOT NULL AND browser != ''
                    ORDER BY browser
                    """
                )
                for (browser,) in cursor.fetchall():
                    options.append((browser, browser.capitalize()))
            except Exception:
                pass
            return options

        if key == "permission_type_filter":
            options = [(self.ALL_TYPES, "All Types")]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT permission_type
                    FROM site_permissions
                    WHERE permission_type IS NOT NULL AND permission_type != ''
                    ORDER BY permission_type
                    """
                )
                for (ptype,) in cursor.fetchall():
                    options.append((ptype, ptype))
            except Exception:
                pass
            return options

        if key == "permission_value_filter":
            options = [(self.ALL_VALUES, "All Values")]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT permission_value
                    FROM site_permissions
                    WHERE permission_value IS NOT NULL AND permission_value != ''
                    ORDER BY permission_value
                    """
                )
                for (pval,) in cursor.fetchall():
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

        tag_filter: List[str] = list(config.get("tag_filter") or [])
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        permission_type_filter = config.get(
            "permission_type_filter", self.ALL_TYPES
        )
        permission_value_filter = config.get(
            "permission_value_filter", self.ALL_VALUES
        )
        group_by = config.get("group_by", self.GROUP_ORIGIN)
        show_origin = bool(config.get("show_origin", True))
        show_type = bool(config.get("show_type", True))
        show_value = bool(config.get("show_value", True))
        show_browser = bool(config.get("show_browser", True))
        show_profile = bool(config.get("show_profile", False))
        show_granted_at = bool(config.get("show_granted_at", True))
        show_expires_at = bool(config.get("show_expires_at", False))
        sort_by = config.get("sort_by", "origin_asc")

        query, params = self._build_query(
            evidence_id,
            tag_filter,
            browser_filter,
            permission_type_filter,
            permission_value_filter,
            sort_by,
        )

        entries: List[Dict[str, Any]] = []
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                entries.append(
                    {
                        "origin": row["origin"] or "",
                        "permission_type": row["permission_type"] or "",
                        "permission_value": row["permission_value"] or "",
                        "browser": (row["browser"] or "").capitalize(),
                        "browser_raw": row["browser"] or "",
                        "profile": row["profile"] or "",
                        "granted_at": self._format_dt(
                            row["granted_at_utc"], date_format
                        ),
                        "expires_at": self._format_dt(
                            row["expires_at_utc"], date_format
                        ),
                    }
                )
        except Exception as exc:
            return (
                f'<div class="module-error">'
                f"Error loading site permissions: {exc}</div>"
            )
        finally:
            db_conn.row_factory = orig_row_factory

        grouped = None
        if group_by != self.GROUP_NONE:
            grouped = self._group_entries(entries, group_by)

        template_dir = get_module_template_dir(__file__)
        env = Environment(
            loader=FileSystemLoader(template_dir), autoescape=True
        )
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            grouped=grouped,
            group_by=group_by,
            group_none=self.GROUP_NONE,
            group_origin=self.GROUP_ORIGIN,
            group_browser=self.GROUP_BROWSER,
            group_type=self.GROUP_TYPE,
            show_origin=show_origin,
            show_type=show_type,
            show_value=show_value,
            show_browser=show_browser,
            show_profile=show_profile,
            show_granted_at=show_granted_at,
            show_expires_at=show_expires_at,
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
        permission_type_filter: str,
        permission_value_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["sp.evidence_id = ?"]

        if browser_filter and browser_filter != self.ALL_BROWSERS:
            conditions.append("sp.browser = ?")
            params.append(browser_filter)

        if permission_type_filter and permission_type_filter != self.ALL_TYPES:
            conditions.append("sp.permission_type = ?")
            params.append(permission_type_filter)

        if (
            permission_value_filter
            and permission_value_filter != self.ALL_VALUES
        ):
            conditions.append("sp.permission_value = ?")
            params.append(permission_value_filter)

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = sp.id
                      AND ta.artifact_type = 'site_permission'
                      AND ta.evidence_id = sp.evidence_id
                      AND t.name IN ({placeholders})
                )
                """
            )
            params.extend(tag_filter)

        order_clause = self._get_order_clause(sort_by)
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT sp.id, sp.origin, sp.permission_type,
                   sp.permission_value, sp.browser, sp.profile,
                   sp.granted_at_utc, sp.expires_at_utc
            FROM site_permissions sp
            WHERE {where_clause}
            {order_clause}
            LIMIT 5000
        """
        return query, params

    @staticmethod
    def _get_order_clause(sort_by: str) -> str:
        order_map = {
            "origin_asc": "ORDER BY sp.origin ASC, sp.permission_type ASC",
            "origin_desc": "ORDER BY sp.origin DESC, sp.permission_type ASC",
            "type_asc": "ORDER BY sp.permission_type ASC, sp.origin ASC",
            "type_desc": "ORDER BY sp.permission_type DESC, sp.origin ASC",
            "granted_desc": "ORDER BY sp.granted_at_utc DESC",
            "granted_asc": "ORDER BY sp.granted_at_utc ASC",
        }
        return order_map.get(sort_by, "ORDER BY sp.origin ASC")

    def _group_entries(
        self, entries: List[Dict[str, Any]], group_by: str
    ) -> List[Dict[str, Any]]:
        key_map = {
            self.GROUP_ORIGIN: "origin",
            self.GROUP_BROWSER: "browser",
            self.GROUP_TYPE: "permission_type",
        }
        key = key_map.get(group_by, "origin")

        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            label = entry.get(key) or "(unknown)"
            groups[label].append(entry)

        result: List[Dict[str, Any]] = []
        for label in sorted(groups.keys(), key=str.lower):
            result.append({"label": label, "entries": groups[label]})
        return result
