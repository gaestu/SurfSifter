"""Appendix Cookies Module.

Displays a compact list of cookies for use as appendix reference
material. Supports multi-select tag filtering, optional browser
filtering, and optional grouping by domain.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixCookiesModule(BaseAppendixModule):
    """Appendix module for listing cookies."""

    ALL_BROWSERS = "all"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_cookies",
            name="Cookies",
            description=(
                "Lists cookies with optional grouping by domain, tag and "
                "browser filters"
            ),
            category="Appendix",
            icon="🍪",
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
                key="group_by_domain",
                label="Group by Domain",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Group cookies by their domain",
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
                key="show_path",
                label="Show Path",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Path column",
                required=False,
            ),
            FilterField(
                key="show_value",
                label="Show Value",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Value column",
                required=False,
            ),
            FilterField(
                key="show_expires",
                label="Show Expires",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Expires column",
                required=False,
            ),
            FilterField(
                key="show_last_access",
                label="Show Last Access",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Last Access column",
                required=False,
            ),
            FilterField(
                key="show_creation",
                label="Show Creation Time",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Creation Time column",
                required=False,
            ),
            FilterField(
                key="show_flags",
                label="Show Flags (Secure / HttpOnly / SameSite)",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show a Flags column with Secure / HttpOnly / SameSite",
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
                    WHERE ta.artifact_type = 'cookie'
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
        group_by_domain = bool(config.get("group_by_domain", True))
        show_browser = bool(config.get("show_browser", True))
        show_path = bool(config.get("show_path", False))
        show_value = bool(config.get("show_value", False))
        show_expires = bool(config.get("show_expires", True))
        show_last_access = bool(config.get("show_last_access", True))
        show_creation = bool(config.get("show_creation", False))
        show_flags = bool(config.get("show_flags", False))

        query, params = self._build_query(evidence_id, tag_filter, browser_filter)

        entries: List[Dict[str, Any]] = []
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                entries.append(
                    {
                        "name": row["name"] or "",
                        "domain": row["domain"] or "",
                        "value": row["value"] or "",
                        "path": row["path"] or "",
                        "browser": row["browser"] or "",
                        "expires": self._format_dt(row["expires_utc"], date_format),
                        "creation": self._format_dt(row["creation_utc"], date_format),
                        "last_access": self._format_dt(
                            row["last_access_utc"], date_format
                        ),
                        "flags": self._format_flags(
                            row["is_secure"],
                            row["is_httponly"],
                            row["samesite"],
                        ),
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading cookies: {exc}</div>'
        finally:
            db_conn.row_factory = orig_row_factory

        # Always-shown main columns: Name + Value-class column area is
        # built per visibility flags. Compute number of columns on the
        # main row for the colspan of grouped section headers.
        col_count = 1  # Name (always shown)
        if not group_by_domain:
            col_count += 1  # Domain column shown inline
        if show_browser:
            col_count += 1
        if show_path:
            col_count += 1
        if show_value:
            col_count += 1
        if show_expires:
            col_count += 1
        if show_last_access:
            col_count += 1
        if show_creation:
            col_count += 1
        if show_flags:
            col_count += 1

        grouped = None
        if group_by_domain:
            grouped = self._group_by_domain(entries)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            grouped=grouped,
            group_by_domain=group_by_domain,
            show_browser=show_browser,
            show_path=show_path,
            show_value=show_value,
            show_expires=show_expires,
            show_last_access=show_last_access,
            show_creation=show_creation,
            show_flags=show_flags,
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

    @staticmethod
    def _format_flags(
        is_secure: Optional[int],
        is_httponly: Optional[int],
        samesite: Optional[str],
    ) -> str:
        parts: List[str] = []
        if is_secure:
            parts.append("Secure")
        if is_httponly:
            parts.append("HttpOnly")
        if samesite:
            parts.append(str(samesite))
        return ", ".join(parts)

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        browser_filter: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["c.evidence_id = ?"]

        if browser_filter and browser_filter != self.ALL_BROWSERS:
            conditions.append("c.browser = ?")
            params.append(browser_filter)

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = c.id
                      AND ta.artifact_type = 'cookie'
                      AND ta.evidence_id = c.evidence_id
                      AND t.name IN ({placeholders})
                )
                """
            )
            params.extend(tag_filter)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT c.id, c.name, c.domain, c.value, c.path,
                   c.browser, c.expires_utc, c.creation_utc,
                   c.last_access_utc, c.is_secure, c.is_httponly, c.samesite
            FROM cookies c
            WHERE {where_clause}
            ORDER BY c.domain ASC, c.name ASC
        """
        return query, params

    def _group_by_domain(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            key = entry["domain"] or "(unknown)"
            groups[key].append(entry)

        result: List[Dict[str, Any]] = []
        for domain in sorted(groups.keys(), key=str.lower):
            result.append({"domain": domain, "entries": groups[domain]})
        return result
