"""Installed Applications Report Module.

Displays installed applications extracted from OS artifacts with tag filtering.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader

from ...dates import format_date
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class InstalledApplicationsModule(BaseReportModule):
    """Module for displaying installed applications in reports."""

    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="installed_applications",
            name="Installed Applications",
            description="Displays installed applications from OS artifacts with tag filters",
            category="System",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return configurable filter fields."""
        return [
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional heading displayed above the applications table",
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
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of applications to show",
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
                key="show_publisher",
                label="Show Publisher",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Publisher column",
                required=False,
            ),
            FilterField(
                key="show_version",
                label="Show Version",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Version column",
                required=False,
            ),
            FilterField(
                key="show_install_date",
                label="Show Install Date",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Install Date column",
                required=False,
            ),
            FilterField(
                key="show_size",
                label="Show Size",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Size column",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display active filters below the table",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag filter."""
        if key != "tag_filter":
            return None

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
                WHERE ta.artifact_type = 'installed_software'
                ORDER BY t.name
                """
            )
            for (tag_name,) in cursor.fetchall():
                options.append((tag_name, tag_name))
        except Exception:
            pass

        return options

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the installed applications list as HTML."""
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        raw_section_title = config.get("section_title", "")
        section_title = (
            raw_section_title.strip() if isinstance(raw_section_title, str) else ""
        )
        if not section_title:
            section_title = translations.get(
                "installed_applications_title", "Installed Applications"
            )

        limit = config.get("limit", "100")
        tag_filter = config.get("tag_filter", self.ALL)
        show_publisher = bool(config.get("show_publisher", True))
        show_version = bool(config.get("show_version", True))
        show_install_date = bool(config.get("show_install_date", True))
        show_size = bool(config.get("show_size", True))
        show_filter_info = bool(config.get("show_filter_info", False))

        query, params = self._build_query(evidence_id, tag_filter)

        applications: List[Dict[str, str]] = []
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
                extra = self._parse_extra_json(row["extra_json"])
                name = row["value"] or extra.get("name") or row["name"] or ""
                install_date = extra.get("install_date_formatted") or extra.get(
                    "install_date", ""
                )
                applications.append(
                    {
                        "name": name,
                        "publisher": str(extra.get("publisher", "") or ""),
                        "version": str(extra.get("version", "") or ""),
                        "install_date": format_date(install_date, date_format)
                        if install_date
                        else "",
                        "install_location": str(
                            extra.get("install_location", "") or ""
                        ),
                        "size": self._format_size_kb(extra.get("size_kb")),
                    }
                )
        except Exception as exc:
            return (
                '<div class="module-error">'
                f"Error loading installed applications: {exc}"
                "</div>"
            )

        shown_count = len(applications)
        is_truncated = shown_count < total_count
        filter_desc = self._build_filter_description(tag_filter, translations)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            applications=applications,
            section_title=section_title,
            show_publisher=show_publisher,
            show_version=show_version,
            show_install_date=show_install_date,
            show_size=show_size,
            show_filter_info=show_filter_info,
            filter_description=filter_desc,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            t=translations,
            locale=locale,
        )

    def _build_query(self, evidence_id: int, tag_filter: str) -> tuple[str, list[Any]]:
        """Build SQL query based on selected tag filter."""
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = [
            "oi.evidence_id = ?",
            "oi.type = 'system:installed_software'",
        ]

        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = oi.id
                    AND ta.artifact_type = 'installed_software'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = oi.id
                    AND ta.artifact_type = 'installed_software'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT
                oi.id,
                oi.name,
                oi.value,
                oi.extra_json
            FROM os_indicators oi
            {join_clause}
            WHERE {where_clause}
            ORDER BY COALESCE(oi.value, oi.name, '') COLLATE NOCASE ASC
        """
        return query, params

    def _build_filter_description(self, tag_filter: str, t: Dict[str, str]) -> str:
        """Build human-readable filter description."""
        if tag_filter == self.ANY_TAG:
            return t.get("filter_any_tag", "with any tag")
        if tag_filter != self.ALL:
            template = t.get("filter_tagged", 'tagged "{tag}"')
            return template.replace("{tag}", tag_filter)
        return t.get("filter_all_tags", "all tags")

    @staticmethod
    def _parse_extra_json(extra_json: str | None) -> Dict[str, Any]:
        """Parse extra_json into a dictionary."""
        if not extra_json:
            return {}
        try:
            parsed = json.loads(extra_json)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _format_size_kb(size_kb: Any) -> str:
        """Format size in KB into a readable string."""
        if size_kb in (None, ""):
            return ""
        try:
            kb = int(float(str(size_kb)))
        except (TypeError, ValueError):
            return str(size_kb)

        if kb < 1024:
            return f"{kb:,} KB"

        mb = kb / 1024
        if mb < 1024:
            return f"{mb:.1f} MB"

        gb = mb / 1024
        return f"{gb:.2f} GB"
