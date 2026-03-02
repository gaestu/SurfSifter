"""Application Execution Report Module.

Displays Windows UserAssist execution artifacts with tag filtering and
optional columns.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import PureWindowsPath
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


class ApplicationExecutionModule(BaseReportModule):
    """Module for displaying application execution entries in reports."""

    ALL = "all"
    ANY_TAG = "any_tag"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="application_execution",
            name="Application Execution",
            description="Displays UserAssist-based application execution artifacts with tag filters",
            category="System",
            icon="▶️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for title/description, tags, and optional columns."""
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
                key="section_description",
                label="Section Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional custom description text (overrides default description)",
                required=False,
            ),
            FilterField(
                key="show_default_description",
                label="Show Default Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show a built-in description explaining application execution artifacts",
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
                key="show_run_count",
                label="Show Run Count",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Run Count column",
                required=False,
            ),
            FilterField(
                key="show_focus_time",
                label="Show Focus Time",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Focus Time column",
                required=False,
            ),
            FilterField(
                key="show_focus_count",
                label="Show Focus Count",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Focus Count column",
                required=False,
            ),
            FilterField(
                key="show_source",
                label="Show Source",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Source column",
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
                WHERE ta.artifact_type = 'app_execution'
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
        """Render the application execution table as HTML."""
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        section_title = config.get("section_title", "")
        section_description = config.get("section_description", "")
        show_default_description = bool(config.get("show_default_description", True))
        tag_filter = config.get("tag_filter", self.ALL)
        show_run_count = bool(config.get("show_run_count", False))
        show_focus_time = bool(config.get("show_focus_time", False))
        show_focus_count = bool(config.get("show_focus_count", False))
        show_source = bool(config.get("show_source", False))

        query, params = self._build_query(evidence_id, tag_filter)

        entries: list[dict[str, Any]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)

            for row in cursor.fetchall():
                extra = self._parse_extra_json(row["extra_json"])
                hive = row["hive"] or ""
                focus_time = self._resolve_focus_time(extra)
                last_run_raw = extra.get("last_run_utc") or extra.get("last_run")

                entries.append(
                    {
                        "application_path": (
                            str(extra.get("decoded_path") or row["value"] or "").strip()
                        ),
                        "run_count": self._to_int_or_none(extra.get("run_count")),
                        "last_run": self._format_last_run(last_run_raw, date_format),
                        "focus_time": focus_time,
                        "focus_count": self._to_int_or_none(extra.get("focus_count")),
                        "source": self._format_source(hive),
                    }
                )
        except Exception as exc:
            return (
                '<div class="module-error">'
                f"Error loading application execution data: {exc}"
                "</div>"
            )

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            section_title=section_title,
            section_description=section_description,
            show_default_description=show_default_description,
            show_run_count=show_run_count,
            show_focus_time=show_focus_time,
            show_focus_count=show_focus_count,
            show_source=show_source,
            t=translations,
            locale=locale,
        )

    def _build_query(self, evidence_id: int, tag_filter: str) -> tuple[str, list[Any]]:
        """Build SQL query based on selected tag filter."""
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = [
            "oi.evidence_id = ?",
            "oi.type = 'execution:user_assist'",
        ]

        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = oi.id
                    AND ta.artifact_type = 'app_execution'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = oi.id
                    AND ta.artifact_type = 'app_execution'
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
                oi.value,
                oi.hive,
                oi.extra_json
            FROM os_indicators oi
            {join_clause}
            WHERE {where_clause}
            ORDER BY
                json_extract(oi.extra_json, '$.last_run_utc') DESC,
                json_extract(oi.extra_json, '$.run_count') DESC,
                COALESCE(oi.value, '') COLLATE NOCASE ASC
        """
        return query, params

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
    def _to_int_or_none(value: Any) -> int | None:
        """Convert value to int when possible."""
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_focus_time(milliseconds: int | None) -> str:
        """Format focus time from milliseconds to human-readable string."""
        if milliseconds is None or milliseconds <= 0:
            return ""

        seconds = milliseconds // 1000
        if seconds < 60:
            return f"{seconds}s"

        minutes = seconds // 60
        remaining_seconds = seconds % 60
        if minutes < 60:
            return f"{minutes}m {remaining_seconds}s"

        hours = minutes // 60
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"

    @staticmethod
    def _format_source(hive_path: str) -> str:
        """Return source filename from hive path."""
        if not hive_path:
            return ""
        try:
            return PureWindowsPath(hive_path).name or hive_path
        except Exception:
            return hive_path

    @staticmethod
    def _format_last_run(last_run_utc: Any, date_format: str) -> str:
        """Format last-run timestamp for report output."""
        if not last_run_utc:
            return ""
        return format_datetime(str(last_run_utc), date_format)

    def _resolve_focus_time(self, extra: Dict[str, Any]) -> str:
        """Resolve focus time from current or legacy keys."""
        formatted = self._format_focus_time(self._to_int_or_none(extra.get("focus_time_ms")))
        if formatted:
            return formatted

        legacy_value = extra.get("focus_time")
        if legacy_value is None:
            return ""
        return str(legacy_value).strip()
