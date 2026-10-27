"""Appendix Jump Lists Module.

Displays a compact list of Windows Jump List entries for use as
appendix reference material. Supports multi-select tag filtering and
optional grouping by application.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

try:
    from core.appid_loader import get_app_name as _registry_get_app_name
except ImportError:  # pragma: no cover - optional dependency at runtime
    _registry_get_app_name = None

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixJumpListsModule(BaseAppendixModule):
    """Appendix module for listing Windows Jump List entries."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_jump_lists",
            name="Jump Lists",
            description=(
                "Lists Windows Jump List entries with optional grouping by "
                "application and tag filters"
            ),
            category="Appendix",
            icon="🪟",
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
                key="group_by_application",
                label="Group by Application",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Group entries by their application name",
                required=False,
            ),
            FilterField(
                key="show_target_path",
                label="Show Target Path",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Target Path column",
                required=False,
            ),
            FilterField(
                key="show_url",
                label="Show URL",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the URL column",
                required=False,
            ),
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Title column",
                required=False,
            ),
            FilterField(
                key="show_access_time",
                label="Show Access Time",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the Access Time column",
                required=False,
            ),
            FilterField(
                key="show_creation_time",
                label="Show Creation Time",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the Creation Time column",
                required=False,
            ),
            FilterField(
                key="show_appid",
                label="Show App ID",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the App ID column",
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
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        tag_filter: List[str] = list(config.get("tag_filter") or [])
        group_by_application = bool(config.get("group_by_application", True))
        show_target_path = bool(config.get("show_target_path", True))
        show_url = bool(config.get("show_url", False))
        show_title = bool(config.get("show_title", False))
        show_access_time = bool(config.get("show_access_time", True))
        show_creation_time = bool(config.get("show_creation_time", False))
        show_appid = bool(config.get("show_appid", False))

        query, params = self._build_query(evidence_id, tag_filter)

        entries: List[Dict[str, Any]] = []
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                appid = row["appid"] or ""
                browser = row["browser"] or ""
                entries.append(
                    {
                        "application": self._resolve_app_name(browser, appid),
                        "appid": appid,
                        "title": row["title"] or "",
                        "url": row["url"] or "",
                        "target_path": row["target_path"] or "",
                        "access_time": self._format_dt(
                            row["lnk_access_time"], date_format
                        ),
                        "creation_time": self._format_dt(
                            row["lnk_creation_time"], date_format
                        ),
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading jump list entries: {exc}</div>'
        finally:
            db_conn.row_factory = orig_row_factory

        # Count visible columns on the MAIN row. Target Path is moved
        # to a separate detail row spanning the table, so it does NOT
        # count toward the main-row column total.
        col_count = sum(
            [
                show_title,
                show_url,
                show_access_time,
                show_creation_time,
                show_appid,
            ]
        )
        if not group_by_application:
            col_count += 1  # application column shown inline
        col_count = max(col_count, 1)

        grouped = None
        if group_by_application:
            grouped = self._group_by_application(entries)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            grouped=grouped,
            group_by_application=group_by_application,
            show_title=show_title,
            show_url=show_url,
            show_target_path=show_target_path,
            show_access_time=show_access_time,
            show_creation_time=show_creation_time,
            show_appid=show_appid,
            col_count=col_count,
            total_count=len(entries),
            t=translations,
            locale=locale,
        )

    @staticmethod
    def _resolve_app_name(browser: str, appid: str) -> str:
        """Mirror of ``JumpListsModule._resolve_app_name``."""
        if browser:
            return browser
        if appid and _registry_get_app_name is not None:
            name = _registry_get_app_name(appid)
            if not name.startswith("Unknown"):
                return name
        return appid or ""

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
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["j.evidence_id = ?"]

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = j.id
                      AND ta.artifact_type = 'jump_list'
                      AND ta.evidence_id = j.evidence_id
                      AND t.name IN ({placeholders})
                )
                """
            )
            params.extend(tag_filter)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT j.id, j.appid, j.browser, j.target_path,
                   j.title, j.url, j.lnk_access_time, j.lnk_creation_time
            FROM jump_list_entries j
            WHERE {where_clause}
            ORDER BY j.lnk_access_time DESC
        """
        return query, params

    def _group_by_application(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            key = entry["application"] or "(unknown)"
            groups[key].append(entry)

        result: List[Dict[str, Any]] = []
        for app in sorted(groups.keys(), key=str.lower):
            result.append({"application": app, "entries": groups[app]})
        return result
