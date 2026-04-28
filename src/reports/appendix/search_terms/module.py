"""Appendix Search Terms Module.

Lists browser search terms for the appendix with multi-select tag, browser
and search engine filters. Mirrors the conventions of other appendix modules
(see ``appendix/url_list/`` and ``appendix/_example/``).
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixSearchTermsModule(BaseAppendixModule):
    """Appendix module for listing browser search terms."""

    MODE_OR = "or"
    MODE_AND = "and"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_search_terms",
            name="Search Terms",
            description=(
                "Lists browser search terms with tag, browser and search "
                "engine filters"
            ),
            category="Appendix",
            icon="🔍",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            # ── 1. Data filters ────────────────────────────────────────
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text=(
                    "Filter by one or more tags "
                    "(leave all unchecked to include everything)"
                ),
                required=False,
            ),
            FilterField(
                key="browser_filter",
                label="Browsers",
                filter_type=FilterType.MULTI_SELECT,
                default=[],
                options=[],
                help_text=(
                    "Filter by one or more browsers "
                    "(leave all unchecked to include everything)"
                ),
                required=False,
            ),
            FilterField(
                key="engine_filter",
                label="Search Engines",
                filter_type=FilterType.MULTI_SELECT,
                default=[],
                options=[],
                help_text=(
                    "Filter by one or more search engines "
                    "(leave all unchecked to include everything)"
                ),
                required=False,
            ),
            # ── 2. Filter combination mode ─────────────────────────────
            FilterField(
                key="filter_mode",
                label="Tag/Browser/Engine Mode",
                filter_type=FilterType.DROPDOWN,
                default=self.MODE_OR,
                options=[
                    (self.MODE_OR, "OR — Match any selected tag, browser or engine"),
                    (self.MODE_AND, "AND — Must match a selected tag AND browser/engine"),
                ],
                help_text=(
                    "How to combine the tag filter with browser/engine "
                    "filters when both have selections."
                ),
                required=False,
            ),
            # ── 3. Display options ─────────────────────────────────────
            FilterField(
                key="show_url",
                label="Show URL Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the URL column",
                required=False,
            ),
            FilterField(
                key="show_browser",
                label="Show Browser Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the browser column",
                required=False,
            ),
            FilterField(
                key="show_engine",
                label="Show Search Engine Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the search engine column",
                required=False,
            ),
            FilterField(
                key="show_timestamp",
                label="Show Timestamp Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the search time column",
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
                key="show_count",
                label="Show Count",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the total number of search terms in the heading",
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
                    WHERE ta.artifact_type = 'browser_search_term'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "browser_filter":
            options = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT browser
                    FROM browser_search_terms
                    WHERE browser IS NOT NULL AND browser != ''
                    ORDER BY browser
                    """
                )
                for (browser,) in cursor.fetchall():
                    options.append((browser, browser.capitalize()))
            except Exception:
                pass
            return options

        if key == "engine_filter":
            options = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT search_engine
                    FROM browser_search_terms
                    WHERE search_engine IS NOT NULL AND search_engine != ''
                    ORDER BY search_engine
                    """
                )
                for (engine,) in cursor.fetchall():
                    options.append((engine, engine))
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
        if isinstance(config.get("tag_filter"), str):
            tag_filter = [config["tag_filter"]]
        browser_filter: List[str] = list(config.get("browser_filter") or [])
        engine_filter: List[str] = list(config.get("engine_filter") or [])
        filter_mode = config.get("filter_mode", self.MODE_OR)

        show_url = bool(config.get("show_url", False))
        show_browser = bool(config.get("show_browser", True))
        show_engine = bool(config.get("show_engine", True))
        show_timestamp = bool(config.get("show_timestamp", True))
        show_profile = bool(config.get("show_profile", False))
        show_count = bool(config.get("show_count", True))

        query, params = self._build_query(
            evidence_id, tag_filter, browser_filter, engine_filter, filter_mode
        )

        items: List[Dict[str, Any]] = []
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                items.append(
                    {
                        "term": row["term"] or "",
                        "url": row["url"] or "",
                        "browser": (row["browser"] or "").capitalize(),
                        "search_engine": row["search_engine"] or "",
                        "search_time": format_datetime(
                            row["search_time_utc"], date_format
                        ) if row["search_time_utc"] else "",
                        "profile": row["profile"] or "",
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading search terms: {exc}</div>'
        finally:
            db_conn.row_factory = orig_row_factory

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            items=items,
            total_count=len(items),
            show_url=show_url,
            show_browser=show_browser,
            show_engine=show_engine,
            show_timestamp=show_timestamp,
            show_profile=show_profile,
            show_count=show_count,
            t=translations,
            locale=locale,
        )

    # ──────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        browser_filter: List[str],
        engine_filter: List[str],
        filter_mode: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["s.evidence_id = ?"]

        tag_condition: Optional[str] = None
        be_condition: Optional[str] = None
        be_params: list[Any] = []

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = s.id
                      AND ta.artifact_type = 'browser_search_term'
                      AND ta.evidence_id = s.evidence_id
                      AND t.name IN ({placeholders})
                )
            """

        # Browser/engine filters are always combined with OR among themselves
        # (e.g. "Chromium OR Firefox" and "Google OR Bing"). The
        # ``filter_mode`` controls how the tag filter combines with the
        # browser/engine selection.
        be_parts: list[str] = []
        if browser_filter:
            placeholders = ", ".join(["?"] * len(browser_filter))
            be_parts.append(f"s.browser IN ({placeholders})")
            be_params.extend(browser_filter)
        if engine_filter:
            placeholders = ", ".join(["?"] * len(engine_filter))
            be_parts.append(f"s.search_engine IN ({placeholders})")
            be_params.extend(engine_filter)
        if be_parts:
            be_condition = "(" + " OR ".join(be_parts) + ")"

        if filter_mode == self.MODE_AND and tag_condition and be_condition:
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(be_condition)
            params.extend(be_params)
        elif tag_condition or be_condition:
            or_parts: list[str] = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if be_condition:
                or_parts.append(be_condition)
                params.extend(be_params)
            conditions.append("(" + " OR ".join(or_parts) + ")")

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT
                s.term,
                s.url,
                s.browser,
                s.search_engine,
                s.search_time_utc,
                s.profile
            FROM browser_search_terms s
            WHERE {where_clause}
            ORDER BY s.term ASC, s.search_time_utc DESC
        """
        return query, params
