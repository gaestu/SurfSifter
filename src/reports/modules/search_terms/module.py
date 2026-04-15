"""Search Terms Report Module.

Displays a table of browser search terms with multi-select tag filtering,
browser/search engine filters, and configurable column visibility.
"""

from __future__ import annotations

import html
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


class SearchTermsModule(BaseReportModule):
    """Module for displaying tagged search terms in reports."""

    # Special filter values
    ALL_BROWSERS = "all"
    ALL_ENGINES = "all"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="search_terms",
            name="Search Terms",
            description="Displays browser search terms with tag and browser filters",
            category="Browser",
            icon="🔍",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields following standard field order."""
        return [
            # ── 1. Title group ──────────────────────────────────────
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional custom heading (leave empty to use default title)",
                required=False,
            ),
            # ── 2. Description group ────────────────────────────────
            FilterField(
                key="section_description",
                label="Section Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Optional custom description text",
                required=False,
            ),
            # ── 3. Data filters ─────────────────────────────────────
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
                key="engine_filter",
                label="Search Engine",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL_ENGINES,
                options=[
                    (self.ALL_ENGINES, "All Search Engines"),
                ],
                help_text="Filter by search engine (loaded dynamically)",
                required=False,
            ),
            # ── 4. Display options ──────────────────────────────────
            FilterField(
                key="show_url",
                label="Show URL Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
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
            # ── 5. Sort & limit ─────────────────────────────────────
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="time_desc",
                options=[
                    ("time_desc", "Search Time (Newest First)"),
                    ("time_asc", "Search Time (Oldest First)"),
                    ("term_asc", "Search Term (A-Z)"),
                    ("term_desc", "Search Term (Z-A)"),
                    ("browser_asc", "Browser (A-Z)"),
                    ("engine_asc", "Search Engine (A-Z)"),
                ],
                help_text="Sort order for the search terms list",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default=self.UNLIMITED,
                options=[
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    ("250", "250"),
                    ("500", "500"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of search terms to show",
                required=False,
            ),
            # ── 6. Footer ──────────────────────────────────────────
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
        """Load dynamic options for browser, search engine, and tag filters."""
        if key == "browser_filter":
            options: List[tuple] = [
                (self.ALL_BROWSERS, "All Browsers"),
            ]
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
            options: List[tuple] = [
                (self.ALL_ENGINES, "All Search Engines"),
            ]
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

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the search terms table as HTML."""
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Title and description
        section_title = config.get("section_title", "")
        section_description = config.get("section_description", "")

        # Data filters
        tag_filter = config.get("tag_filter") or []
        if isinstance(tag_filter, str):
            tag_filter = [tag_filter]
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        engine_filter = config.get("engine_filter", self.ALL_ENGINES)

        # Display options
        show_url = bool(config.get("show_url", True))
        show_browser = bool(config.get("show_browser", True))
        show_engine = bool(config.get("show_engine", True))
        show_timestamp = bool(config.get("show_timestamp", True))
        show_profile = bool(config.get("show_profile", False))

        # Sort & limit
        sort_by = config.get("sort_by", "time_desc")
        limit = config.get("limit", self.UNLIMITED)
        show_filter_info = bool(config.get("show_filter_info", False))

        # Build and execute query
        query, params = self._build_query(
            evidence_id, tag_filter, browser_filter, engine_filter, sort_by
        )

        search_terms: List[Dict[str, Any]] = []
        total_count = 0
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            # Apply limit
            if limit != self.UNLIMITED:
                limit_int = int(limit)
                all_rows = all_rows[:limit_int]

            for row in all_rows:
                search_terms.append(
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
            return f'<div class="module-error">Error loading search terms: {html.escape(str(exc))}</div>'
        finally:
            db_conn.row_factory = orig_row_factory

        # Truncation
        shown_count = len(search_terms)
        is_truncated = shown_count < total_count

        # Filter description
        filter_description = self._build_filter_description(
            tag_filter, browser_filter, engine_filter, translations
        )

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            search_terms=search_terms,
            section_title=section_title,
            section_description=section_description,
            show_url=show_url,
            show_browser=show_browser,
            show_engine=show_engine,
            show_timestamp=show_timestamp,
            show_profile=show_profile,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        browser_filter: str,
        engine_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query for search terms with filters."""
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["s.evidence_id = ?"]

        # Browser filter
        if browser_filter != self.ALL_BROWSERS:
            conditions.append("s.browser = ?")
            params.append(browser_filter)

        # Search engine filter
        if engine_filter != self.ALL_ENGINES:
            conditions.append("s.search_engine = ?")
            params.append(engine_filter)

        # Tag filter (multi-select OR logic)
        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = s.id
                      AND ta.artifact_type = 'browser_search_term'
                      AND ta.evidence_id = s.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        # ORDER BY
        order_clause = self._get_order_clause(sort_by)

        query = f"""
            SELECT
                s.term,
                s.url,
                s.browser,
                s.search_engine,
                s.search_time_utc,
                s.profile
            FROM browser_search_terms s
            WHERE {' AND '.join(conditions)}
            {order_clause}
        """

        return query, params

    def _get_order_clause(self, sort_by: str) -> str:
        """Get ORDER BY clause for sort option."""
        order_map = {
            "time_desc": "ORDER BY s.search_time_utc DESC NULLS LAST",
            "time_asc": "ORDER BY s.search_time_utc ASC NULLS FIRST",
            "term_asc": "ORDER BY s.term ASC",
            "term_desc": "ORDER BY s.term DESC",
            "browser_asc": "ORDER BY s.browser ASC, s.term ASC",
            "engine_asc": "ORDER BY s.search_engine ASC, s.term ASC",
        }
        return order_map.get(sort_by, "ORDER BY s.search_time_utc DESC NULLS LAST")

    def _build_filter_description(
        self,
        tag_filter: List[str],
        browser_filter: str,
        engine_filter: str,
        t: Dict[str, str],
    ) -> str:
        """Build human-readable filter description for the footer."""
        parts = []

        if tag_filter:
            parts.append(f"Tags: {', '.join(tag_filter)}")
        if browser_filter != self.ALL_BROWSERS:
            parts.append(f"Browser: {browser_filter.capitalize()}")
        if engine_filter != self.ALL_ENGINES:
            parts.append(f"Search Engine: {engine_filter}")

        return "; ".join(parts) if parts else t.get("filter_all", "All")
