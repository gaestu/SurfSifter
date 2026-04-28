"""Search Term Frequency Report Module.

Aggregates browser search terms by term (case-insensitive, trimmed) and
shows how often each term was searched together with the first/last seen
timestamps and the browsers/engines involved. Designed to keep reports
compact when the same term was searched many times.
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


class SearchTermFrequencyModule(BaseReportModule):
    """Module for displaying frequency-aggregated search terms in reports."""

    # Special filter values
    ALL_BROWSERS = "all"
    ALL_ENGINES = "all"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="search_term_frequency",
            name="Search Term Frequency",
            description=(
                "Aggregates search terms and shows how often each term was "
                "searched, including first/last seen timestamps."
            ),
            category="Browser",
            icon="📊",
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
            FilterField(
                key="min_count",
                label="Minimum Occurrences",
                filter_type=FilterType.NUMBER,
                default=1,
                help_text="Only include terms searched at least this many times",
                required=False,
            ),
            # ── 4. Display options ──────────────────────────────────
            FilterField(
                key="show_browsers",
                label="Show Browsers Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the browsers column",
                required=False,
            ),
            FilterField(
                key="show_engines",
                label="Show Search Engines Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the search engines column",
                required=False,
            ),
            FilterField(
                key="show_first_seen",
                label="Show First Seen Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the first-seen timestamp column",
                required=False,
            ),
            FilterField(
                key="show_last_seen",
                label="Show Last Seen Column",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the last-seen timestamp column",
                required=False,
            ),
            FilterField(
                key="show_profiles",
                label="Show Profiles Column",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display the browser profiles column",
                required=False,
            ),
            # ── 5. Sort & limit ─────────────────────────────────────
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="count_desc",
                options=[
                    ("count_desc", "Count (Most First)"),
                    ("count_asc", "Count (Least First)"),
                    ("last_seen_desc", "Last Seen (Newest First)"),
                    ("first_seen_asc", "First Seen (Oldest First)"),
                    ("term_asc", "Search Term (A-Z)"),
                    ("term_desc", "Search Term (Z-A)"),
                ],
                help_text="Sort order for the aggregated search terms",
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
                help_text="Maximum number of aggregated search terms to show",
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
        """Load dynamic options for browser, engine, and tag filters."""
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
        """Render the search term frequency table as HTML."""
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Title and description (defaults from translations)
        section_title = config.get("section_title", "") or translations.get(
            "search_term_frequency_title", "Search Term Frequency"
        )
        section_description = config.get("section_description", "") or translations.get(
            "search_term_frequency_desc",
            "Aggregated view of search terms grouped by term.",
        )

        # Data filters
        tag_filter = config.get("tag_filter") or []
        if isinstance(tag_filter, str):
            tag_filter = [tag_filter]
        browser_filter = config.get("browser_filter", self.ALL_BROWSERS)
        engine_filter = config.get("engine_filter", self.ALL_ENGINES)

        try:
            min_count = int(config.get("min_count", 1) or 1)
        except (TypeError, ValueError):
            min_count = 1
        if min_count < 1:
            min_count = 1

        # Display options
        show_browsers = bool(config.get("show_browsers", True))
        show_engines = bool(config.get("show_engines", True))
        show_first_seen = bool(config.get("show_first_seen", True))
        show_last_seen = bool(config.get("show_last_seen", True))
        show_profiles = bool(config.get("show_profiles", False))

        # Sort & limit
        sort_by = config.get("sort_by", "count_desc")
        limit = config.get("limit", self.UNLIMITED)
        show_filter_info = bool(config.get("show_filter_info", False))

        # Build and execute query
        query, params = self._build_query(
            evidence_id,
            tag_filter,
            browser_filter,
            engine_filter,
            min_count,
            sort_by,
        )

        rows: List[Dict[str, Any]] = []
        total_count = 0
        orig_row_factory = db_conn.row_factory
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            all_rows = cursor.fetchall()
            total_count = len(all_rows)

            if limit != self.UNLIMITED:
                try:
                    limit_int = int(limit)
                    all_rows = all_rows[:limit_int]
                except (TypeError, ValueError):
                    pass

            for row in all_rows:
                rows.append(
                    {
                        "term": row["term"] or "",
                        "count": int(row["occurrences"] or 0),
                        "browsers": self._format_list(
                            row["browsers"], capitalize=True
                        ),
                        "engines": self._format_list(row["engines"]),
                        "profiles": self._format_list(row["profiles"]),
                        "first_seen": format_datetime(
                            row["first_seen"], date_format
                        ) if row["first_seen"] else "",
                        "last_seen": format_datetime(
                            row["last_seen"], date_format
                        ) if row["last_seen"] else "",
                    }
                )
        except Exception as exc:
            return (
                f'<div class="module-error">Error loading search term '
                f'frequencies: {html.escape(str(exc))}</div>'
            )
        finally:
            db_conn.row_factory = orig_row_factory

        shown_count = len(rows)
        is_truncated = shown_count < total_count

        filter_description = self._build_filter_description(
            tag_filter,
            browser_filter,
            engine_filter,
            min_count,
            translations,
        )

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            rows=rows,
            section_title=section_title,
            section_description=section_description,
            show_browsers=show_browsers,
            show_engines=show_engines,
            show_first_seen=show_first_seen,
            show_last_seen=show_last_seen,
            show_profiles=show_profiles,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            t=translations,
            locale=locale,
        )

    @staticmethod
    def _format_list(value: Optional[str], capitalize: bool = False) -> str:
        """Convert a GROUP_CONCAT result into a deduplicated, sorted string."""
        if not value:
            return ""
        items = sorted({v.strip() for v in value.split(",") if v and v.strip()})
        if capitalize:
            items = [v.capitalize() for v in items]
        return ", ".join(items)

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        browser_filter: str,
        engine_filter: str,
        min_count: int,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query for aggregated search terms with filters."""
        params: list[Any] = [evidence_id]
        conditions: list[str] = [
            "s.evidence_id = ?",
            "s.term IS NOT NULL",
            "TRIM(s.term) != ''",
        ]

        if browser_filter != self.ALL_BROWSERS:
            conditions.append("s.browser = ?")
            params.append(browser_filter)

        if engine_filter != self.ALL_ENGINES:
            conditions.append("s.search_engine = ?")
            params.append(engine_filter)

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

        order_clause = self._get_order_clause(sort_by)

        query = f"""
            SELECT
                MIN(s.term)                            AS term,
                COUNT(*)                               AS occurrences,
                MIN(s.search_time_utc)                 AS first_seen,
                MAX(s.search_time_utc)                 AS last_seen,
                GROUP_CONCAT(DISTINCT s.browser)       AS browsers,
                GROUP_CONCAT(DISTINCT s.search_engine) AS engines,
                GROUP_CONCAT(DISTINCT s.profile)       AS profiles
            FROM browser_search_terms s
            WHERE {' AND '.join(conditions)}
            GROUP BY LOWER(TRIM(s.term))
            HAVING occurrences >= ?
            {order_clause}
        """
        params.append(min_count)

        return query, params

    def _get_order_clause(self, sort_by: str) -> str:
        """Get ORDER BY clause for sort option."""
        order_map = {
            "count_desc": "ORDER BY occurrences DESC, last_seen DESC",
            "count_asc": "ORDER BY occurrences ASC, last_seen DESC",
            "last_seen_desc": "ORDER BY last_seen DESC NULLS LAST",
            "first_seen_asc": "ORDER BY first_seen ASC NULLS LAST",
            "term_asc": "ORDER BY LOWER(MIN(s.term)) ASC",
            "term_desc": "ORDER BY LOWER(MIN(s.term)) DESC",
        }
        return order_map.get(sort_by, "ORDER BY occurrences DESC, last_seen DESC")

    def _build_filter_description(
        self,
        tag_filter: List[str],
        browser_filter: str,
        engine_filter: str,
        min_count: int,
        t: Dict[str, str],
    ) -> str:
        """Build human-readable filter description for the footer."""
        parts: List[str] = []

        if tag_filter:
            parts.append(f"{t.get('tags', 'Tags')}: {', '.join(tag_filter)}")
        if browser_filter != self.ALL_BROWSERS:
            parts.append(
                f"{t.get('search_browser', 'Browser')}: {browser_filter.capitalize()}"
            )
        if engine_filter != self.ALL_ENGINES:
            parts.append(
                f"{t.get('search_engine', 'Search Engine')}: {engine_filter}"
            )
        if min_count > 1:
            label = t.get("filter_min_occurrences", "≥{count} occurrences").replace(
                "{count}", str(min_count)
            )
            parts.append(label)

        return "; ".join(parts) if parts else t.get("filter_all", "All")
