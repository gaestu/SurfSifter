"""URL Summary Report Module.

Displays a table of distinct URLs with occurrence counts and timestamps,
with filtering by source, match, and tag.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
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


class UrlSummaryModule(BaseReportModule):
    """Module for displaying URL summary with occurrence counts in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    ANY_MATCH = "any_match"
    ANY_SOURCE = "any_source"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="url_summary",
            name="URL Summary",
            description="Displays distinct URLs with occurrence counts, first/last seen timestamps",
            category="URLs",
            icon="🔗",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for source, match, tag, and sort."""
        return [
            FilterField(
                key="source_filter",
                label="Source",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_SOURCE, "Any Source"),
                ],
                help_text="Filter by discovery source (extractors loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Matches",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_MATCH, "Any Match"),
                ],
                help_text="Filter by reference list match (lists loaded dynamically)",
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
                key="group_by_domain",
                label="Group by Domain",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Group URLs by domain for better readability",
                required=False,
            ),
            FilterField(
                key="show_dates",
                label="Show Dates",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show First Seen and Last Seen date columns",
                required=False,
            ),
            FilterField(
                key="sub_url_limit",
                label="Sub-URLs to Show",
                filter_type=FilterType.DROPDOWN,
                default="10",
                options=[
                    ("0", "None (domain summary only)"),
                    ("5", "5 per domain"),
                    ("10", "10 per domain"),
                    ("20", "20 per domain"),
                    ("all", "All"),
                ],
                help_text="Number of sub-URLs to show per domain (when grouped)",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="occurrences_desc",
                options=[
                    ("occurrences_desc", "Occurrences (Most First)"),
                    ("occurrences_asc", "Occurrences (Least First)"),
                    ("first_seen_desc", "First Seen (Newest First)"),
                    ("first_seen_asc", "First Seen (Oldest First)"),
                    ("last_seen_desc", "Last Seen (Newest First)"),
                    ("last_seen_asc", "Last Seen (Oldest First)"),
                    ("url_asc", "URL (A-Z)"),
                    ("url_desc", "URL (Z-A)"),
                    ("domain_length_asc", "Domain Length (Shortest First)"),
                    ("domain_length_desc", "Domain Length (Longest First)"),
                ],
                help_text="Sort order for the URL list (applies to domains when grouped)",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the URL list",
                required=False,
            ),
            FilterField(
                key="section_title",
                label="Section Title",
                filter_type=FilterType.TEXT,
                default="Domänen",
                help_text="Optional heading displayed above the URL table (leave empty to hide)",
                required=False,
            ),
            FilterField(
                key="shorten_urls",
                label="Shorten URLs",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Truncate long URLs to fit on one line with ellipsis",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for source, match, and tag filters.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "source_filter":
            # Get all distinct sources (discovered_by)
            options = [
                (self.ALL, "All"),
                (self.ANY_SOURCE, "Any Source"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT discovered_by
                    FROM urls
                    WHERE discovered_by IS NOT NULL AND discovered_by != ''
                    ORDER BY discovered_by
                    """
                )
                for (source,) in cursor.fetchall():
                    options.append((source, source))
            except Exception:
                pass
            return options

        elif key == "match_filter":
            # Get all reference lists with URL matches
            options = [
                (self.ALL, "All"),
                (self.ANY_MATCH, "Any Match"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT list_name
                    FROM url_matches
                    ORDER BY list_name
                    """
                )
                for (list_name,) in cursor.fetchall():
                    options.append((list_name, list_name))
            except Exception:
                pass
            return options

        elif key == "tag_filter":
            # Get all tags used on URLs
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
                    WHERE ta.artifact_type = 'url'
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
        """Render the URL summary as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        from jinja2 import Environment, FileSystemLoader

        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Extract config values
        source_filter = config.get("source_filter", self.ALL)
        match_filter = config.get("match_filter", self.ALL)
        tag_filter = config.get("tag_filter", self.ALL)
        group_by_domain = config.get("group_by_domain", False)
        show_dates = config.get("show_dates", True)
        sub_url_limit = config.get("sub_url_limit", "10")
        sort_by = config.get("sort_by", "occurrences_desc")
        show_filter_info = config.get("show_filter_info", False)
        section_title = config.get("section_title", "Domänen")
        shorten_urls = config.get("shorten_urls", False)

        # Build query
        query, params = self._build_query(
            evidence_id, source_filter, match_filter, tag_filter, sort_by
        )

        # Execute query
        urls = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                urls.append(
                    {
                        "url": row["url"],
                        "domain": row["domain"] or self._extract_domain(row["url"]),
                        "occurrences": row["occurrences"],
                        "first_seen": self._format_datetime(row["first_seen"], date_format),
                        "last_seen": self._format_datetime(row["last_seen"], date_format),
                    }
                )
        except Exception as e:
            # Return error HTML
            return f'<div class="module-error">Error loading URL summary: {e}</div>'

        # Group by domain if requested
        grouped_data = None
        if group_by_domain:
            grouped_data = self._group_by_domain(urls, sub_url_limit, sort_by)

        # Build filter description
        filter_desc = self._build_filter_description(
            source_filter, match_filter, tag_filter, sort_by, group_by_domain, translations
        )

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            urls=urls,
            grouped_data=grouped_data,
            group_by_domain=group_by_domain,
            show_dates=show_dates,
            filter_description=filter_desc,
            total_count=len(urls),
            total_domains=len(grouped_data) if grouped_data else 0,
            show_filter_info=show_filter_info,
            section_title=section_title,
            shorten_urls=shorten_urls,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        source_filter: str,
        match_filter: str,
        tag_filter: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query based on filters.

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = ["u.evidence_id = ?"]

        # Handle source filter
        if source_filter == self.ANY_SOURCE:
            conditions.append("u.discovered_by IS NOT NULL AND u.discovered_by != ''")
        elif source_filter != self.ALL:
            conditions.append("u.discovered_by = ?")
            params.append(source_filter)

        # Handle match filter
        if match_filter == self.ANY_MATCH:
            joins.append(
                """
                INNER JOIN url_matches m ON m.url_id = u.id
                """
            )
        elif match_filter != self.ALL:
            joins.append(
                """
                INNER JOIN url_matches m ON m.url_id = u.id
                """
            )
            conditions.append("m.list_name = ?")
            params.append(match_filter)

        # Handle tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = u.id AND ta.artifact_type = 'url'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = u.id AND ta.artifact_type = 'url'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        # Build ORDER BY
        order_map = {
            "occurrences_desc": "occurrences DESC, u.url ASC",
            "occurrences_asc": "occurrences ASC, u.url ASC",
            "first_seen_desc": "first_seen DESC NULLS LAST, u.url ASC",
            "first_seen_asc": "first_seen ASC NULLS FIRST, u.url ASC",
            "last_seen_desc": "last_seen DESC NULLS LAST, u.url ASC",
            "last_seen_asc": "last_seen ASC NULLS FIRST, u.url ASC",
            "url_asc": "u.url ASC",
            "url_desc": "u.url DESC",
            "domain_length_asc": "LENGTH(u.domain) ASC NULLS LAST, u.url ASC",
            "domain_length_desc": "LENGTH(u.domain) DESC NULLS FIRST, u.url ASC",
        }
        order_by = order_map.get(sort_by, "occurrences DESC, u.url ASC")

        # Combine query parts
        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                u.url,
                u.domain,
                COUNT(*) as occurrences,
                MIN(u.first_seen_utc) as first_seen,
                MAX(u.last_seen_utc) as last_seen
            FROM urls u
            {join_clause}
            WHERE {where_clause}
            GROUP BY u.url
            ORDER BY {order_by}
        """

        return query, params

    def _format_datetime(self, ts: str | None, date_format: str) -> str:
        """Format timestamp using selected date format.

        Args:
            ts: ISO timestamp string or None

        Returns:
            Formatted date string or empty string if None/empty
        """
        if ts is None or ts == "" or ts == "None":
            return ""
        return format_datetime(ts, date_format, include_time=True, include_seconds=True)

    def _extract_base_domain(self, domain: str) -> str:
        """Extract base domain (eTLD+1) from a full domain.

        Uses tldextract to properly handle multi-part TLDs like .co.uk.
        Examples:
            www.example.com -> example.com
            eb821efc-cd28-4024-8128-60264d678cd7.example.com -> example.com
            accounts.google.co.uk -> google.co.uk

        Args:
            domain: Full domain string (e.g., from URL netloc)

        Returns:
            Base domain (registered domain) or original if extraction fails
        """
        if not domain:
            return ""

        try:
            import tldextract
            extracted = tldextract.extract(domain)
            # Combine domain and suffix (e.g., "google" + "co.uk" -> "google.co.uk")
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            elif extracted.domain:
                return extracted.domain
            else:
                return domain  # Fallback to original
        except Exception:
            return domain

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL.

        Args:
            url: Full URL string

        Returns:
            Domain string or empty string
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc or ""
        except Exception:
            return ""

    def _group_by_domain(
        self,
        urls: List[Dict[str, Any]],
        sub_url_limit: str,
        sort_by: str,
    ) -> List[Dict[str, Any]]:
        """Group URLs by base domain (eTLD+1) with aggregate statistics.

        Args:
            urls: List of URL dictionaries
            sub_url_limit: Limit for sub-URLs ("0", "5", "10", "20", "all")
            sort_by: Sort order key

        Returns:
            List of domain groups with sub-URLs
        """
        from collections import defaultdict

        # Group URLs by base domain (eTLD+1)
        domain_groups: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "domain": "",
                "urls": [],
                "total_urls": 0,
                "total_occurrences": 0,
                "first_seen": None,
                "last_seen": None,
            }
        )

        for url_data in urls:
            # Extract base domain for grouping
            full_domain = url_data["domain"] or self._extract_domain(url_data["url"])
            base_domain = self._extract_base_domain(full_domain) or "(no domain)"

            group = domain_groups[base_domain]
            group["domain"] = base_domain
            group["urls"].append(url_data)
            group["total_urls"] += 1
            group["total_occurrences"] += url_data["occurrences"]

            # Track first/last seen (compare as strings, ISO format sorts correctly)
            if url_data["first_seen"]:
                if group["first_seen"] is None or url_data["first_seen"] < group["first_seen"]:
                    group["first_seen"] = url_data["first_seen"]
            if url_data["last_seen"]:
                if group["last_seen"] is None or url_data["last_seen"] > group["last_seen"]:
                    group["last_seen"] = url_data["last_seen"]

        # Convert to list
        groups = list(domain_groups.values())

        # Sort groups based on sort_by
        sort_key_map = {
            "occurrences_desc": lambda g: (-g["total_occurrences"], g["domain"]),
            "occurrences_asc": lambda g: (g["total_occurrences"], g["domain"]),
            "first_seen_desc": lambda g: (g["first_seen"] or "", g["domain"]),
            "first_seen_asc": lambda g: (g["first_seen"] or "9999", g["domain"]),
            "last_seen_desc": lambda g: (g["last_seen"] or "", g["domain"]),
            "last_seen_asc": lambda g: (g["last_seen"] or "9999", g["domain"]),
            "url_asc": lambda g: g["domain"],
            "url_desc": lambda g: g["domain"],
            "domain_length_asc": lambda g: (len(g["domain"]), g["domain"]),
            "domain_length_desc": lambda g: (-len(g["domain"]), g["domain"]),
        }
        sort_key = sort_key_map.get(sort_by, lambda g: (-g["total_occurrences"], g["domain"]))
        reverse = sort_by in ("first_seen_desc", "last_seen_desc", "url_desc")
        groups.sort(key=sort_key, reverse=reverse)

        # Apply sub-URL limit
        if sub_url_limit == "0":
            limit = 0
        elif sub_url_limit == "all":
            limit = None
        else:
            try:
                limit = int(sub_url_limit)
            except ValueError:
                limit = 10

        for group in groups:
            total = len(group["urls"])
            if limit is not None and limit < total:
                group["shown_urls"] = group["urls"][:limit]
                group["hidden_count"] = total - limit
            else:
                group["shown_urls"] = group["urls"]
                group["hidden_count"] = 0

        return groups

    def _build_filter_description(
        self,
        source_filter: str,
        match_filter: str,
        tag_filter: str,
        sort_by: str,
        group_by_domain: bool = False,
        t: Dict[str, str] | None = None,
    ) -> str:
        """Build human-readable filter description.

        Args:
            source_filter: Source filter value
            match_filter: Match filter value
            tag_filter: Tag filter value
            sort_by: Sort order key
            group_by_domain: Whether grouped by domain

        Returns:
            Description string
        """
        parts = []

        t = t or {}

        # Source description
        if source_filter == self.ALL:
            pass  # No filter applied
        elif source_filter == self.ANY_SOURCE:
            parts.append(t.get("filter_any_source", "from any source"))
        else:
            parts.append(
                t.get("filter_from_source", 'from "{source}"').format(source=source_filter)
            )

        # Match description
        if match_filter == self.ALL:
            pass
        elif match_filter == self.ANY_MATCH:
            parts.append(t.get("filter_any_match", "with any match"))
        else:
            parts.append(
                t.get("filter_matching", 'matching "{match}"').format(match=match_filter)
            )

        # Tag description
        if tag_filter == self.ALL:
            pass
        elif tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        else:
            parts.append(
                t.get("filter_tagged", 'tagged "{tag}"').format(tag=tag_filter)
            )

        # Grouping description
        if group_by_domain:
            parts.append(t.get("filter_grouped_by_domain", "grouped by domain"))

        # Sort description
        sort_labels = {
            "occurrences_desc": t.get("sort_most_frequent_first", "most frequent first"),
            "occurrences_asc": t.get("sort_least_frequent_first", "least frequent first"),
            "first_seen_desc": t.get("sort_first_seen_newest", "first seen newest"),
            "first_seen_asc": t.get("sort_first_seen_oldest", "first seen oldest"),
            "last_seen_desc": t.get("sort_last_seen_newest", "last seen newest"),
            "last_seen_asc": t.get("sort_last_seen_oldest", "last seen oldest"),
            "url_asc": t.get("sort_url_az", "URL A-Z"),
            "url_desc": t.get("sort_url_za", "URL Z-A"),
            "domain_length_asc": t.get("sort_domain_shortest_first", "domain shortest first"),
            "domain_length_desc": t.get("sort_domain_longest_first", "domain longest first"),
        }
        parts.append(
            t.get("filter_sorted_by", "sorted by {sort}").format(
                sort=sort_labels.get(sort_by, sort_by)
            )
        )

        return ", ".join(parts) if parts else t.get("filter_all_urls", "all URLs")

    def format_config_summary(self, config: dict[str, Any]) -> str:
        """Format configuration for display in section card.

        Args:
            config: Module configuration

        Returns:
            Short summary string
        """
        parts = []

        source = config.get("source_filter", self.ALL)
        if source == self.ANY_SOURCE:
            parts.append("Any Source")
        elif source != self.ALL:
            parts.append(f"Source: {source}")

        match = config.get("match_filter", self.ALL)
        if match == self.ANY_MATCH:
            parts.append("Any Match")
        elif match != self.ALL:
            parts.append(f"Match: {match}")

        tag = config.get("tag_filter", self.ALL)
        if tag == self.ANY_TAG:
            parts.append("Any Tag")
        elif tag != self.ALL:
            parts.append(f"Tag: {tag}")

        if config.get("group_by_domain"):
            parts.append("Grouped")

        return ", ".join(parts) if parts else "All URLs"
