"""URL Activity Timeline Report Module.

Displays URL/domain activity time ranges with:
- Per-domain summary (first seen, last seen, access count)
- Optional URL detail within domains
- Filtering by tags, matches, sources

Answers the investigator question: "When were specific URLs/domains accessed?"
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from jinja2 import Environment, FileSystemLoader

from ...dates import format_datetime
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class UrlActivityTimelineModule(BaseReportModule):
    """Module for displaying URL/domain activity timelines in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    ANY_MATCH = "any_match"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="url_activity_timeline",
            name="URL Activity Timeline",
            description="Per-domain URL activity with time ranges",
            category="Timeline",
            icon="🌐",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for URL activity timeline configuration."""
        return [
            FilterField(
                key="tag_filter",
                label="Filter by Tag",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All URLs"),
                    (self.ANY_TAG, "Any Tagged URL"),
                ],
                help_text="Filter URLs by tag (specific tags loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Filter by Match",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All URLs"),
                    (self.ANY_MATCH, "Any Matched URL"),
                ],
                help_text="Filter by reference list match",
                required=False,
            ),
            FilterField(
                key="domain_filter",
                label="Domain Contains",
                filter_type=FilterType.TEXT,
                default="",
                placeholder="e.g., keyword1, keyword2",
                help_text="Show only domains containing this text (comma-separated for multiple)",
                required=False,
            ),
            FilterField(
                key="min_occurrences",
                label="Minimum Occurrences",
                filter_type=FilterType.DROPDOWN,
                default="1",
                options=[
                    ("1", "1+ occurrence"),
                    ("5", "5+ occurrences"),
                    ("10", "10+ occurrences"),
                    ("25", "25+ occurrences"),
                    ("50", "50+ occurrences"),
                ],
                help_text="Show only domains with this many or more events",
                required=False,
            ),
            FilterField(
                key="show_urls",
                label="Show Individual URLs",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display individual URLs under each domain",
                required=False,
            ),
            FilterField(
                key="urls_per_domain",
                label="URLs per Domain",
                filter_type=FilterType.DROPDOWN,
                default="5",
                options=[
                    ("3", "3 URLs"),
                    ("5", "5 URLs"),
                    ("10", "10 URLs"),
                    ("20", "20 URLs"),
                    ("all", "All URLs"),
                ],
                help_text="Maximum URLs to show per domain (when enabled)",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort Domains By",
                filter_type=FilterType.DROPDOWN,
                default="count_desc",
                options=[
                    ("count_desc", "Most Events First"),
                    ("count_asc", "Least Events First"),
                    ("first_seen_asc", "First Seen (Oldest)"),
                    ("first_seen_desc", "First Seen (Newest)"),
                    ("last_seen_desc", "Last Seen (Newest)"),
                    ("last_seen_asc", "Last Seen (Oldest)"),
                    ("domain_asc", "Domain A-Z"),
                ],
                help_text="How to order the domain list",
                required=False,
            ),
            FilterField(
                key="max_domains",
                label="Max Domains to Show",
                filter_type=FilterType.DROPDOWN,
                default="50",
                options=[
                    ("25", "25 domains"),
                    ("50", "50 domains"),
                    ("100", "100 domains"),
                    ("200", "200 domains"),
                    ("all", "All domains"),
                ],
                help_text="Maximum number of domains to display",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for tag and match filters."""
        if key == "tag_filter":
            options = [
                (self.ALL, "All URLs"),
                (self.ANY_TAG, "Any Tagged URL"),
            ]
            # Load tags from database
            try:
                rows = db_conn.execute(
                    "SELECT DISTINCT name FROM tags ORDER BY name"
                ).fetchall()
                for row in rows:
                    tag_name = row[0]
                    options.append((f"tag:{tag_name}", f"Tag: {tag_name}"))
            except sqlite3.Error:
                pass
            return options

        if key == "match_filter":
            options = [
                (self.ALL, "All URLs"),
                (self.ANY_MATCH, "Any Matched URL"),
            ]
            # Load reference list names from matches
            try:
                rows = db_conn.execute(
                    "SELECT DISTINCT list_name FROM url_matches WHERE list_name IS NOT NULL ORDER BY list_name"
                ).fetchall()
                for row in rows:
                    list_name = row[0]
                    options.append((f"match:{list_name}", f"Match: {list_name}"))
            except sqlite3.Error:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the URL activity timeline as HTML."""
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Extract config
        tag_filter = config.get("tag_filter", self.ALL)
        match_filter = config.get("match_filter", self.ALL)
        domain_filter = config.get("domain_filter", "")
        min_occurrences = int(config.get("min_occurrences", 1))
        show_urls = config.get("show_urls", False)
        urls_per_domain = config.get("urls_per_domain", "5")
        sort_by = config.get("sort_by", "count_desc")
        max_domains = config.get("max_domains", "50")

        # Query URL events from timeline (browser_visit events)
        events = self._query_url_events(
            db_conn, evidence_id, tag_filter, match_filter, domain_filter
        )

        # Aggregate by domain
        domain_data = self._aggregate_by_domain(events, min_occurrences, date_format)

        # Sort domains
        domain_data = self._sort_domains(domain_data, sort_by)

        # Apply limit
        total_domains = len(domain_data)
        if max_domains != "all":
            domain_data = domain_data[:int(max_domains)]

        # Add URL details if requested
        if show_urls:
            self._add_url_details(domain_data, events, urls_per_domain)

        # Calculate summary stats
        summary = self._calculate_summary(events, domain_data, total_domains, date_format)

        # Build filter description
        filter_desc = self._build_filter_description(
            tag_filter, match_filter, domain_filter, min_occurrences, translations
        )

        # Load and render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True,
        )
        template = env.get_template("template.html")

        return template.render(
            domains=domain_data,
            summary=summary,
            filter_description=filter_desc,
            show_urls=show_urls,
            total_domains=total_domains,
            shown_domains=len(domain_data),
            t=translations,
            locale=locale,
        )

    def _query_url_events(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        tag_filter: str,
        match_filter: str,
        domain_filter: str,
    ) -> List[Dict[str, Any]]:
        """Query URL-related timeline events."""
        # Start with browser_history events (they have URLs)
        # Also include url_discovered events
        query = """
            SELECT
                t.ts_utc,
                t.kind,
                t.note,
                t.ref_table,
                t.ref_id,
                CASE
                    WHEN t.ref_table = 'browser_history' THEN bh.url
                    WHEN t.ref_table = 'urls' THEN u.url
                    ELSE NULL
                END as url
            FROM timeline t
            LEFT JOIN browser_history bh ON t.ref_table = 'browser_history'
                AND t.ref_id = bh.id
                AND bh.evidence_id = ?
            LEFT JOIN urls u ON t.ref_table = 'urls'
                AND t.ref_id = u.id
                AND u.evidence_id = ?
            WHERE t.evidence_id = ?
            AND t.kind IN ('browser_visit', 'url_discovered')
            AND t.ts_utc IS NOT NULL
            ORDER BY t.ts_utc ASC
        """

        rows = db_conn.execute(
            query, (evidence_id, evidence_id, evidence_id)
        ).fetchall()

        events = []
        for row in rows:
            url = row["url"]
            if not url:
                continue

            # Apply domain filter if specified
            if domain_filter:
                domain = self._extract_domain(url)
                filter_terms = [t.strip().lower() for t in domain_filter.split(",")]
                if not any(term in domain.lower() for term in filter_terms):
                    continue

            events.append(dict(row))

        # Apply tag filter
        if tag_filter == self.ANY_TAG or tag_filter.startswith("tag:"):
            events = self._filter_by_tag(db_conn, evidence_id, events, tag_filter)

        # Apply match filter
        if match_filter == self.ANY_MATCH or match_filter.startswith("match:"):
            events = self._filter_by_match(db_conn, evidence_id, events, match_filter)

        return events

    def _filter_by_tag(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        events: List[Dict[str, Any]],
        tag_filter: str,
    ) -> List[Dict[str, Any]]:
        """Filter events to those with tagged URLs."""
        # Get tagged URL IDs
        if tag_filter == self.ANY_TAG:
            query = """
                SELECT DISTINCT ta.artifact_id
                FROM tag_associations ta
                WHERE ta.artifact_type = 'url'
            """
            params = ()
        else:
            # Specific tag
            tag_name = tag_filter.replace("tag:", "")
            query = """
                SELECT DISTINCT ta.artifact_id
                FROM tag_associations ta
                JOIN tags t ON ta.tag_id = t.id
                WHERE ta.artifact_type = 'url'
                AND t.name = ?
            """
            params = (tag_name,)

        try:
            rows = db_conn.execute(query, params).fetchall()
            tagged_ids = {row[0] for row in rows}
        except sqlite3.Error:
            return events

        # Filter events where ref_table='urls' and ref_id is tagged
        return [
            e for e in events
            if e["ref_table"] == "urls" and e["ref_id"] in tagged_ids
        ]

    def _filter_by_match(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        events: List[Dict[str, Any]],
        match_filter: str,
    ) -> List[Dict[str, Any]]:
        """Filter events to those with matched URLs."""
        if match_filter == self.ANY_MATCH:
            query = "SELECT DISTINCT url_id FROM url_matches"
            params = ()
        else:
            list_name = match_filter.replace("match:", "")
            query = "SELECT DISTINCT url_id FROM url_matches WHERE list_name = ?"
            params = (list_name,)

        try:
            rows = db_conn.execute(query, params).fetchall()
            matched_ids = {row[0] for row in rows}
        except sqlite3.Error:
            return events

        return [
            e for e in events
            if e["ref_table"] == "urls" and e["ref_id"] in matched_ids
        ]

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc or url
        except Exception:
            return url

    def _format_timestamp(self, ts_str: str | None, date_format: str) -> str | None:
        """Format ISO timestamp using selected date format."""
        if not ts_str:
            return None
        return format_datetime(ts_str, date_format, include_time=True, include_seconds=True)

    def _aggregate_by_domain(
        self,
        events: List[Dict[str, Any]],
        min_occurrences: int,
        date_format: str,
    ) -> List[Dict[str, Any]]:
        """Aggregate events by domain."""
        domains: Dict[str, Dict[str, Any]] = {}

        for event in events:
            url = event.get("url", "")
            if not url:
                continue

            domain = self._extract_domain(url)
            ts_str = event.get("ts_utc", "")

            if domain not in domains:
                domains[domain] = {
                    "domain": domain,
                    "count": 0,
                    "first_seen_raw": None,  # Keep raw for sorting
                    "last_seen_raw": None,
                    "first_seen": None,
                    "last_seen": None,
                    "urls": defaultdict(int),
                }

            d = domains[domain]
            d["count"] += 1
            d["urls"][url] += 1

            # Track first/last seen (keep raw for sorting, format for display)
            if ts_str:
                if d["first_seen_raw"] is None or ts_str < d["first_seen_raw"]:
                    d["first_seen_raw"] = ts_str
                    d["first_seen"] = self._format_timestamp(ts_str, date_format)
                if d["last_seen_raw"] is None or ts_str > d["last_seen_raw"]:
                    d["last_seen_raw"] = ts_str
                    d["last_seen"] = self._format_timestamp(ts_str, date_format)

        # Filter by min occurrences and convert to list
        result = [
            d for d in domains.values()
            if d["count"] >= min_occurrences
        ]

        return result

    def _sort_domains(
        self,
        domains: List[Dict[str, Any]],
        sort_by: str,
    ) -> List[Dict[str, Any]]:
        """Sort domain list by specified criteria."""
        if sort_by == "count_desc":
            return sorted(domains, key=lambda x: x["count"], reverse=True)
        elif sort_by == "count_asc":
            return sorted(domains, key=lambda x: x["count"])
        elif sort_by == "first_seen_asc":
            return sorted(domains, key=lambda x: x["first_seen_raw"] or "")
        elif sort_by == "first_seen_desc":
            return sorted(domains, key=lambda x: x["first_seen_raw"] or "", reverse=True)
        elif sort_by == "last_seen_desc":
            return sorted(domains, key=lambda x: x["last_seen_raw"] or "", reverse=True)
        elif sort_by == "last_seen_asc":
            return sorted(domains, key=lambda x: x["last_seen_raw"] or "")
        elif sort_by == "domain_asc":
            return sorted(domains, key=lambda x: x["domain"].lower())
        return domains

    def _add_url_details(
        self,
        domains: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
        urls_per_domain: str,
    ) -> None:
        """Add individual URL details to each domain."""
        for domain in domains:
            # Get URLs sorted by count
            url_items = sorted(
                domain["urls"].items(),
                key=lambda x: x[1],
                reverse=True,
            )

            # Apply limit
            if urls_per_domain != "all":
                limit = int(urls_per_domain)
                shown_urls = url_items[:limit]
                hidden_count = len(url_items) - limit
            else:
                shown_urls = url_items
                hidden_count = 0

            domain["url_details"] = [
                {"url": url, "count": count}
                for url, count in shown_urls
            ]
            domain["hidden_url_count"] = max(0, hidden_count)

    def _calculate_summary(
        self,
        events: List[Dict[str, Any]],
        domains: List[Dict[str, Any]],
        total_domains: int,
        date_format: str,
    ) -> Dict[str, Any]:
        """Calculate summary statistics."""
        if not events:
            return {
                "total_events": 0,
                "total_domains": 0,
                "unique_urls": 0,
                "earliest": None,
                "latest": None,
            }

        unique_urls = len(set(e.get("url") for e in events if e.get("url")))
        timestamps = [e.get("ts_utc") for e in events if e.get("ts_utc")]

        return {
            "total_events": len(events),
            "total_domains": total_domains,
            "unique_urls": unique_urls,
            "earliest": self._format_timestamp(min(timestamps), date_format) if timestamps else None,
            "latest": self._format_timestamp(max(timestamps), date_format) if timestamps else None,
        }

    def _build_filter_description(
        self,
        tag_filter: str,
        match_filter: str,
        domain_filter: str,
        min_occurrences: int,
        t: Dict[str, str] | None = None,
    ) -> str:
        """Build human-readable filter description."""
        t = t or {}
        parts = []

        if tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_tagged_urls", "tagged URLs"))
        elif tag_filter.startswith("tag:"):
            parts.append(
                t.get("filter_tag_value", "tag: {tag}").format(tag=tag_filter[4:])
            )

        if match_filter == self.ANY_MATCH:
            parts.append(t.get("filter_matched_urls", "matched URLs"))
        elif match_filter.startswith("match:"):
            parts.append(
                t.get("filter_match_value", "match: {match}").format(match=match_filter[6:])
            )

        if domain_filter:
            parts.append(
                t.get("filter_domain_contains", "domain contains: {domain}").format(
                    domain=domain_filter
                )
            )

        if min_occurrences > 1:
            parts.append(
                t.get("filter_min_occurrences", "≥{count} occurrences").format(
                    count=min_occurrences
                )
            )

        return " | ".join(parts) if parts else t.get("filter_all_urls", "All URLs")
