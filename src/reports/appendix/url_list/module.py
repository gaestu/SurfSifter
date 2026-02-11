"""Appendix URL List Module.

Displays a list of URLs with optional grouping by domain (FQDN).
Supports multi-select filtering by tags and reference list matches.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...paths import get_module_template_dir


class AppendixUrlListModule(BaseAppendixModule):
    """Appendix module for listing URLs grouped by domain."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_url_list",
            name="URL List",
            description="Lists URLs with optional grouping by domain and tag/match filters",
            category="Appendix",
            icon="🔗",
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
                key="match_filter",
                label="Matches",
                filter_type=FilterType.MULTI_SELECT,
                help_text="Filter by one or more reference list matches",
                required=False,
            ),
            FilterField(
                key="filter_mode",
                label="Filter Mode",
                filter_type=FilterType.DROPDOWN,
                default="or",
                options=[
                    ("or", "OR - Any tag or any match"),
                    ("and", "AND - Must have tag AND match"),
                ],
                help_text="OR: URLs with any selected tag or match. AND: URLs must have a selected tag AND a selected match.",
                required=False,
            ),
            FilterField(
                key="group_by_domain",
                label="Group by Domain (FQDN)",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Group URLs by their domain",
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
                    WHERE ta.artifact_type = 'url'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "match_filter":
            options: List[tuple] = []
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

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})

        tag_filter = config.get("tag_filter") or []
        match_filter = config.get("match_filter") or []
        filter_mode = config.get("filter_mode", "or")
        group_by_domain = bool(config.get("group_by_domain", True))

        query, params = self._build_query(evidence_id, tag_filter, match_filter, filter_mode)

        urls: List[Dict[str, str]] = []
        seen_urls: set[str] = set()
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                url = row["url"]
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                urls.append(
                    {
                        "url": url,
                        "domain": row["domain"] or self._extract_domain(url) or "(no domain)",
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading URLs: {exc}</div>'

        # Sort alphabetically by URL
        urls.sort(key=lambda x: x["url"].lower())

        grouped = None
        if group_by_domain:
            grouped = self._group_by_domain(urls)

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            urls=urls,
            grouped=grouped,
            group_by_domain=group_by_domain,
            total_count=len(urls),
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        match_filter: List[str],
        filter_mode: str = "or",
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["u.evidence_id = ?"]

        tag_condition = None
        match_condition = None

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = u.id
                      AND ta.artifact_type = 'url'
                      AND ta.evidence_id = u.evidence_id
                      AND t.name IN ({placeholders})
                )
                """

        if match_filter:
            placeholders = ", ".join(["?"] * len(match_filter))
            match_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM url_matches m
                    WHERE m.url_id = u.id
                      AND m.evidence_id = u.evidence_id
                      AND m.list_name IN ({placeholders})
                )
                """

        # Apply filter logic based on mode
        if filter_mode == "and" and tag_filter and match_filter:
            # AND mode: must have a selected tag AND a selected match
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(f"({match_condition})")
            params.extend(match_filter)
        elif tag_condition or match_condition:
            # OR mode (default): any selected tag OR any selected match
            or_parts = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if match_condition:
                or_parts.append(match_condition)
                params.extend(match_filter)
            if or_parts:
                conditions.append(f"({' OR '.join(or_parts)})")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT u.url, u.domain
            FROM urls u
            WHERE {where_clause}
            ORDER BY COALESCE(u.domain, ''), u.url
        """

        return query, params

    def _group_by_domain(
        self, urls: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for item in urls:
            # Normalize domain by stripping www. prefix for grouping
            normalized_domain = self._normalize_domain(item["domain"])
            groups[normalized_domain].append(item)

        result = []
        for domain in sorted(groups.keys(), key=str.lower):
            # Sort URLs within each group alphabetically
            sorted_urls = sorted(groups[domain], key=lambda x: x["url"].lower())
            result.append(
                {
                    "domain": domain,
                    "urls": sorted_urls,
                }
            )
        return result

    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain by removing www. prefix for grouping."""
        if domain.lower().startswith("www."):
            return domain[4:]
        return domain

    def _extract_domain(self, url: str) -> str:
        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            return parsed.netloc or ""
        except Exception:
            return ""
