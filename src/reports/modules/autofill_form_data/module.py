"""Autofill Form Data Report Module.

Displays autofill form data grouped by domain/URL with tag filtering.
Shows form field entries as tables per domain.

Initial implementation for forensic reports.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
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


class AutofillFormDataModule(BaseReportModule):
    """Module for displaying autofill form data grouped by domain in reports."""

    # Regex to extract domain from notes field (e.g., "domain:secure.startups.ch")
    _DOMAIN_PATTERN = re.compile(r'domain:([^;]+)')

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="autofill_form_data",
            name="Autofill Form Data",
            description="Displays autofill form data grouped by domain with tag filtering",
            category="Browser",
            icon="📝",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags and display options."""
        return [
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display module title",
                required=False,
            ),
            FilterField(
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display explanatory description for non-technical readers",
                required=False,
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags (multi-select)",
                required=False,
            ),
            FilterField(
                key="group_by_domain",
                label="Group by Domain",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Group autofill entries by domain/URL (entries without domain shown separately)",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="last_used_desc",
                options=[
                    ("last_used_desc", "Last Used (Newest First)"),
                    ("last_used_asc", "Last Used (Oldest First)"),
                    ("first_used_desc", "First Used (Newest First)"),
                    ("first_used_asc", "First Used (Oldest First)"),
                    ("name_asc", "Field Name (A-Z)"),
                    ("name_desc", "Field Name (Z-A)"),
                ],
                help_text="Sort order for entries within each domain",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the content",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for tag filter.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'autofill'
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
        """Render the autofill form data as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Extract config values
        show_title = bool(config.get("show_title", True))
        show_description = bool(config.get("show_description", True))
        tag_filter = config.get("tag_filter") or []
        group_by_domain = bool(config.get("group_by_domain", True))
        sort_by = config.get("sort_by", "last_used_desc")
        show_filter_info = bool(config.get("show_filter_info", False))

        # Get autofill entries
        entries_by_domain = self._get_autofill_entries(
            db_conn,
            evidence_id,
            tag_filter,
            sort_by,
            date_format,
            group_by_domain,
        )

        # Build filter description
        filter_parts = []
        if tag_filter:
            filter_parts.append(f"Tags: {', '.join(tag_filter)}")
        filter_description = "; ".join(filter_parts) if filter_parts else translations.get("filter_all_urls", "All")

        # Count totals
        total_domains = len(entries_by_domain)
        total_entries = sum(len(domain_data["entries"]) for domain_data in entries_by_domain)

        # Load template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            domains=entries_by_domain,
            total_domains=total_domains,
            total_entries=total_entries,
            show_title=show_title,
            show_description=show_description,
            group_by_domain=group_by_domain,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            t=translations,
            locale=locale,
        )

    def _extract_domain(self, notes: str | None) -> str:
        """Extract domain from notes field.

        Edge autofill data stores domain as 'domain:example.com' in notes.
        """
        if not notes:
            return ""
        match = self._DOMAIN_PATTERN.search(notes)
        if match:
            return match.group(1).strip()
        return ""

    def _get_autofill_entries(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        tag_filter: List[str],
        sort_by: str,
        date_format: str,
        group_by_domain: bool,
    ) -> List[Dict[str, Any]]:
        """Get autofill entries grouped by domain.

        Args:
            db_conn: SQLite connection
            evidence_id: Evidence ID
            tag_filter: List of tag names to filter by
            sort_by: Sort order key
            date_format: Date format ('eu' or 'us')
            group_by_domain: Whether to group by domain

        Returns:
            List of domain dicts with their autofill entries
        """
        db_conn.row_factory = sqlite3.Row

        # Build query with tag filtering
        params: List[Any] = [evidence_id]
        conditions = ["a.evidence_id = ?"]

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            conditions.append(f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = a.id
                      AND ta.artifact_type = 'autofill'
                      AND ta.evidence_id = a.evidence_id
                      AND t.name IN ({placeholders})
                )
            """)
            params.extend(tag_filter)

        # Build ORDER BY clause
        order_clause = self._get_order_clause(sort_by)

        query = f"""
            SELECT a.id, a.name, a.value, a.date_created_utc, a.date_last_used_utc,
                   a.browser, a.profile, a.notes
            FROM autofill a
            WHERE {' AND '.join(conditions)}
            ORDER BY {order_clause}
        """

        try:
            cursor = db_conn.execute(query, params)
            rows = cursor.fetchall()
        except Exception:
            return []

        # Group by domain
        domain_entries: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for row in rows:
            domain = self._extract_domain(row["notes"]) if group_by_domain else ""

            # Use empty string key for entries without domain
            domain_key = domain if domain else ""

            entry = {
                "name": row["name"] or "",
                "value": row["value"] or "",
                "first_used": format_datetime(row["date_created_utc"], date_format) if row["date_created_utc"] else "",
                "last_used": format_datetime(row["date_last_used_utc"], date_format) if row["date_last_used_utc"] else "",
            }
            domain_entries[domain_key].append(entry)

        # Convert to list format for template
        result: List[Dict[str, Any]] = []

        # Sort domains alphabetically, but put empty domain (no domain) last
        sorted_domains = sorted(
            domain_entries.keys(),
            key=lambda d: (d == "", d.lower())
        )

        for domain in sorted_domains:
            entries = domain_entries[domain]
            result.append({
                "domain": domain,
                "entries": entries,
            })

        return result

    def _get_order_clause(self, sort_by: str) -> str:
        """Get SQL ORDER BY clause for sort option."""
        order_map = {
            "last_used_desc": "a.date_last_used_utc DESC NULLS LAST",
            "last_used_asc": "a.date_last_used_utc ASC NULLS LAST",
            "first_used_desc": "a.date_created_utc DESC NULLS LAST",
            "first_used_asc": "a.date_created_utc ASC NULLS LAST",
            "name_asc": "a.name ASC",
            "name_desc": "a.name DESC",
        }
        return order_map.get(sort_by, "a.date_last_used_utc DESC NULLS LAST")
