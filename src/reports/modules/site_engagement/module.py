"""Site Engagement Report Module.

Displays site engagement data from Chromium browsers with filtering by tags.
"""

from __future__ import annotations

import sqlite3
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


class SiteEngagementModule(BaseReportModule):
    """Module for displaying site engagement data in reports."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="site_engagement",
            name="Site Engagement",
            description="Displays browser site engagement scores with tag filters",
            category="Browser",
            icon="📊",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for tags, limit, and sort."""
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
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show a brief explanation of what site engagement data represents",
                required=False,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default="50",
                options=[
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of entries to show",
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
                key="engagement_type",
                label="Engagement Type",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    ("site_engagement", "Site Engagement"),
                    ("media_engagement", "Media Engagement"),
                ],
                help_text="Filter by engagement type",
                required=False,
            ),
            FilterField(
                key="min_score",
                label="Minimum Score",
                filter_type=FilterType.NUMBER,
                default=0,
                help_text="Show only entries with score >= this value",
                required=False,
            ),
            FilterField(
                key="show_type",
                label="Show Type",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the engagement type column (Site/Media)",
                required=False,
            ),
            FilterField(
                key="show_score",
                label="Show Score",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the engagement score column",
                required=False,
            ),
            FilterField(
                key="show_visits",
                label="Show Visits",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the visits column",
                required=False,
            ),
            FilterField(
                key="show_browser",
                label="Show Browser",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the browser column",
                required=False,
            ),
            FilterField(
                key="show_profile",
                label="Show Profile",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the profile column",
                required=False,
            ),
            FilterField(
                key="show_last_engagement",
                label="Show Last Engagement",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the last engagement time column",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="score_desc",
                options=[
                    ("score_desc", "Score (Highest First)"),
                    ("score_asc", "Score (Lowest First)"),
                    ("visits_desc", "Visits (Most First)"),
                    ("visits_asc", "Visits (Least First)"),
                    ("last_engagement_desc", "Last Engagement (Newest First)"),
                    ("last_engagement_asc", "Last Engagement (Oldest First)"),
                    ("origin_asc", "Origin (A-Z)"),
                    ("origin_desc", "Origin (Z-A)"),
                ],
                help_text="Sort order for the engagement list",
                required=False,
            ),
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the list",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for tag filter.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "tag_filter":
            # Get all tags used on site_engagement artifacts
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
                    WHERE ta.artifact_type = 'site_engagement'
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
        """Render the site engagement list as HTML.

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
        section_title = config.get("section_title", "")
        show_description = config.get("show_description", False)
        limit = config.get("limit", "50")
        tag_filter = config.get("tag_filter", self.ALL)
        engagement_type = config.get("engagement_type", self.ALL)
        min_score = config.get("min_score", 0)
        show_type = config.get("show_type", True)
        show_score = config.get("show_score", True)
        show_visits = config.get("show_visits", True)
        show_browser = config.get("show_browser", True)
        show_profile = config.get("show_profile", False)
        show_last_engagement = config.get("show_last_engagement", True)
        sort_by = config.get("sort_by", "score_desc")
        show_filter_info = config.get("show_filter_info", False)

        # Ensure min_score is numeric
        try:
            min_score = float(min_score) if min_score else 0
        except (ValueError, TypeError):
            min_score = 0

        # Build query
        query, params = self._build_query(
            evidence_id, tag_filter, engagement_type, min_score, sort_by
        )

        # Execute query
        engagements = []
        total_count = 0
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
                engagements.append(
                    {
                        "origin": row["origin"] or "",
                        "engagement_type": row["engagement_type"] or "",
                        "score": row["raw_score"],
                        "visits": row["visits"],
                        "browser": row["browser"] or "",
                        "profile": row["profile"] or "",
                        "last_engagement": self._format_datetime(
                            row["last_engagement_time_utc"], date_format
                        ),
                    }
                )
        except Exception as e:
            # Return error HTML
            return f'<div class="module-error">Error loading site engagement: {e}</div>'

        # Determine if list is truncated
        shown_count = len(engagements)
        is_truncated = shown_count < total_count

        # Build filter description
        filter_desc = self._build_filter_description(
            tag_filter, engagement_type, min_score, sort_by, translations
        )

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            engagements=engagements,
            section_title=section_title,
            show_description=show_description,
            show_type=show_type,
            show_score=show_score,
            show_visits=show_visits,
            show_browser=show_browser,
            show_profile=show_profile,
            show_last_engagement=show_last_engagement,
            filter_description=filter_desc,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            show_filter_info=show_filter_info,
            t=translations,
            locale=locale,
        )

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: str,
        engagement_type: str,
        min_score: float,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        """Build SQL query based on filters.

        Returns:
            Tuple of (query_string, parameters)
        """
        params: list[Any] = [evidence_id]
        joins: list[str] = []
        conditions: list[str] = ["se.evidence_id = ?"]

        # Handle tag filter
        if tag_filter == self.ANY_TAG:
            # Join to tag_associations for site_engagement
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = se.id AND ta.artifact_type = 'site_engagement'
                """
            )
        elif tag_filter != self.ALL:
            joins.append(
                """
                INNER JOIN tag_associations ta
                    ON ta.artifact_id = se.id AND ta.artifact_type = 'site_engagement'
                INNER JOIN tags t ON t.id = ta.tag_id
                """
            )
            conditions.append("t.name = ?")
            params.append(tag_filter)

        # Engagement type filter
        if engagement_type != self.ALL:
            conditions.append("se.engagement_type = ?")
            params.append(engagement_type)

        # Minimum score filter
        if min_score > 0:
            conditions.append("se.raw_score >= ?")
            params.append(min_score)

        # Build ORDER BY
        order_map = {
            "score_desc": "se.raw_score DESC NULLS LAST",
            "score_asc": "se.raw_score ASC NULLS FIRST",
            "visits_desc": "se.visits DESC NULLS LAST",
            "visits_asc": "se.visits ASC NULLS FIRST",
            "last_engagement_desc": "se.last_engagement_time_utc DESC NULLS LAST",
            "last_engagement_asc": "se.last_engagement_time_utc ASC NULLS FIRST",
            "origin_asc": "se.origin ASC",
            "origin_desc": "se.origin DESC",
        }
        order_by = order_map.get(sort_by, "se.raw_score DESC NULLS LAST")

        # Combine query parts
        join_clause = "\n".join(joins)
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT
                se.id, se.origin, se.engagement_type, se.raw_score,
                se.visits, se.browser, se.profile, se.last_engagement_time_utc
            FROM site_engagement se
            {join_clause}
            WHERE {where_clause}
            ORDER BY {order_by}
        """

        return query, params

    def _build_filter_description(
        self,
        tag_filter: str,
        engagement_type: str,
        min_score: float,
        sort_by: str,
        t: Dict[str, str],
    ) -> str:
        """Build human-readable filter description.

        Args:
            tag_filter: Tag filter value
            engagement_type: Engagement type filter
            min_score: Minimum score filter
            sort_by: Sort option value
            t: Translation dictionary

        Returns:
            Filter description string
        """
        parts = []

        # Tag filter
        if tag_filter == self.ANY_TAG:
            parts.append(t.get("filter_any_tag", "with any tag"))
        elif tag_filter != self.ALL:
            template = t.get("filter_tagged", 'tagged "{tag}"')
            parts.append(template.replace("{tag}", tag_filter))

        # Engagement type
        if engagement_type == "site_engagement":
            parts.append(t.get("site_engagement_type_site", "site engagement"))
        elif engagement_type == "media_engagement":
            parts.append(t.get("site_engagement_type_media", "media engagement"))

        # Minimum score
        if min_score > 0:
            template = t.get("site_engagement_min_score", "score ≥ {score}")
            parts.append(template.replace("{score}", str(int(min_score))))

        # Sort option
        sort_labels = {
            "score_desc": t.get("sort_score_highest", "highest score first"),
            "score_asc": t.get("sort_score_lowest", "lowest score first"),
            "visits_desc": t.get("sort_visits_most", "most visits first"),
            "visits_asc": t.get("sort_visits_least", "least visits first"),
            "last_engagement_desc": t.get("sort_newest_first", "newest first"),
            "last_engagement_asc": t.get("sort_oldest_first", "oldest first"),
            "origin_asc": t.get("sort_url_az", "origin A-Z"),
            "origin_desc": t.get("sort_url_za", "origin Z-A"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        sort_template = t.get("filter_sorted_by", "sorted by {sort}")
        parts.append(sort_template.replace("{sort}", sort_label))

        return ", ".join(parts) if parts else ""

    def _format_datetime(self, value: str | None, date_format: str) -> str:
        """Format datetime string for display.

        Args:
            value: ISO datetime string or None
            date_format: 'eu' or 'us' format

        Returns:
            Formatted date string or empty string
        """
        if not value:
            return ""
        return format_datetime(value, date_format)
