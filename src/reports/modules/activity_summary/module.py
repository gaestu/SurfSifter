"""Activity Summary Report Module.

Displays system/browser activity overview with:
- Total events and date range
- Activity density by day (visual representation)
- Significant inactivity gaps
- Event type breakdown

Answers the investigator question: "When was the system actively used?"
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader

from ...dates import format_date, format_datetime
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class ActivitySummaryModule(BaseReportModule):
    """Module for displaying system/browser activity summary in reports."""

    # All available event kinds with human-readable labels and group membership
    # Ordered by group for UI presentation
    EVENT_KIND_OPTIONS = [
        # Browser activity
        ("browser_visit", "Browser Visit"),
        ("url_discovered", "URL Discovered"),
        ("tab_accessed", "Tab Accessed"),
        ("tab_closed", "Tab Closed"),
        ("tab_navigated", "Tab Navigated"),
        ("bookmark_added", "Bookmark Added"),
        ("search_performed", "Search Performed"),
        # Downloads
        ("download_started", "Download Started"),
        ("download_completed", "Download Completed"),
        # Authentication
        ("credential_saved", "Credential Saved"),
        ("credential_used", "Credential Used"),
        ("autofill_created", "Autofill Created"),
        ("autofill_used", "Autofill Used"),
        # Media
        ("media_played", "Media Played"),
        # Filesystem (unchecked by default — timestamps reflect copy, not original activity)
        ("file_created", "File Created"),
        ("file_modified", "File Modified"),
        ("file_accessed", "File Accessed"),
        # Extensions & Engagement
        ("site_engaged", "Site Engaged"),
        ("extension_installed", "Extension Installed"),
        ("extension_updated", "Extension Updated"),
        # Other
        ("cookie_created", "Cookie Created"),
        ("cookie_accessed", "Cookie Accessed"),
        ("os_artifact", "OS Artifact"),
        ("hsts_observed", "HSTS Observed"),
        ("hsts_expiry", "HSTS Expiry"),
        ("jumplist_accessed", "Jump List Accessed"),
        ("jumplist_created", "Jump List Created"),
        ("image_extracted", "Image Extracted"),
        ("form_data_deleted", "Form Data Deleted"),
    ]

    # Kinds excluded from the default selection (unreliable timestamps)
    _DEFAULT_UNCHECKED = {"file_created", "file_modified", "file_accessed"}

    # Map event kinds to groups (kept for backward compat and group labels)
    KIND_TO_GROUP = {
        # Browser activity
        "browser_visit": "browser",
        "url_discovered": "browser",
        "tab_accessed": "browser",
        "bookmark_added": "browser",
        "search_performed": "browser",
        "tab_closed": "browser",
        "tab_navigated": "browser",
        # Downloads
        "download_started": "downloads",
        "download_completed": "downloads",
        # Authentication
        "credential_saved": "authentication",
        "credential_used": "authentication",
        "autofill_created": "authentication",
        "autofill_used": "authentication",
        # Media
        "media_played": "media",
        # Filesystem
        "file_created": "filesystem",
        "file_modified": "filesystem",
        "file_accessed": "filesystem",
        # Extensions & Engagement
        "site_engaged": "extensions",
        "extension_installed": "extensions",
        "extension_updated": "extensions",
        # Other (included in "all" only)
        "cookie_created": "other",
        "cookie_accessed": "other",
        "os_artifact": "other",
        "hsts_observed": "other",
        "hsts_expiry": "other",
        "jumplist_accessed": "other",
        "jumplist_created": "other",
        "image_extracted": "other",
        "form_data_deleted": "other",
    }

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="activity_summary",
            name="Activity Summary",
            description="System/browser activity overview with timeline density and inactivity gaps",
            category="Timeline",
            icon="📊",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for activity summary configuration."""
        return [
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a title at the top of this section",
                required=False,
            ),
            FilterField(
                key="custom_title",
                label="Custom Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom title (leave empty for default)",
                required=False,
            ),
            FilterField(
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a short description below the title",
                required=False,
            ),
            FilterField(
                key="custom_description",
                label="Custom Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom description (leave empty for default)",
                required=False,
            ),
            FilterField(
                key="date_from",
                label="From Date",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Start date filter (YYYY-MM-DD). Leave empty for no limit.",
                required=False,
            ),
            FilterField(
                key="date_to",
                label="To Date",
                filter_type=FilterType.TEXT,
                default="",
                help_text="End date filter (YYYY-MM-DD). Leave empty for no limit.",
                required=False,
            ),
            FilterField(
                key="event_kinds",
                label="Event Types",
                filter_type=FilterType.MULTI_SELECT,
                default=[
                    kind for kind, _ in self.EVENT_KIND_OPTIONS
                    if kind not in self._DEFAULT_UNCHECKED
                ],
                options=self.EVENT_KIND_OPTIONS,
                help_text="Select which event types to include (file timestamps unchecked by default — they reflect copy time, not original activity)",
                required=False,
            ),
            FilterField(
                key="min_gap_hours",
                label="Minimum Gap (hours)",
                filter_type=FilterType.DROPDOWN,
                default="24",
                options=[
                    ("6", "6 hours"),
                    ("12", "12 hours"),
                    ("24", "24 hours"),
                    ("48", "48 hours"),
                    ("72", "72 hours"),
                ],
                help_text="Show inactivity gaps longer than this duration",
                required=False,
            ),
            FilterField(
                key="show_daily_breakdown",
                label="Show Daily Breakdown",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Include day-by-day activity visualization",
                required=False,
            ),
            FilterField(
                key="max_days_shown",
                label="Max Days to Show",
                filter_type=FilterType.DROPDOWN,
                default="60",
                options=[
                    ("30", "30 days"),
                    ("60", "60 days"),
                    ("90", "90 days"),
                    ("180", "180 days"),
                    ("all", "All days"),
                ],
                help_text="Maximum number of days to display in breakdown",
                required=False,
            ),
            FilterField(
                key="show_event_breakdown",
                label="Show Event Type Breakdown",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Include breakdown of events by type",
                required=False,
            ),
            FilterField(
                key="confidence_filter",
                label="Minimum Confidence",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=[
                    ("all", "All Confidence Levels"),
                    ("low", "Low and above"),
                    ("medium", "Medium and above"),
                    ("high", "High only"),
                ],
                help_text="Filter events by confidence level",
                required=False,
            ),
        ]

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the activity summary as HTML."""
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        # Extract config
        event_kinds = config.get("event_kinds", None)
        min_gap_hours = int(config.get("min_gap_hours", 24))
        show_daily = config.get("show_daily_breakdown", True)
        max_days = config.get("max_days_shown", "60")
        show_event_breakdown = config.get("show_event_breakdown", True)
        confidence_filter = config.get("confidence_filter", "all")

        # Title and description
        show_title = config.get("show_title", True)
        custom_title = config.get("custom_title", "")
        show_description = config.get("show_description", True)
        custom_description = config.get("custom_description", "")
        date_from = config.get("date_from", "")
        date_to = config.get("date_to", "")

        # Backward compat: old configs may have event_group instead of event_kinds
        if event_kinds is None:
            event_group = config.get("event_group", "all")
            if event_group and event_group != "all":
                event_kinds = [
                    kind for kind, group in self.KIND_TO_GROUP.items()
                    if group == event_group
                ]
            # else: event_kinds stays None → no kind filter (all kinds)

        # Query timeline data
        events = self._query_events(
            db_conn, evidence_id, event_kinds, confidence_filter,
            date_from, date_to,
        )

        # Calculate statistics
        stats = self._calculate_stats(events, date_format)
        daily_counts = self._calculate_daily_counts(events, max_days, date_format)
        gaps = self._find_gaps(events, min_gap_hours, date_format, translations)
        event_breakdown = self._calculate_event_breakdown(events) if show_event_breakdown else {}

        # Load and render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True,
        )
        template = env.get_template("template.html")

        t = translations
        return template.render(
            stats=stats,
            daily_counts=daily_counts if show_daily else [],
            gaps=gaps,
            event_breakdown=event_breakdown,
            show_daily=show_daily,
            show_event_breakdown=show_event_breakdown,
            selected_kinds_label=self._get_kinds_label(event_kinds, translations),
            min_gap_hours=min_gap_hours,
            show_title=show_title,
            title_text=custom_title or t.get("activity_summary_title", "Activity Summary"),
            show_description=show_description,
            description_text=custom_description or t.get(
                "activity_summary_desc",
                "Overview of system and browser activity patterns, showing when the device was actively used, significant periods of inactivity, and the distribution of events across different categories.",
            ),
            t=translations,
            locale=locale,
        )

    def _query_events(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        event_kinds: Optional[List[str]],
        confidence_filter: str,
        date_from: str = "",
        date_to: str = "",
    ) -> List[Dict[str, Any]]:
        """Query timeline events with optional filtering.

        Args:
            event_kinds: List of kind strings to include, or None for all.
        """
        conditions = ["evidence_id = ?", "ts_utc IS NOT NULL"]
        params: List[Any] = [evidence_id]

        # Filter by selected event kinds
        if event_kinds is not None and len(event_kinds) > 0:
            all_kinds = {k for k, _ in self.EVENT_KIND_OPTIONS}
            if set(event_kinds) != all_kinds:
                placeholders = ",".join("?" * len(event_kinds))
                conditions.append(f"kind IN ({placeholders})")
                params.extend(event_kinds)

        # Filter by confidence
        if confidence_filter == "high":
            conditions.append("confidence = 'high'")
        elif confidence_filter == "medium":
            conditions.append("confidence IN ('high', 'medium')")
        elif confidence_filter == "low":
            conditions.append("confidence IN ('high', 'medium', 'low')")

        # Date range filtering (validate YYYY-MM-DD format)
        if date_from:
            try:
                datetime.strptime(date_from[:10], "%Y-%m-%d")
                conditions.append("ts_utc >= ?")
                params.append(date_from[:10])
            except ValueError:
                pass  # Skip invalid date
        if date_to:
            try:
                datetime.strptime(date_to[:10], "%Y-%m-%d")
                conditions.append("ts_utc <= ?")
                params.append(date_to[:10] + "T23:59:59")
            except ValueError:
                pass  # Skip invalid date

        query = f"""
            SELECT ts_utc, kind, confidence, note
            FROM timeline
            WHERE {' AND '.join(conditions)}
            ORDER BY ts_utc ASC
        """

        rows = db_conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _calculate_stats(
        self, events: List[Dict[str, Any]], date_format: str
    ) -> Dict[str, Any]:
        """Calculate overall statistics from events."""
        if not events:
            return {
                "total_events": 0,
                "earliest": None,
                "latest": None,
                "span_days": 0,
                "avg_events_per_day": 0,
            }

        total = len(events)
        earliest = events[0]["ts_utc"]
        latest = events[-1]["ts_utc"]

        # Calculate span
        try:
            earliest_dt = datetime.fromisoformat(earliest.replace("Z", "+00:00"))
            latest_dt = datetime.fromisoformat(latest.replace("Z", "+00:00"))
            span = (latest_dt - earliest_dt).days + 1
        except (ValueError, AttributeError):
            span = 1

        avg_per_day = total / span if span > 0 else total

        return {
            "total_events": total,
            "earliest": self._format_timestamp(earliest, date_format),
            "latest": self._format_timestamp(latest, date_format),
            "span_days": span,
            "avg_events_per_day": round(avg_per_day, 1),
        }

    def _calculate_daily_counts(
        self,
        events: List[Dict[str, Any]],
        max_days: str,
        date_format: str,
    ) -> List[Dict[str, Any]]:
        """Calculate event counts per day for visualization."""
        import math

        if not events:
            return []

        # Count events by date
        daily: Dict[str, int] = defaultdict(int)
        for event in events:
            ts = event.get("ts_utc", "")
            if ts:
                date_str = ts[:10]  # YYYY-MM-DD
                daily[date_str] += 1

        if not daily:
            return []

        # Sort by date
        sorted_days = sorted(daily.items())

        # Apply limit
        if max_days != "all":
            limit = int(max_days)
            sorted_days = sorted_days[:limit]

        # Find max for scaling (use log scale for better visibility of low values)
        max_count = max(count for _, count in sorted_days) if sorted_days else 1

        # Use square root scaling to make low values more visible
        # This compresses high values and expands low values
        max_sqrt = math.sqrt(max_count) if max_count > 0 else 1

        # Build result with bar height percentage
        result = []
        for date_str, count in sorted_days:
            # Square root scaling: sqrt(count) / sqrt(max) * 100
            # Ensures minimum 10% height for any non-zero value
            if count > 0:
                scaled = (math.sqrt(count) / max_sqrt) * 100
                bar_height = max(10, int(scaled))  # Minimum 10% height
            else:
                bar_height = 0
            display_date = format_date(date_str, date_format)
            result.append({
                "date": display_date,
                "count": count,
                "bar_width": bar_height,  # Keep for backward compat
                "bar_height": bar_height,
            })

        return result

    def _find_gaps(
        self,
        events: List[Dict[str, Any]],
        min_gap_hours: int,
        date_format: str,
        t: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Find significant gaps in activity."""
        if len(events) < 2:
            return []

        gaps = []
        min_gap = timedelta(hours=min_gap_hours)

        prev_ts = None
        for event in events:
            ts_str = event.get("ts_utc", "")
            if not ts_str:
                continue

            try:
                current_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                continue

            if prev_ts is not None:
                gap = current_ts - prev_ts
                if gap >= min_gap:
                    gap_hours = gap.total_seconds() / 3600
                    gaps.append({
                        "start": format_datetime(
                            prev_ts.isoformat(), date_format, include_time=True, include_seconds=True
                        ),
                        "end": format_datetime(
                            current_ts.isoformat(), date_format, include_time=True, include_seconds=True
                        ),
                        "duration_hours": round(gap_hours, 1),
                        "duration_display": self._format_duration(gap_hours, t),
                    })

            prev_ts = current_ts

        # Sort by end date (newest first)
        gaps.sort(key=lambda x: x["end"], reverse=True)
        return gaps[:20]  # Limit to top 20 gaps

    def _format_duration(self, hours: float, t: Dict[str, str]) -> str:
        """Format duration in human-readable form."""
        hours_label = t.get("hours", "hours")
        days_label = t.get("days", "days")
        weeks_label = t.get("weeks", "weeks")
        if hours < 24:
            return f"{hours:.1f} {hours_label}"
        days = hours / 24
        if days < 7:
            return f"{days:.1f} {days_label}"
        weeks = days / 7
        return f"{weeks:.1f} {weeks_label}"

    def _format_timestamp(self, ts_str: str | None, date_format: str) -> str | None:
        """Format ISO timestamp using selected date format."""
        if not ts_str:
            return None
        return format_datetime(ts_str, date_format, include_time=True, include_seconds=True)

    def _get_kinds_label(self, event_kinds: Optional[List[str]], t: Dict[str, str]) -> str:
        """Build a label summarizing which event kinds are selected."""
        all_kinds = {k for k, _ in self.EVENT_KIND_OPTIONS}
        if event_kinds is None or set(event_kinds) >= all_kinds:
            return t.get("event_group_all", "All Events")
        if not event_kinds:
            return t.get("no_events_selected", "No Events Selected")
        # Summarize: show count
        return f"{len(event_kinds)} / {len(all_kinds)} " + t.get("event_types_selected", "event types selected")

    def _calculate_event_breakdown(
        self,
        events: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """Calculate event counts by type."""
        breakdown: Dict[str, int] = defaultdict(int)
        for event in events:
            kind = event.get("kind", "unknown")
            # Format kind for display
            display_name = kind.replace("_", " ").title()
            breakdown[display_name] += 1

        # Sort by count descending
        return dict(sorted(breakdown.items(), key=lambda x: x[1], reverse=True))
