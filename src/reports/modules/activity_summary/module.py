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

    # Event type groups for filtering
    EVENT_GROUPS = {
        "all": "All Events",
        "browser": "Browser Activity",
        "downloads": "Downloads",
        "authentication": "Authentication",
        "media": "Media Playback",
    }

    # Map event kinds to groups
    KIND_TO_GROUP = {
        # Browser activity
        "browser_visit": "browser",
        "url_discovered": "browser",
        "tab_accessed": "browser",
        "bookmark_added": "browser",
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
        # Other (included in "all" only)
        "cookie_created": "other",
        "cookie_accessed": "other",
        "os_artifact": "other",
        "hsts_observed": "other",
        "hsts_expiry": "other",
        "jumplist_accessed": "other",
        "jumplist_created": "other",
        "image_extracted": "other",
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
                key="event_group",
                label="Event Types",
                filter_type=FilterType.DROPDOWN,
                default="all",
                options=list(self.EVENT_GROUPS.items()),
                help_text="Which types of events to include in the summary",
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
        event_group = config.get("event_group", "all")
        min_gap_hours = int(config.get("min_gap_hours", 24))
        show_daily = config.get("show_daily_breakdown", True)
        max_days = config.get("max_days_shown", "60")
        show_event_breakdown = config.get("show_event_breakdown", True)
        confidence_filter = config.get("confidence_filter", "all")

        # Query timeline data
        events = self._query_events(
            db_conn, evidence_id, event_group, confidence_filter
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

        return template.render(
            stats=stats,
            daily_counts=daily_counts if show_daily else [],
            gaps=gaps,
            event_breakdown=event_breakdown,
            show_daily=show_daily,
            show_event_breakdown=show_event_breakdown,
            event_group_name=self._get_event_group_label(event_group, translations),
            min_gap_hours=min_gap_hours,
            t=translations,
            locale=locale,
        )

    def _query_events(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        event_group: str,
        confidence_filter: str,
    ) -> List[Dict[str, Any]]:
        """Query timeline events with optional filtering."""
        conditions = ["evidence_id = ?", "ts_utc IS NOT NULL"]
        params: List[Any] = [evidence_id]

        # Filter by event group
        if event_group != "all":
            kinds_in_group = [
                kind for kind, group in self.KIND_TO_GROUP.items()
                if group == event_group
            ]
            if kinds_in_group:
                placeholders = ",".join("?" * len(kinds_in_group))
                conditions.append(f"kind IN ({placeholders})")
                params.extend(kinds_in_group)

        # Filter by confidence
        if confidence_filter == "high":
            conditions.append("confidence = 'high'")
        elif confidence_filter == "medium":
            conditions.append("confidence IN ('high', 'medium')")
        elif confidence_filter == "low":
            conditions.append("confidence IN ('high', 'medium', 'low')")

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

    def _get_event_group_label(self, event_group: str, t: Dict[str, str]) -> str:
        """Get localized event group label for report output."""
        key_map = {
            "all": "event_group_all",
            "browser": "event_group_browser",
            "downloads": "event_group_downloads",
            "authentication": "event_group_authentication",
            "media": "event_group_media",
        }
        key = key_map.get(event_group, "")
        if key:
            return t.get(key, self.EVENT_GROUPS.get(event_group, "All Events"))
        return self.EVENT_GROUPS.get(event_group, "All Events")

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
