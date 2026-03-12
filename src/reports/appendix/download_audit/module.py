"""Appendix Download Audit Module.

Displays the download audit log table for traceability and forensic auditing.
Shows every download attempt with outcome, HTTP status, duration, and reason.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir


class AppendixDownloadAuditModule(BaseAppendixModule):
    """Appendix module for displaying the download audit trail."""

    # Special filter values
    ALL = "all"

    # Outcome display configuration: (css_class, icon)
    OUTCOME_STYLES: Dict[str, tuple] = {
        "success": ("outcome-success", "✅"),
        "failed": ("outcome-failed", "❌"),
        "blocked": ("outcome-blocked", "🚫"),
        "cancelled": ("outcome-cancelled", "⚠️"),
        "error": ("outcome-error", "💥"),
    }

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_download_audit",
            name="Download Audit Log",
            description="Displays the download audit trail for traceability and forensic review",
            category="Appendix",
            icon="📋",
        )

    def get_default_title(self) -> str:
        """Return default title for the appendix section."""
        return "Download Audit Log"

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="outcome_filter",
                label="Outcome",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All Outcomes"),
                    ("success", "Success"),
                    ("failed", "Failed"),
                    ("blocked", "Blocked"),
                    ("cancelled", "Cancelled"),
                    ("error", "Error"),
                ],
                help_text="Filter by download outcome",
                required=False,
            ),
            FilterField(
                key="include_reason",
                label="Include Reason",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the reason/error column",
                required=False,
            ),
            FilterField(
                key="include_caller",
                label="Include Caller Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the caller_info column (which component initiated the download)",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="date_desc",
                options=[
                    ("date_desc", "Date (Newest First)"),
                    ("date_asc", "Date (Oldest First)"),
                    ("outcome_asc", "Outcome (A-Z)"),
                    ("url_asc", "URL (A-Z)"),
                ],
                help_text="Sort order for the audit entries",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for the outcome filter."""
        if key == "outcome_filter":
            options: List[tuple] = [
                (self.ALL, "All Outcomes"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT outcome
                    FROM download_audit
                    ORDER BY outcome
                    """
                )
                for (outcome,) in cursor.fetchall():
                    if outcome:
                        label = outcome.capitalize()
                        options.append((outcome, label))
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
        """Render the download audit log as an HTML table."""
        # Extract config values
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        outcome_filter = config.get("outcome_filter", self.ALL)
        include_reason = bool(config.get("include_reason", True))
        include_caller = bool(config.get("include_caller", False))
        sort_by = config.get("sort_by", "date_desc")

        # Build and execute query
        query, params = self._build_query(evidence_id, outcome_filter, sort_by)

        entries: List[Dict[str, Any]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                entries.append(self._process_entry(dict(row), date_format, translations))
        except Exception as exc:
            return (
                f'<div class="module-error">'
                f"Error loading download audit log: {exc}</div>"
            )

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            entries=entries,
            total_count=len(entries),
            include_reason=include_reason,
            include_caller=include_caller,
            outcome_styles=self.OUTCOME_STYLES,
            t=translations,
            locale=locale,
        )

    # ── Query building ────────────────────────────────────────────────

    def _build_query(
        self,
        evidence_id: int,
        outcome_filter: str,
        sort_by: str,
    ) -> tuple[str, list]:
        """Build SQL query for the audit log."""
        params: list = [evidence_id]
        where = ["evidence_id = ?"]

        if outcome_filter not in (self.ALL, None, ""):
            where.append("outcome = ?")
            params.append(outcome_filter)

        order_map = {
            "date_desc": "ts_utc DESC, id DESC",
            "date_asc": "ts_utc ASC, id ASC",
            "outcome_asc": "outcome ASC, ts_utc DESC",
            "url_asc": "url ASC, ts_utc DESC",
        }
        order = order_map.get(sort_by, "ts_utc DESC, id DESC")

        query = f"""
            SELECT
                id, evidence_id, ts_utc, url, method, outcome, blocked,
                reason, status_code, attempts, duration_s, bytes_written,
                content_type, caller_info, created_at_utc
            FROM download_audit
            WHERE {' AND '.join(where)}
            ORDER BY {order}
        """
        return query, params

    # ── Data processing ───────────────────────────────────────────────

    def _process_entry(
        self,
        row: Dict[str, Any],
        date_format: str,
        t: Dict[str, str],
    ) -> Dict[str, Any]:
        """Process an audit row into display data."""
        outcome = row.get("outcome", "")
        style_class, icon = self.OUTCOME_STYLES.get(outcome, ("", ""))

        # Format timestamp
        ts_display = format_datetime(
            row.get("ts_utc", ""),
            date_format,
            include_time=True,
            include_seconds=True,
        )

        # Format duration
        duration_s = row.get("duration_s")
        if duration_s is not None:
            if duration_s < 1:
                duration_display = f"{duration_s * 1000:.0f} ms"
            else:
                duration_display = f"{duration_s:.1f} s"
        else:
            duration_display = ""

        # Format bytes
        bytes_written = row.get("bytes_written")
        bytes_display = self._format_size(bytes_written) if bytes_written else ""

        return {
            "id": row["id"],
            "timestamp": ts_display,
            "url": row.get("url", ""),
            "method": row.get("method", ""),
            "outcome": outcome,
            "outcome_icon": icon,
            "outcome_class": style_class,
            "blocked": bool(row.get("blocked", 0)),
            "reason": row.get("reason", ""),
            "status_code": row.get("status_code"),
            "attempts": row.get("attempts"),
            "duration": duration_display,
            "bytes_written": bytes_display,
            "content_type": row.get("content_type", ""),
            "caller_info": row.get("caller_info", ""),
        }

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size for display."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"


