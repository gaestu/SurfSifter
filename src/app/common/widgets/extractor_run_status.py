"""
Extractor Run Status Widget - Displays extraction/ingestion status in extractor cards.

Shows last successful run timestamps and record counts for extractors in the
Extraction tab. Uses process_log data to provide traceability.

Initial implementation.
Added warning count badge for extraction warnings.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QWidget, QHBoxLayout, QLabel

if TYPE_CHECKING:
    from core.process_log_service import RunInfo


class ExtractorRunStatusWidget(QWidget):
    """
    Compact widget showing extraction/ingestion run status.

    Displays:
    - Extraction: ✅ timestamp or ❌ "Not run yet"
    - Ingestion: ✅ timestamp + "Records: N" or ❌ "Not run yet"
    - Warning badge: ⚠️ N if warnings exist (color-coded by severity)

    Run IDs are shown in tooltips for traceability.

    Example:
        >>> status_widget = ExtractorRunStatusWidget(
        ...     extraction_info={"finished_at": "2025-01-15T14:30:00", "run_id": "abc123"},
        ...     ingestion_info={"finished_at": "2025-01-15T14:32:00", "run_id": "def456", "records_ingested": 1234},
        ...     warning_counts={"total": 3, "info": 1, "warning": 2, "error": 0}
        ... )
        >>> layout.addWidget(status_widget)
    """

    def __init__(
        self,
        extraction_info: Optional["RunInfo"] = None,
        ingestion_info: Optional["RunInfo"] = None,
        warning_counts: Optional[dict] = None,
        parent: Optional[QWidget] = None,
    ):
        """
        Initialize the status widget.

        Args:
            extraction_info: Dict with 'finished_at', 'run_id' from last successful extraction.
            ingestion_info: Dict with 'finished_at', 'run_id', 'records_ingested' from last
                            successful ingestion.
            warning_counts: Dict with 'total', 'info', 'warning', 'error' counts.
            parent: Parent widget.
        """
        super().__init__(parent)
        self._extraction_info = extraction_info
        self._ingestion_info = ingestion_info
        self._warning_counts = warning_counts
        self._setup_ui()

    def _setup_ui(self):
        """Build the compact status display."""
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 2, 5, 2)
        layout.setSpacing(15)
        self._populate_layout(layout)

    def _populate_layout(self, layout):
        """Populate the layout with status labels."""
        # Extraction status
        extraction_label = self._create_extraction_label()
        layout.addWidget(extraction_label)

        # Separator
        sep = QLabel("|")
        sep.setStyleSheet("color: #888;")
        layout.addWidget(sep)

        # Ingestion status
        ingestion_label = self._create_ingestion_label()
        layout.addWidget(ingestion_label)

        # Warning badge (if warnings exist)
        warning_label = self._create_warning_badge()
        if warning_label:
            layout.addWidget(warning_label)

        layout.addStretch()

    def _create_extraction_label(self) -> QLabel:
        """Create label for extraction status."""
        if self._extraction_info:
            ts = self._format_timestamp(self._extraction_info.get("finished_at"))
            text = f"✅ Extraction: {ts}"
            tooltip = self._build_tooltip("Extraction", self._extraction_info)
        else:
            text = "❌ Extraction: Not run yet"
            tooltip = "No successful extraction recorded for this evidence"

        label = QLabel(text)
        label.setStyleSheet(self._get_style(bool(self._extraction_info)))
        label.setToolTip(tooltip)
        return label

    def _create_ingestion_label(self) -> QLabel:
        """Create label for ingestion status."""
        if self._ingestion_info:
            ts = self._format_timestamp(self._ingestion_info.get("finished_at"))
            records = self._ingestion_info.get("records_ingested", 0) or 0
            text = f"✅ Ingestion: {ts} ({records:,} records)"
            tooltip = self._build_tooltip("Ingestion", self._ingestion_info)
        else:
            text = "❌ Ingestion: Not run yet"
            tooltip = "No successful ingestion recorded for this evidence"

        label = QLabel(text)
        label.setStyleSheet(self._get_style(bool(self._ingestion_info)))
        label.setToolTip(tooltip)
        return label

    def _format_timestamp(self, ts: Optional[str]) -> str:
        """Format ISO timestamp for compact display."""
        if not ts:
            return "N/A"

        try:
            # Handle ISO format: 2025-01-15T14:30:45.123456+00:00
            if "T" in ts:
                date_part, time_part = ts.split("T")
                time_part = time_part.split(".")[0]  # Remove microseconds
                time_part = time_part.split("+")[0]  # Remove timezone
                time_part = time_part.split("Z")[0]  # Remove Z suffix
                return f"{date_part} {time_part[:5]}"  # YYYY-MM-DD HH:MM
            return ts[:16]
        except Exception:
            return ts[:16] if len(ts) > 16 else ts

    def _build_tooltip(self, phase: str, info: "RunInfo") -> str:
        """Build detailed tooltip with run ID."""
        lines = [f"<b>{phase} Status</b>"]

        if info.get("finished_at"):
            lines.append(f"Completed: {info['finished_at']}")
        elif info.get("started_at"):
            lines.append(f"Started: {info['started_at']} (not finalized)")

        if info.get("run_id"):
            lines.append(f"Run ID: <code>{info['run_id']}</code>")

        if info.get("records_ingested") is not None:
            lines.append(f"Records: {info['records_ingested']:,}")

        return "<br>".join(lines)

    def _get_style(self, success: bool) -> str:
        """Get CSS style based on status."""
        if success:
            return "color: #28a745; font-size: 9pt;"
        return "color: #dc3545; font-size: 9pt;"

    def _create_warning_badge(self) -> Optional[QLabel]:
        """Create warning badge if warnings exist."""
        if not self._warning_counts:
            return None

        total = self._warning_counts.get("total", 0)
        if total == 0:
            return None

        error_count = self._warning_counts.get("error", 0)
        warning_count = self._warning_counts.get("warning", 0)
        info_count = self._warning_counts.get("info", 0)

        # Color based on highest severity present
        if error_count > 0:
            color = "#dc3545"  # Red for errors
            icon = "❌"
        elif warning_count > 0:
            color = "#ffc107"  # Yellow for warnings
            icon = "⚠️"
        else:
            color = "#17a2b8"  # Blue for info
            icon = "ℹ️"

        label = QLabel(f"{icon} {total}")
        label.setStyleSheet(f"color: {color}; font-size: 9pt; font-weight: bold;")

        # Build tooltip with breakdown
        tooltip_lines = ["<b>Extraction Warnings</b>"]
        if error_count > 0:
            tooltip_lines.append(f"❌ Errors: {error_count}")
        if warning_count > 0:
            tooltip_lines.append(f"⚠️ Warnings: {warning_count}")
        if info_count > 0:
            tooltip_lines.append(f"ℹ️ Info: {info_count}")
        tooltip_lines.append("<br><i>View in Audit → Warnings tab</i>")
        label.setToolTip("<br>".join(tooltip_lines))

        return label

    def update_status(
        self,
        extraction_info: Optional["RunInfo"] = None,
        ingestion_info: Optional["RunInfo"] = None,
        warning_counts: Optional[dict] = None,
    ):
        """
        Update displayed status with new data.

        Args:
            extraction_info: New extraction info or None.
            ingestion_info: New ingestion info or None.
            warning_counts: Dict with 'total', 'info', 'warning', 'error' counts.
        """
        self._extraction_info = extraction_info
        self._ingestion_info = ingestion_info
        self._warning_counts = warning_counts

        # Clear existing widgets from layout
        layout = self.layout()
        if layout:
            while layout.count():
                item = layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

            # Rebuild content within existing layout
            self._populate_layout(layout)
