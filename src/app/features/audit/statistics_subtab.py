"""
Statistics Subtab - Summary cards showing extractor run statistics.

This subtab is displayed within the Audit tab (per-evidence).

Initial implementation as standalone tab.
Moved to subtab within Audit tab.
Relocated from app.features.statistics to app.features.audit.
"""

from __future__ import annotations

from typing import Optional, List

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QFrame,
    QScrollArea,
    QGridLayout,
)
from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QFont

from core.statistics_collector import StatisticsCollector, ExtractorRunStats
from core.logging import get_logger

LOGGER = get_logger("app.features.audit.statistics")


class StatisticCard(QFrame):
    """A single statistic card showing extractor results."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet("""
            StatisticCard {
                background-color: #ffffff;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                padding: 12px;
            }
            StatisticCard:hover {
                border-color: #1976d2;
            }
        """)
        self.setMinimumWidth(280)
        self.setMaximumWidth(350)

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # Header: extractor name + status badge
        header = QHBoxLayout()
        self._name_label = QLabel()
        self._name_label.setFont(QFont("", 11, QFont.Bold))
        header.addWidget(self._name_label)
        header.addStretch()
        self._status_label = QLabel()
        self._status_label.setStyleSheet("padding: 2px 8px; border-radius: 4px;")
        header.addWidget(self._status_label)
        layout.addLayout(header)

        # Runtime info
        self._runtime_label = QLabel()
        self._runtime_label.setStyleSheet("color: #666;")
        layout.addWidget(self._runtime_label)

        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color: #e0e0e0;")
        layout.addWidget(line)

        # Stats grid
        self._stats_widget = QWidget()
        self._stats_layout = QGridLayout(self._stats_widget)
        self._stats_layout.setSpacing(4)
        layout.addWidget(self._stats_widget)

    def set_stats(self, stats: ExtractorRunStats) -> None:
        """Update card with extractor statistics."""
        # Name - convert snake_case to Title Case
        display_name = stats.extractor_name.replace("_", " ").title()
        self._name_label.setText(display_name)

        # Status badge colors
        status_colors = {
            "running": ("#fff3e0", "#e65100"),
            "success": ("#e8f5e9", "#2e7d32"),
            "partial": ("#fff8e1", "#f57f17"),
            "failed": ("#ffebee", "#c62828"),
            "cancelled": ("#eceff1", "#546e7a"),
            "skipped": ("#e3f2fd", "#1565c0"),  # Blue - no artifacts found
        }
        bg, fg = status_colors.get(stats.status, ("#eceff1", "#546e7a"))
        self._status_label.setText(stats.status.upper())
        self._status_label.setStyleSheet(
            f"background-color: {bg}; color: {fg}; padding: 2px 8px; border-radius: 4px;"
        )

        # Runtime
        if stats.duration_seconds is not None:
            if stats.duration_seconds < 60:
                runtime = f"{stats.duration_seconds:.1f}s"
            else:
                mins = int(stats.duration_seconds // 60)
                secs = stats.duration_seconds % 60
                runtime = f"{mins}m {secs:.0f}s"
        else:
            runtime = "Running..."
        self._runtime_label.setText(f"⏱ {runtime}")

        # Clear old stats
        while self._stats_layout.count():
            item = self._stats_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # Build stats grid
        row = 0
        categories = [
            ("discovered", "🔍 Discovered", "#1976d2"),
            ("ingested", "✅ Ingested", "#388e3c"),
            ("failed", "❌ Failed", "#d32f2f"),
            ("skipped", "⏭ Skipped", "#757575"),
        ]

        for attr, label, color in categories:
            data = getattr(stats, attr)
            if not data:
                continue

            # Category header
            cat_label = QLabel(label)
            cat_label.setStyleSheet(f"color: {color}; font-weight: bold;")
            self._stats_layout.addWidget(cat_label, row, 0, 1, 2)
            row += 1

            # Items
            for item_type, count in sorted(data.items()):
                type_label = QLabel(f"  {item_type}:")
                type_label.setStyleSheet("color: #666;")
                count_label = QLabel(f"{count:,}")
                count_label.setStyleSheet("font-weight: bold;")
                count_label.setAlignment(Qt.AlignRight)
                self._stats_layout.addWidget(type_label, row, 0)
                self._stats_layout.addWidget(count_label, row, 1)
                row += 1


class AggregatedTotalsCard(QFrame):
    """Summary card showing totals across all extractors."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet("""
            AggregatedTotalsCard {
                background-color: #e3f2fd;
                border: 2px solid #1976d2;
                border-radius: 8px;
                padding: 16px;
            }
        """)

        layout = QVBoxLayout(self)

        title = QLabel("📊 Totals Across All Extractors")
        title.setFont(QFont("", 12, QFont.Bold))
        title.setStyleSheet("color: #1976d2;")
        layout.addWidget(title)

        self._content = QLabel()
        self._content.setWordWrap(True)
        layout.addWidget(self._content)

    def set_totals(self, totals: dict) -> None:
        """Update with aggregated totals."""
        lines = []

        discovered = totals.get("discovered", {})
        if discovered:
            items = ", ".join(f"{k}: {v:,}" for k, v in sorted(discovered.items()))
            lines.append(f"🔍 <b>Discovered:</b> {items}")

        ingested = totals.get("ingested", {})
        if ingested:
            items = ", ".join(f"{k}: {v:,}" for k, v in sorted(ingested.items()))
            lines.append(f"✅ <b>Ingested:</b> {items}")

        failed = totals.get("failed", {})
        if failed:
            items = ", ".join(f"{k}: {v:,}" for k, v in sorted(failed.items()))
            lines.append(f"❌ <b>Failed:</b> {items}")

        skipped = totals.get("skipped", {})
        if skipped:
            items = ", ".join(f"{k}: {v:,}" for k, v in sorted(skipped.items()))
            lines.append(f"⏭ <b>Skipped:</b> {items}")

        if not lines:
            lines.append("No extraction statistics available yet.")

        self._content.setText("<br>".join(lines))


class StatisticsSubtab(QWidget):
    """
    Statistics subtab with summary cards (per-evidence).

    Displays extractor run statistics as cards within the Audit tab.
    """

    def __init__(self, evidence_id: int, evidence_label: str = "", parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._evidence_id = evidence_id
        self._evidence_label = evidence_label
        self._cards: List[StatisticCard] = []
        self._connected = False
        self._loaded = False  # Track if we've loaded from DB

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        self._setup_ui()
        self._connect_signals()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)

        # Header
        header = QLabel("📈 Extraction Statistics")
        header.setFont(QFont("", 16, QFont.Bold))
        layout.addWidget(header)

        # Aggregated totals card
        self._totals_card = AggregatedTotalsCard()
        layout.addWidget(self._totals_card)

        # Scroll area for extractor cards
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        self._cards_container = QWidget()
        self._cards_layout = QGridLayout(self._cards_container)
        self._cards_layout.setSpacing(16)
        self._cards_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        scroll.setWidget(self._cards_container)
        layout.addWidget(scroll, 1)

        # Empty state
        self._empty_label = QLabel(
            "No extraction statistics available.\n\n"
            "Run extractors to see their statistics here."
        )
        self._empty_label.setAlignment(Qt.AlignCenter)
        self._empty_label.setStyleSheet("color: #666; font-size: 14px;")
        layout.addWidget(self._empty_label)

        # Start with empty state
        self._totals_card.hide()

    def _connect_signals(self) -> None:
        """Connect to collector signals."""
        collector = StatisticsCollector.get_instance()
        if collector and not self._connected:
            collector.stats_updated.connect(self._on_stats_updated)
            collector.run_started.connect(self._on_stats_updated)
            collector.run_finished.connect(self._on_stats_updated)
            self._connected = True

    def refresh(self) -> None:
        """Refresh statistics display."""
        # Ensure signals are connected (collector may be installed after init)
        self._connect_signals()

        collector = StatisticsCollector.get_instance()
        if not collector:
            self._show_empty_state()
            return

        stats_list = collector.get_all_stats_for_evidence(self._evidence_id)

        if not stats_list:
            self._show_empty_state()
            return

        self._empty_label.hide()
        self._totals_card.show()

        # Update totals
        totals = collector.get_aggregated_totals(self._evidence_id)
        self._totals_card.set_totals(totals)

        # Clear existing cards
        for card in self._cards:
            card.deleteLater()
        self._cards.clear()

        # Create cards for each extractor
        cols = 3  # Cards per row
        for i, stats in enumerate(sorted(stats_list, key=lambda s: s.extractor_name)):
            card = StatisticCard()
            card.set_stats(stats)
            self._cards.append(card)
            self._cards_layout.addWidget(card, i // cols, i % cols)

    def _show_empty_state(self) -> None:
        """Show empty state message."""
        self._totals_card.hide()
        for card in self._cards:
            card.deleteLater()
        self._cards.clear()
        self._empty_label.show()

    @Slot(int, str)
    def _on_stats_updated(self, evidence_id: int, extractor_name: str) -> None:
        """Handle statistics updates from collector."""
        if evidence_id == self._evidence_id:
            self.refresh()

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    def showEvent(self, event) -> None:
        """Load stats from database on first show, refresh if stale."""
        super().showEvent(event)

        # Load from database on first show (if evidence_label is available)
        if not self._loaded and self._evidence_label:
            collector = StatisticsCollector.get_instance()
            if collector:
                collector.load_evidence_stats(self._evidence_id, self._evidence_label)
                self._loaded = True
                # BUG FIX: Must refresh UI after loading from DB
                self.refresh()
                return  # Already refreshed, skip stale check

        # Only refresh if marked stale (was: always refresh on show)
        # Statistics are updated in real-time via signals, so only refresh
        # when explicitly marked stale by data_changed
        if self._data_stale:
            self._data_stale = False
            self.refresh()


# Backward compatibility alias
StatisticsTab = StatisticsSubtab
