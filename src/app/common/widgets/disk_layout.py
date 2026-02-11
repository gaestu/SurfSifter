"""Disk layout visualization widget for displaying partition information."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QVBoxLayout,
    QWidget,
)


class DiskLayoutWidget(QWidget):
    """Visual representation of disk partitions."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.partitions: List[Dict[str, Any]] = []
        self.total_size: int = 0
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # Title
        self.title_label = QLabel("Disk Layout")
        title_font = self.title_label.font()
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        layout.addWidget(self.title_label)

        # Visual partition bar
        self.partition_bar = PartitionBarWidget()
        layout.addWidget(self.partition_bar)

        # Partition details
        self.details_label = QLabel()
        self.details_label.setWordWrap(True)
        self.details_label.setTextFormat(Qt.RichText)
        layout.addWidget(self.details_label)

        layout.addStretch()
        self.setLayout(layout)

    def set_partitions(self, partition_info_json: Optional[str], evidence_label: str = "") -> None:
        """
        Set partition information from JSON string.

        Args:
            partition_info_json: JSON string containing partition metadata
            evidence_label: Label of the evidence (e.g., "image.E01 [Partition 2]")
        """
        if not partition_info_json:
            self.clear()
            return

        try:
            partition_data = json.loads(partition_info_json)
            # Check if it's a single partition dict or list
            if isinstance(partition_data, dict):
                self.partitions = [partition_data]
            else:
                self.partitions = partition_data if isinstance(partition_data, list) else []

            # Calculate total size (sum of all partitions)
            self.total_size = sum(p.get('length', 0) for p in self.partitions)

            # Update UI
            self._update_display(evidence_label)

        except (json.JSONDecodeError, KeyError) as exc:
            self.clear()

    def _update_display(self, evidence_label: str) -> None:
        """Update the visual display with current partition data."""
        # Update partition bar
        self.partition_bar.set_partitions(self.partitions, self.total_size)

        # Build details text
        details_html = f"<p><b>Evidence:</b> {evidence_label}</p>"
        details_html += f"<p><b>Total Size:</b> {self._format_size(self.total_size)}</p>"
        details_html += f"<p><b>Partitions:</b> {len(self.partitions)}</p>"

        # List partitions
        if self.partitions:
            details_html += "<table style='margin-top: 10px;'>"
            details_html += "<tr><th align='left'>ID</th>"
            details_html += "<th align='left'>Size</th>"
            details_html += "<th align='left'>Type</th>"
            details_html += "<th align='left'>Offset</th></tr>"

            for part in self.partitions:
                part_id = part.get('index', part.get('addr', '?'))
                size = self._format_size(part.get('length', 0))
                desc = part.get('description', 'Unknown')
                offset = self._format_size(part.get('offset', 0))
                details_html += f"<tr><td>{part_id}</td><td>{size}</td><td>{desc}</td><td>{offset}</td></tr>"

            details_html += "</table>"

        self.details_label.setText(details_html)

    def clear(self) -> None:
        """Clear partition display."""
        self.partitions = []
        self.total_size = 0
        self.partition_bar.set_partitions([], 0)
        self.details_label.setText("<i>No partition information available.</i>")

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format byte size to human-readable string."""
        if size_bytes == 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_idx = 0
        size = float(size_bytes)

        while size >= 1024 and unit_idx < len(units) - 1:
            size /= 1024
            unit_idx += 1

        return f"{size:.2f} {units[unit_idx]}"


class PartitionBarWidget(QFrame):
    """Visual bar representation of disk partitions."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.partitions: List[Dict[str, Any]] = []
        self.total_size: int = 0
        self.setMinimumHeight(40)
        self.setMaximumHeight(60)
        self.setFrameStyle(QFrame.Box | QFrame.Plain)

    def set_partitions(self, partitions: List[Dict[str, Any]], total_size: int) -> None:
        """Set partitions to display."""
        self.partitions = partitions
        self.total_size = total_size
        self.update()  # Trigger repaint

    def paintEvent(self, event) -> None:  # noqa: ARG002
        """Draw partition bars."""
        super().paintEvent(event)

        if not self.partitions or self.total_size == 0:
            return

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Drawing area
        rect = self.contentsRect()
        x_offset = rect.x() + 5
        y = rect.y() + 5
        height = rect.height() - 10
        available_width = rect.width() - 10

        # Draw each partition
        for part in self.partitions:
            part_size = part.get('length', 0)
            if part_size == 0:
                continue

            # Calculate width proportional to size
            width = int((part_size / self.total_size) * available_width)
            if width < 1:
                width = 1

            # Choose color based on type
            color = self._get_partition_color(part)
            painter.fillRect(x_offset, y, width, height, color)

            # Draw border
            painter.setPen(QPen(Qt.black, 1))
            painter.drawRect(x_offset, y, width, height)

            # Draw label if wide enough
            if width > 30:
                painter.setPen(Qt.white if color.lightness() < 128 else Qt.black)
                label = str(part.get('index', part.get('addr', '?')))
                painter.drawText(x_offset, y, width, height, Qt.AlignCenter, label)

            x_offset += width

    @staticmethod
    def _get_partition_color(partition: Dict[str, Any]) -> QColor:
        """Get color for partition based on type."""
        desc = partition.get('description', '').lower()

        # NTFS partitions (typical Windows system)
        if 'ntfs' in desc or '0x07' in desc:
            return QColor(0, 120, 212)  # Blue

        # FAT partitions
        if 'fat' in desc or '0x0b' in desc or '0x0c' in desc:
            return QColor(76, 175, 80)  # Green

        # EFI/Boot partitions
        if 'efi' in desc or 'boot' in desc or '0xef' in desc:
            return QColor(255, 152, 0)  # Orange

        # Recovery/Hidden partitions
        if 'recovery' in desc or 'hidden' in desc:
            return QColor(156, 39, 176)  # Purple

        # Linux ext partitions
        if 'ext' in desc or 'linux' in desc or '0x83' in desc:
            return QColor(33, 150, 243)  # Light Blue

        # Unknown/Other
        return QColor(158, 158, 158)  # Gray
