"""
Remove evidence dialog - select and confirm evidence removal.

Added evidence removal functionality.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QRadioButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)


class RemoveEvidenceDialog(QDialog):
    """
    Dialog for selecting and confirming evidence removal.

    Shows a list of evidences as radio buttons, displays a summary
    of data that will be deleted, and requires confirmation.
    """

    def __init__(
        self,
        evidences: List[Dict[str, Any]],
        get_counts_callback,
        parent: Optional[QWidget] = None,
    ) -> None:
        """
        Initialize the dialog.

        Args:
            evidences: List of evidence dicts with id, label, source_path, etc.
            get_counts_callback: Callable(evidence_id) -> dict of table counts
            parent: Parent widget
        """
        super().__init__(parent)
        self.evidences = evidences
        self.get_counts_callback = get_counts_callback
        self.selected_evidence_id: Optional[int] = None
        self._selected_evidence_label: Optional[str] = None

        self.setWindowTitle("Remove Evidence")
        self.setMinimumWidth(500)
        self.setMinimumHeight(400)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout()

        # Warning header
        warning_label = QLabel(
            "⚠️ <b>Warning:</b> Removing evidence will permanently delete all "
            "extracted data, downloads, and logs associated with that evidence. "
            "This action cannot be undone."
        )
        warning_label.setWordWrap(True)
        warning_label.setStyleSheet("color: #d97000; padding: 10px; background: #fff8e6; border-radius: 4px;")
        layout.addWidget(warning_label)

        # Evidence selection group
        selection_group = QGroupBox("Select Evidence to Remove")
        selection_layout = QVBoxLayout()

        if not self.evidences:
            no_evidence_label = QLabel("No evidences available to remove.")
            no_evidence_label.setStyleSheet("color: gray; font-style: italic;")
            selection_layout.addWidget(no_evidence_label)
        else:
            # Create scroll area for many evidences
            scroll_area = QScrollArea()
            scroll_area.setWidgetResizable(True)
            scroll_area.setMaximumHeight(200)

            scroll_widget = QWidget()
            scroll_layout = QVBoxLayout()
            scroll_layout.setContentsMargins(5, 5, 5, 5)

            self.radio_group = QButtonGroup(self)
            self.radio_buttons: Dict[int, QRadioButton] = {}

            for evidence in self.evidences:
                ev_id = evidence["id"]
                label = evidence.get("label", f"Evidence {ev_id}")
                source = evidence.get("source_path", "Unknown source")

                # Create radio button with evidence info
                radio_text = f"{label}\n    Source: {source}"
                radio = QRadioButton(radio_text)
                radio.setProperty("evidence_id", ev_id)
                radio.setProperty("evidence_label", label)

                self.radio_group.addButton(radio, ev_id)
                self.radio_buttons[ev_id] = radio
                scroll_layout.addWidget(radio)

            scroll_layout.addStretch()
            scroll_widget.setLayout(scroll_layout)
            scroll_area.setWidget(scroll_widget)
            selection_layout.addWidget(scroll_area)

            # Connect selection changed
            self.radio_group.buttonClicked.connect(self._on_selection_changed)

        selection_group.setLayout(selection_layout)
        layout.addWidget(selection_group)

        # Summary group (shows what will be deleted)
        self.summary_group = QGroupBox("Data Summary")
        self.summary_layout = QVBoxLayout()

        self.summary_label = QLabel("Select an evidence to see what will be deleted.")
        self.summary_label.setStyleSheet("color: gray; font-style: italic;")
        self.summary_label.setWordWrap(True)
        self.summary_layout.addWidget(self.summary_label)

        self.summary_group.setLayout(self.summary_layout)
        layout.addWidget(self.summary_group)

        # Buttons
        self.button_box = QDialogButtonBox()
        self.remove_button = self.button_box.addButton(
            "Remove Evidence", QDialogButtonBox.AcceptRole
        )
        self.remove_button.setEnabled(False)
        self.remove_button.setStyleSheet("background-color: #d9534f; color: white;")

        self.cancel_button = self.button_box.addButton(QDialogButtonBox.Cancel)

        self.button_box.accepted.connect(self._confirm_removal)
        self.button_box.rejected.connect(self.reject)

        layout.addWidget(self.button_box)

        self.setLayout(layout)

    def _on_selection_changed(self, button: QRadioButton) -> None:
        """Handle evidence selection change."""
        ev_id = button.property("evidence_id")
        ev_label = button.property("evidence_label")

        self.selected_evidence_id = ev_id
        self._selected_evidence_label = ev_label
        self.remove_button.setEnabled(True)

        # Get and display counts
        try:
            counts = self.get_counts_callback(ev_id)
            self._update_summary(ev_label, counts)
        except Exception as e:
            self.summary_label.setText(f"Error loading data summary: {e}")
            self.summary_label.setStyleSheet("color: red;")

    def _update_summary(self, label: str, counts: Dict[str, int]) -> None:
        """Update the summary label with deletion details."""
        # Key tables to highlight
        key_tables = {
            "urls": "URLs",
            "images": "Images",
            "browser_history": "Browser History Records",
            "cookies": "Cookies",
            "bookmarks": "Bookmarks",
            "browser_downloads": "Browser Downloads",
            "file_list": "File List Entries",
            "os_indicators": "OS Indicators",
            "timeline": "Timeline Events",
        }

        lines = [f"<b>Evidence:</b> {label}", "", "<b>Data to be deleted:</b>"]

        total_records = 0
        for table, display_name in key_tables.items():
            count = counts.get(table, 0)
            if count > 0:
                lines.append(f"  • {display_name}: {count:,}")
                total_records += count

        # Sum remaining tables
        other_count = sum(
            c for t, c in counts.items()
            if t not in key_tables and c > 0
        )
        if other_count > 0:
            lines.append(f"  • Other records: {other_count:,}")
            total_records += other_count

        if total_records == 0:
            lines.append("  • No extracted data found")

        lines.append("")
        lines.append("<b>Also deleted:</b>")
        lines.append("  • Evidence database file")
        lines.append("  • Extracted artifacts folder")
        lines.append("  • Downloaded files")
        lines.append("  • Thumbnails")
        lines.append("  • Evidence log file")

        self.summary_label.setText("<br>".join(lines))
        self.summary_label.setStyleSheet("")  # Reset style

    def _confirm_removal(self) -> None:
        """Show final confirmation dialog before accepting."""
        if self.selected_evidence_id is None:
            return

        label = self._selected_evidence_label or f"Evidence {self.selected_evidence_id}"

        reply = QMessageBox.warning(
            self,
            "Confirm Evidence Removal",
            f"Are you sure you want to permanently remove:\n\n"
            f"  {label}\n\n"
            f"This will delete all associated data and cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            self.accept()

    def get_selected_evidence(self) -> Optional[Dict[str, Any]]:
        """
        Get the selected evidence dict.

        Returns:
            Evidence dict if selected, None otherwise
        """
        if self.selected_evidence_id is None:
            return None

        for ev in self.evidences:
            if ev["id"] == self.selected_evidence_id:
                return ev
        return None
