"""
Case Information Widget
Shows and edits case metadata (case number, name, investigator, notes).
Displays evidence list with case-wide batch operations.
"""

from typing import Any, Dict, List

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QFormLayout, QLineEdit,
    QTextEdit, QLabel, QGroupBox, QHBoxLayout, QPushButton
)
from PySide6.QtCore import Signal

from .evidence_list import EvidenceListWidget


class CaseInfoWidget(QWidget):
    """Widget for displaying and editing case metadata."""

    # Signal emitted when any field changes (field_name, new_value)
    field_changed = Signal(str, str)

    # Signal emitted when case-wide extraction is requested (list of evidence_ids)
    extract_all_requested = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._evidences: List[Dict[str, Any]] = []
        self._setup_ui()
        self._connect_signals()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Case details group
        group = QGroupBox("Case Information")
        form = QFormLayout()

        self.case_number_edit = QLineEdit()
        self.case_number_edit.setPlaceholderText("e.g., 2025-10-001")
        form.addRow("Case Number:", self.case_number_edit)

        self.case_name_edit = QLineEdit()
        self.case_name_edit.setPlaceholderText("e.g., Investigation XYZ")
        form.addRow("Case Name:", self.case_name_edit)

        self.investigator_edit = QLineEdit()
        self.investigator_edit.setPlaceholderText("e.g., John Doe")
        form.addRow("Investigator:", self.investigator_edit)

        self.case_notes_edit = QTextEdit()
        self.case_notes_edit.setPlaceholderText(
            "Enter case notes, background, objectives, etc."
        )
        self.case_notes_edit.setMinimumHeight(150)
        form.addRow("Case Notes:", self.case_notes_edit)

        group.setLayout(form)
        layout.addWidget(group)

        # === Evidence list group ===
        self.evidence_group = QGroupBox("Case Evidences")
        ev_layout = QVBoxLayout()

        self.evidence_list_widget = EvidenceListWidget()
        ev_layout.addWidget(self.evidence_list_widget)

        # Batch operation buttons
        ops_layout = QHBoxLayout()

        self.extract_ingest_btn = QPushButton("⚡ Extract && Ingest All Selected")
        self.extract_ingest_btn.setToolTip(
            "Run extraction and ingestion for all selected evidences"
        )
        self.extract_ingest_btn.clicked.connect(self._on_extract_ingest_clicked)
        self.extract_ingest_btn.setEnabled(False)
        ops_layout.addWidget(self.extract_ingest_btn)

        ops_layout.addStretch()
        ev_layout.addLayout(ops_layout)

        self.evidence_group.setLayout(ev_layout)
        self.evidence_group.setVisible(False)  # Hidden until case loaded
        layout.addWidget(self.evidence_group)

        layout.addStretch()

        # Info label
        info = QLabel(
            "Changes are automatically saved to the case database."
        )
        info.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(info)

    def _connect_signals(self):
        """Connect edit signals to emit field_changed."""
        self.case_number_edit.textChanged.connect(
            lambda text: self.field_changed.emit("case_number", text)
        )
        self.case_name_edit.textChanged.connect(
            lambda text: self.field_changed.emit("case_name", text)
        )
        self.investigator_edit.textChanged.connect(
            lambda text: self.field_changed.emit("investigator", text)
        )
        self.case_notes_edit.textChanged.connect(
            lambda: self.field_changed.emit("notes", self.case_notes_edit.toPlainText())
        )

        # Connect evidence selection to button state
        self.evidence_list_widget.selection_changed.connect(
            self._on_evidence_selection_changed
        )

    def load_case_data(self, case_data: dict):
        """Load case metadata into form fields."""
        # Block signals while loading to avoid triggering saves
        self.case_number_edit.blockSignals(True)
        self.case_name_edit.blockSignals(True)
        self.investigator_edit.blockSignals(True)
        self.case_notes_edit.blockSignals(True)

        self.case_number_edit.setText(case_data.get("case_number") or "")
        self.case_name_edit.setText(case_data.get("case_name") or "")
        self.investigator_edit.setText(case_data.get("investigator") or "")
        self.case_notes_edit.setPlainText(case_data.get("notes") or "")

        self.case_number_edit.blockSignals(False)
        self.case_name_edit.blockSignals(False)
        self.investigator_edit.blockSignals(False)
        self.case_notes_edit.blockSignals(False)

    def load_evidences(self, evidences: List[Dict[str, Any]]):
        """
        Load evidence list for batch operations.

        Args:
            evidences: List of evidence dicts from CaseDataAccess.list_evidences()
        """
        self._evidences = evidences
        self.evidence_list_widget.load_evidences(evidences)

        # Show evidence group only if there are evidences
        has_evidences = len(evidences) > 0
        self.evidence_group.setVisible(has_evidences)

        # Update button state
        self._on_evidence_selection_changed(
            self.evidence_list_widget.get_selected_evidence_ids()
        )

    def get_case_data(self) -> dict:
        """Get current form data as dict."""
        return {
            "case_number": self.case_number_edit.text() or None,
            "case_name": self.case_name_edit.text() or None,
            "investigator": self.investigator_edit.text() or None,
            "notes": self.case_notes_edit.toPlainText() or None,
        }

    def _on_evidence_selection_changed(self, selected_ids: List[int]):
        """Update button state based on evidence selection."""
        has_selection = len(selected_ids) > 0
        self.extract_ingest_btn.setEnabled(has_selection)

        # Update button text to show count
        if has_selection:
            self.extract_ingest_btn.setText(
                f"⚡ Extract && Ingest All Selected ({len(selected_ids)})"
            )
        else:
            self.extract_ingest_btn.setText("⚡ Extract && Ingest All Selected")

    def _on_extract_ingest_clicked(self):
        """Emit signal to trigger case-wide extraction."""
        selected_ids = self.evidence_list_widget.get_selected_evidence_ids()
        if selected_ids:
            self.extract_all_requested.emit(selected_ids)

