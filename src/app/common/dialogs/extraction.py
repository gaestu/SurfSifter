"""
Extraction-related dialogs - bulk extractor reuse and case-wide extraction.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)


class BulkExtractorReuseDialog(QDialog):
    """
    Dialog for prompting user about reusing existing bulk_extractor output.

    Shows metadata about existing output and asks whether to:
    - Skip entirely (use existing database data, don't run or ingest)
    - Reuse existing output (skip bulk_extractor, re-ingest from files)
    - Overwrite and re-run bulk_extractor
    - Cancel extraction

    Also offers option to delete existing database records before re-ingesting.
    """

    def __init__(
        self,
        output_dir: Path,
        evidence_conn: Optional[sqlite3.Connection] = None,
        evidence_id: Optional[int] = None,
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.output_dir = output_dir
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self.selected_policy = "skip"  # Default to skip (safest - doesn't change anything)

        self.setWindowTitle("Existing bulk_extractor Output Found")
        self.setModal(True)
        self.setMinimumWidth(600)

        layout = QVBoxLayout(self)

        # Info message
        info_label = QLabel(
            f"bulk_extractor output already exists at:\n{str(output_dir)}"
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # File metadata group
        file_meta_group = QGroupBox("Output Files")
        file_meta_layout = QFormLayout()

        # Get file metadata
        file_count = sum(1 for _ in output_dir.rglob("*.txt"))
        total_size = sum(f.stat().st_size for f in output_dir.rglob("*") if f.is_file())
        size_mb = total_size / (1024 * 1024)

        # Try to get timestamp from report.xml
        report_xml = output_dir / "report.xml"
        timestamp_str = "Unknown"
        if report_xml.exists():
            import datetime
            mtime = report_xml.stat().st_mtime
            timestamp_str = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

        file_meta_layout.addRow("Files:", QLabel(str(file_count)))
        file_meta_layout.addRow("Total Size:", QLabel(f"{size_mb:.1f} MB"))
        file_meta_layout.addRow("Last Modified:", QLabel(timestamp_str))

        file_meta_group.setLayout(file_meta_layout)
        layout.addWidget(file_meta_group)

        # Database metadata group (if connection provided)
        if evidence_conn and evidence_id is not None:
            db_meta_group = QGroupBox("Database Records")
            db_meta_layout = QFormLayout()

            # Count existing records by discovered_by = 'bulk_extractor'
            cursor = evidence_conn.cursor()

            # URLs
            cursor.execute(
                "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND discovered_by = 'bulk_extractor'",
                (evidence_id,)
            )
            url_count = cursor.fetchone()[0]

            # Other artifacts (email, domain, etc.)
            cursor.execute(
                "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND discovered_by = 'bulk_extractor' AND url NOT LIKE 'http%'",
                (evidence_id,)
            )
            artifact_count = cursor.fetchone()[0]

            db_meta_layout.addRow("URLs:", QLabel(str(url_count - artifact_count)))
            db_meta_layout.addRow("Other Artifacts:", QLabel(str(artifact_count)))
            db_meta_layout.addRow("Total Records:", QLabel(str(url_count)))

            db_meta_group.setLayout(db_meta_layout)
            layout.addWidget(db_meta_group)

            self.has_db_data = url_count > 0
        else:
            self.has_db_data = False

        # Policy selection
        policy_group = QGroupBox("Action")
        policy_layout = QVBoxLayout()

        # Option 1: Skip entirely (safest - don't change anything)
        self.skip_radio = QRadioButton(
            "Skip entirely (use existing database data - fastest, no changes)"
        )
        if self.has_db_data:
            self.skip_radio.setChecked(True)  # Default if DB has data
        policy_layout.addWidget(self.skip_radio)

        # Option 2: Reuse output files (re-ingest from files)
        self.reuse_radio = QRadioButton(
            "Reuse existing output (skip bulk_extractor, re-ingest from files)"
        )
        if not self.has_db_data:
            self.reuse_radio.setChecked(True)  # Default if no DB data yet
        policy_layout.addWidget(self.reuse_radio)

        # Option 3: Overwrite everything (fresh run)
        self.overwrite_radio = QRadioButton(
            "Overwrite and re-run bulk_extractor (delete files, fresh extraction)"
        )
        policy_layout.addWidget(self.overwrite_radio)

        policy_group.setLayout(policy_layout)
        layout.addWidget(policy_group)

        # Delete database records checkbox (only for reuse/overwrite)
        self.delete_db_checkbox = QCheckBox(
            "Delete existing database records before ingesting (recommended if previous ingest was incomplete)"
        )
        self.delete_db_checkbox.setEnabled(self.has_db_data)  # Only enable if DB has data
        layout.addWidget(self.delete_db_checkbox)

        # Connect radio buttons to update delete checkbox state
        self.skip_radio.toggled.connect(self._update_delete_checkbox_state)
        self.reuse_radio.toggled.connect(self._update_delete_checkbox_state)
        self.overwrite_radio.toggled.connect(self._update_delete_checkbox_state)

        # Set initial delete checkbox state based on default selection
        self._update_delete_checkbox_state()

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _update_delete_checkbox_state(self):
        """Update delete checkbox enabled state based on selected option."""
        # Only enable delete checkbox for reuse/overwrite (not skip)
        if self.skip_radio.isChecked():
            self.delete_db_checkbox.setEnabled(False)
            self.delete_db_checkbox.setChecked(False)
        else:
            self.delete_db_checkbox.setEnabled(self.has_db_data)

    def get_policy(self) -> str:
        """
        Get the selected policy.

        Returns:
            "skip" if skip selected
            "reuse" if reuse selected
            "overwrite" if overwrite selected
        """
        if self.skip_radio.isChecked():
            return "skip"
        elif self.reuse_radio.isChecked():
            return "reuse"
        else:
            return "overwrite"

    def should_delete_db_records(self) -> bool:
        """
        Check if user wants to delete existing database records.

        Returns:
            True if delete checkbox is checked and policy is not 'skip'
        """
        return self.delete_db_checkbox.isChecked() and not self.skip_radio.isChecked()


class CaseWideExtractIngestDialog(QDialog):
    """
    Dialog for selecting extractors to run across multiple evidences.

    Shows:
    - Selected evidences (read-only list)
    - Extractor selection with checkboxes (registry-driven)
    - Overwrite mode selection (using existing mode names)
    - Progress tracking during execution

    Args:
        evidences: List of evidence dicts to process
        parent: Parent widget
    """

    def __init__(
        self,
        evidences: List[Dict[str, Any]],
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.evidences = evidences
        self.selected_extractors: List[str] = []
        self.overwrite_mode = 'overwrite'
        self.extractor_configs: Dict[str, Dict[str, Any]] = {}
        self._extractor_by_name: Dict[str, Any] = {}

        self.setWindowTitle("Case-Wide Extract & Ingest")
        self.setMinimumSize(700, 600)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(
            f"<h3>⚡ Case-Wide Extract & Ingest</h3>"
            f"<p>Process <b>{len(self.evidences)} evidence(s)</b> with selected extractors.</p>"
        )
        header.setWordWrap(True)
        layout.addWidget(header)

        # Splitter: Evidence list | Extractor selection
        splitter = QSplitter(Qt.Horizontal)

        # Left: Evidence list (read-only)
        evidence_group = QGroupBox(f"Selected Evidences ({len(self.evidences)})")
        ev_layout = QVBoxLayout(evidence_group)
        self.evidence_list = QListWidget()
        self.evidence_list.setEnabled(False)  # Read-only
        for ev in self.evidences:
            self.evidence_list.addItem(ev.get('label', f"Evidence {ev['id']}"))
        ev_layout.addWidget(self.evidence_list)
        splitter.addWidget(evidence_group)

        # Right: Extractor selection (registry-driven)
        extractor_group = QGroupBox("Select Extractors")
        ext_layout = QVBoxLayout(extractor_group)

        self.extractor_list = QListWidget()
        self._populate_extractors_from_registry()
        ext_layout.addWidget(self.extractor_list)

        # Select All/None buttons
        btn_row = QHBoxLayout()
        select_all = QPushButton("Select All")
        select_all.clicked.connect(self._select_all_extractors)
        select_none = QPushButton("Select None")
        select_none.clicked.connect(self._select_none_extractors)
        btn_row.addWidget(select_all)
        btn_row.addWidget(select_none)

        configure_btn = QPushButton("Configure Selected…")
        configure_btn.clicked.connect(self._configure_selected_extractor)
        btn_row.addWidget(configure_btn)
        btn_row.addStretch()
        ext_layout.addLayout(btn_row)

        splitter.addWidget(extractor_group)
        splitter.setSizes([300, 400])
        layout.addWidget(splitter)

        # Overwrite mode (using existing mode names from extraction.py)
        mode_group = QGroupBox("Ingestion Mode")
        mode_layout = QHBoxLayout(mode_group)
        self.mode_overwrite = QRadioButton("Overwrite existing")
        self.mode_overwrite.setChecked(True)
        self.mode_append = QRadioButton("Append to existing")
        self.mode_skip = QRadioButton("Skip if exists")
        mode_layout.addWidget(self.mode_overwrite)
        mode_layout.addWidget(self.mode_append)
        mode_layout.addWidget(self.mode_skip)
        mode_layout.addStretch()
        layout.addWidget(mode_group)

        # Warning
        warning = QLabel(
            "⚠️ <b>Note:</b> Evidences will be processed sequentially. "
            "This may take a long time for large images."
        )
        warning.setWordWrap(True)
        warning.setStyleSheet("color: #d97000; padding: 8px;")
        layout.addWidget(warning)

        # Buttons
        btn_box = QDialogButtonBox()
        self.run_btn = QPushButton("▶️ Run")
        self.run_btn.setEnabled(False)
        self.run_btn.clicked.connect(self.accept)
        btn_box.addButton(self.run_btn, QDialogButtonBox.AcceptRole)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_box.addButton(cancel_btn, QDialogButtonBox.RejectRole)

        layout.addWidget(btn_box)

        # Connect extractor selection to enable/disable run button
        self.extractor_list.itemChanged.connect(self._update_run_button)

    def _populate_extractors_from_registry(self):
        """Populate extractor list dynamically from ExtractorRegistry using shared sections."""
        from extractors import ExtractorRegistry
        from core.extractor_sections import EXTRACTOR_SECTIONS, group_extractors_by_section

        registry = ExtractorRegistry()
        all_extractors = [e for e in registry.get_all() if e.metadata.can_extract]

        # Group using shared EXTRACTOR_SECTIONS for consistency
        grouped = group_extractors_by_section(all_extractors)

        # Store extractor references
        for extractor in all_extractors:
            self._extractor_by_name[extractor.metadata.name] = extractor

        # Render sections in defined order
        for section_def in EXTRACTOR_SECTIONS:
            section_name = section_def["name"]
            extractors_in_section = grouped.get(section_name, [])

            if not extractors_in_section:
                continue

            # Sort extractors by defined order within section
            section_order = section_def["extractors"]

            def get_sort_index(ext):
                name = ext.metadata.name
                if name in section_order:
                    return section_order.index(name)
                return 999

            sorted_extractors = sorted(extractors_in_section, key=get_sort_index)

            # Add section header
            header_text = f"{section_def['icon']} {section_name}"
            header = QListWidgetItem(header_text)
            header.setFlags(Qt.NoItemFlags)
            font = header.font()
            font.setBold(True)
            header.setFont(font)
            self.extractor_list.addItem(header)

            # Add extractors in section
            for extractor in sorted_extractors:
                meta = extractor.metadata
                item = QListWidgetItem(f"    {meta.display_name}")
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
                item.setCheckState(Qt.Unchecked)
                item.setData(Qt.UserRole, meta.name)  # Store name, not instance
                item.setToolTip(meta.description)
                self.extractor_list.addItem(item)

    def _select_all_extractors(self):
        """Select all extractors."""
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if item.flags() & Qt.ItemIsUserCheckable:
                item.setCheckState(Qt.Checked)

    def _select_none_extractors(self):
        """Deselect all extractors."""
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if item.flags() & Qt.ItemIsUserCheckable:
                item.setCheckState(Qt.Unchecked)

    def _update_run_button(self):
        """Enable/disable run button based on selection."""
        has_selection = any(
            self.extractor_list.item(i).checkState() == Qt.Checked
            for i in range(self.extractor_list.count())
            if self.extractor_list.item(i).flags() & Qt.ItemIsUserCheckable
        )
        self.run_btn.setEnabled(has_selection)

    def _configure_selected_extractor(self):
        """Open config dialog for the currently selected extractor."""
        item = self.extractor_list.currentItem()
        if not item or not (item.flags() & Qt.ItemIsUserCheckable):
            return

        name = item.data(Qt.UserRole)
        extractor = self._extractor_by_name.get(name)
        if not extractor:
            return

        config_widget = extractor.get_config_widget(self)
        if not config_widget:
            QMessageBox.information(
                self,
                "Configuration",
                f"{extractor.metadata.display_name} has no configuration options."
            )
            return

        dialog = QDialog(self)
        dialog.setWindowTitle(f"{extractor.metadata.display_name} - Configuration")
        dialog.resize(600, 400)
        dlg_layout = QVBoxLayout()
        dlg_layout.addWidget(config_widget)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        dlg_layout.addWidget(buttons)
        dialog.setLayout(dlg_layout)

        if dialog.exec() == QDialog.Accepted and hasattr(config_widget, 'get_config'):
            config = config_widget.get_config()
            extractor._config = config  # Store on instance for reuse
            self.extractor_configs[name] = config

    def get_selected_extractors(self) -> List[str]:
        """Return list of selected extractor names."""
        selected = []
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if (item.flags() & Qt.ItemIsUserCheckable) and item.checkState() == Qt.Checked:
                selected.append(item.data(Qt.UserRole))
        return selected

    def get_extractor_configs(self) -> Dict[str, Dict[str, Any]]:
        """Return extractor configs captured in the dialog."""
        return self.extractor_configs.copy()

    def get_overwrite_mode(self) -> str:
        """Return selected ingestion mode using existing naming convention."""
        if self.mode_append.isChecked():
            return 'append'
        if self.mode_skip.isChecked():
            return 'skip_existing'  # Match existing code in extraction.py
        return 'overwrite'
