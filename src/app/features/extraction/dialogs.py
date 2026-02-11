"""
Extraction feature dialogs.

Provides dialog components for extraction workflows:
- PurgeDataDialog: Purge all ingested data for an evidence
- ExtractAndIngestDialog: Combined extraction and ingestion workflow

Extracted from features/extraction/tab.py
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QMessageBox,
    QDialog,
    QDialogButtonBox,
    QCheckBox,
    QListWidget,
    QListWidgetItem,
    QSplitter,
)
from PySide6.QtGui import QFont

from core.logging import get_logger
from core.extractor_sections import (
    EXTRACTOR_SECTIONS,
    group_extractors_by_section,
)

if TYPE_CHECKING:
    from core.audit_logging import AuditLogger

LOGGER = get_logger(__name__)


class PurgeDataDialog(QDialog):
    """
    Dialog for purging all ingested artifact data for an evidence.

    Shows preview of what will be deleted with table-by-table counts,
    requires explicit confirmation before executing deletion.

    Preserves:
    - browser_cache_inventory (extraction metadata)
    - report_config (user preferences)
    - Schema metadata
    """

    def __init__(
        self,
        evidence_id: int,
        evidence_label: str,
        db_manager,
        audit_logger: Optional["AuditLogger"] = None,
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.db_manager = db_manager
        self.audit_logger = audit_logger
        self.confirmed = False

        self.setWindowTitle("⚠️ Purge Evidence Data")
        self.setMinimumSize(700, 600)

        self._setup_ui()
        self._load_counts()

    def _setup_ui(self):
        """Build dialog UI."""
        from PySide6.QtWidgets import QTableWidget, QTableWidgetItem

        layout = QVBoxLayout(self)

        # Warning header
        warning = QLabel(
            f"<h2>⚠️ Purge Evidence Data</h2>"
            f"<p><b>Warning:</b> This will permanently delete ALL ingested artifact data for "
            f"evidence <b>{self.evidence_label}</b>.</p>"
            f"<p>Extraction output files and configuration will be preserved, allowing you to "
            f"re-run ingestion with different settings.</p>"
        )
        warning.setWordWrap(True)
        layout.addWidget(warning)

        # Table counts preview
        counts_label = QLabel("<b>Data to be deleted:</b>")
        layout.addWidget(counts_label)

        self.counts_table = QTableWidget()
        self.counts_table.setColumnCount(2)
        self.counts_table.setHorizontalHeaderLabels(["Table", "Records"])
        self.counts_table.horizontalHeader().setStretchLastSection(True)
        self.counts_table.setAlternatingRowColors(True)
        self.counts_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.counts_table.setSelectionMode(QTableWidget.NoSelection)
        layout.addWidget(self.counts_table)

        # Total summary
        self.total_label = QLabel()
        total_font = QFont()
        total_font.setBold(True)
        self.total_label.setFont(total_font)
        layout.addWidget(self.total_label)

        # Confirmation checkbox
        self.confirm_checkbox = QCheckBox(
            "I understand this action cannot be undone and will delete all ingested data"
        )
        self.confirm_checkbox.stateChanged.connect(self._on_confirm_changed)
        layout.addWidget(self.confirm_checkbox)

        # Buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok
        )
        self.ok_button = buttons.button(QDialogButtonBox.Ok)
        self.ok_button.setText("🗑️ Purge All Data")
        self.ok_button.setEnabled(False)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load_counts(self):
        """Load and display table counts."""
        from PySide6.QtWidgets import QTableWidgetItem

        try:
            from core.database import get_evidence_table_counts

            # Get evidence connection
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id,
                self.evidence_label
            )

            try:
                counts = get_evidence_table_counts(evidence_conn, self.evidence_id)
            finally:
                evidence_conn.close()

            # Filter out zero-count tables
            non_zero_counts = {k: v for k, v in counts.items() if v > 0}

            # Populate table
            self.counts_table.setRowCount(len(non_zero_counts) + 1)  # +1 for total row

            total = 0
            row = 0
            for table, count in sorted(non_zero_counts.items()):
                self.counts_table.setItem(row, 0, QTableWidgetItem(table))
                self.counts_table.setItem(row, 1, QTableWidgetItem(f"{count:,}"))
                total += count
                row += 1

            # Add total row
            total_table_item = QTableWidgetItem("TOTAL")
            total_table_item.setFont(QFont("", -1, QFont.Bold))
            total_count_item = QTableWidgetItem(f"{total:,}")
            total_count_item.setFont(QFont("", -1, QFont.Bold))
            self.counts_table.setItem(row, 0, total_table_item)
            self.counts_table.setItem(row, 1, total_count_item)

            self.total_label.setText(
                f"<b>Total records to be deleted: {total:,}</b>"
            )

        except Exception as e:
            LOGGER.exception("Failed to load evidence counts")
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to load data counts:\n{e}"
            )
            self.reject()

    def _on_confirm_changed(self, state):
        """Enable OK button only when confirmed."""
        # stateChanged emits int, must compare with .value
        self.ok_button.setEnabled(state == Qt.Checked.value)

    def accept(self):
        """Execute purge if confirmed."""
        if not self.confirm_checkbox.isChecked():
            return

        try:
            from core.database import purge_evidence_data

            # Get evidence connection
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id,
                self.evidence_label
            )

            try:
                deleted_count = purge_evidence_data(evidence_conn, self.evidence_id)

                # Clear statistics cache so UI doesn't show stale cards
                from core.statistics_collector import StatisticsCollector
                collector = StatisticsCollector.get_instance()
                if collector:
                    collector.clear_evidence_stats(self.evidence_id, self.evidence_label)

                # Log to audit trail if available
                if self.audit_logger and self.audit_logger.case_logger:
                    self.audit_logger.case_logger._write_to_db(
                        "INFO",
                        "data_management",
                        "purge_data",
                        "evidence",
                        self.evidence_id,
                        {"evidence_label": self.evidence_label, "deleted_count": deleted_count}
                    )

                LOGGER.info(f"Successfully purged {deleted_count} records for evidence {self.evidence_id}")
                self.confirmed = True

            finally:
                evidence_conn.close()

            super().accept()

        except Exception as e:
            LOGGER.exception("Purge failed")
            QMessageBox.critical(
                self,
                "Purge Failed",
                f"Failed to purge data:\n{e}"
            )


class ExtractAndIngestDialog(QDialog):
    """
    Dialog for combined extraction and ingestion workflow.

    Shows checkboxes for each extractor, allows mode selection for ingestion,
    then runs both extraction and ingestion phases sequentially.
    """

    def __init__(
        self,
        extractors: list,
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.extractors = extractors
        self.selected_extractors: list = []
        self.selected_mode = 'overwrite'

        self.setWindowTitle("Extract & Ingest")
        self.setMinimumSize(700, 550)
        self.resize(800, 600)

        self._setup_ui()

    def _setup_ui(self):
        """Build the dialog UI."""
        layout = QVBoxLayout(self)

        # Info header
        info = QLabel(
            "<h3>⚡ Extract & Ingest</h3>"
            "<p>Extract data from evidence and immediately ingest into database in one step.</p>"
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        # Main content with splitter
        splitter = QSplitter(Qt.Horizontal)

        # Left: Extractor list with checkboxes
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)

        list_label = QLabel("<b>Available Extractors:</b>")
        left_layout.addWidget(list_label)

        self.extractor_list = QListWidget()
        self.extractor_list.setAlternatingRowColors(True)

        # Group extractors using shared EXTRACTOR_SECTIONS
        grouped = group_extractors_by_section(self.extractors)

        for section_def in EXTRACTOR_SECTIONS:
            section_name = section_def["name"]
            extractors_in_section = grouped.get(section_name, [])

            # Filter to extractors that can extract
            extractable = [e for e in extractors_in_section if e.metadata.can_extract]

            if not extractable:
                continue

            # Add section header (non-checkable)
            header_text = f"{section_def['icon']} {section_name}"
            header_item = QListWidgetItem(header_text)
            header_item.setFlags(Qt.NoItemFlags)  # Not selectable
            header_font = QFont()
            header_font.setBold(True)
            header_item.setFont(header_font)
            header_item.setBackground(self.palette().alternateBase())
            self.extractor_list.addItem(header_item)

            # Sort extractors by defined order within section
            section_order = section_def["extractors"]

            def get_sort_index(ext):
                name = ext.metadata.name
                if name in section_order:
                    return section_order.index(name)
                return 999

            for extractor in sorted(extractable, key=get_sort_index):
                meta = extractor.metadata

                # Mark non-ingestible extractors
                suffix = "" if meta.can_ingest else " (extract only)"
                item = QListWidgetItem(f"    {meta.display_name}{suffix}")
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(Qt.Unchecked)
                item.setData(Qt.UserRole, extractor)
                item.setToolTip(meta.description)
                self.extractor_list.addItem(item)

        self.extractor_list.itemChanged.connect(self._update_preview)
        left_layout.addWidget(self.extractor_list)

        # Select All / None buttons
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self._select_all)
        select_none_btn = QPushButton("Select None")
        select_none_btn.clicked.connect(self._select_none)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(select_none_btn)
        left_layout.addLayout(btn_layout)

        splitter.addWidget(left_widget)

        # Right: Mode selector and preview
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Mode selector (for ingestion phase)
        mode_label = QLabel("<b>Ingestion Mode:</b>")
        right_layout.addWidget(mode_label)

        from PySide6.QtWidgets import QRadioButton, QButtonGroup
        self.mode_group = QButtonGroup()

        self.overwrite_radio = QRadioButton("Overwrite existing data")
        self.overwrite_radio.setToolTip("Delete existing data before ingesting (recommended)")
        self.overwrite_radio.setChecked(True)
        self.mode_group.addButton(self.overwrite_radio, 0)
        right_layout.addWidget(self.overwrite_radio)

        self.append_radio = QRadioButton("Append to existing data")
        self.append_radio.setToolTip("Keep existing data, add new records (may create duplicates)")
        self.mode_group.addButton(self.append_radio, 1)
        right_layout.addWidget(self.append_radio)

        self.skip_radio = QRadioButton("Skip if data exists")
        self.skip_radio.setToolTip("Skip ingestion for extractors with existing data")
        self.mode_group.addButton(self.skip_radio, 2)
        right_layout.addWidget(self.skip_radio)

        self.mode_group.buttonClicked.connect(self._update_mode)

        right_layout.addSpacing(20)

        # Preview (dual-column table)
        preview_label = QLabel("<b>Execution Preview:</b>")
        right_layout.addWidget(preview_label)

        from PySide6.QtWidgets import QTableWidget, QHeaderView
        self.preview_table = QTableWidget()
        self.preview_table.setColumnCount(3)
        self.preview_table.setHorizontalHeaderLabels(["Extractor", "Extract", "Ingest"])
        self.preview_table.horizontalHeader().setStretchLastSection(True)
        self.preview_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.preview_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.preview_table.setSelectionMode(QTableWidget.NoSelection)
        self.preview_table.setAlternatingRowColors(True)
        right_layout.addWidget(self.preview_table)

        # Summary label below table
        self.summary_label = QLabel("")
        self.summary_label.setWordWrap(True)
        right_layout.addWidget(self.summary_label)

        splitter.addWidget(right_widget)
        splitter.setSizes([350, 350])

        layout.addWidget(splitter)

        # Dialog buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok
        )
        self.run_btn = buttons.button(QDialogButtonBox.Ok)
        self.run_btn.setText("⚡ Extract & Ingest")
        self.run_btn.setEnabled(False)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._update_preview()

    def _select_all(self):
        """Select all extractors."""
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if item.flags() & Qt.ItemIsUserCheckable:
                item.setCheckState(Qt.Checked)

    def _select_none(self):
        """Deselect all extractors."""
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if item.flags() & Qt.ItemIsUserCheckable:
                item.setCheckState(Qt.Unchecked)

    def _update_mode(self):
        """Update selected mode based on radio buttons."""
        if self.overwrite_radio.isChecked():
            self.selected_mode = 'overwrite'
        elif self.append_radio.isChecked():
            self.selected_mode = 'append'
        elif self.skip_radio.isChecked():
            self.selected_mode = 'skip_existing'
        self._update_preview()

    def _update_preview(self):
        """Update the execution order preview (dual-column table)."""
        from PySide6.QtWidgets import QTableWidgetItem
        from PySide6.QtCore import Qt

        selected = self.get_selected_extractors()
        self.run_btn.setEnabled(len(selected) > 0)

        if not selected:
            self.preview_table.setRowCount(0)
            self.summary_label.setText("")
            return

        # Populate table
        self.preview_table.setRowCount(len(selected))
        extract_count = 0
        ingest_count = 0

        for i, extractor in enumerate(selected):
            meta = extractor.metadata

            # Column 0: Extractor name
            name_item = QTableWidgetItem(meta.display_name)
            self.preview_table.setItem(i, 0, name_item)

            # Column 1: Extract phase
            extract_item = QTableWidgetItem("✓ Extract")
            extract_item.setTextAlignment(Qt.AlignCenter)
            self.preview_table.setItem(i, 1, extract_item)
            extract_count += 1

            # Column 2: Ingest phase
            if meta.can_ingest:
                ingest_item = QTableWidgetItem("✓ Ingest")
                ingest_item.setTextAlignment(Qt.AlignCenter)
                ingest_count += 1
            else:
                ingest_item = QTableWidgetItem("⊗ N/A")
                ingest_item.setTextAlignment(Qt.AlignCenter)
                ingest_item.setForeground(Qt.gray)
            self.preview_table.setItem(i, 2, ingest_item)

        # Update summary
        mode_text = self.selected_mode.replace('_', ' ').title()
        self.summary_label.setText(
            f"<b>Ingestion Mode:</b> {mode_text}<br>"
            f"<b>Total Operations:</b> {extract_count} extractions, {ingest_count} ingestions"
        )

    def get_selected_extractors(self) -> list:
        """Return list of selected extractors in order."""
        selected = []
        for i in range(self.extractor_list.count()):
            item = self.extractor_list.item(i)
            if item.flags() & Qt.ItemIsUserCheckable and item.checkState() == Qt.Checked:
                extractor = item.data(Qt.UserRole)
                if extractor:
                    selected.append(extractor)
        return selected

    def get_selected_mode(self) -> str:
        """Return selected ingestion mode."""
        return self.selected_mode
