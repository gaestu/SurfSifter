from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import csv
from pathlib import Path

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
    QTabWidget,
    QFileDialog,
    QMessageBox,
)

from app.common.dialogs.tagging import TagArtifactsDialog
from app.data.case_data import CaseDataAccess
from app.features.os_artifacts.models import IndicatorsTableModel, JumpListsTableModel, InstalledSoftwareModel
from core.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class IndicatorFilters:
    indicator_type: str = ""


class OSArtifactsTab(QWidget):
    def __init__(self, case_data: Optional[CaseDataAccess] = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.filters = IndicatorFilters()

        # Phase 3: Lazy loading state
        self._data_loaded = False
        self._load_pending = False

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        layout = QVBoxLayout()

        # Create tab widget for Registry and Platform Detections
        self.tabs = QTabWidget()

        # ===== Registry Tab =====
        registry_widget = QWidget()
        registry_layout = QVBoxLayout(registry_widget)

        # Registry filters
        registry_filter_layout = QHBoxLayout()
        registry_filter_layout.addWidget(QLabel("Indicator Type"))
        self.type_combo = QComboBox()
        self.type_combo.addItem("All", userData="")
        self.type_combo.currentIndexChanged.connect(self._on_filters_changed)
        registry_filter_layout.addWidget(self.type_combo)

        # Export button for registry
        export_registry_btn = QPushButton("📄 Export CSV")
        export_registry_btn.clicked.connect(self._export_registry_csv)
        registry_filter_layout.addWidget(export_registry_btn)

        registry_filter_layout.addStretch()
        registry_layout.addLayout(registry_filter_layout)

        # Registry table
        self.model = IndicatorsTableModel(case_data)
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSortingEnabled(True)
        registry_layout.addWidget(self.table)

        self.summary_label = QLabel("No indicators loaded.")
        registry_layout.addWidget(self.summary_label)

        self.tabs.addTab(registry_widget, "Registry Findings")

        # ===== Jump Lists Tab =====
        self.jump_lists_widget = QWidget()
        jump_lists_layout = QVBoxLayout(self.jump_lists_widget)

        # Jump Lists filters
        jl_filter_layout = QHBoxLayout()
        jl_filter_layout.addWidget(QLabel("Browser"))
        self.jl_browser_combo = QComboBox()
        self.jl_browser_combo.addItem("All", userData="")
        self.jl_browser_combo.currentIndexChanged.connect(self._on_jump_lists_filters_changed)
        jl_filter_layout.addWidget(self.jl_browser_combo)

        jl_filter_layout.addWidget(QLabel("Pin Status"))
        self.jl_pin_combo = QComboBox()
        self.jl_pin_combo.addItem("All", userData="")
        self.jl_pin_combo.addItem("Recent", userData="recent")
        self.jl_pin_combo.addItem("Pinned", userData="pinned")
        self.jl_pin_combo.currentIndexChanged.connect(self._on_jump_lists_filters_changed)
        jl_filter_layout.addWidget(self.jl_pin_combo)

        self.jl_urls_only_checkbox = QCheckBox("URLs Only")
        self.jl_urls_only_checkbox.stateChanged.connect(self._on_jump_lists_filters_changed)
        jl_filter_layout.addWidget(self.jl_urls_only_checkbox)

        # Export button for Jump Lists
        export_jl_btn = QPushButton("📄 Export CSV")
        export_jl_btn.clicked.connect(self._export_jump_lists_csv)
        jl_filter_layout.addWidget(export_jl_btn)

        jl_filter_layout.addStretch()
        jump_lists_layout.addLayout(jl_filter_layout)

        # Jump Lists table (model initialized later when evidence is set)
        self.jl_model: Optional[JumpListsTableModel] = None
        self.jl_table = QTableView()
        self.jl_table.horizontalHeader().setStretchLastSection(True)
        self.jl_table.setSelectionBehavior(QTableView.SelectRows)
        self.jl_table.setSortingEnabled(True)
        self.jl_table.doubleClicked.connect(self._on_jump_list_double_clicked)
        jump_lists_layout.addWidget(self.jl_table)

        self.jl_summary_label = QLabel("No Jump List entries loaded.")
        jump_lists_layout.addWidget(self.jl_summary_label)

        self.tabs.addTab(self.jump_lists_widget, "Jump Lists")

        # ===== Installed Applications Tab =====
        self.software_widget = QWidget()
        software_layout = QVBoxLayout(self.software_widget)

        # Software filters
        sw_filter_layout = QHBoxLayout()
        sw_filter_layout.addWidget(QLabel("Search"))
        self.sw_search_edit = QLineEdit()
        self.sw_search_edit.setPlaceholderText("Filter by name or publisher...")
        self.sw_search_edit.textChanged.connect(self._on_software_filters_changed)
        sw_filter_layout.addWidget(self.sw_search_edit)

        self.sw_forensic_checkbox = QCheckBox("Forensic Interest Only")
        self.sw_forensic_checkbox.stateChanged.connect(self._on_software_filters_changed)
        sw_filter_layout.addWidget(self.sw_forensic_checkbox)

        # Export button for software
        export_sw_btn = QPushButton("📄 Export CSV")
        export_sw_btn.clicked.connect(self._export_software_csv)
        sw_filter_layout.addWidget(export_sw_btn)

        sw_filter_layout.addStretch()
        software_layout.addLayout(sw_filter_layout)

        # Software table (model initialized later when evidence is set)
        self.sw_model: Optional[InstalledSoftwareModel] = None
        self.sw_table = QTableView()
        self.sw_table.horizontalHeader().setStretchLastSection(True)
        self.sw_table.setSelectionBehavior(QTableView.SelectRows)
        self.sw_table.setSelectionMode(QTableView.ExtendedSelection)
        self.sw_table.setSortingEnabled(True)
        self.sw_table.doubleClicked.connect(self._on_software_double_clicked)
        self.sw_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.sw_table.customContextMenuRequested.connect(self._show_software_context_menu)
        software_layout.addWidget(self.sw_table)

        self.sw_summary_label = QLabel("No installed software loaded.")
        software_layout.addWidget(self.sw_summary_label)

        self.tabs.addTab(self.software_widget, "Installed Applications")

        layout.addWidget(self.tabs)

        # Analyze section
        from PySide6.QtWidgets import QGroupBox
        analyze_group = QGroupBox("ANALYZE")
        analyze_layout = QVBoxLayout()
        self.setLayout(layout)

    def set_case_data(self, case_data: Optional[CaseDataAccess], defer_load: bool = False) -> None:
        """
        Set the case data access object.

        Args:
            case_data: CaseDataAccess instance
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.case_data = case_data
        self.model.set_case_data(case_data)
        if not defer_load:
            self._populate_filters()
        else:
            self._data_loaded = False
            self._load_pending = True

    def set_evidence(self, evidence_id: Optional[int], defer_load: bool = False) -> None:
        """
        Set the current evidence ID.

        Args:
            evidence_id: Evidence ID to display
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.evidence_id = evidence_id
        self._data_loaded = False

        if not defer_load:
            # Immediate loading (legacy behavior)
            self.model.set_evidence(evidence_id)
            self._init_jump_lists_model()  # Initialize Jump Lists model
            self._init_software_model()  # Initialize Installed Software model
            self._populate_filters()
            self._populate_jump_lists_filters()
            self._update_summary()
            self._update_jump_lists_summary()
            self._update_software_summary()
        else:
            # Deferred loading - just store the ID, load on showEvent
            self._load_pending = True

    def refresh(self) -> None:
        self.model.reload()
        if self.jl_model:
            self.jl_model.reload()
        if self.sw_model:
            self.sw_model.reload()
        self._update_summary()
        self._update_jump_lists_summary()
        self._update_software_summary()

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    def showEvent(self, event):
        """Override showEvent to perform lazy loading when tab becomes visible."""
        super().showEvent(event)

        # Phase 3: Lazy loading - load data on first visibility
        if self._load_pending and not self._data_loaded:
            QTimer.singleShot(10, self._perform_deferred_load)
        # Refresh if data was marked stale while tab was hidden
        elif self._data_stale and self._data_loaded:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    def _perform_deferred_load(self) -> None:
        """Perform the deferred data loading."""
        if self._data_loaded:
            return

        self._data_loaded = True
        self._load_pending = False

        # Now actually load the data
        if self.evidence_id is not None:
            self.model.set_evidence(self.evidence_id)
            self._init_jump_lists_model()
            self._init_software_model()
        self._populate_filters()
        self._populate_jump_lists_filters()
        self._update_summary()
        self._update_jump_lists_summary()
        self._update_software_summary()

    def _populate_filters(self) -> None:
        """Populate registry indicator type filters."""
        if not self.case_data or self.evidence_id is None:
            self.type_combo.blockSignals(True)
            self.type_combo.clear()
            self.type_combo.addItem("All", userData="")
            self.type_combo.blockSignals(False)
            self._on_filters_changed()
            return
        types = self.case_data.list_indicator_types(int(self.evidence_id))
        current = self.type_combo.currentData()
        self.type_combo.blockSignals(True)
        self.type_combo.clear()
        self.type_combo.addItem("All", userData="")
        for indicator_type in types:
            self.type_combo.addItem(indicator_type.title(), userData=indicator_type)
        self.type_combo.blockSignals(False)
        if current is not None:
            index = self.type_combo.findData(current)
            if index != -1:
                self.type_combo.setCurrentIndex(index)
        self._on_filters_changed()

    def _on_filters_changed(self) -> None:
        self.filters.indicator_type = self.type_combo.currentData() or ""
        indicator_type = self.filters.indicator_type or None
        self.model.set_filter_type(indicator_type)
        self._update_summary()

    def _update_summary(self) -> None:
        count = self.model.rowCount()
        if count == 0:
            self.summary_label.setText("No indicators found for current filters.")
            return
        self.summary_label.setText(f"Indicators: {count}")

    # ===== Jump Lists Methods =====

    def _init_jump_lists_model(self) -> None:
        """Initialize Jump Lists model when evidence is set."""
        if not self.case_data or self.evidence_id is None:
            return

        # Get db_manager and evidence_label from case_data
        db_manager = self.case_data._db_manager
        evidence_label = self.case_data.get_evidence_label(self.evidence_id)

        if not db_manager or not evidence_label:
            LOGGER.warning("Cannot initialize Jump Lists model: missing db_manager or evidence_label")
            return

        self.jl_model = JumpListsTableModel(
            db_manager=db_manager,
            evidence_id=self.evidence_id,
            evidence_label=evidence_label,
            case_data=self.case_data,
            parent=self
        )
        self.jl_table.setModel(self.jl_model)

    def _populate_jump_lists_filters(self) -> None:
        """Populate Jump Lists browser filter combo."""
        if not self.jl_model:
            return

        browsers = self.jl_model.get_browsers()
        current = self.jl_browser_combo.currentData()

        self.jl_browser_combo.blockSignals(True)
        self.jl_browser_combo.clear()
        self.jl_browser_combo.addItem("All", userData="")
        for browser in browsers:
            self.jl_browser_combo.addItem(browser, userData=browser)
        self.jl_browser_combo.blockSignals(False)

        if current:
            index = self.jl_browser_combo.findData(current)
            if index != -1:
                self.jl_browser_combo.setCurrentIndex(index)

    def _on_jump_lists_filters_changed(self) -> None:
        """Apply Jump Lists filters."""
        if not self.jl_model:
            return

        browser = self.jl_browser_combo.currentData() or ""
        pin_status = self.jl_pin_combo.currentData() or ""
        urls_only = self.jl_urls_only_checkbox.isChecked()

        self.jl_model.set_filters(browser=browser, pin_status=pin_status, urls_only=urls_only)
        self._update_jump_lists_summary()

    def _update_jump_lists_summary(self) -> None:
        """Update Jump Lists summary label."""
        if not self.jl_model:
            self.jl_summary_label.setText("No Jump List entries loaded.")
            return

        count = self.jl_model.rowCount()
        if count == 0:
            self.jl_summary_label.setText("No Jump List entries found for current filters.")
            return

        stats = self.jl_model.get_stats()
        url_count = stats.get("url_count", 0)
        pinned_count = stats.get("pinned_count", 0)

        summary = f"Jump List Entries: {count}"
        if url_count > 0:
            summary += f" | URLs: {url_count}"
        if pinned_count > 0:
            summary += f" | Pinned: {pinned_count}"

        self.jl_summary_label.setText(summary)

    def _on_jump_list_double_clicked(self, index) -> None:
        """Handle double-click on Jump List entry to show details."""
        if not self.jl_model:
            return

        row_data = self.jl_model.get_row_data(index.row())
        if not row_data:
            return

        # Show details dialog
        from app.features.os_artifacts.dialogs import JumpListDetailsDialog
        dialog = JumpListDetailsDialog(row_data, parent=self)
        dialog.exec()

    def _export_registry_csv(self) -> None:
        """Export registry indicators to CSV."""
        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(self, "Export Error", "No case data available.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Registry Indicators",
            f"registry_indicators_{self.evidence_id}.csv",
            "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write headers
                writer.writerow([
                    "Type", "Name", "Value", "Path", "Hive",
                    "Confidence", "Detected (UTC)", "Provenance"
                ])

                # Write data
                indicator_type = self.filters.indicator_type or None
                for indicator in self.case_data.iter_indicators(self.evidence_id, indicator_type=indicator_type):
                    writer.writerow([
                        indicator.get("indicator_type", ""),
                        indicator.get("name", ""),
                        indicator.get("value", ""),
                        indicator.get("path", ""),
                        indicator.get("hive", ""),
                        indicator.get("confidence", ""),
                        indicator.get("detected_at_utc", ""),
                        indicator.get("provenance", "")
                    ])

            QMessageBox.information(
                self,
                "Export Complete",
                f"Exported {self.model.rowCount()} registry indicators to:\n{file_path}"
            )
            LOGGER.info(f"Exported {self.model.rowCount()} registry indicators to {file_path}")
        except Exception as e:
            LOGGER.error(f"Export failed: {e}")
            QMessageBox.critical(self, "Export Error", f"Failed to export: {e}")

    def _export_jump_lists_csv(self) -> None:
        """Export Jump List entries to CSV."""
        if not self.jl_model or self.evidence_id is None:
            QMessageBox.warning(self, "Export Error", "No Jump List data available.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Jump List Entries",
            f"jump_list_entries_{self.evidence_id}.csv",
            "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write headers
                writer.writerow([
                    "Browser", "URL", "Title", "Access Time", "Creation Time",
                    "Modification Time", "Access Count", "Pin Status", "App ID",
                    "Target Path", "Arguments", "Jump List Path", "Run ID"
                ])

                # Write data from model
                for row_idx in range(self.jl_model.rowCount()):
                    row_data = self.jl_model.get_row_data(row_idx)
                    if row_data:
                        writer.writerow([
                            row_data.get("browser", ""),
                            row_data.get("url", ""),
                            row_data.get("title", ""),
                            row_data.get("lnk_access_time", ""),
                            row_data.get("lnk_creation_time", ""),
                            row_data.get("lnk_modification_time", ""),
                            row_data.get("access_count", ""),
                            row_data.get("pin_status", ""),
                            row_data.get("appid", ""),
                            row_data.get("target_path", ""),
                            row_data.get("arguments", ""),
                            row_data.get("jumplist_path", ""),
                            row_data.get("run_id", ""),
                        ])

            QMessageBox.information(
                self,
                "Export Complete",
                f"Exported {self.jl_model.rowCount()} Jump List entries to:\n{file_path}"
            )
            LOGGER.info(f"Exported {self.jl_model.rowCount()} Jump List entries to {file_path}")
        except Exception as e:
            LOGGER.error(f"Export failed: {e}")
            QMessageBox.critical(self, "Export Error", f"Failed to export: {e}")

    # ===== Installed Applications Methods =====

    def _init_software_model(self) -> None:
        """Initialize Installed Software model when evidence is set."""
        if not self.case_data or self.evidence_id is None:
            return

        # Get db_manager and evidence_label from case_data
        db_manager = self.case_data._db_manager
        evidence_label = self.case_data.get_evidence_label(self.evidence_id)

        if not db_manager or not evidence_label:
            LOGGER.warning("Cannot initialize Installed Software model: missing db_manager or evidence_label")
            return

        self.sw_model = InstalledSoftwareModel(
            db_manager=db_manager,
            evidence_id=self.evidence_id,
            evidence_label=evidence_label,
            parent=self
        )
        self.sw_table.setModel(self.sw_model)

        # Resize columns for better display
        self.sw_table.resizeColumnsToContents()

    def _on_software_filters_changed(self) -> None:
        """Apply Installed Software filters."""
        if not self.sw_model:
            return

        search_text = self.sw_search_edit.text()
        forensic_only = self.sw_forensic_checkbox.isChecked()

        self.sw_model.set_filters(search_text=search_text, forensic_only=forensic_only)
        self._update_software_summary()

    def _update_software_summary(self) -> None:
        """Update Installed Software summary label."""
        if not self.sw_model:
            self.sw_summary_label.setText("No installed software loaded.")
            return

        count = self.sw_model.rowCount()
        if count == 0:
            self.sw_summary_label.setText("No installed software found for current filters.")
            return

        stats = self.sw_model.get_stats()
        forensic_count = stats.get("forensic_count", 0)
        with_date = stats.get("with_install_date", 0)

        summary = f"Installed Software: {count}"
        if forensic_count > 0:
            summary += f" | ⚠️ Forensic Interest: {forensic_count}"
        if with_date > 0:
            summary += f" | With Install Date: {with_date}"

        self.sw_summary_label.setText(summary)

    def _on_software_double_clicked(self, index) -> None:
        """Handle double-click on software entry to show details."""
        if not self.sw_model:
            return

        row_data = self.sw_model.get_row_data(index.row())
        if not row_data:
            return

        # Show details dialog
        from app.features.os_artifacts.dialogs import SoftwareDetailsDialog
        dialog = SoftwareDetailsDialog(row_data, parent=self)
        dialog.exec()

    def _export_software_csv(self) -> None:
        """Export installed software to CSV."""
        if not self.sw_model or self.evidence_id is None:
            QMessageBox.warning(self, "Export Error", "No installed software data available.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Installed Software",
            f"installed_software_{self.evidence_id}.csv",
            "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write headers
                writer.writerow([
                    "Software Name", "Publisher", "Version", "Install Date",
                    "Install Location", "Size (KB)", "Forensic Interest",
                    "Forensic Category", "Registry Key", "Uninstall Command",
                    "Architecture", "Registry Path"
                ])

                # Write data from model
                for row_idx in range(self.sw_model.rowCount()):
                    row_data = self.sw_model.get_row_data(row_idx)
                    if row_data:
                        writer.writerow([
                            row_data.get("name", ""),
                            row_data.get("publisher", ""),
                            row_data.get("version", ""),
                            row_data.get("install_date", ""),
                            row_data.get("install_location", ""),
                            row_data.get("size_kb", ""),
                            "Yes" if row_data.get("forensic_interest") else "No",
                            row_data.get("forensic_category", ""),
                            row_data.get("registry_key", ""),
                            row_data.get("uninstall_command", ""),
                            row_data.get("architecture", ""),
                            row_data.get("path", ""),
                        ])

            QMessageBox.information(
                self,
                "Export Complete",
                f"Exported {self.sw_model.rowCount()} installed software entries to:\n{file_path}"
            )
            LOGGER.info(f"Exported {self.sw_model.rowCount()} installed software entries to {file_path}")
        except Exception as e:
            LOGGER.error(f"Export failed: {e}")
            QMessageBox.critical(self, "Export Error", f"Failed to export: {e}")

    def _show_software_context_menu(self, pos) -> None:
        """Show context menu for software table."""
        index = self.sw_table.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)

        # View details action
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._on_software_double_clicked(index))

        menu.addSeparator()

        # Tag action
        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected_software)

        menu.exec(self.sw_table.viewport().mapToGlobal(pos))

    def _tag_selected_software(self) -> None:
        """Launch tagging dialog for selected software entries."""
        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(self, "Tagging Unavailable", "Case data is not loaded.")
            return

        if not self.sw_model:
            return

        # Get selected IDs
        selection_model = self.sw_table.selectionModel()
        if not selection_model:
            return

        selected_ids = []
        for index in selection_model.selectedRows():
            row_data = self.sw_model.get_row_data(index.row())
            if row_data and row_data.get("id") is not None:
                selected_ids.append(int(row_data["id"]))

        if not selected_ids:
            QMessageBox.information(self, "No Selection", "Select at least one application to tag.")
            return

        dialog = TagArtifactsDialog(
            self.case_data, self.evidence_id, "installed_software", selected_ids, self
        )
        dialog.tags_changed.connect(self._on_software_tags_changed)
        dialog.exec()

    def _on_software_tags_changed(self) -> None:
        """Refresh after software tag changes."""
        if self.case_data:
            self.case_data.invalidate_filter_cache(self.evidence_id)
        # No model refresh needed since tags don't affect displayed columns
