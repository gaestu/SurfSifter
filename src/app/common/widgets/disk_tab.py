"""
Disk Tab Widget
Shows disk layout and partition selection for E01 evidence files.
"""

import json
from typing import List, Dict, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QCheckBox, QPushButton, QLabel,
    QGroupBox, QHeaderView, QMessageBox
)
from PySide6.QtCore import Qt, Signal

from .disk_layout import DiskLayoutWidget


class DiskTabWidget(QWidget):
    """Widget for partition visualization (optionally selection)."""

    # Signal emitted when user applies partition selection (legacy behavior)
    selection_changed = Signal(list, bool)  # (selected_indices, scan_slack_space)
    # Signal emitted when user requests partition rescan
    rescan_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._partitions = []
        self._evidence_label = ""
        self._selection_enabled = True
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Disk layout visualization
        self.disk_layout = DiskLayoutWidget()
        layout.addWidget(self.disk_layout)

        # Partition selection group
        selection_group = QGroupBox("Partition Selection")
        selection_layout = QVBoxLayout()

        # Info label
        self.info_label = QLabel(
            "Select which partitions to scan. All partitions are selected by default."
        )
        self.info_label.setWordWrap(True)
        selection_layout.addWidget(self.info_label)

        # Partition table with checkboxes
        self.partition_table = QTableWidget()
        self.partition_table.setColumnCount(6)
        self.partition_table.setHorizontalHeaderLabels([
            "Scan",
            "Partition",
            "Size",
            "Type",
            "Filesystem",
            "Files"
        ])
        self.partition_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.partition_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.partition_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.partition_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.partition_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.partition_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.partition_table.setSelectionMode(QTableWidget.NoSelection)
        self.partition_table.setEditTriggers(QTableWidget.NoEditTriggers)
        selection_layout.addWidget(self.partition_table)

        # Slack space checkbox
        slack_layout = QHBoxLayout()
        self.slack_checkbox = QCheckBox("Scan unallocated/slack space")
        self.slack_checkbox.setChecked(True)  # Default enabled
        self.slack_checkbox.setToolTip(
            "Enable to scan unallocated space between partitions and at end of disk"
        )
        slack_layout.addWidget(self.slack_checkbox)
        slack_layout.addStretch()
        selection_layout.addLayout(slack_layout)

        # Buttons
        button_layout = QHBoxLayout()

        # Rescan button on the left
        self.rescan_btn = QPushButton("Rescan Partitions")
        self.rescan_btn.clicked.connect(self._rescan_partitions)
        self.rescan_btn.setToolTip(
            "Re-detect partitions from the E01 image"
        )
        button_layout.addWidget(self.rescan_btn)

        button_layout.addStretch()

        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all)
        button_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("Deselect All")
        self.deselect_all_btn.clicked.connect(self._deselect_all)
        button_layout.addWidget(self.deselect_all_btn)

        self.apply_btn = QPushButton("Apply Selection")
        self.apply_btn.clicked.connect(self._apply_selection)
        self.apply_btn.setToolTip(
            "Save partition selection. Run 'All Extractors' to process selected partitions."
        )
        button_layout.addWidget(self.apply_btn)

        selection_layout.addLayout(button_layout)
        selection_group.setLayout(selection_layout)
        layout.addWidget(selection_group)

    def set_partitions(self, partition_info_list: List[Dict[str, Any]],
                      evidence_label: str,
                      current_selections: Optional[List[int]] = None,
                      scan_slack: bool = True):
        """
        Load partition data and display in table.

        Args:
            partition_info_list: List of partition dicts from list_ewf_partitions()
            evidence_label: Evidence file label
            current_selections: Previously selected partition indices (None = all)
            scan_slack: Whether slack space scanning is enabled
        """
        self._partitions = partition_info_list
        self._evidence_label = evidence_label

        # Update disk layout visualization
        if partition_info_list:
            # Convert to JSON format expected by disk_layout
            partition_info_json = json.dumps(partition_info_list)
            self.disk_layout.set_partitions(partition_info_json, evidence_label)
            self.disk_layout.setVisible(True)
        else:
            self.disk_layout.setVisible(False)

        # Populate table
        self.partition_table.setRowCount(len(partition_info_list))

        for row, part in enumerate(partition_info_list):
            if self._selection_enabled:
                checkbox = QCheckBox()
                if current_selections is None:
                    checkbox.setChecked(True)
                else:
                    checkbox.setChecked(part['index'] in current_selections)
                checkbox.setEnabled(self._selection_enabled)
                checkbox_widget = QWidget()
                checkbox_layout = QHBoxLayout(checkbox_widget)
                checkbox_layout.addWidget(checkbox)
                checkbox_layout.setAlignment(Qt.AlignCenter)
                checkbox_layout.setContentsMargins(0, 0, 0, 0)
                self.partition_table.setCellWidget(row, 0, checkbox_widget)
            else:
                self.partition_table.setCellWidget(row, 0, QWidget())

            # Partition ID
            self.partition_table.setItem(row, 1, QTableWidgetItem(str(part['index'])))

            # Size
            size_str = self._format_size(part.get('length', 0))
            self.partition_table.setItem(row, 2, QTableWidgetItem(size_str))

            # Type (description)
            desc = part.get('description', 'Unknown')
            self.partition_table.setItem(row, 3, QTableWidgetItem(desc))

            # Filesystem readable
            fs_status = "Yes" if part.get('filesystem_readable') else "No"
            self.partition_table.setItem(row, 4, QTableWidgetItem(fs_status))

            # File count
            file_count = part.get('root_file_count')
            file_str = str(file_count) if file_count is not None else "-"
            self.partition_table.setItem(row, 5, QTableWidgetItem(file_str))

        # Set slack space checkbox
        self.slack_checkbox.blockSignals(True)
        self.slack_checkbox.setChecked(scan_slack)
        self.slack_checkbox.blockSignals(False)

        # Ensure column visibility matches mode
        self.partition_table.setColumnHidden(0, not self._selection_enabled)

    def set_selection_enabled(self, enabled: bool) -> None:
        """Enable or disable manual partition selection controls."""
        self._selection_enabled = enabled
        self.partition_table.setColumnHidden(0, not enabled)
        self.slack_checkbox.setVisible(enabled)
        self.select_all_btn.setVisible(enabled)
        self.deselect_all_btn.setVisible(enabled)
        self.apply_btn.setVisible(enabled)
        if enabled:
            self.info_label.setText(
                "Select which partitions to scan. All partitions are selected by default."
            )
        else:
            self.info_label.setText(
                "Partitions are displayed for reference. All will be scanned automatically."
            )

    def _format_size(self, bytes_val: int) -> str:
        """Convert bytes to human-readable format."""
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024**2:
            return f"{bytes_val / 1024:.2f} KB"
        elif bytes_val < 1024**3:
            return f"{bytes_val / 1024**2:.2f} MB"
        elif bytes_val < 1024**4:
            return f"{bytes_val / 1024**3:.2f} GB"
        else:
            return f"{bytes_val / 1024**4:.2f} TB"

    def _select_all(self):
        """Select all partition checkboxes."""
        if not self._selection_enabled:
            return
        for row in range(self.partition_table.rowCount()):
            checkbox_widget = self.partition_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox:
                    checkbox.setChecked(True)

    def _deselect_all(self):
        """Deselect all partition checkboxes."""
        if not self._selection_enabled:
            return
        for row in range(self.partition_table.rowCount()):
            checkbox_widget = self.partition_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox:
                    checkbox.setChecked(False)

    def _rescan_partitions(self):
        """Request partition rescan from E01 image."""
        reply = QMessageBox.question(
            self,
            "Rescan Partitions",
            "This will re-detect partitions from the E01 image. Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            self.rescan_requested.emit()

    def _apply_selection(self):
        """Emit signal with selected partition indices."""
        if not self._selection_enabled:
            return
        selected = []
        for row in range(self.partition_table.rowCount()):
            checkbox_widget = self.partition_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox and checkbox.isChecked():
                    part_idx = self._partitions[row]['index']
                    selected.append(part_idx)

        if not selected and not self.slack_checkbox.isChecked():
            QMessageBox.warning(
                self,
                "No Selection",
                "Please select at least one partition or enable slack space scanning."
            )
            return

        scan_slack = self.slack_checkbox.isChecked()
        self.selection_changed.emit(selected, scan_slack)

        # Show confirmation
        QMessageBox.information(
            self,
            "Selection Applied",
            "Partition selection saved. Run 'All Extractors' to process selected partitions."
        )

    def get_selected_partitions(self) -> List[int]:
        """Get currently selected partition indices."""
        if not self._selection_enabled:
            return [p['index'] for p in self._partitions]
        selected = []
        for row in range(self.partition_table.rowCount()):
            checkbox_widget = self.partition_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox and checkbox.isChecked():
                    part_idx = self._partitions[row]['index']
                    selected.append(part_idx)
        return selected

    def is_slack_space_enabled(self) -> bool:
        """Get slack space checkbox state."""
        if not self._selection_enabled:
            return True
        return self.slack_checkbox.isChecked()
