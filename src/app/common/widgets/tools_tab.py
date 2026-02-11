"""
Enhanced Tools Tab for Preferences Dialog

Provides comprehensive tool management with:
- Auto-discovery of all forensic tools
- Version checking and validation
- Custom path configuration
- Tool testing functionality
- Detailed capability and requirement information

Moved from features/tools/ to common/widgets/ to fix feature-to-feature
         dependency (settings importing from tools).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import webbrowser

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QGroupBox, QPushButton, QLabel, QFileDialog, QMessageBox, QTextEdit
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont

from core.tool_registry import ToolRegistry, ToolInfo


class ToolsTab(QWidget):
    """Enhanced tool management UI for Preferences dialog."""

    tools_updated = Signal()  # Emitted when tool configuration changes

    def __init__(self, tool_registry: ToolRegistry, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.tool_registry = tool_registry
        self.selected_tool_name: Optional[str] = None

        layout = QVBoxLayout()

        # Info label
        info = QLabel(
            "Tool Discovery runs automatically on startup. "
            "You can override auto-detected paths or test tool functionality."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        # Forensic tools table
        forensic_group = QGroupBox("FORENSIC TOOLS")
        forensic_layout = QVBoxLayout()

        self.forensic_table = self._create_tools_table()
        forensic_layout.addWidget(self.forensic_table)

        forensic_group.setLayout(forensic_layout)
        layout.addWidget(forensic_group)

        # Python libraries table
        python_group = QGroupBox("PYTHON LIBRARIES")
        python_layout = QVBoxLayout()

        self.python_table = self._create_tools_table()
        python_layout.addWidget(self.python_table)

        python_group.setLayout(python_layout)
        layout.addWidget(python_group)

        # Tool details section
        details_label = QLabel("Selected Tool:")
        self.details_tool_label = QLabel("(select a tool)")
        self.details_tool_label.setStyleSheet("font-weight: bold;")

        details_header = QHBoxLayout()
        details_header.addWidget(details_label)
        details_header.addWidget(self.details_tool_label)
        details_header.addStretch()

        layout.addLayout(details_header)

        details_group = QGroupBox("TOOL DETAILS")
        details_layout = QVBoxLayout()

        self.details_widget = self._create_details_widget()
        details_layout.addWidget(self.details_widget)

        details_group.setLayout(details_layout)
        layout.addWidget(details_group)

        # Action buttons
        buttons = QHBoxLayout()

        refresh_btn = QPushButton("Refresh All")
        refresh_btn.clicked.connect(self._refresh_all_tools)

        guide_btn = QPushButton("Download Tools Guide")
        guide_btn.clicked.connect(self._open_tools_guide)

        buttons.addWidget(refresh_btn)
        buttons.addWidget(guide_btn)
        buttons.addStretch()

        layout.addLayout(buttons)

        self.setLayout(layout)

        # Populate tables with current tool info
        self._populate_tables()

    def _create_tools_table(self) -> QTableWidget:
        """Create empty tools table widget."""
        table = QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels([
            "Tool",
            "Status",
            "Version",
            "Path / Notes"
        ])
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setSelectionMode(QTableWidget.SingleSelection)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        table.itemSelectionChanged.connect(self._on_tool_selected)

        # Make columns resizable
        table.horizontalHeader().setStretchLastSection(True)

        return table

    def _create_details_widget(self) -> QWidget:
        """Create tool details panel."""
        widget = QWidget()
        layout = QVBoxLayout()

        self.status_label = QLabel()
        self.version_label = QLabel()
        self.path_label = QLabel()
        self.path_label.setWordWrap(True)

        layout.addWidget(self.status_label)
        layout.addWidget(self.version_label)
        layout.addWidget(self.path_label)

        # Spacer
        spacer = QLabel()
        spacer.setFixedHeight(10)
        layout.addWidget(spacer)

        # Capabilities
        caps_title = QLabel("Capabilities:")
        caps_title.setStyleSheet("font-weight: bold;")
        layout.addWidget(caps_title)

        self.capabilities_label = QLabel()
        self.capabilities_label.setWordWrap(True)
        layout.addWidget(self.capabilities_label)

        # Spacer
        spacer2 = QLabel()
        spacer2.setFixedHeight(10)
        layout.addWidget(spacer2)

        # Required by
        req_title = QLabel("Required by:")
        req_title.setStyleSheet("font-weight: bold;")
        layout.addWidget(req_title)

        self.required_by_label = QLabel()
        self.required_by_label.setWordWrap(True)
        layout.addWidget(self.required_by_label)

        # Spacer
        spacer3 = QLabel()
        spacer3.setFixedHeight(10)
        layout.addWidget(spacer3)

        # Action buttons
        btn_layout = QHBoxLayout()

        self.test_btn = QPushButton("Test Tool")
        self.test_btn.clicked.connect(self._test_selected_tool)
        self.test_btn.setEnabled(False)

        self.custom_path_btn = QPushButton("Set Custom Path...")
        self.custom_path_btn.clicked.connect(self._set_custom_path)
        self.custom_path_btn.setEnabled(False)

        self.reset_btn = QPushButton("Reset to Auto")
        self.reset_btn.clicked.connect(self._reset_to_auto)
        self.reset_btn.setEnabled(False)

        btn_layout.addWidget(self.test_btn)
        btn_layout.addWidget(self.custom_path_btn)
        btn_layout.addWidget(self.reset_btn)
        btn_layout.addStretch()

        layout.addLayout(btn_layout)
        layout.addStretch()

        widget.setLayout(layout)
        return widget

    def _populate_tables(self):
        """Populate both tables with tool information."""
        forensic_tools = ["bulk_extractor", "foremost", "scalpel", "exiftool", "firejail", "ewfmount"]
        python_tools = ["pytsk3", "pyewf"]

        self._populate_table(self.forensic_table, forensic_tools)
        self._populate_table(self.python_table, python_tools)

    def _populate_table(self, table: QTableWidget, tool_names: list):
        """Populate a table with specific tools."""
        table.setRowCount(len(tool_names))

        for row, tool_name in enumerate(tool_names):
            tool_info = self.tool_registry.get_tool_info(tool_name)

            if not tool_info:
                # Tool not discovered yet, show placeholder
                table.setItem(row, 0, QTableWidgetItem(tool_name))
                table.setItem(row, 1, QTableWidgetItem("⏳ Discovering..."))
                table.setItem(row, 2, QTableWidgetItem("-"))
                table.setItem(row, 3, QTableWidgetItem("Not checked yet"))
                continue

            # Tool name
            table.setItem(row, 0, QTableWidgetItem(tool_name))

            # Status with icon
            status_text = "✅ Found" if tool_info.status == "found" else (
                "⚠️ Missing" if tool_info.status == "missing" else "❌ Error"
            )
            table.setItem(row, 1, QTableWidgetItem(status_text))

            # Version
            version_text = tool_info.version or "-"
            table.setItem(row, 2, QTableWidgetItem(version_text))

            # Path or error message
            path_text = str(tool_info.path) if tool_info.path else (
                tool_info.error_message or "Not found on PATH"
            )
            table.setItem(row, 3, QTableWidgetItem(path_text))

        table.resizeColumnsToContents()

    def _on_tool_selected(self):
        """Handle tool selection in table."""
        # Get selected tool from either table
        forensic_selected = self.forensic_table.selectedItems()
        python_selected = self.python_table.selectedItems()

        if forensic_selected:
            tool_name = forensic_selected[0].text()
            # Clear python table selection
            self.python_table.clearSelection()
        elif python_selected:
            tool_name = python_selected[0].text()
            # Clear forensic table selection
            self.forensic_table.clearSelection()
        else:
            self._clear_details_panel()
            return

        # Update details panel
        tool_info = self.tool_registry.get_tool_info(tool_name)
        if tool_info:
            self.selected_tool_name = tool_name
            self._update_details_panel(tool_name, tool_info)

    def _update_details_panel(self, tool_name: str, tool_info: ToolInfo):
        """Update details panel with tool information."""
        self.details_tool_label.setText(tool_name)

        status_icon = "✅" if tool_info.status == "found" else (
            "⚠️" if tool_info.status == "missing" else "❌"
        )
        self.status_label.setText(f"Status: {status_icon} {tool_info.status.title()}")

        self.version_label.setText(f"Version: {tool_info.version or 'Unknown'}")

        path_text = str(tool_info.path) if tool_info.path else (
            tool_info.error_message or "Not found on PATH"
        )
        self.path_label.setText(f"Path: {path_text}")

        # Capabilities
        caps_text = "\n".join(f"  • {cap.replace('_', ' ').title()}"
                             for cap in tool_info.capabilities)
        self.capabilities_label.setText(caps_text or "  (none)")

        # Required by
        tool_spec = self.tool_registry.KNOWN_TOOLS.get(tool_name, {})
        req_text = "\n".join(f"  • {req}"
                            for req in tool_spec.get("required_by", []))
        self.required_by_label.setText(req_text or "  (none)")

        # Enable/disable buttons based on tool status
        can_test = tool_info.status == "found"
        self.test_btn.setEnabled(can_test)

        # Can always set custom path (to fix missing/error tools)
        self.custom_path_btn.setEnabled(True)

        # Can only reset if custom path is set
        has_custom_path = tool_name in self.tool_registry._custom_paths
        self.reset_btn.setEnabled(has_custom_path)

    def _clear_details_panel(self):
        """Clear the details panel when no tool selected."""
        self.details_tool_label.setText("(select a tool)")
        self.status_label.setText("")
        self.version_label.setText("")
        self.path_label.setText("")
        self.capabilities_label.setText("")
        self.required_by_label.setText("")
        self.test_btn.setEnabled(False)
        self.custom_path_btn.setEnabled(False)
        self.reset_btn.setEnabled(False)
        self.selected_tool_name = None

    def _test_selected_tool(self):
        """Test the selected tool."""
        if not self.selected_tool_name:
            return

        success, message = self.tool_registry.test_tool(self.selected_tool_name)

        if success:
            QMessageBox.information(
                self,
                "Tool Test Successful",
                message
            )
        else:
            QMessageBox.warning(
                self,
                "Tool Test Failed",
                message
            )

    def _set_custom_path(self):
        """Set custom path for selected tool."""
        if not self.selected_tool_name:
            return

        file_path, _ = QFileDialog.getOpenFileName(
            self,
            f"Select {self.selected_tool_name} Executable",
            "",
            "All Files (*)"
        )

        if file_path:
            from pathlib import Path
            self.tool_registry.set_custom_path(self.selected_tool_name, Path(file_path))
            self._populate_tables()

            # Update details panel
            tool_info = self.tool_registry.get_tool_info(self.selected_tool_name)
            if tool_info:
                self._update_details_panel(self.selected_tool_name, tool_info)

            self.tools_updated.emit()

    def _reset_to_auto(self):
        """Reset selected tool to auto-discovery."""
        if not self.selected_tool_name:
            return

        # Remove custom path
        if self.selected_tool_name in self.tool_registry._custom_paths:
            del self.tool_registry._custom_paths[self.selected_tool_name]
            self.tool_registry._save_custom_paths()

            # Re-discover tool
            self.tool_registry._tools[self.selected_tool_name] = (
                self.tool_registry.discover_tool(self.selected_tool_name)
            )

            self._populate_tables()

            # Update details panel
            tool_info = self.tool_registry.get_tool_info(self.selected_tool_name)
            if tool_info:
                self._update_details_panel(self.selected_tool_name, tool_info)

            self.tools_updated.emit()

    def _refresh_all_tools(self):
        """Re-discover all tools."""
        self.tool_registry.discover_all_tools()
        self._populate_tables()

        # Refresh details panel if a tool is selected
        if self.selected_tool_name:
            tool_info = self.tool_registry.get_tool_info(self.selected_tool_name)
            if tool_info:
                self._update_details_panel(self.selected_tool_name, tool_info)

        self.tools_updated.emit()

    def _open_tools_guide(self):
        """Open download/installation guide for missing tools."""
        # Open LICENSES.md which has tool download guidance
        guide_url = "https://github.com/gaestu/surfsifter/blob/main/docs/LICENSES.md"
        webbrowser.open(guide_url)
