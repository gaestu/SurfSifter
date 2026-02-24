"""
OS Artifacts Dialogs

Dialog windows for OS artifacts details display.
"""
from __future__ import annotations

from typing import Dict, Any, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QFormLayout,
    QLabel,
    QTextEdit,
    QPushButton,
)


class JumpListDetailsDialog(QDialog):
    """Dialog showing full details for a Windows Jump List entry."""

    def __init__(self, row_data: Dict[str, Any], parent=None):
        """
        Initialize Jump List details dialog.

        Args:
            row_data: Jump List entry data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Jump List Entry Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Browser info
        browser = self.row_data.get("browser") or "Unknown"
        form.addRow("Browser:", QLabel(browser))

        appid = self.row_data.get("appid") or "N/A"
        form.addRow("App ID:", QLabel(appid))

        form.addRow("", QLabel(""))  # Spacer

        # URL and Title
        url = self.row_data.get("url") or ""
        url_label = QLabel(url or "[No URL]")
        url_label.setWordWrap(True)
        url_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("URL:", url_label)

        title = self.row_data.get("title") or ""
        title_label = QLabel(title or "[No Title]")
        title_label.setWordWrap(True)
        form.addRow("Title:", title_label)

        form.addRow("", QLabel(""))  # Spacer

        # LNK Timestamps (forensically important)
        form.addRow("Access Time:", QLabel(self.row_data.get("lnk_access_time") or "N/A"))
        form.addRow("Creation Time:", QLabel(self.row_data.get("lnk_creation_time") or "N/A"))
        form.addRow("Modification Time:", QLabel(self.row_data.get("lnk_modification_time") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Access metadata
        access_count = self.row_data.get("access_count")
        form.addRow("Access Count:", QLabel(str(access_count) if access_count is not None else "N/A"))

        pin_status = (self.row_data.get("pin_status") or "recent").title()
        form.addRow("Pin Status:", QLabel(pin_status))

        form.addRow("", QLabel(""))  # Spacer

        # Target info
        target_path = self.row_data.get("target_path") or ""
        if target_path:
            target_label = QLabel(target_path)
            target_label.setWordWrap(True)
            form.addRow("Target Path:", target_label)

        arguments = self.row_data.get("arguments") or ""
        if arguments:
            args_label = QLabel(arguments)
            args_label.setWordWrap(True)
            form.addRow("Arguments:", args_label)

        form.addRow("", QLabel(""))  # Spacer

        # Provenance
        form.addRow("Run ID:", QLabel(self.row_data.get("run_id") or "N/A"))
        form.addRow("Discovered By:", QLabel(self.row_data.get("discovered_by") or "N/A"))

        layout.addLayout(form)

        # Jump List Path section
        layout.addWidget(QLabel("Jump List File:"))
        jl_path_text = QTextEdit()
        jl_path_text.setReadOnly(True)
        jl_path_text.setMaximumHeight(60)
        jl_path_text.setPlainText(self.row_data.get("jumplist_path") or "N/A")
        layout.addWidget(jl_path_text)

        # Source path if different
        source_path = self.row_data.get("source_path") or ""
        if source_path and source_path != self.row_data.get("jumplist_path"):
            layout.addWidget(QLabel("Source Path:"))
            source_text = QTextEdit()
            source_text.setReadOnly(True)
            source_text.setMaximumHeight(60)
            source_text.setPlainText(source_path)
            layout.addWidget(source_text)

        # Buttons
        layout.addStretch()
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        # Copy URL button (if URL exists)
        if url:
            copy_url_btn = QPushButton("Copy URL")
            copy_url_btn.clicked.connect(self._copy_url)
            button_layout.addWidget(copy_url_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)

    def _copy_url(self) -> None:
        """Copy URL to clipboard."""
        from PySide6.QtWidgets import QApplication
        url = self.row_data.get("url", "")
        if url:
            clipboard = QApplication.clipboard()
            clipboard.setText(url)


class SoftwareDetailsDialog(QDialog):
    """Dialog showing full details for an installed software entry."""

    def __init__(self, row_data: Dict[str, Any], parent=None):
        """
        Initialize Installed Software details dialog.

        Args:
            row_data: Software entry data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Installed Software Details")
        self.setModal(True)
        self.resize(600, 550)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Software name (large, bold)
        name = self.row_data.get("name") or "Unknown"
        name_label = QLabel(f"<b>{name}</b>")
        name_label.setStyleSheet("font-size: 14px;")
        form.addRow("Software:", name_label)

        # Publisher
        publisher = self.row_data.get("publisher") or "N/A"
        form.addRow("Publisher:", QLabel(publisher))

        # Version
        version = self.row_data.get("version") or "N/A"
        form.addRow("Version:", QLabel(version))

        form.addRow("", QLabel(""))  # Spacer

        # Install info
        install_date = self.row_data.get("install_date") or "N/A"
        form.addRow("Install Date:", QLabel(install_date))

        install_location = self.row_data.get("install_location") or "N/A"
        loc_label = QLabel(install_location)
        loc_label.setWordWrap(True)
        form.addRow("Install Location:", loc_label)

        install_source = self.row_data.get("install_source") or ""
        if install_source:
            source_label = QLabel(install_source)
            source_label.setWordWrap(True)
            form.addRow("Install Source:", source_label)

        # Size
        size_kb = self.row_data.get("size_kb")
        if size_kb:
            size_mb = int(size_kb) / 1024
            form.addRow("Size:", QLabel(f"{int(size_kb):,} KB ({size_mb:.1f} MB)"))

        # Architecture
        architecture = self.row_data.get("architecture") or ""
        if architecture:
            form.addRow("Architecture:", QLabel(architecture))

        form.addRow("", QLabel(""))  # Spacer

        # Forensic interest section
        if self.row_data.get("forensic_interest"):
            forensic_label = QLabel("FORENSIC INTEREST")
            forensic_label.setStyleSheet("color: red; font-weight: bold;")
            form.addRow("", forensic_label)

            category = self.row_data.get("forensic_category") or "unknown"
            category_display = {
                "system_restore": "System Restore / Reboot-to-Restore Software",
                "anti_forensic": "Anti-Forensic / Privacy Tool",
                "forensic_interest": "Forensically Interesting Software",
            }.get(category, category.title())
            form.addRow("Category:", QLabel(category_display))

            form.addRow("", QLabel(""))  # Spacer

        # URL
        url = self.row_data.get("url") or ""
        if url:
            url_label = QLabel(f'<a href="{url}">{url}</a>')
            url_label.setOpenExternalLinks(True)
            url_label.setWordWrap(True)
            form.addRow("Website:", url_label)

        # Comments
        comments = self.row_data.get("comments") or ""
        if comments:
            comments_label = QLabel(comments)
            comments_label.setWordWrap(True)
            form.addRow("Comments:", comments_label)

        layout.addLayout(form)

        # Registry info section
        layout.addWidget(QLabel("<b>Registry Information</b>"))

        registry_text = QTextEdit()
        registry_text.setReadOnly(True)
        registry_text.setMaximumHeight(100)

        registry_info = []
        registry_key = self.row_data.get("registry_key") or ""
        if registry_key:
            registry_info.append(f"Key Name: {registry_key}")

        path = self.row_data.get("path") or ""
        if path:
            registry_info.append(f"Full Path: {path}")

        hive = self.row_data.get("hive") or ""
        if hive:
            registry_info.append(f"Hive: {hive}")

        registry_text.setPlainText("\n".join(registry_info) or "N/A")
        layout.addWidget(registry_text)

        # Uninstall command
        uninstall = self.row_data.get("uninstall_command") or ""
        if uninstall:
            layout.addWidget(QLabel("<b>Uninstall Command</b>"))
            uninstall_text = QTextEdit()
            uninstall_text.setReadOnly(True)
            uninstall_text.setMaximumHeight(60)
            uninstall_text.setPlainText(uninstall)
            layout.addWidget(uninstall_text)

        # Buttons
        layout.addStretch()
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)


class AppExecutionDetailsDialog(QDialog):
    """Dialog showing full details for an application execution (UserAssist) entry."""

    def __init__(self, row_data: Dict[str, Any], parent=None):
        """
        Initialize Application Execution details dialog.

        Args:
            row_data: UserAssist entry data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Application Execution Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # Decoded path (large, bold)
        decoded_path = self.row_data.get("decoded_path") or "Unknown"
        path_label = QLabel(f"<b>{decoded_path}</b>")
        path_label.setStyleSheet("font-size: 13px;")
        path_label.setWordWrap(True)
        path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Application:", path_label)

        form.addRow("", QLabel(""))  # Spacer

        # Execution stats
        run_count = self.row_data.get("run_count")
        form.addRow("Run Count:", QLabel(str(run_count) if run_count is not None else "N/A"))

        focus_count = self.row_data.get("focus_count")
        form.addRow("Focus Count:", QLabel(str(focus_count) if focus_count is not None else "N/A"))

        focus_time_ms = self.row_data.get("focus_time_ms")
        if focus_time_ms is not None:
            from app.features.os_artifacts.models.app_execution_model import _format_focus_time
            form.addRow("Focus Time:", QLabel(_format_focus_time(focus_time_ms)))

        last_run = self.row_data.get("last_run_utc") or "N/A"
        if last_run != "N/A":
            last_run = last_run.replace("T", " ").split("+")[0]
        form.addRow("Last Run (UTC):", QLabel(last_run))

        form.addRow("", QLabel(""))  # Spacer

        # Forensic interest section
        if self.row_data.get("forensic_interest"):
            forensic_label = QLabel("FORENSIC INTEREST")
            forensic_label.setStyleSheet("color: red; font-weight: bold;")
            form.addRow("", forensic_label)

            category = self.row_data.get("forensic_category") or "unknown"
            category_display = {
                "browser": "Web Browser",
                "wiping_tool": "Data Wiping / Anti-Forensic Tool",
                "tor": "Tor / Anonymization",
                "encryption": "Encryption Software",
                "privacy": "Privacy / VPN Tool",
                "file_sharing": "File Sharing / P2P",
            }.get(category, category.title())
            form.addRow("Category:", QLabel(category_display))

            form.addRow("", QLabel(""))  # Spacer

        layout.addLayout(form)

        # Registry info section
        layout.addWidget(QLabel("<b>Registry Information</b>"))

        registry_text = QTextEdit()
        registry_text.setReadOnly(True)
        registry_text.setMaximumHeight(120)

        registry_info = []
        hive = self.row_data.get("hive") or ""
        if hive:
            registry_info.append(f"Hive: {hive}")

        path = self.row_data.get("path") or ""
        if path:
            registry_info.append(f"Registry Path: {path}")

        rot13_name = self.row_data.get("rot13_name") or ""
        if rot13_name:
            registry_info.append(f"Original ROT13 Name: {rot13_name}")

        registry_text.setPlainText("\n".join(registry_info) or "N/A")
        layout.addWidget(registry_text)

        # Buttons
        layout.addStretch()
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        # Copy path button
        copy_path_btn = QPushButton("Copy Path")
        copy_path_btn.clicked.connect(self._copy_path)
        button_layout.addWidget(copy_path_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)

    def _copy_path(self) -> None:
        """Copy decoded path to clipboard."""
        from PySide6.QtWidgets import QApplication
        path = self.row_data.get("decoded_path", "")
        if path:
            clipboard = QApplication.clipboard()
            clipboard.setText(path)
