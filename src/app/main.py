from __future__ import annotations

import logging
import html
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Tuple

from PySide6.QtCore import Qt, QThreadPool, QCoreApplication, QEvent, QUrl, Signal, QObject, QTimer
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QDialog,
    QMessageBox,
    QProgressDialog,
    QPushButton,
    QRadioButton,
    QTabWidget,
    QTabBar,
    QTextBrowser,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtGui import QAction, QDesktopServices, QIcon, QPixmap, QColor

from core.config import AppConfig, load_app_config
from core.validation import validate_case_full, ValidationReport
from core.audit_logging import AuditLogger
from core.database import create_process_log, finalize_process_log, init_db
from core.database import DatabaseManager, find_case_database, slugify_label
from core.evidence_fs import MountedFS, PyEwfTskFS, find_ewf_segments, list_ewf_partitions
from core.logging import configure_logging, get_logger

from .data.case_data import CaseDataAccess, EvidenceCounts
from .features.urls import UrlsTab
from .features.images import ImagesTab
from .features.os_artifacts import OSArtifactsTab
from .features.timeline import TimelineTab
from .features.tags import TagsTab
from .features.settings import PreferencesDialog
from .common.dialogs import show_error_dialog, CreateCaseDialog, ValidationDialog, ExportDialog, ImportDialog, RemoveEvidenceDialog
from .services.workers import (
    ExecutorTask,
    ExecutorTaskConfig,
    start_task,
    DownloadTask,
    DownloadTaskConfig,
    DownloadPostProcessTask,
    DownloadPostProcessConfig,
    CaseLoadTask,
    CaseLoadTaskConfig,
    CaseLoadResult,
    ValidationWorker,
)
from .config.settings import AppSettings, settings_path
from .features.downloads.legacy import DownloadManagerDialog, DownloadQueueItem
from .common.widgets import DiskLayoutWidget, CaseInfoWidget, DiskTabWidget

LOGGER = get_logger("app.main")


class MainWindow(QMainWindow):
    def __init__(self, base_dir: Path) -> None:
        super().__init__()
        self.base_dir = base_dir
        self.app_config: AppConfig = load_app_config(base_dir)
        self.case_path: Optional[Path] = None
        self.case_db_path: Optional[Path] = None  # Track the actual database file path
        self.db_manager: Optional[DatabaseManager] = None
        self.case_db_id: Optional[int] = None
        self.conn: Optional[sqlite3.Connection] = None
        self.case_data: Optional[CaseDataAccess] = None
        self._current_counts: Optional[EvidenceCounts] = None

        self.setWindowTitle("SurfSifter")
        self.setWindowIcon(QIcon(str(Path(__file__).resolve().parent.parent.parent / "config" / "branding" / "surfsifter.png")))
        self.resize(1100, 720)

        # Apply logging config (level, rotation sizes)
        log_level = getattr(logging, self.app_config.logging.level.upper(), logging.INFO)
        configure_logging(
            self.app_config.logs_dir,
            level=log_level,
            max_bytes=self.app_config.logging.app_log_max_mb * 1024 * 1024,
            backup_count=self.app_config.logging.app_log_backup_count
        )
        self.logger = get_logger("app.gui")

        settings_file = settings_path(base_dir)
        self.settings = AppSettings.load(settings_file)
        self.settings_file = settings_file

        # Initialize tool registry and discover tools
        from core.tool_registry import ToolRegistry
        self.tool_registry = ToolRegistry()
        self.tool_registry.discover_all_tools()

        # Removed global log widget - using per-evidence logs instead
        # Keeping minimal log_widget for backward compatibility with other code
        self.log_widget = QTextEdit()
        self.log_widget.setVisible(False)  # Not displayed anywhere
        self.log_widget.setReadOnly(True)
        self._evidence_tabs = {}  # Map evidence_id -> tab_index

        self.thread_pool = QThreadPool.globalInstance()
        self.progress_dialog: Optional[QProgressDialog] = None
        self._download_task: Optional[DownloadTask] = None
        self._download_dialog: Optional[DownloadManagerDialog] = None
        # TODO: Refactor report task
        self._report_task: Optional[ReportTask] = None
        self._report_progress: Optional[QProgressDialog] = None
        self._current_reports_tab = None  # Reference to the reports tab being used
        self._report_success = False
        self._report_failed = False
        self._report_canceled = False

        # Validation support
        self._validation_report: Optional[ValidationReport] = None
        self._validation_worker: Optional[ValidationWorker] = None

        # Per-evidence log widgets
        self._evidence_log_widgets: Dict[int, QTextEdit] = {}

        # Forensic audit logging
        logging_config = {
            "case_log_max_mb": self.app_config.logging.case_log_max_mb,
            "case_log_backup_count": self.app_config.logging.case_log_backup_count,
            "evidence_log_max_mb": self.app_config.logging.evidence_log_max_mb,
            "evidence_log_backup_count": self.app_config.logging.evidence_log_backup_count,
        }
        self.audit_logger = AuditLogger(logging_config)

        # Background case loading (Phase 2)
        self._case_load_task: Optional[CaseLoadTask] = None
        self._case_load_progress: Optional[QProgressDialog] = None

        self._setup_ui()
        self.main_tabs.currentChanged.connect(self._on_main_tab_changed)

        # Track active tasks for cleanup
        self._active_tasks = []

    def closeEvent(self, event):
        """Handle application close - cancel all running tasks and wait for thread pool."""
        LOGGER.info("Application closing - shutting down workers...")

        # Shutdown all tab workers in evidence tabs
        if hasattr(self, 'main_tabs'):
            for i in range(self.main_tabs.count()):
                evidence_tab_widget = self.main_tabs.widget(i)
                if evidence_tab_widget and hasattr(evidence_tab_widget, 'count'):
                    # Iterate subtabs within evidence tab
                    for j in range(evidence_tab_widget.count()):
                        tab = evidence_tab_widget.widget(j)
                        if hasattr(tab, 'shutdown'):
                            try:
                                tab.shutdown()
                            except Exception as e:
                                LOGGER.warning("Error during tab shutdown: %s", e)

        # Wait for thread pool to finish (with timeout)
        LOGGER.info("Waiting for background tasks to complete...")
        if not self.thread_pool.waitForDone(5000):  # 5 second timeout
            LOGGER.warning("Some background tasks did not complete in time - forcing exit")

        # Close audit loggers
        if hasattr(self, 'audit_logger') and self.audit_logger:
            self.audit_logger.close()

        if self.db_manager is not None:
            self.db_manager.close_all()
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

        LOGGER.info("Application shutdown complete")
        event.accept()

    # UI construction -----------------------------------------------------

    def _setup_ui(self) -> None:
        central = QWidget()
        layout = QVBoxLayout()

        # Header with buttons and settings
        header_layout = QHBoxLayout()
        self.case_label = QLabel("No case open.")
        self.case_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        self.open_case_button = QPushButton("Open Case")
        self.open_case_button.clicked.connect(self._open_case_file_dialog)

        self.create_case_button = QPushButton("Create Case")
        self.create_case_button.clicked.connect(self._create_case_dialog)

        self.add_evidence_button = QPushButton("Add Evidence")
        self.add_evidence_button.setEnabled(False)
        self.add_evidence_button.clicked.connect(self._add_evidence_dialog)

        self.remove_evidence_button = QPushButton("Remove Evidence")
        self.remove_evidence_button.setEnabled(False)
        self.remove_evidence_button.clicked.connect(self._remove_evidence_dialog)

        header_layout.addWidget(self.open_case_button)
        header_layout.addWidget(self.create_case_button)
        header_layout.addWidget(self.add_evidence_button)
        header_layout.addWidget(self.remove_evidence_button)
        header_layout.addStretch()
        header_layout.addWidget(self.case_label)

        layout.addLayout(header_layout)

        # Main tab widget: Case | Evidence1 | Evidence2 | ...
        self.main_tabs = QTabWidget()
        self.main_tabs.setTabsClosable(True)
        self.main_tabs.tabCloseRequested.connect(self._on_close_evidence_tab)

        # Case tab (always first)
        self.case_info_tab = CaseInfoWidget()
        self.case_info_tab.field_changed.connect(self._on_case_field_changed)
        self.case_info_tab.extract_all_requested.connect(self._on_case_wide_extract_requested)
        self.main_tabs.addTab(self.case_info_tab, "Case")
        self.main_tabs.tabBar().setTabButton(0, QTabBar.RightSide, None)  # Disable close on Case tab

        layout.addWidget(self.main_tabs)
        central.setLayout(layout)
        self.setCentralWidget(central)

        # Menu bar
        self.settings_menu = self.menuBar().addMenu("Settings")
        self.preferences_action = QAction("Preferences…", self)
        self.preferences_action.setShortcut("Ctrl+,")
        self.preferences_action.triggered.connect(self._open_preferences_dialog)
        self.settings_menu.addAction(self.preferences_action)

        # Tools menu with validation, export, import
        self.tools_menu = self.menuBar().addMenu("Tools")
        self.validate_case_action = QAction("Validate Case", self)
        self.validate_case_action.setShortcut("Ctrl+Shift+V")
        self.validate_case_action.triggered.connect(self._validate_case_manual)
        self.validate_case_action.setEnabled(False)  # Enabled when case is open
        self.tools_menu.addAction(self.validate_case_action)

        self.tools_menu.addSeparator()

        self.export_case_action = QAction("Export Case...", self)
        self.export_case_action.setShortcut("Ctrl+Shift+E")
        self.export_case_action.triggered.connect(self._export_case)
        self.export_case_action.setEnabled(False)  # Enabled when case is open
        self.tools_menu.addAction(self.export_case_action)

        self.import_case_action = QAction("Import Case...", self)
        self.import_case_action.setShortcut("Ctrl+Shift+I")
        self.import_case_action.triggered.connect(self._import_case)
        self.tools_menu.addAction(self.import_case_action)

        # Status bar with validation warning icon
        status_bar = self.statusBar()
        self.validation_warning_label = QLabel()
        self.validation_warning_label.setVisible(False)
        self.validation_warning_label.setStyleSheet("QLabel { color: #d97000; }")  # Orange text
        self.validation_warning_label.setText("⚠ Validation issues")
        self.validation_warning_label.setCursor(Qt.PointingHandCursor)
        self.validation_warning_label.mousePressEvent = lambda _: self._show_validation_dialog()
        status_bar.addPermanentWidget(self.validation_warning_label)

        # Set window title
        self.setWindowTitle("SurfSifter")

    def _build_overview_tab(self) -> QWidget:
        """
        Build Overview tab - Summary display + disk/partition selection.
        Disk selection moved from Extraction tab (UI refinements phase).
        """
        widget = QWidget()
        layout = QVBoxLayout()

        # Info label explaining workflow
        info_label = QLabel(
            "All partitions are processed automatically. Review them below before running extractors."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: gray; font-style: italic; padding: 8px;")
        layout.addWidget(info_label)

        # Evidence summary statistics (read-only)
        stats_group = QGroupBox("Evidence Summary")
        stats_form = QFormLayout()
        urls_count_label = QLabel("—")
        images_count_label = QLabel("—")
        indicators_count_label = QLabel("—")
        last_run_label = QLabel("—")
        coverage_label = QLabel("Full Image (all partitions)")
        coverage_label.setStyleSheet("color: green; font-weight: bold;")
        stats_form.addRow(QLabel("Partition Coverage"), coverage_label)
        stats_form.addRow(QLabel("URLs"), urls_count_label)
        stats_form.addRow(QLabel("Images"), images_count_label)
        stats_form.addRow(QLabel("Indicators"), indicators_count_label)
        stats_form.addRow(QLabel("Last Extraction (UTC)"), last_run_label)
        stats_group.setLayout(stats_form)
        layout.addWidget(stats_group)

        # Disk/Partition overview (view-only)
        disk_group = QGroupBox("Disk & Partitions")
        disk_layout = QVBoxLayout()

        disk_info_label = QLabel(
            "Partitions are detected for reference. The analyzer processes the entire E01 image."
        )
        disk_info_label.setWordWrap(True)
        disk_info_label.setStyleSheet("color: gray;")
        disk_layout.addWidget(disk_info_label)

        # DiskTabWidget for partition visualization (no selection UI)
        disk_tab_widget = DiskTabWidget()
        disk_tab_widget.set_selection_enabled(False)
        disk_tab_widget.rescan_requested.connect(self._on_partition_rescan_requested)
        disk_layout.addWidget(disk_tab_widget)

        disk_group.setLayout(disk_layout)
        layout.addWidget(disk_group)

        # Overview text browser (read-only)
        overview_text = QTextBrowser()
        overview_text.setReadOnly(True)
        overview_text.setOpenExternalLinks(True)
        overview_text.setOpenLinks(True)
        layout.addWidget(overview_text)

        layout.addStretch()
        widget.setLayout(layout)

        # Store references as properties for later access
        widget.setProperty("disk_tab_widget", disk_tab_widget)

        return widget

    def _build_evidence_tab(self, evidence_id: int, evidence_label: str, defer_load: bool = False) -> QTabWidget:
        """
        Create a tab widget with sub-tabs for a specific evidence.

        Args:
            evidence_id: Evidence ID
            evidence_label: Label for the evidence tab
            defer_load: If True, defer data loading until tabs are visible (Phase 3)

        UI refinements - disk selection in Overview, "Extractors" tab renamed.
        Added defer_load for lazy loading (Phase 3).
        """
        evidence_tabs = QTabWidget()

        # 1. Overview (includes disk/partition selection)
        overview_widget = self._build_overview_tab()
        evidence_tabs.addTab(overview_widget, "Overview")

        # 2. Extractors (renamed from "Extraction", disk section moved to Overview)
        from app.features.extraction import ExtractionTab
        extraction_tab = ExtractionTab(
            evidence_id,
            self.case_data,
            self.app_config.rules_dir,
            self.tool_registry,  # Pass tool registry for tool availability checking
        )
        extraction_tab.extraction_started.connect(self._on_extraction_started)
        extraction_tab.extraction_finished.connect(self._on_extraction_finished)
        extraction_tab.log_message.connect(self._on_extraction_log_message)  # (evidence_id, message)
        extraction_tab.data_changed.connect(lambda: self._on_data_changed(evidence_tabs))
        evidence_tabs.addTab(extraction_tab, "Extractors")

        # 3. File List (NEW in, moved after Extractors)
        # Added defer_load for Phase 3 lazy loading
        from app.features.file_list import FileListTab
        file_list_tab = FileListTab(
            self.case_path, evidence_id, self.case_db_path,
            defer_load=defer_load
        )
        if self.case_data:
            file_list_tab.set_case_data(self.case_data)
        evidence_tabs.addTab(file_list_tab, "File List")

        # 4. Browser/Cache Inventory (NEW)
        from app.features.browser_inventory import BrowserInventoryTab
        browser_inventory_tab = BrowserInventoryTab(
            self.case_path,
            evidence_id,
            self.case_db_path,
            case_data=self.case_data,
        )
        # Re-Ingest feature retired - extraction is the supported workflow
        # browser_inventory_tab.reingest_requested.connect(self._on_reingest_requested)
        evidence_tabs.addTab(browser_inventory_tab, "Browser Inventory")

        # 5. URLs - supports lazy loading
        from app.features.urls import UrlsTab
        urls_tab = UrlsTab()
        # downloadRequested signal removed - downloads now handled in Download tab
        if self.case_data:
            urls_tab.set_case_data(self.case_data, defer_load=defer_load)
        urls_tab.set_evidence(evidence_id, defer_load=defer_load, evidence_label=evidence_label)  # pass label
        evidence_tabs.addTab(urls_tab, "URLs")

        # 6. Images - supports lazy loading
        from app.features.images import ImagesTab
        images_tab = ImagesTab()
        images_tab.set_thumbnail_size(self.settings.general.thumbnail_size)
        images_tab.hashLookupFinished.connect(self._refresh_counts)
        if self.case_data:
            images_tab.set_case_data(self.case_data, case_folder=self.case_path, defer_load=defer_load)
            images_tab.set_hash_db_path(
                Path(self.settings.hash.db_path) if self.settings.hash.db_path else None
            )
        images_tab.set_evidence(evidence_id, defer_load=defer_load)
        evidence_tabs.addTab(images_tab, "Images")

        # 7. OS Artifacts - supports lazy loading
        from app.features.os_artifacts import OSArtifactsTab
        os_tab = OSArtifactsTab()
        if self.case_data:
            os_tab.set_case_data(self.case_data, defer_load=defer_load)
        os_tab.set_evidence(evidence_id, defer_load=defer_load)
        evidence_tabs.addTab(os_tab, "OS Artifacts")

        # 8. Timeline - supports lazy loading
        from app.features.timeline import TimelineTab
        timeline_tab = TimelineTab()
        if self.case_data:
            timeline_tab.set_case_data(self.case_data, defer_load=defer_load)
        timeline_tab.set_evidence(evidence_id, defer_load=defer_load)
        evidence_tabs.addTab(timeline_tab, "Timeline")

        # 9. Download (Refactored with 3 subtabs)
        from app.features.downloads import DownloadTab
        download_tab = DownloadTab(evidence_id, self.case_data, self.case_path)
        download_tab.download_started.connect(self._on_download_started_from_tab)
        download_tab.download_paused.connect(self._on_download_paused)
        download_tab.download_cancelled.connect(self._on_download_cancelled)
        evidence_tabs.addTab(download_tab, "Download")

        # 10. Reports
        from app.features.reports import ReportsTab
        reports_tab = ReportsTab()
        reports_tab.manage_text_blocks_requested.connect(
            lambda: self._open_preferences_dialog(initial_tab="Text Blocks")
        )
        # Set global default branding settings from preferences
        reports_tab.set_default_settings(self.settings.reports, self.settings_file.parent)
        # Set database manager for section persistence, then case data and evidence
        if self.db_manager:
            reports_tab.set_database_manager(self.db_manager)
        if self.case_data:
            reports_tab.set_case_data(self.case_data)
        reports_tab.set_evidence(evidence_id, evidence_label)
        evidence_tabs.addTab(reports_tab, "Reports")

        # 11. Screenshots (NEW)
        from app.features.screenshots import ScreenshotsTab
        screenshots_tab = ScreenshotsTab(self.case_data, self.case_path, self.db_manager)
        screenshots_tab.set_evidence(evidence_id, evidence_label)
        evidence_tabs.addTab(screenshots_tab, "Screenshots")

        # 12. Tags (NEW)
        tags_tab = TagsTab()
        # IMPORTANT: Set case_data BEFORE evidence_id to ensure reload() has data access
        if self.case_data:
            tags_tab.set_case_data(self.case_data)
        tags_tab.set_evidence(evidence_id)
        evidence_tabs.addTab(tags_tab, "Tags")

        # 13. Audit (NEW) - includes Statistics, Warnings, and Logs subtabs
        from app.features.audit import AuditTab
        audit_tab = AuditTab(
            self.db_manager,
            evidence_id,
            evidence_label,
            case_path=self.case_path,
            audit_logger=self.audit_logger,
        )
        evidence_tabs.addTab(audit_tab, "📋 Audit")

        # Store log widget reference for log message routing (now in audit tab)
        if not hasattr(self, '_evidence_log_widgets'):
            self._evidence_log_widgets = {}
        self._evidence_log_widgets[evidence_id] = audit_tab.log_widget

        # Store references for updates
        evidence_tabs.setProperty("evidence_id", evidence_id)
        evidence_tabs.setProperty("extraction_tab", extraction_tab)
        evidence_tabs.setProperty("file_list_tab", file_list_tab)
        evidence_tabs.setProperty("browser_inventory_tab", browser_inventory_tab)
        evidence_tabs.setProperty("urls_tab", urls_tab)
        evidence_tabs.setProperty("images_tab", images_tab)
        evidence_tabs.setProperty("os_tab", os_tab)
        evidence_tabs.setProperty("timeline_tab", timeline_tab)
        evidence_tabs.setProperty("download_tab", download_tab)
        evidence_tabs.setProperty("reports_tab", reports_tab)
        evidence_tabs.setProperty("tags_tab", tags_tab)
        evidence_tabs.setProperty("audit_tab", audit_tab)
        # Statistics tab is now inside audit_tab - access via audit_tab.statistics_tab
        evidence_tabs.setProperty("statistics_tab", audit_tab.statistics_tab)
        evidence_tabs.setProperty("overview_widget", overview_widget)

        # Phase 3: Connect tab change for lazy loading trigger
        if defer_load:
            evidence_tabs.currentChanged.connect(
                lambda idx, tabs=evidence_tabs: self._on_evidence_subtab_changed(tabs, idx)
            )

        return evidence_tabs

    def _update_case_label(self, case_path: Optional[Path]) -> None:
        if not case_path:
            self.case_label.setText("No case open.")
        else:
            self.case_label.setText(f"Case: {case_path}")

    def _collect_recent_logs(self, limit: int = 50) -> list[str]:
        lines = self.log_widget.toPlainText().splitlines()
        if limit <= 0:
            return lines
        return lines[-limit:]

    def _build_placeholder_tab(self, label: str) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout()
        layout.addWidget(QLabel(f"{label} tab placeholder – functionality coming soon."))
        layout.addStretch()
        widget.setLayout(layout)
        return widget

    # Validation ------------------------------------------------

    def _run_quick_validation(self) -> None:
        """Run quick validation in background after case open."""
        if not self.case_path:
            return

        # Cancel any existing validation
        if self._validation_worker is not None:
            self._validation_worker = None

        # Start background validation
        worker = ValidationWorker(self.case_path, quick=True)
        worker.signals.finished.connect(self._on_validation_finished)
        worker.signals.error.connect(self._on_validation_error)
        self._validation_worker = worker
        self.thread_pool.start(worker)
        LOGGER.debug("Started quick validation in background")

    def _on_validation_finished(self, report: ValidationReport) -> None:
        """Handle validation completion."""
        self._validation_report = report
        self._validation_worker = None

        # Count warnings and errors
        warning_count = report.warning_count
        error_count = report.error_count

        # Update status bar warning icon
        has_warnings_or_errors = warning_count > 0 or error_count > 0
        self.validation_warning_label.setVisible(has_warnings_or_errors)

        if error_count > 0:
            self.validation_warning_label.setText(f"⚠ {error_count} validation error(s)")
            self.validation_warning_label.setStyleSheet("QLabel { color: #cc0000; }")  # Red
        elif warning_count > 0:
            self.validation_warning_label.setText(f"⚠ {warning_count} validation warning(s)")
            self.validation_warning_label.setStyleSheet("QLabel { color: #d97000; }")  # Orange

        LOGGER.info("Quick validation completed: %d pass, %d warning, %d error",
                    report.pass_count, warning_count, error_count)

    def _on_validation_error(self, error_msg: str) -> None:
        """Handle validation error."""
        self._validation_worker = None
        LOGGER.error("Validation failed: %s", error_msg)
        # Don't show error dialog for background validation - just log it

    def _validate_case_manual(self) -> None:
        """Run full validation and show results dialog (triggered by menu)."""
        if not self.case_path:
            QMessageBox.warning(
                self,
                "No Case Open",
                "Please open a case before running validation."
            )
            return

        # Run full validation (synchronous, with progress feedback)
        progress = QProgressDialog(
            "Running validation checks…",
            "Cancel",
            0, 0,
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setWindowTitle("Validation")
        progress.setCancelButton(None)  # No cancel for now
        progress.show()
        QCoreApplication.processEvents()

        try:
            report = validate_case_full(self.case_path)
            self._validation_report = report
            progress.close()

            # Show validation dialog
            dialog = ValidationDialog(report, parent=self)
            dialog.exec()

            # Update status bar based on latest report
            self._on_validation_finished(report)

        except Exception as exc:
            progress.close()
            LOGGER.exception("Full validation failed")
            QMessageBox.critical(
                self,
                "Validation Error",
                f"Validation failed:\n{exc}"
            )

    def _show_validation_dialog(self) -> None:
        """Show validation dialog for cached report (triggered by status bar click)."""
        if self._validation_report is None:
            # No cached report - run full validation
            self._validate_case_manual()
            return

        dialog = ValidationDialog(self._validation_report, parent=self)
        dialog.exec()

    # Case handling -------------------------------------------------------

    def _open_case_file_dialog(self) -> None:
        """Open an existing case by selecting its SQLite database file."""
        db_file, _ = QFileDialog.getOpenFileName(
            self,
            "Select Case Database",
            str(self.base_dir),
            "SQLite Database Files (*.sqlite *.db);;All Files (*)"
        )
        if not db_file:
            return

        db_path = Path(db_file)

        # Validate that it's a valid case database
        if not self._validate_case_database(db_path):
            QMessageBox.critical(
                self,
                "Invalid Case Database",
                "The selected file is not a valid forensic case database.\n\n"
                "Expected tables: cases, evidences"
            )
            return

        # Open the case (case_path is the database's parent folder)
        case_folder = db_path.parent
        self._open_case(case_folder, db_path)

    def _create_case_dialog(self) -> None:
        """Show dialog to create a new case with metadata."""
        dialog = CreateCaseDialog(self, self.base_dir)
        if dialog.exec() != QDialog.Accepted:
            return

        case_data = dialog.get_case_data()
        case_folder = case_data['case_folder']
        db_filename = case_data['db_filename']

        # Check if folder already exists (user chose to open it)
        if case_folder.exists():
            db_path = case_folder / db_filename
            if db_path.exists():
                # Open existing case
                self._open_case(case_folder, db_path)
                return
            else:
                # Folder exists but no database - error
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Case folder exists but database file not found:\n{str(db_path)}"
                )
                return

        # Create new case folder and database
        try:
            case_folder.mkdir(parents=True, exist_ok=False)
            self.logger.info("Created case folder: %s", case_folder)
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to create case folder:\n{exc}"
            )
            self.logger.exception("Failed to create case folder")
            return

        # Initialize database
        db_path = case_folder / db_filename
        try:
            conn = init_db(case_folder, db_path)

            # Insert case metadata
            utc_now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            with conn:
                conn.execute(
                    """
                    INSERT INTO cases(case_id, title, investigator, notes, created_at_utc, case_number, case_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        case_data['case_number'],  # case_id (legacy)
                        case_data['case_name'],    # title (legacy)
                        case_data['investigator'],
                        case_data['notes'],
                        utc_now,
                        case_data['case_number'],  # case_number (new)
                        case_data['case_name'],    # case_name (new)
                    ),
                )
            conn.close()
            self.logger.info("Initialized case database: %s", db_path)
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to initialize case database:\n{exc}"
            )
            self.logger.exception("Failed to initialize database")
            # Clean up failed folder
            try:
                case_folder.rmdir()
            except OSError:
                pass
            return

        # Open the newly created case
        self._open_case(case_folder, db_path)

    def _validate_case_database(self, db_path: Path) -> bool:
        """
        Validate that a SQLite file is a forensic case database.
        Returns True if valid, False otherwise.
        """
        if not db_path.exists() or not db_path.is_file():
            return False

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Check for required tables
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('cases', 'evidences')"
            )
            tables = {row[0] for row in cursor.fetchall()}
            conn.close()

            # Must have both cases and evidences tables
            return 'cases' in tables and 'evidences' in tables
        except Exception as exc:
            self.logger.warning("Failed to validate database %s: %s", db_path, exc)
            return False

    def _open_case(self, case_path: Path, db_path: Optional[Path] = None) -> None:
        """
        Open a case folder with background loading (Phase 2).

        Shows a progress dialog while loading case data in a background thread,
        keeping the UI responsive.
        """
        # Clean up any existing case state
        if self.db_manager is not None:
            self.db_manager.close_all()
            self.db_manager = None
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

        # Store for use after loading
        self._pending_case_path = case_path
        self._pending_db_path = db_path

        # Create and configure the background task
        config = CaseLoadTaskConfig(case_path=case_path, db_path=db_path)
        self._case_load_task = CaseLoadTask(config)

        # Create progress dialog
        self._case_load_progress = QProgressDialog(
            "Opening case...",
            "Cancel",
            0, 100,
            self
        )
        self._case_load_progress.setWindowTitle("Opening Case")
        self._case_load_progress.setWindowModality(Qt.WindowModal)
        self._case_load_progress.setAutoClose(True)
        self._case_load_progress.setAutoReset(False)
        self._case_load_progress.setMinimumDuration(200)  # Show after 200ms
        self._case_load_progress.canceled.connect(self._on_case_load_cancelled)

        # Connect signals
        self._case_load_task.signals.progress.connect(self._on_case_load_progress)
        self._case_load_task.signals.result.connect(self._on_case_load_result)
        self._case_load_task.signals.error.connect(self._on_case_load_error)

        # Start the background task
        self.logger.info("Starting background case load for %s", case_path)
        start_task(self._case_load_task)

    def _on_case_load_progress(self, percent: int, message: str) -> None:
        """Handle progress updates from case loading task."""
        if self._case_load_progress:
            self._case_load_progress.setValue(percent)
            self._case_load_progress.setLabelText(message)

    def _on_case_load_cancelled(self) -> None:
        """Handle cancellation of case loading."""
        if self._case_load_task:
            self._case_load_task.cancel()
        self._case_load_progress = None
        self._case_load_task = None
        self.logger.info("Case loading cancelled by user")

    def _on_case_load_error(self, error: str, traceback_str: str) -> None:
        """Handle error from case loading task."""
        # Disconnect and close progress dialog
        if self._case_load_progress:
            try:
                self._case_load_progress.canceled.disconnect(self._on_case_load_cancelled)
            except RuntimeError:
                pass
            self._case_load_progress.close()
            self._case_load_progress = None
        self._case_load_task = None
        QMessageBox.critical(
            self,
            "Error",
            f"Failed to open case:\n{error}",
        )
        self.logger.error("Case load error: %s\n%s", error, traceback_str)

    def _on_case_load_result(self, result: CaseLoadResult) -> None:
        """Handle successful completion of case loading."""
        # Close and clean up progress dialog
        # Disconnect canceled signal first to prevent spurious "cancelled by user" log
        if self._case_load_progress:
            try:
                self._case_load_progress.canceled.disconnect(self._on_case_load_cancelled)
            except RuntimeError:
                pass  # Already disconnected
            self._case_load_progress.close()
            self._case_load_progress = None
        self._case_load_task = None

        if result.error:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to open case:\n{result.error}",
            )
            self.logger.error("Case load failed: %s", result.error)
            return

        # Apply the loaded case data to the UI
        self._apply_loaded_case(result)

    def _apply_loaded_case(self, result: CaseLoadResult) -> None:
        """Apply loaded case data to the UI (runs on main thread)."""
        self.db_manager = result.db_manager
        self.conn = self.db_manager.get_case_conn() if self.db_manager else None
        self.case_path = result.case_path
        self.case_db_path = result.db_path

        # Install statistics collector (singleton)
        from core.statistics_collector import StatisticsCollector
        StatisticsCollector.install(db_manager=self.db_manager)

        # Initialize case-level audit logging
        case_number = result.case_metadata.get("case_id", result.case_path.name)
        self.audit_logger.set_case(result.case_path, case_number, result.db_path)

        self._update_case_label(result.case_path)
        self.add_evidence_button.setEnabled(True)
        self.remove_evidence_button.setEnabled(True)
        self.validate_case_action.setEnabled(True)
        self.export_case_action.setEnabled(True)

        # Get case ID from metadata
        self.case_db_id = result.case_metadata.get("id")

        # Create CaseDataAccess with the loaded manager
        self.case_data = CaseDataAccess(result.case_path, db_manager=result.db_manager)
        self._current_counts = None

        # Load case metadata into Case Info tab
        self.case_info_tab.load_case_data(result.case_metadata)

        # Load evidences into Case Info tab for batch operations
        self.case_info_tab.load_evidences(result.evidences)

        # Load all evidences with lazy loading for sub-tabs (Phase 3)
        self._load_existing_evidences_lazy(result.evidences)

        # Run quick validation in background
        self._run_quick_validation()

        self.logger.info("Case loaded successfully: %s", result.case_path)

    def _ensure_case_record(self) -> None:
        assert self.conn is not None
        row = self.conn.execute("SELECT id FROM cases LIMIT 1").fetchone()
        if row:
            self.case_db_id = int(row["id"])
            return

        # For new cases, extract case number from folder name if it follows the pattern
        folder_name = self.case_path.name if self.case_path else "CASE"
        case_number = folder_name
        case_name = folder_name

        # If folder name follows pattern "{number}_browser_analyzing", extract the case number
        if folder_name.endswith("_browser_analyzing"):
            case_number = folder_name[:-len("_browser_analyzing")]
            case_name = case_number

        utc_now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO cases(case_id, title, created_at_utc, case_number, case_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    case_number,
                    case_name,
                    utc_now,
                    case_number,
                    case_name,
                ),
            )
        self.case_db_id = int(cur.lastrowid)

    def _load_existing_evidences(self) -> None:
        """Load evidences with immediate data loading (legacy behavior)."""
        # Remove all evidence tabs (keep Case tab at index 0)
        while self.main_tabs.count() > 1:
            self.main_tabs.removeTab(1)
        self._evidence_tabs.clear()

        if not self.case_data:
            return

        # Create a tab for each evidence
        for evidence in self.case_data.list_evidences():
            evidence_id = int(evidence["id"])
            evidence_label = evidence["label"]

            # Create evidence tab with sub-tabs (immediate loading)
            evidence_tab_widget = self._build_evidence_tab(evidence_id, evidence_label, defer_load=False)
            tab_index = self.main_tabs.addTab(evidence_tab_widget, evidence_label)
            self._evidence_tabs[evidence_id] = tab_index

            # Initialize extraction tab context (modular extractors)
            extraction_tab = evidence_tab_widget.property("extraction_tab")
            if extraction_tab and hasattr(extraction_tab, 'set_current_case'):
                # Get case object
                case_meta = self.case_data.get_case_metadata() if self.case_data else {}

                # Create a simple case object (since we don't have the full Case class here)
                from types import SimpleNamespace
                case = SimpleNamespace(
                    workspace_dir=str(self.case_path),
                    database_path=str(self.case_db_path) if self.case_db_path else None
                )

                # Set context (evidence_fs will be None until evidence is mounted)
                # Pass db_manager so worker can create thread-local connections
                extraction_tab.set_current_case(
                    case=case,
                    evidence=evidence,
                    evidence_fs=None,  # Will be set when evidence is mounted
                    db_manager=self.db_manager,  # Pass DatabaseManager, not connection
                    audit_logger=self.audit_logger  # For persistent logging
                )

            # Load partition data if available
            if evidence.get('partition_info'):
                self._load_partition_data_into_tab(evidence_tab_widget, evidence)

        # Select first evidence tab if exists
        if self.main_tabs.count() > 1:
            self.main_tabs.setCurrentIndex(1)

    def _load_existing_evidences_lazy(self, evidences: List[Dict[str, Any]]) -> None:
        """
        Load evidences with deferred data loading (Phase 3).

        Creates evidence tabs immediately but defers heavy data loading
        until each tab becomes visible.
        """
        # Remove all evidence tabs (keep Case tab at index 0)
        while self.main_tabs.count() > 1:
            self.main_tabs.removeTab(1)
        self._evidence_tabs.clear()

        if not self.case_data or not evidences:
            return

        # Create a tab for each evidence with deferred loading
        for evidence in evidences:
            evidence_id = int(evidence["id"])
            evidence_label = evidence["label"]

            # Create evidence tab with sub-tabs (deferred loading)
            evidence_tab_widget = self._build_evidence_tab(evidence_id, evidence_label, defer_load=True)
            tab_index = self.main_tabs.addTab(evidence_tab_widget, evidence_label)
            self._evidence_tabs[evidence_id] = tab_index

            # Initialize extraction tab context (modular extractors)
            extraction_tab = evidence_tab_widget.property("extraction_tab")
            if extraction_tab and hasattr(extraction_tab, 'set_current_case'):
                from types import SimpleNamespace
                case = SimpleNamespace(
                    workspace_dir=str(self.case_path),
                    database_path=str(self.case_db_path) if self.case_db_path else None
                )
                extraction_tab.set_current_case(
                    case=case,
                    evidence=evidence,
                    evidence_fs=None,
                    db_manager=self.db_manager,
                    audit_logger=self.audit_logger
                )

            # Load partition data if available
            if evidence.get('partition_info'):
                self._load_partition_data_into_tab(evidence_tab_widget, evidence)

        # Select first evidence tab if exists
        if self.main_tabs.count() > 1:
            self.main_tabs.setCurrentIndex(1)

    def _record_evidence(self, source_path: Path, partition_index: Optional[int] = None,
                        partition_info: Optional[str] = None) -> int:
        assert self.conn is not None
        assert self.case_db_id is not None
        utc_now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        size = source_path.stat().st_size if source_path.exists() else None

        # Auto-derive label from E01 filename
        # Extract base name (strip .E01, .E02, etc. extensions)
        from core.evidence_fs import find_ewf_segments
        base_name = source_path.stem
        # For multi-segment E01s, all segments use same base name
        # Example: "4Dell Latitude CPi.E01" -> "4Dell Latitude CPi"
        label = base_name
        if partition_index is not None:
            label = f"{base_name} [Partition {partition_index}]"

        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO evidences(case_id, label, source_path, size, added_at_utc, read_only,
                                     partition_index, partition_info)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    self.case_db_id,
                    label,
                    str(source_path),
                    size,
                    utc_now,
                    partition_index,
                    partition_info,
                ),
            )
        evidence_id = int(cur.lastrowid)
        self.logger.info("Registered evidence %s (id=%s, partition=%s)",
                        source_path, evidence_id, partition_index)

        # Log to case audit log
        if hasattr(self, 'audit_logger') and self.audit_logger.case_logger:
            try:
                self.audit_logger.case_logger.log_evidence_added(
                    evidence_id=evidence_id,
                    label=label,
                    source_path=str(source_path),
                    size_bytes=size
                )
            except Exception as e:
                self.logger.warning(f"Failed to log evidence added to case audit: {e}")

        return evidence_id

    def _auto_generate_file_list(
        self,
        evidence_id: int,
        evidence_label: str,
        ewf_segments: List[Path],
    ) -> bool:
        """
        Auto-generate file list from E01 using SleuthKit fls.

        This is called after evidence addition if:
        - SleuthKit (fls) is available
        - auto_generate_file_list config is enabled

        Args:
            evidence_id: Database ID of the evidence
            evidence_label: Evidence label for database connection
            ewf_segments: List of EWF segment paths

        Returns:
            True if generation succeeded, False otherwise
        """
        from PySide6.QtCore import QThread
        from extractors.system.file_list.sleuthkit_utils import get_sleuthkit_bin

        # Check if fls is available
        fls_path = get_sleuthkit_bin("fls")
        if not fls_path:
            self.logger.info(
                "SleuthKit (fls) not found (bundled or PATH) - skipping auto file list generation. "
                "Install SleuthKit or bundle binaries for automatic file list generation."
            )
            return False

        # Get evidence database connection
        if not self.db_manager:
            self.logger.warning("No database manager - skipping auto file list generation")
            return False

        try:
            evidence_conn = self.db_manager.get_evidence_conn(evidence_id, evidence_label)
        except Exception as e:
            self.logger.warning(f"Could not connect to evidence database: {e}")
            return False

        # Import generator
        from extractors.system.file_list.sleuthkit_generator import SleuthKitFileListGenerator

        # Create generator
        generator = SleuthKitFileListGenerator(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            ewf_paths=ewf_segments,
        )

        if not generator.fls_available:
            evidence_conn.close()
            return False

        # Show progress dialog
        progress = QProgressDialog(
            "Generating file list from E01...\nThis enables fast file discovery for all extractors.",
            "Cancel",
            0, 0,
            self
        )
        progress.setWindowTitle("Auto File List Generation")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)  # Show immediately
        progress.show()
        QApplication.processEvents()

        # Run generation in thread
        class FileListGenerationThread(QThread):
            def __init__(self, gen, parent=None):
                super().__init__(parent)
                self.generator = gen
                self.result = None
                self.error = None
                self._cancelled = False

            def request_cancel(self):
                self._cancelled = True

            def run(self):
                try:
                    def progress_cb(files: int, part_idx: int, msg: str):
                        if self._cancelled:
                            raise InterruptedError("Cancelled by user")

                    self.result = self.generator.generate(progress_callback=progress_cb)
                except InterruptedError:
                    self.result = None  # Cancelled
                except Exception as e:
                    self.error = e

        gen_thread = FileListGenerationThread(generator, self)
        gen_thread.start()

        # Wait for thread while keeping UI responsive
        while gen_thread.isRunning():
            QApplication.processEvents()
            if progress.wasCanceled():
                gen_thread.request_cancel()
                # Give it a moment to finish cleanly
                gen_thread.wait(500)
                break
            gen_thread.wait(100)

        progress.close()
        evidence_conn.close()

        # Check results
        if gen_thread.error:
            self.logger.warning(f"File list generation failed: {gen_thread.error}")
            return False

        if gen_thread.result is None:
            self.logger.info("File list generation cancelled by user")
            return False

        result = gen_thread.result
        if result.success:
            self.logger.info(
                "Auto-generated file list: %d entries from %d partition(s) in %.1fs",
                result.total_files,
                result.partitions_processed,
                result.duration_seconds
            )
            return True
        else:
            self.logger.warning(f"File list generation failed: {result.error_message}")
            return False

    def _open_preferences_dialog(self, initial_tab: Optional[str] = None) -> None:
        dialog = PreferencesDialog(
            self.settings,
            self.settings_file.parent,
            self.app_config.rules_dir,
            self.tool_registry,  # Pass tool registry for enhanced tools tab
            initial_tab=initial_tab,
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted or not dialog.result_settings:
            return
        new_settings = dialog.result_settings
        self.settings = new_settings
        try:
            self.settings.save(self.settings_file)
        except Exception as exc:  # pragma: no cover - file system error path
            QMessageBox.warning(
                self,
                "Preferences",
                f"Failed to save settings: {exc}",
            )

        # Update all open evidence tabs' ImagesTab instances with new settings
        # self.main_tabs contains: [0] = Case tab, [1...n] = Evidence tabs
        for i in range(1, self.main_tabs.count()):  # Skip Case tab at index 0
            evidence_tab_widget = self.main_tabs.widget(i)
            if evidence_tab_widget and isinstance(evidence_tab_widget, QTabWidget):
                # Find the Images tab (index 4 in the evidence sub-tabs after  File List addition)
                images_tab = evidence_tab_widget.widget(4)  # Images is at index 4
                if images_tab and hasattr(images_tab, 'set_hash_db_path'):
                    images_tab.set_hash_db_path(
                        Path(self.settings.hash.db_path) if self.settings.hash.db_path else None
                    )
                    images_tab.set_thumbnail_size(self.settings.general.thumbnail_size)

        self.logger.info("Preferences updated")

    def _load_partition_data_into_tab(self, evidence_tab_widget: QTabWidget, evidence: dict) -> None:
        """
        Load partition information into the Overview tab.
        Moved from Extraction tab to Overview tab.
        """
        import json

        # Get the Overview widget
        overview_widget = evidence_tab_widget.property("overview_widget")
        if not overview_widget:
            return

        # Find the DiskTabWidget within Overview
        disk_tab = None
        for child in overview_widget.findChildren(DiskTabWidget):
            disk_tab = child
            break

        if not disk_tab:
            return

        if not evidence.get('partition_info'):
            disk_tab.set_partitions([], evidence.get('label', ''), current_selections=None)
            return

        # Parse partition data (display only)
        partition_info_list = json.loads(evidence['partition_info'])
        scan_slack = bool(evidence.get('scan_slack_space', 1))  # Default True

        disk_tab.set_partitions(
            partition_info_list,
            evidence.get('label', ''),
            current_selections=None,
            scan_slack=scan_slack
        )

    # Evidence actions ----------------------------------------------------

    def _add_evidence_dialog(self) -> None:
        if not self.case_path:
            QMessageBox.warning(
                self,
                "No case",
                "Open or create a case before adding evidence.",
            )
            return
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select EWF files",
            str(self.case_path),
            "EWF Images (*.E01 *.e01 *.e0*);;All files (*)",
        )
        if not files:
            return

        for path_str in files:
            path = Path(path_str)

            # Check if E01 file - show partition selection
            is_ewf = path.suffix.lower() in {'.e01', '.e02', '.e03', '.e04', '.e05',
                                              '.e06', '.e07', '.e08', '.e09'}

            if is_ewf:
                # Discover segments and partitions in background thread
                try:
                    ewf_segments = find_ewf_segments(path)

                    # Show progress dialog during partition detection
                    progress = QProgressDialog(
                        "Analyzing E01 image structure...\nThis may take a moment for large images.",
                        "Cancel",
                        0, 0,
                        self
                    )
                    progress.setWindowTitle("Detecting Partitions")
                    progress.setWindowModality(Qt.WindowModal)
                    progress.setMinimumDuration(500)  # Show after 500ms
                    progress.setCancelButton(None)  # Can't cancel partition detection
                    progress.show()
                    QApplication.processEvents()  # Force immediate display

                    # Run partition detection in thread
                    from PySide6.QtCore import QThread

                    class PartitionDetectionThread(QThread):
                        def __init__(self, ewf_segments):
                            super().__init__()
                            self.ewf_segments = ewf_segments
                            self.partitions = None
                            self.error = None

                        def run(self):
                            try:
                                self.partitions = list_ewf_partitions(self.ewf_segments)
                            except Exception as e:
                                self.error = e

                    detection_thread = PartitionDetectionThread(ewf_segments)
                    detection_thread.start()

                    # Wait for thread to finish while keeping UI responsive
                    while detection_thread.isRunning():
                        QApplication.processEvents()
                        detection_thread.wait(100)

                    progress.close()

                    # Check for errors
                    if detection_thread.error:
                        raise detection_thread.error

                    partitions = detection_thread.partitions

                    # Store ALL partitions - user will select which to scan in Disk tab
                    import json
                    partition_info_json = json.dumps(partitions)

                    # Add evidence with all partition data
                    evidence_id = self._record_evidence(
                        path,
                        partition_index=None,  # No specific partition - user will select
                        partition_info=partition_info_json
                    )

                    # Use canonical evidence label from the database for filesystem paths/logging
                    evidence_data = self.case_data.get_evidence(evidence_id) if self.case_data else None
                    evidence_label = evidence_data.get("label") if evidence_data else path.stem

                    # Create evidence tab (display name keeps original path for clarity)
                    evidence_tab_widget = self._build_evidence_tab(evidence_id, evidence_label)
                    tab_index = self.main_tabs.addTab(evidence_tab_widget, path.name)
                    self._evidence_tabs[evidence_id] = tab_index

                    # Initialize extraction tab context (modular extractors)
                    extraction_tab = evidence_tab_widget.property("extraction_tab")
                    if extraction_tab and hasattr(extraction_tab, 'set_current_case'):
                        # Create case object
                        from types import SimpleNamespace
                        case = SimpleNamespace(
                            workspace_dir=str(self.case_path),
                            database_path=str(self.case_db_path) if self.case_db_path else None
                        )

                        # Set context for extraction tab
                        extraction_tab.set_current_case(
                            case=case,
                            evidence=evidence_data,
                            evidence_fs=None,  # Will be set when evidence is mounted
                            db_manager=self.db_manager,
                            audit_logger=self.audit_logger
                        )

                    # Load partition data into Disk tab
                    if evidence_data and evidence_data.get('partition_info'):
                        self._load_partition_data_into_tab(evidence_tab_widget, evidence_data)

                    # Auto-generate file list if enabled
                    if self.app_config.extraction.auto_generate_file_list:
                        self._auto_generate_file_list(
                            evidence_id=evidence_id,
                            evidence_label=evidence_label,
                            ewf_segments=ewf_segments,
                        )

                    self.logger.info(
                        "Added E01 evidence with %d partition(s). Use Disk tab to select which to scan.",
                        len(partitions)
                    )

                except Exception as exc:
                    self.logger.exception("Failed to detect partitions")
                    QMessageBox.warning(
                        self,
                        "Partition Detection Failed",
                        f"Could not detect partitions: {exc}\n\n"
                        "The evidence will be added without partition selection."
                    )
                    # Fallback: add without partition info
                    evidence_id = self._record_evidence(path)
                    evidence_data = self.case_data.get_evidence(evidence_id) if self.case_data else None
                    evidence_label = evidence_data.get("label") if evidence_data else path.stem
                    evidence_tab_widget = self._build_evidence_tab(evidence_id, evidence_label)
                    tab_index = self.main_tabs.addTab(evidence_tab_widget, path.name)
                    self._evidence_tabs[evidence_id] = tab_index

                    # Initialize extraction tab context
                    extraction_tab = evidence_tab_widget.property("extraction_tab")
                    if extraction_tab and hasattr(extraction_tab, 'set_current_case'):
                        from types import SimpleNamespace
                        case = SimpleNamespace(
                            workspace_dir=str(self.case_path),
                            database_path=str(self.case_db_path) if self.case_db_path else None
                        )
                        extraction_tab.set_current_case(
                            case=case,
                            evidence=evidence_data,
                            evidence_fs=None,
                            db_manager=self.db_manager,
                            audit_logger=self.audit_logger
                        )

                    # Auto-generate file list even on partition detection failure
                    if self.app_config.extraction.auto_generate_file_list:
                        self._auto_generate_file_list(
                            evidence_id=evidence_id,
                            evidence_label=evidence_label,
                            ewf_segments=ewf_segments,
                        )
            else:
                # Non-E01 file - add directly
                evidence_id = self._record_evidence(path)
                evidence_data = self.case_data.get_evidence(evidence_id) if self.case_data else None
                evidence_label = evidence_data.get("label") if evidence_data else path.stem
                evidence_tab_widget = self._build_evidence_tab(evidence_id, evidence_label)
                tab_index = self.main_tabs.addTab(evidence_tab_widget, path.name)
                self._evidence_tabs[evidence_id] = tab_index

                # Initialize extraction tab context
                extraction_tab = evidence_tab_widget.property("extraction_tab")
                if extraction_tab and hasattr(extraction_tab, 'set_current_case'):
                    from types import SimpleNamespace
                    case = SimpleNamespace(
                        workspace_dir=str(self.case_path),
                        database_path=str(self.case_db_path) if self.case_db_path else None
                    )
                    extraction_tab.set_current_case(
                        case=case,
                        evidence=evidence_data,
                        evidence_fs=None,
                        db_manager=self.db_manager,
                        audit_logger=self.audit_logger
                    )

        self.logger.info("Added %d evidence file(s).", len(files))

        # Refresh Case Info tab evidence list
        if self.case_data:
            self.case_info_tab.load_evidences(self.case_data.list_evidences())

        # Switch to the newly added evidence tab if one was added
        if len(files) > 0 and self.main_tabs.count() > 1:
            self.main_tabs.setCurrentIndex(self.main_tabs.count() - 1)

    def _remove_evidence_dialog(self) -> None:
        """
        Show dialog to select and remove an evidence from the case.

        Added evidence removal functionality.

        This will:
        1. Cancel any running tasks on the evidence
        2. Close the evidence tab
        3. Delete the evidence folder (database, artifacts, downloads, thumbnails)
        4. Delete the evidence log file
        5. Remove the evidence record from the case database
        6. Log the action to the case audit log
        """
        if not self.case_path or not self.case_data:
            QMessageBox.warning(
                self,
                "No Case",
                "Open or create a case first.",
            )
            return

        # Get list of evidences
        evidences = self.case_data.list_evidences()
        if not evidences:
            QMessageBox.information(
                self,
                "No Evidence",
                "There are no evidences to remove.",
            )
            return

        # Callback to get evidence table counts
        def get_evidence_counts(evidence_id: int) -> dict:
            """Get counts for an evidence from its database."""
            evidence = next((e for e in evidences if e["id"] == evidence_id), None)
            if not evidence:
                return {}

            label = evidence.get("label", f"Evidence {evidence_id}")

            # Check if evidence DB exists
            if not self.db_manager or not self.db_manager.evidence_db_exists(evidence_id, label):
                return {}

            try:
                from core.database.helpers.batch import get_evidence_table_counts
                conn = self.db_manager.get_evidence_conn(evidence_id, label)
                return get_evidence_table_counts(conn, evidence_id)
            except Exception as e:
                self.logger.warning("Failed to get evidence counts: %s", e)
                return {}

        # Show dialog
        dialog = RemoveEvidenceDialog(evidences, get_evidence_counts, parent=self)
        if dialog.exec() != QDialog.Accepted:
            return

        selected_evidence = dialog.get_selected_evidence()
        if not selected_evidence:
            return

        evidence_id = selected_evidence["id"]
        evidence_label = selected_evidence.get("label", f"Evidence {evidence_id}")

        self.logger.info("Removing evidence: %s (id=%d)", evidence_label, evidence_id)

        # Perform the removal
        try:
            self._remove_evidence(evidence_id, evidence_label)

            QMessageBox.information(
                self,
                "Evidence Removed",
                f"Evidence '{evidence_label}' has been removed successfully.",
            )

        except Exception as e:
            self.logger.exception("Failed to remove evidence: %s", e)
            QMessageBox.critical(
                self,
                "Removal Failed",
                f"Failed to remove evidence:\n\n{e}",
            )

    def _remove_evidence(self, evidence_id: int, evidence_label: str) -> None:
        """
        Remove an evidence from the case.

        Args:
            evidence_id: ID of the evidence to remove
            evidence_label: Label of the evidence (for paths and logging)

        Raises:
            Exception: If removal fails
        """
        import shutil

        # 1. Cancel any running tasks and close the evidence tab
        self._close_evidence_tab_by_id(evidence_id)

        # 2. Close any cached database connections for this evidence
        if self.db_manager:
            # Close evidence logger first (releases file handle)
            if hasattr(self, 'audit_logger') and evidence_id in self.audit_logger._evidence_loggers:
                self.audit_logger._evidence_loggers[evidence_id].close()
                del self.audit_logger._evidence_loggers[evidence_id]

            # Note: We can't selectively close just this evidence's connection
            # but the connection will be orphaned when we delete the file

        # 3. Delete the evidence folder
        slug = slugify_label(evidence_label, evidence_id)
        evidence_folder = self.case_path / "evidences" / slug
        if evidence_folder.exists():
            self.logger.info("Deleting evidence folder: %s", evidence_folder)
            shutil.rmtree(evidence_folder)

        # 4. Delete the evidence log file
        evidence_log = self.case_path / "logs" / f"evidence_{evidence_id}.log"
        if evidence_log.exists():
            self.logger.info("Deleting evidence log: %s", evidence_log)
            evidence_log.unlink()
        # Also delete rotated logs (evidence_1.log.1, evidence_1.log.2, etc.)
        for rotated_log in self.case_path.glob(f"logs/evidence_{evidence_id}.log.*"):
            self.logger.info("Deleting rotated log: %s", rotated_log)
            rotated_log.unlink()

        # 5. Remove from case database (cascades to report_sections)
        if self.conn:
            with self.conn:
                self.conn.execute(
                    "DELETE FROM evidences WHERE id = ?",
                    (evidence_id,)
                )
            self.logger.info("Removed evidence record from database")

        # 6. Log to case audit
        if hasattr(self, 'audit_logger') and self.audit_logger.case_logger:
            self.audit_logger.case_logger.log_evidence_removed(evidence_id, evidence_label)

        # 7. Clean up in-memory state
        if evidence_id in self._evidence_log_widgets:
            del self._evidence_log_widgets[evidence_id]

        # 8. Refresh Case Info tab
        if self.case_data:
            self.case_info_tab.load_evidences(self.case_data.list_evidences())

        self.logger.info("Evidence removal complete: %s (id=%d)", evidence_label, evidence_id)

    def _close_evidence_tab_by_id(self, evidence_id: int) -> None:
        """
        Close and cleanup an evidence tab by evidence ID.

        This handles:
        - Shutting down any running workers/tasks
        - Removing the tab from the UI
        - Cleaning up internal mappings

        Args:
            evidence_id: ID of the evidence whose tab to close
        """
        if evidence_id not in self._evidence_tabs:
            return

        tab_index = self._evidence_tabs[evidence_id]

        # Get evidence tab widget
        evidence_tab_widget = self.main_tabs.widget(tab_index)

        # Shutdown workers in the closing evidence tab
        if evidence_tab_widget and hasattr(evidence_tab_widget, 'count'):
            for j in range(evidence_tab_widget.count()):
                tab = evidence_tab_widget.widget(j)
                if hasattr(tab, 'shutdown'):
                    try:
                        tab.shutdown()
                    except Exception as e:
                        self.logger.warning("Error during tab shutdown on evidence removal: %s", e)
                # Also try to cancel any active tasks
                if hasattr(tab, 'cancel_tasks'):
                    try:
                        tab.cancel_tasks()
                    except Exception as e:
                        self.logger.warning("Error cancelling tasks on evidence removal: %s", e)

        # Remove from mapping first
        del self._evidence_tabs[evidence_id]

        # Remove the tab
        self.main_tabs.removeTab(tab_index)

        # Update remaining tab indices
        for eid, idx in list(self._evidence_tabs.items()):
            if idx > tab_index:
                self._evidence_tabs[eid] = idx - 1

    # Counts --------------------------------------------------------------

    def _refresh_counts(self) -> None:
        # Counts are now per-evidence tab
        # This method is kept for compatibility but does nothing
        # Each evidence tab updates its own counts
        pass

    def _clear_counts(self) -> None:
        # Counts are now per-evidence tab
        pass

    def _update_overview_summary(self) -> None:
        # Overview is now per-evidence tab
        pass

    # Task handlers -------------------------------------------------------

    def _run_all_extractors(self) -> None:
        # Get current evidence tab
        current_index = self.main_tabs.currentIndex()
        if current_index == 0:  # Case tab
            QMessageBox.information(
                self,
                "Select Evidence",
                "Select an evidence tab before running extractors.",
            )
            return

        if not self.case_path or not self.case_data:
            QMessageBox.warning(
                self,
                "No Case",
                "Please open a case first.",
            )
            return

        evidence_tab_widget = self.main_tabs.widget(current_index)
        if not evidence_tab_widget:
            return

        evidence_id = evidence_tab_widget.property("evidence_id")
        if not evidence_id:
            QMessageBox.warning(
                self,
                "Missing Evidence",
                "Unable to determine evidence identifier.",
            )
            return

        # Get evidence data from database
        evidence_data = self.case_data.get_evidence(evidence_id)
        if not evidence_data:
            return

        evidence_path_str = evidence_data.get("source_path")
        evidence_path = Path(evidence_path_str) if evidence_path_str else None
        is_ewf = evidence_path and evidence_path.suffix.lower() in {'.e01', '.e02', '.e03', '.e04', '.e05',
                                                                      '.e06', '.e07', '.e08', '.e09'}

        config_kwargs = {
            "case_root": self.case_path,
            "db_path": self.case_db_path,
            "evidence_id": int(evidence_id),
            "db_manager": self.db_manager,
        }

        if is_ewf and evidence_path:
            # Direct E01 reading - discover all segments
            try:
                ewf_segments = find_ewf_segments(evidence_path)

                # Always analyze the full E01 image (auto partition detection)
                partition_index = -1
                self.logger.info(
                    "Using direct E01 reading with %d segment(s) across the full image (auto partition detection).",
                    len(ewf_segments),
                )
                config_kwargs["ewf_paths"] = ewf_segments
                config_kwargs["partition_index"] = partition_index
            except Exception as exc:
                # Fall back to asking for mount directory
                self.logger.warning("Cannot use direct E01 reading: %s", exc)
                QMessageBox.warning(
                    self,
                    "Direct E01 Reading Unavailable",
                    f"Could not read E01 directly: {str(exc)}\n\nPlease mount the image and select the mount point.",
                )
                mount_dir = QFileDialog.getExistingDirectory(
                    self,
                    "Select Mounted Evidence Root",
                    str(self.case_path),
                )
                if not mount_dir:
                    return
                config_kwargs["mount_root"] = Path(mount_dir)
        else:
            # Not an E01 or no path - ask for mount directory
            mount_dir = QFileDialog.getExistingDirectory(
                self,
                "Select Mounted Evidence Root",
                str(self.case_path),
            )
            if not mount_dir:
                return
            config_kwargs["mount_root"] = Path(mount_dir)

        config = ExecutorTaskConfig(**config_kwargs)
        task = ExecutorTask(config)
        task.signals.progress.connect(self._on_task_progress)
        task.signals.result.connect(self._on_executor_result)
        task.signals.error.connect(self._on_task_error)
        task.signals.finished.connect(self._on_task_finished)

        self.progress_dialog = QProgressDialog(
            "Running extractors…",
            "Cancel",
            0,
            0,
            self,
        )
        self.progress_dialog.setWindowTitle("Extraction in progress")
        self.progress_dialog.setModal(True)
        self.progress_dialog.show()
        self.progress_dialog.canceled.connect(task.cancel)

        start_task(task, self.thread_pool)

    def _on_task_progress(self, percent: int, message: str) -> None:  # noqa: ARG002
        if self.progress_dialog:
            self.progress_dialog.setLabelText(message)

    def _on_executor_result(self, summary: object) -> None:  # noqa: ARG002
        self.logger.info("Extraction completed successfully")

        # Refresh the current evidence tab's views
        current_index = self.main_tabs.currentIndex()
        if current_index > 0:  # Not Case tab
            evidence_tab_widget = self.main_tabs.widget(current_index)
            if evidence_tab_widget:
                # Refresh each sub-tab
                for tab_name in ('urls_tab', 'images_tab', 'os_tab', 'timeline_tab'):
                    tab = evidence_tab_widget.property(tab_name)
                    if tab and hasattr(tab, 'refresh'):
                        tab.refresh()

    def _on_task_error(self, message: str, details: str) -> None:
        LOGGER.error("Worker error: %s", message)
        show_error_dialog(
            self,
            "Task Error",
            message,
            details,
            log_provider=lambda: self._collect_recent_logs(),
        )

    def _on_task_finished(self) -> None:
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None

    def _get_evidence_label(self, evidence_id: int) -> Optional[str]:
        """
        Get evidence label for the given evidence ID.

        Helper for computing download folder paths.

        Args:
            evidence_id: Evidence ID

        Returns:
            Evidence label or None if not found
        """
        if not self.case_data:
            return None
        try:
            with sqlite3.connect(self.case_db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT label FROM evidences WHERE id = ?",
                    (evidence_id,)
                ).fetchone()
                return row["label"] if row else None
        except Exception as e:
            self.logger.warning(f"Failed to get evidence label for {evidence_id}: {e}")
            return None

    def _cancel_download_task(self) -> None:
        if self._download_task:
            self._download_task.cancel()

    # New  handlers -----------------------------------------------

    def _on_extraction_started(self) -> None:
        """Handle extraction started signal from ExtractionTab."""
        self.log_widget.append("Extraction started...")

    def _on_extraction_finished(self, summary: dict) -> None:
        """Handle extraction finished signal from ExtractionTab.

        Changed to lazy refresh pattern - marks non-visible tabs as stale
        instead of refreshing all tabs synchronously.
        """
        self.log_widget.append(
            f"Extraction finished: {summary}"
        )

        # Refresh the currently visible evidence subtab and mark others as stale
        current_index = self.main_tabs.currentIndex()
        if current_index > 0:  # Not on Case tab
            evidence_tabs = self.main_tabs.widget(current_index)
            if isinstance(evidence_tabs, QTabWidget):
                current_subtab_index = evidence_tabs.currentIndex()

                for tab_index in range(evidence_tabs.count()):
                    tab_widget = evidence_tabs.widget(tab_index)

                    # Only process tabs with mark_stale method
                    if hasattr(tab_widget, 'mark_stale'):
                        if tab_index == current_subtab_index:
                            # Current tab: refresh immediately
                            if hasattr(tab_widget, 'refresh'):
                                tab_widget.refresh()
                                self.logger.info(f"Refreshed {tab_widget.__class__.__name__} after extraction")
                        else:
                            # Other tabs: mark as stale for lazy refresh
                            tab_widget.mark_stale()

        self._refresh_counts()

    def _on_data_changed(self, evidence_tabs: QTabWidget) -> None:
        """
        Handle data_changed signal from ExtractionTab.

        Changed to lazy refresh pattern - only refreshes the currently
        visible tab immediately, marks all other tabs as stale for refresh
        when they become visible. This prevents multi-second UI freezes after
        ingestion when all tabs were being refreshed synchronously.
        """
        self.logger.info("Data changed - marking tabs stale, refreshing current")

        # Get the currently visible tab index
        current_index = evidence_tabs.currentIndex()

        for tab_index in range(evidence_tabs.count()):
            tab_widget = evidence_tabs.widget(tab_index)
            tab_name = evidence_tabs.tabText(tab_index)

            # Mark all tabs with mark_stale() method as stale
            if hasattr(tab_widget, 'mark_stale'):
                if tab_index == current_index:
                    # Current tab: refresh immediately
                    if hasattr(tab_widget, 'refresh'):
                        tab_widget.refresh()
                        self.logger.info(f"Refreshed {tab_name} tab (visible)")
                    elif hasattr(tab_widget, 'load_inventory'):
                        tab_widget.load_inventory()
                        self.logger.info(f"Refreshed {tab_name} tab (visible)")
                else:
                    # Other tabs: mark as stale for lazy refresh
                    tab_widget.mark_stale()
                    self.logger.debug(f"Marked {tab_name} tab as stale")

        # Refresh counts
        self._refresh_counts()

        self.log_widget.append("Data updated - current tab refreshed, others will refresh on view")

    def _on_extraction_log_message(self, evidence_id: int, message: str) -> None:
        """
        Forward log messages from ExtractionTab to the correct per-evidence log widget.

        Refactored for per-evidence isolated logs (performance optimization).
        Each evidence only sees its own extraction/ingestion messages.

        Args:
            evidence_id: ID of the evidence generating the log
            message: Log message text
        """
        # Route to the specific evidence's log widget
        if hasattr(self, '_evidence_log_widgets') and evidence_id in self._evidence_log_widgets:
            self._evidence_log_widgets[evidence_id].append(message)

    def _on_download_started_from_tab(self, selected_items: list) -> None:
        """
        Handle download started signal from DownloadTab.

        Refactored to handle URL dict list from new Download tab.
        Creates download records in database, then starts DownloadTask.

        Args:
            selected_items: List of URL dicts with id, url, domain, etc.
        """
        if not self.case_path or not self.case_data:
            QMessageBox.warning(
                self,
                "Downloads",
                "Open a case before downloading.",
            )
            return

        # Get current evidence tab
        current_index = self.main_tabs.currentIndex()
        if current_index == 0:  # Case tab
            QMessageBox.warning(
                self,
                "Downloads",
                "Select an evidence tab before downloading.",
            )
            return

        evidence_tab_widget = self.main_tabs.widget(current_index)
        if not evidence_tab_widget:
            return

        evidence_id = evidence_tab_widget.property("evidence_id")
        if not evidence_id:
            QMessageBox.warning(
                self,
                "Downloads",
                "Unable to determine evidence identifier.",
            )
            return

        from core.file_classifier import classify_file_type, get_extension

        # Prepare download items with database records
        urls_for_download: List[Dict[str, Any]] = []

        for item in selected_items:
            url = item.get("url")
            if not url:
                continue

            parsed = urlparse(url)
            domain = item.get("domain") or (parsed.hostname or "unknown")
            filename = Path(parsed.path).name or "download.bin"
            file_type = classify_file_type(filename)
            file_ext = get_extension(filename)
            url_id = item.get("id")  # From urls table

            # Insert pending download record
            try:
                download_id = self.case_data.insert_download(
                    int(evidence_id),
                    url=url,
                    domain=domain,
                    file_type=file_type,
                    file_extension=file_ext,
                    url_id=url_id,
                    status="pending",
                    filename=filename,
                )
            except Exception as e:
                self.logger.warning("Failed to insert download record: %s", e)
                download_id = 0

            urls_for_download.append({
                "url": url,
                "domain": domain,
                "filename": filename,
                "url_id": url_id,
                "download_id": download_id,
            })

        if not urls_for_download:
            QMessageBox.information(
                self,
                "Downloads",
                "No valid URLs to download.",
            )
            return

        self.log_widget.append(f"Starting download of {len(urls_for_download)} items...")

        # Show download dialog and start task (reusing existing flow)
        dialog = DownloadManagerDialog(self)
        queue = [
            DownloadQueueItem(
                item_id=idx,
                url=row["url"],
                domain=row["domain"],
                filename=row["filename"],
                url_id=row.get("url_id"),
                download_id=row.get("download_id"),
            )
            for idx, row in enumerate(urls_for_download, start=1)
        ]
        dialog.set_queue(queue)
        # Use evidences/{slug}/_downloads/ path (aligned with other extractors)
        evidence_label = self._get_evidence_label(int(evidence_id))
        evidence_slug = slugify_label(evidence_label, int(evidence_id)) if evidence_label else f"evidence_{int(evidence_id)}"
        dialog.set_download_root(
            (self.case_path / "evidences" / evidence_slug / "_downloads")
            if self.case_path
            else Path()
        )
        dialog.pause_button.setEnabled(False)
        self._download_dialog = dialog

        network = self.settings.network

        # Track successful image downloads for post-processing
        images_to_process: List[Tuple[int, int, Path]] = []  # (item_id, download_id, path)

        def start_download() -> None:
            if self._download_task is not None:
                return
            dialog.info_label.setText(dialog.tr("Starting downloads…"))
            dialog.cancel_button.setText(dialog.tr("Cancel"))
            images_to_process.clear()  # Reset for new download run

            config = DownloadTaskConfig(
                case_root=self.case_path,
                case_db_path=self.case_db_path,
                evidence_id=int(evidence_id),
                items=urls_for_download,
                network=network,
                db_manager=self.db_manager,
            )
            task = DownloadTask(config)
            self._download_task = task

            def handle_progress(item_id: int, pct: int, note: str) -> None:
                dialog.update_item(item_id, progress=pct, status=note)

            def handle_finished(
                item_id: int,
                ok: bool,
                path: str,
                err: str,
                bytes_written: int,
                sha256: str,
                content_type: str,
                duration: float,
                download_id: int,
                md5: str,
            ) -> None:
                status_label = dialog.tr("Completed") if ok else dialog.tr("Failed")
                if content_type:
                    status_label = f"{status_label} ({content_type})"
                dialog.update_item(
                    item_id,
                    progress=100 if ok else 0,
                    status=status_label,
                    error=err,
                    dest_path=path,
                    bytes_written=bytes_written,
                    sha256=sha256,
                    content_type=content_type,
                    duration_s=duration,
                )

                # Update downloads table
                if download_id and self.case_data:
                    try:
                        if ok:
                            # Compute case-relative path
                            rel_path = path
                            if path and self.case_path:
                                try:
                                    rel_path = str(Path(path).relative_to(self.case_path))
                                except ValueError:
                                    pass

                            self.case_data.update_download_status(
                                int(evidence_id),
                                download_id,
                                "completed",
                                dest_path=rel_path,
                                filename=Path(path).name if path else None,
                                size_bytes=bytes_written,
                                md5=md5,
                                sha256=sha256,
                                content_type=content_type,
                                duration_seconds=duration,
                            )

                            # Copy tags from source URL
                            queue_item = next((q for q in queue if q.item_id == item_id), None)
                            if queue_item and queue_item.url_id:
                                try:
                                    self.case_data.copy_tags_from_url(
                                        int(evidence_id),
                                        queue_item.url_id,
                                        download_id,
                                    )
                                except Exception:
                                    pass

                            # Track image downloads for post-processing
                            if path:
                                from core.file_classifier import classify_file_type, get_extension
                                ext = get_extension(path)
                                file_type = classify_file_type(path)
                                if file_type == "image" and download_id:
                                    images_to_process.append((item_id, download_id, Path(path)))
                        else:
                            self.case_data.update_download_status(
                                int(evidence_id),
                                download_id,
                                "failed",
                                error_message=err,
                                duration_seconds=duration,
                            )
                    except Exception as e:
                        self.logger.warning("Failed to update download status: %s", e)

            task.signals.item_progress.connect(handle_progress)
            task.signals.item_finished.connect(handle_finished)
            task.signals.result.connect(
                lambda summary: dialog.info_label.setText(
                    dialog.tr("Completed {success}/{total}").format(
                        success=sum(1 for r in summary["results"] if r["ok"]),
                        total=len(queue),
                    )
                )
            )
            task.signals.error.connect(self._on_task_error)
            task.signals.finished.connect(
                lambda: self._on_download_tab_finished(evidence_tab_widget, int(evidence_id), images_to_process)
            )
            start_task(task, self.thread_pool)
            dialog.start_button.setEnabled(False)

        def cancel_download() -> None:
            if self._download_task:
                self._download_task.cancel()
            dialog.reject()

        dialog.start_button.clicked.connect(start_download)
        dialog.cancel_button.clicked.connect(cancel_download)
        dialog.finished.connect(lambda _: self._cancel_download_task())
        dialog.exec()
        self._download_dialog = None

    def _on_download_tab_finished(
        self,
        evidence_tabs: QWidget,
        evidence_id: int = 0,
        images_to_process: Optional[List[Tuple[int, int, Path]]] = None,
    ) -> None:
        """
        Handle download completion from Download tab - refresh the tab and start post-processing.

        Extended to accept list of image downloads for pHash/EXIF processing.
        """
        if self._download_dialog:
            self._download_dialog.info_label.setText(self._download_dialog.tr("Downloads finished."))
            self._download_dialog.start_button.setEnabled(True)
            self._download_dialog.cancel_button.setText(self._download_dialog.tr("Close"))
        self._download_task = None

        # Refresh Download tab to show new downloads
        if isinstance(evidence_tabs, QTabWidget):
            download_tab = evidence_tabs.property("download_tab")
            if download_tab and hasattr(download_tab, 'refresh'):
                download_tab.refresh()

        # Start image post-processing if there are images to process
        if images_to_process and self.case_path and self.case_db_path:
            self._start_image_post_processing(evidence_id, images_to_process)

        self._refresh_counts()

    def _start_image_post_processing(
        self,
        evidence_id: int,
        images: List[Tuple[int, int, Path]],
    ) -> None:
        """
        Start background task to compute pHash, EXIF, and dimensions for downloaded images.

        Post-processing for image downloads.
        """
        if not images:
            return

        self.logger.info("Starting post-processing for %d downloaded images", len(images))

        # Get evidence label for thumbnail path
        evidence_label = self._get_evidence_label(evidence_id)

        config = DownloadPostProcessConfig(
            case_root=self.case_path,
            case_db_path=self.case_db_path,
            evidence_id=evidence_id,
            items=images,
            db_manager=self.db_manager,
            evidence_label=evidence_label,
        )
        task = DownloadPostProcessTask(config)

        def on_result(result: Dict[str, Any]) -> None:
            processed = result.get("processed", 0)
            failed = result.get("failed", 0)
            self.logger.info(
                "Image post-processing complete: %d processed, %d failed",
                processed, failed,
            )
            if self._download_dialog:
                self._download_dialog.info_label.setText(
                    self._download_dialog.tr("Post-processing complete ({processed} images)").format(
                        processed=processed
                    )
                )

        def on_error(error: str, tb: str) -> None:
            self.logger.warning("Image post-processing error: %s", error)

        task.signals.result.connect(on_result)
        task.signals.error.connect(on_error)
        start_task(task, self.thread_pool)

    def _on_download_paused(self) -> None:
        """Handle download paused signal from DownloadTab."""
        self.log_widget.append("Downloads paused")
        if self._download_task:
            # TODO(post-beta): Implement pause functionality
            # See docs/developer/DEFERRED_FEATURES.md
            pass

    def _on_download_cancelled(self) -> None:
        """Handle download cancelled signal from DownloadTab."""
        self.log_widget.append("Downloads cancelled")
        self._cancel_download_task()

    # Events --------------------------------------------------------------

    def _on_main_tab_changed(self, index: int) -> None:
        """Handle main tab changes (Case tab or Evidence tabs)."""
        if index == 0:
            # Case tab selected
            return

        # Evidence tab selected - refresh counts if needed
        self._refresh_counts()

    def _on_evidence_subtab_changed(self, evidence_tabs: QTabWidget, index: int) -> None:
        """
        Handle evidence sub-tab changes for lazy loading (Phase 3).

        Triggers deferred data loading when a lazy-loaded tab becomes visible.
        """
        widget = evidence_tabs.widget(index)
        if widget is None:
            return

        # Check if the widget has a _perform_deferred_load method (lazy loading support)
        if hasattr(widget, '_perform_deferred_load') and hasattr(widget, '_load_pending'):
            if widget._load_pending and not getattr(widget, '_data_loaded', False):
                self.logger.debug("Triggering deferred load for tab index %d", index)
                QTimer.singleShot(10, widget._perform_deferred_load)

    def _on_close_evidence_tab(self, index: int) -> None:
        """Handle closing an evidence tab."""
        if index == 0:
            # Can't close Case tab
            return

        # Get evidence tab widget before removing
        evidence_tab_widget = self.main_tabs.widget(index)

        # Shutdown workers in the closing evidence tab
        if evidence_tab_widget and hasattr(evidence_tab_widget, 'count'):
            for j in range(evidence_tab_widget.count()):
                tab = evidence_tab_widget.widget(j)
                if hasattr(tab, 'shutdown'):
                    try:
                        tab.shutdown()
                    except Exception as e:
                        self.logger.warning("Error during tab shutdown on evidence close: %s", e)

        # Remove from evidence mapping
        if evidence_tab_widget:
            evidence_id = evidence_tab_widget.property("evidence_id")
            if evidence_id in self._evidence_tabs:
                del self._evidence_tabs[evidence_id]

        self.main_tabs.removeTab(index)

        # Update tab indices in mapping
        for eid, tab_idx in list(self._evidence_tabs.items()):
            if tab_idx > index:
                self._evidence_tabs[eid] = tab_idx - 1

    def _on_rescan_partitions_requested(self, evidence_id: int) -> None:
        """Handle partition rescan request for an evidence."""
        if not self.case_data:
            return

        # Get evidence data
        evidence_data = self.case_data.get_evidence(evidence_id)
        if not evidence_data:
            return

        source_path = Path(evidence_data['source_path'])

        # Check if it's an E01 file
        if source_path.suffix.lower() not in {'.e01', '.e02', '.e03', '.e04', '.e05',
                                               '.e06', '.e07', '.e08', '.e09'}:
            QMessageBox.information(
                self,
                "Not an E01 Image",
                "Partition detection is only available for E01 images."
            )
            return

        try:
            # Find all segments
            ewf_segments = find_ewf_segments(source_path)
            self.logger.info("Discovered %d EWF segment(s) for %s", len(ewf_segments), source_path.name)

            # Show progress dialog
            from PySide6.QtWidgets import QProgressDialog
            progress = QProgressDialog(
                "Re-analyzing E01 image structure...\nThis may take a moment for large images.",
                "Cancel",
                0, 0,
                self
            )
            progress.setWindowTitle("Rescanning Partitions")
            progress.setWindowModality(Qt.WindowModal)
            progress.setMinimumDuration(500)
            progress.setCancelButton(None)
            progress.show()
            QApplication.processEvents()

            # Run partition detection in thread
            from PySide6.QtCore import QThread

            class PartitionDetectionThread(QThread):
                def __init__(self, ewf_segments):
                    super().__init__()
                    self.ewf_segments = ewf_segments
                    self.partitions = None
                    self.error = None

                def run(self):
                    try:
                        self.partitions = list_ewf_partitions(self.ewf_segments)
                    except Exception as e:
                        self.error = e

            detection_thread = PartitionDetectionThread(ewf_segments)
            detection_thread.start()

            # Wait for thread to finish while keeping UI responsive
            while detection_thread.isRunning():
                QApplication.processEvents()
                detection_thread.wait(100)

            progress.close()

            # Check for errors
            if detection_thread.error:
                raise detection_thread.error

            partitions = detection_thread.partitions

            # Update database with new partition info
            import json
            partition_info_json = json.dumps(partitions)
            self.case_data.update_partition_info(evidence_id, partition_info_json)

            # Reload partition data into Disk tab
            evidence_tab_index = self._evidence_tabs.get(evidence_id)
            if evidence_tab_index is not None:
                evidence_tab_widget = self.main_tabs.widget(evidence_tab_index)
                if evidence_tab_widget:
                    updated_evidence = self.case_data.get_evidence(evidence_id)
                    if updated_evidence:
                        self._load_partition_data_into_tab(evidence_tab_widget, updated_evidence)

            self.logger.info("Rescanned partitions for evidence %d: found %d partition(s)",
                           evidence_id, len(partitions))

            QMessageBox.information(
                self,
                "Rescan Complete",
                f"Found {len(partitions)} partition(s) in {source_path.name}",
            )

        except Exception as exc:
            self.logger.exception("Failed to rescan partitions")
            QMessageBox.critical(
                self,
                "Rescan Failed",
                f"Could not rescan partitions: {str(exc)}",
            )

    def _on_partition_rescan_requested(self) -> None:
        """
        Handle partition rescan request from Overview tab's DiskTabWidget.
        """
        # Get current evidence
        current_index = self.main_tabs.currentIndex()
        if current_index < 0:
            return

        evidence_tab_widget = self.main_tabs.widget(current_index)
        if not evidence_tab_widget:
            return

        evidence_id = evidence_tab_widget.property("evidence_id")
        if not evidence_id:
            return

        # Delegate to existing rescan handler
        self._on_rescan_partitions_requested(evidence_id)


    def _on_case_field_changed(self, field_name: str, value: str) -> None:
        """Handle case metadata field changes and save to database."""
        if not self.case_data:
            return

        # Get current case data
        case_data = self.case_info_tab.get_case_data()

        # Update database
        self.case_data.update_case_metadata(
            case_number=case_data.get("case_number"),
            case_name=case_data.get("case_name"),
            investigator=case_data.get("investigator"),
            notes=case_data.get("notes")
        )

        self.logger.info("Updated case field '%s'", field_name)

    def _export_case(self) -> None:
        """Handle Export Case menu action."""
        if not self.case_path or not self.case_data:
            QMessageBox.warning(
                self,
                "No Case Open",
                "Please open a case before exporting."
            )
            return

        # Get case ID for dialog
        case_metadata = self.case_data.get_case_metadata()
        case_id = case_metadata.get("case_id", "Unknown")

        # Show export dialog
        dialog = ExportDialog(self.case_path, case_id, self)
        if dialog.exec() == QDialog.Accepted:
            self.statusBar().showMessage("Case exported successfully", 5000)

    def _import_case(self) -> None:
        """Handle Import Case menu action."""
        # Get cases directory (parent of current case, or base_dir if no case open)
        if self.case_path:
            cases_dir = self.case_path.parent
        else:
            cases_dir = self.base_dir / "images"

        # Show import dialog (dialog now has destination picker)
        dialog = ImportDialog(cases_dir, self)
        if dialog.exec() == QDialog.Accepted:
            # Get import result from dialog
            result = getattr(dialog, 'import_result', None)

            if result and result.success and result.imported_path:
                imported_case_folder = result.imported_path

                # Find the case database in the imported folder
                db_path_found = find_case_database(imported_case_folder)
                if not db_path_found:
                    # Fallback: any sqlite that's not an evidence DB
                    db_files = [
                        f for f in imported_case_folder.glob("*.sqlite")
                        if "evidence" not in f.stem.lower()
                    ]
                    db_path_found = sorted(db_files)[0] if db_files else None

                if db_path_found:
                    reply = QMessageBox.question(
                        self,
                        "Open Imported Case?",
                        f"Case '{result.imported_case_id}' imported successfully to:\n"
                        f"{imported_case_folder}\n\n"
                        f"Would you like to open it now?",
                        QMessageBox.Yes | QMessageBox.No,
                        QMessageBox.Yes
                    )

                    if reply == QMessageBox.Yes:
                        self._open_case(imported_case_folder, db_path_found)
                    else:
                        self.statusBar().showMessage(
                            f"Case imported to: {imported_case_folder}", 10000
                        )
                else:
                    QMessageBox.warning(
                        self,
                        "Import Warning",
                        f"Case imported but no database found in:\n{imported_case_folder}"
                    )
            else:
                # Legacy path - result not stored or incomplete
                self.statusBar().showMessage(
                    f"Case '{dialog.validation_result.manifest.get('case_id')}' imported successfully",
                    5000
                )

    # =========================================================================
    # Case-Wide Extract & Ingest
    # =========================================================================

    def _on_case_wide_extract_requested(self, evidence_ids: List[int]) -> None:
        """Handle case-wide extract & ingest request from Case tab."""
        from .common.dialogs import CaseWideExtractIngestDialog
        from .services.workers import CaseWideExtractAndIngestWorker
        from extractors import ExtractorRegistry

        if not self.case_data or not self.db_manager:
            QMessageBox.warning(self, "No Case", "Please open a case first.")
            return

        # Fetch FULL evidence data for each selected ID (includes partition_index)
        selected_evidences = []
        for eid in evidence_ids:
            ev = self.case_data.get_evidence(eid)
            if ev:
                selected_evidences.append(ev)

        if not selected_evidences:
            QMessageBox.warning(self, "No Selection", "Please select at least one evidence.")
            return

        # Show dialog
        dialog = CaseWideExtractIngestDialog(selected_evidences, self)
        if dialog.exec() != QDialog.Accepted:
            return

        extractor_names = dialog.get_selected_extractors()
        overwrite_mode = dialog.get_overwrite_mode()  # Returns 'overwrite', 'append', or 'skip_existing'
        extractor_configs = dialog.get_extractor_configs()

        if not extractor_names:
            return

        # Compute total phases for progress bar (extract + ingest per extractor)
        registry = ExtractorRegistry()
        phases_per_evidence = 0
        for name in extractor_names:
            ext = registry.get(name)
            if ext and ext.metadata.can_extract:
                phases_per_evidence += 1 + (1 if ext.metadata.can_ingest else 0)
        total_phases = max(1, phases_per_evidence * len(selected_evidences))

        # Create and start worker (orchestrates ExtractAndIngestWorker per evidence)
        self._case_wide_worker = CaseWideExtractAndIngestWorker(
            evidence_ids=evidence_ids,
            extractor_names=extractor_names,
            extractor_configs=extractor_configs,
            case_data=self.case_data,
            case_path=self.case_path,
            db_manager=self.db_manager,
            overwrite_mode=overwrite_mode,
            audit_logger=self.audit_logger,
            parent=self
        )

        # Create progress dialog
        self._case_wide_progress = QProgressDialog(
            "Initializing...",
            "Cancel",
            0, total_phases,
            self
        )
        self._case_wide_progress.setWindowTitle("Case-Wide Extract & Ingest")
        self._case_wide_progress.setWindowModality(Qt.WindowModal)
        self._case_wide_progress.canceled.connect(self._on_case_wide_cancelled)

        # Connect signals
        self._case_wide_worker.progress.connect(self._on_case_wide_progress)
        self._case_wide_worker.log_message.connect(self._on_case_wide_log)
        self._case_wide_worker.batch_finished.connect(self._on_case_wide_finished)

        # Start
        self._case_wide_worker.start()

    def _on_case_wide_progress(self, current: int, total: int, message: str) -> None:
        """Handle progress update from case-wide worker."""
        if hasattr(self, '_case_wide_progress') and self._case_wide_progress:
            self._case_wide_progress.setValue(current)
            self._case_wide_progress.setLabelText(message)

    def _on_case_wide_log(self, evidence_id: int, message: str) -> None:
        """Route log message to appropriate evidence's log tab."""
        self.logger.info("[Evidence %d] %s", evidence_id, message)
        # Note: Could route to specific evidence tab's log widget if needed

    def _on_case_wide_cancelled(self) -> None:
        """Handle cancel request for case-wide operation."""
        if hasattr(self, '_case_wide_worker') and self._case_wide_worker:
            self._case_wide_worker.cancel()

    def _on_case_wide_finished(self, results: dict) -> None:
        """Handle completion of case-wide operation."""
        # Close progress dialog
        if hasattr(self, '_case_wide_progress') and self._case_wide_progress:
            self._case_wide_progress.close()
            self._case_wide_progress = None
        self._case_wide_worker = None

        # Show summary
        succeeded = len(results.get('succeeded', []))
        failed = len(results.get('failed', []))
        skipped = len(results.get('skipped', []))

        msg = f"Case-wide processing complete.\n\n"
        msg += f"✅ Succeeded: {succeeded} evidence(s)\n"
        msg += f"❌ Failed: {failed} evidence(s)\n"
        msg += f"⏭️ Skipped: {skipped} evidence(s)"

        if failed > 0:
            msg += "\n\nCheck individual evidence logs for details."
            QMessageBox.warning(self, "Completed with Errors", msg)
        else:
            QMessageBox.information(self, "Complete", msg)

        # Refresh all evidence tabs to show new data
        for i in range(1, self.main_tabs.count()):  # Skip Case tab at index 0
            evidence_tab_widget = self.main_tabs.widget(i)
            if evidence_tab_widget:
                # Refresh extraction tab status
                extraction_tab = evidence_tab_widget.property("extraction_tab")
                if extraction_tab and hasattr(extraction_tab, '_refresh_ui'):
                    extraction_tab._refresh_ui()



def main() -> int:
    if getattr(sys, 'frozen', False):
        # Running in a PyInstaller bundle
        base_dir = Path(sys._MEIPASS)
    else:
        # Running from source
        base_dir = Path(__file__).resolve().parents[2]

    app = QApplication(sys.argv)
    window = MainWindow(base_dir)
    window.show()
    return app.exec()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
