"""
Extractors Tab Widget MODULAR
Modular extractor architecture with registry-based UI generation.

Each extractor is a self-contained module with its own:
- Configuration UI
- Execution logic
- Ingestion logic
- Status reporting

Dialogs and workers extracted to separate modules.
"""

from __future__ import annotations

import shutil
import sys
from typing import Dict, Optional, TYPE_CHECKING
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGroupBox,
    QPushButton,
    QLabel,
    QMessageBox,
    QScrollArea,
    QProgressDialog,
    QFrame,
    QFileDialog,
    QSplitter,
    QTextEdit,
    QDialog,
    QDialogButtonBox,
)
from PySide6.QtGui import QFont

from app.data.case_data import CaseDataAccess
from app.common.widgets import ExtractorRunStatusWidget, CollapsibleSection
from extractors import ExtractorRegistry
from extractors.workers import ExtractionWorker, IngestionWorker
from core.database import slugify_label
from core.logging import get_logger
from core.process_log_service import get_extractor_run_status
from core.extractor_sections import (
    EXTRACTOR_SECTIONS,
    group_extractors_by_section,
    get_section_by_name,
)

# Dialogs and workers extracted to separate modules
from app.features.extraction.dialogs import PurgeDataDialog, ExtractAndIngestDialog
from app.services.workers import ExtractAndIngestWorker

if TYPE_CHECKING:
    from extractors.base import BaseExtractor
    from core.audit_logging import AuditLogger, EvidenceLogger

LOGGER = get_logger(__name__)

class ExtractionTab(QWidget):
    """
    Extractors Tab MODULAR

    Dynamic UI generated from ExtractorRegistry.
    Each extractor has individual run/ingest buttons.
    No pipeline orchestration - each extractor is independent.
    """

    # Signals (kept for backward compatibility with main window)
    extraction_started = Signal(str)  # extractor_name
    extraction_finished = Signal(str, bool, str)  # extractor_name, success, message
    log_message = Signal(int, str)  # evidence_id, message (per-evidence logs)
    data_changed = Signal()  # Emitted when data is ingested - other tabs should refresh

    def __init__(
        self,
        evidence_id: int,
        case_data: Optional[CaseDataAccess] = None,
        rules_dir: Optional[Path] = None,
        tool_registry = None,
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.evidence_id = evidence_id
        self.case_data = case_data
        if rules_dir:
            self.rules_dir = rules_dir
        elif getattr(sys, "frozen", False):
            self.rules_dir = Path(getattr(sys, "_MEIPASS", ".")) / "rules"
        else:
            self.rules_dir = Path(__file__).resolve().parents[3] / "rules"
        self.tool_registry = tool_registry

        # Modular architecture
        self.registry = ExtractorRegistry()
        self._extractors: Dict[str, BaseExtractor] = {}
        self._active_workers: Dict[str, ExtractionWorker | IngestionWorker] = {}

        # Current context (set via set_current_case)
        self.current_case = None
        self.current_evidence = None
        self.evidence_fs = None
        self.db_manager = None  # DatabaseManager for creating thread-local connections
        self.audit_logger: Optional["AuditLogger"] = None  # For persistent logging
        self.evidence_logger: Optional["EvidenceLogger"] = None  # Evidence-specific logging

        # Run status widget tracking (keyed by extractor name)
        self._run_status_widgets: Dict[str, ExtractorRunStatusWidget] = {}

        self._setup_ui()

    def cancel_active_extraction(self):
        """Cancel any active extraction tasks. Called during application shutdown."""
        for worker in self._active_workers.values():
            if hasattr(worker, 'cancel'):
                worker.cancel()

        for worker in list(self._active_workers.values()):
            worker.wait(1000)  # Wait up to 1 second

    def shutdown(self) -> None:
        """
        Gracefully stop all background workers before widget destruction.

        Called by MainWindow.closeEvent() and _on_close_evidence_tab() to prevent
        Qt abort from destroying QThread while still running.
        """
        # First cancel any active extraction/ingestion workers
        self.cancel_active_extraction()

        # Stop Run All (extract and ingest) worker if running
        if hasattr(self, '_run_all_worker') and self._run_all_worker is not None:
            if hasattr(self._run_all_worker, 'cancel'):
                self._run_all_worker.cancel()
            if self._run_all_worker.isRunning():
                self._run_all_worker.requestInterruption()
                self._run_all_worker.quit()
                if not self._run_all_worker.wait(2000):
                    LOGGER.warning("ExtractAndIngestWorker did not stop in 2s, terminating")
                    self._run_all_worker.terminate()
                    self._run_all_worker.wait(500)
            self._run_all_worker = None

        # Close any progress dialogs
        for dialog_attr in ['_batch_progress', '_run_all_progress']:
            if hasattr(self, dialog_attr):
                dialog = getattr(self, dialog_attr)
                if dialog is not None:
                    try:
                        dialog.close()
                    except RuntimeError:
                        pass
                    setattr(self, dialog_attr, None)

        LOGGER.debug("ExtractionTab shutdown complete")

    def _setup_ui(self):
        """Build the UI dynamically from registry."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Scroll area for small windows
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QScrollArea.NoFrame)

        # Container
        container = QWidget()
        container_layout = QVBoxLayout(container)

        # Header
        header = self._create_header()
        container_layout.addWidget(header)

        # Create sections from registry
        self._create_extractor_sections(container_layout)

        container_layout.addStretch()

        scroll_area.setWidget(container)
        layout.addWidget(scroll_area)

    def _create_header(self) -> QWidget:
        """Create header with info about modular architecture."""
        header = QFrame()
        header.setFrameShape(QFrame.StyledPanel)
        layout = QVBoxLayout(header)

        # Title row with batch button
        title_row = QHBoxLayout()
        title = QLabel("<h3>🔧 Modular Extractors</h3>")
        title_row.addWidget(title)
        title_row.addStretch()

        # Run All button (extract + ingest combined)
        self.run_all_btn = QPushButton("▶️ Run All")
        self.run_all_btn.setToolTip("Extract from evidence and ingest to database in one step")
        self.run_all_btn.setMinimumWidth(160)
        self.run_all_btn.clicked.connect(self._show_run_all_dialog)
        title_row.addWidget(self.run_all_btn)

        # Purge Data button
        self.purge_btn = QPushButton("🗑️ Purge Data")
        self.purge_btn.setToolTip("Delete all ingested data (preserves extraction outputs)")
        self.purge_btn.setMinimumWidth(150)
        self.purge_btn.setStyleSheet("QPushButton { border: 2px solid #d32f2f; color: #d32f2f; } QPushButton:hover { background-color: #ffebee; }")
        self.purge_btn.clicked.connect(self._show_purge_dialog)
        title_row.addWidget(self.purge_btn)

        layout.addLayout(title_row)

        info = QLabel(
            "Each extractor is independent with its own configuration and execution. "
            "Click <b>⚙️ Configure</b> to set options, <b>▶️ Run Extraction</b> to extract from evidence, "
            "or <b>📥 Ingest Results</b> to load existing output files into the database. "
            "Use <b>▶️ Run All</b> on section headers to batch extract and ingest all extractors in a section."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: gray; font-size: 10pt;")
        layout.addWidget(info)

        return header

    def _create_extractor_sections(self, parent_layout: QVBoxLayout):
        """Create UI sections dynamically from registry using EXTRACTOR_SECTIONS."""
        # Group extractors by section
        grouped = group_extractors_by_section(self.registry.get_all())

        # Track section widgets for later updates
        self._section_widgets = {}

        # Create sections in defined order
        for section_def in EXTRACTOR_SECTIONS:
            section_name = section_def["name"]
            section_extractors = grouped.get(section_name, [])

            # Create collapsible section
            section_widget = CollapsibleSection(
                title=section_name,
                icon=section_def["icon"],
                count=len(section_extractors),
                collapsed=section_def["collapsed"],
                show_run_all=True,
                parent=self,
            )

            # Connect Run All signal
            section_widget.run_all_clicked.connect(
                lambda sn=section_name: self._on_run_all_section(sn)
            )

            # Sort extractors within section by defined order
            section_extractor_names = section_def["extractors"]

            def get_sort_index(extractor):
                name = extractor.metadata.name
                if name in section_extractor_names:
                    return section_extractor_names.index(name)
                return 999

            sorted_extractors = sorted(section_extractors, key=get_sort_index)

            # Store reference for later (use sorted list for Run All)
            self._section_widgets[section_name] = {
                "widget": section_widget,
                "extractors": sorted_extractors,
            }

            # Add extractor widgets to section
            if sorted_extractors:
                for extractor in sorted_extractors:
                    extractor_widget = self._create_extractor_widget(extractor)
                    section_widget.add_widget(extractor_widget)
            else:
                # Empty section - show grayed message
                section_widget.set_empty_state(True, "No extractors available in this section")

            parent_layout.addWidget(section_widget)

    def _on_run_all_section(self, section_name: str):
        """
        Handle Run All button click for a section.

        Shows confirmation dialog, then runs extract + ingest for all
        extractors in the section using the existing batch workflow.
        """
        # Validation
        if not self.current_case or not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        # Get extractors for this section
        section_info = self._section_widgets.get(section_name)
        if not section_info:
            return

        extractors = section_info.get("extractors", [])
        if not extractors:
            QMessageBox.information(
                self,
                "No Extractors",
                f"No extractors available in {section_name}."
            )
            return

        # Filter to extractors that can extract
        runnable = [e for e in extractors if e.metadata.can_extract]
        if not runnable:
            QMessageBox.information(
                self,
                "No Runnable Extractors",
                f"No extractors in {section_name} can run extraction."
            )
            return

        # Show confirmation dialog
        section_def = get_section_by_name(section_name)
        icon = section_def["icon"] if section_def else ""

        extractor_names = [e.metadata.display_name for e in runnable]

        # Build message
        if len(extractor_names) <= 5:
            names_text = "\n".join(f"  • {name}" for name in extractor_names)
        else:
            names_text = "\n".join(f"  • {name}" for name in extractor_names[:5])
            names_text += f"\n  ... ({len(extractor_names) - 5} more)"

        msg = QMessageBox(self)
        msg.setWindowTitle(f"Run All: {icon} {section_name}")
        msg.setIcon(QMessageBox.Question)
        msg.setText(f"Extract and ingest all extractors in <b>{section_name}</b>?")
        msg.setInformativeText(
            f"This will run:\n{names_text}\n\n"
            f"Using current extractor configurations.\n"
            f"Total: {len(runnable)} extractor(s)"
        )
        msg.setStandardButtons(QMessageBox.Cancel | QMessageBox.Ok)
        msg.button(QMessageBox.Ok).setText("Run All")

        if msg.exec() != QMessageBox.Ok:
            return

        # Run using existing extract and ingest workflow
        self._run_all(runnable, 'overwrite')

    def _show_run_all_dialog(self):
        """Show the Run All dialog and run selected extractors."""
        # Validation
        if not self.current_case or not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        # Get all extractors (evidence_fs will be auto-mounted if needed by batch extraction)
        extractors = self.registry.get_all()

        if not extractors:
            QMessageBox.warning(
                self,
                "No Extractors",
                "No extractors are available."
            )
            return

        # Show dialog
        dialog = ExtractAndIngestDialog(extractors, self)
        if dialog.exec() != QDialog.Accepted:
            return

        selected = dialog.get_selected_extractors()
        mode = dialog.get_selected_mode()

        if not selected:
            return

        # Run extract & ingest
        self._run_all(selected, mode)

    def _run_all(self, extractors: list, mode: str):
        """Run extraction + ingestion for multiple extractors in sequence."""
        import inspect

        total = len(extractors)

        # Mount evidence filesystem once (if any extractor needs it)
        needs_fs = any(
            'evidence_fs' in inspect.signature(e.run_extraction).parameters
            for e in extractors
        )

        if needs_fs and self.evidence_fs is None:
            self.log_message.emit(self.evidence_id, "Mounting evidence filesystem for Run All...")
            self.evidence_fs = self.mount_evidence_filesystem()
            if self.evidence_fs is None:
                # Some extractors may still work without FS
                self.log_message.emit(
                    self.evidence_id,
                    "⚠️ Could not mount filesystem - some extractors may be skipped"
                )

        # Get evidence info
        evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
        evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)
        evidence_source_path = self.current_evidence.get("source_path") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "source_path", None)
        if evidence_source_path and isinstance(evidence_source_path, str):
            evidence_source_path = Path(evidence_source_path)

        # Calculate total phases dynamically: 1 for extract, +1 if can_ingest
        total_phases = sum(1 + (1 if e.metadata.can_ingest else 0) for e in extractors)

        # Create progress dialog
        self._run_all_progress = QProgressDialog(
            f"Run All: 0/{total} complete",
            "Cancel All",
            0, total_phases,  # Actual number of phases
            self
        )
        self._run_all_progress.setWindowModality(Qt.WindowModal)
        self._run_all_progress.setWindowTitle("Run All")
        self._run_all_progress.setMinimumDuration(0)
        self._run_all_progress.setMinimumWidth(500)

        # Create and start worker
        self._run_all_worker = ExtractAndIngestWorker(
            extractors=extractors,
            evidence_fs=self.evidence_fs,
            evidence_source_path=evidence_source_path,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
            workspace_dir=Path(self.current_case.workspace_dir),
            db_manager=self.db_manager,
            overwrite_mode=mode,
            evidence_logger=self.evidence_logger,
            parent=self
        )

        # Track state
        self._run_all_succeeded = []
        self._run_all_skipped = []
        self._run_all_failed = []
        self._run_all_total = total
        self._run_all_current_step = 0
        # Track cumulative phase index for progress bar
        self._run_all_phase_index = {}  # meta.name -> cumulative_phase_start
        self._run_all_name_to_display = {}  # name -> display_name for reverse lookup

        # Build phase index map (cumulative phase positions)
        # Use meta.name (unique) not display_name (can have duplicates)
        phase_idx = 0
        for ext in extractors:
            self._run_all_phase_index[ext.metadata.name] = phase_idx
            self._run_all_name_to_display[ext.metadata.name] = ext.metadata.display_name
            phase_idx += 1  # Extract phase
            if ext.metadata.can_ingest:
                phase_idx += 1  # Ingest phase

        # Connect signals
        self._run_all_worker.extractor_started.connect(self._on_run_all_extractor_started)
        self._run_all_worker.extractor_finished.connect(self._on_run_all_extractor_finished)
        self._run_all_worker.batch_finished.connect(self._on_run_all_finished)
        self._run_all_worker.log_message.connect(lambda msg: self.log_message.emit(self.evidence_id, msg))

        # Handle cancel button
        self._run_all_progress.canceled.connect(self._run_all_worker.cancel)

        # Start worker
        self._run_all_worker.start()

    def _on_run_all_extractor_started(self, index: int, extractor_name: str, phase: str):
        """Handle Run All start signal.

        Args:
            index: Extractor index in batch
            extractor_name: meta.name (unique identifier, not display_name)
            phase: 'extract' or 'ingest'
        """
        phase_label = "Extracting" if phase == "extract" else "Ingesting"

        # Get cumulative phase position from map (extractor_name is meta.name)
        base_phase = self._run_all_phase_index.get(extractor_name, 0)
        # Add 1 if this is ingestion phase (base + extract=0, ingest=1)
        step = base_phase if phase == "extract" else base_phase + 1

        # Get display name for UI (reverse lookup)
        display_name = self._run_all_name_to_display.get(extractor_name, extractor_name)

        self._run_all_progress.setLabelText(
            f"{phase_label} {display_name}...\n"
            f"Phase {step + 1}/{self._run_all_progress.maximum()}\n"
            f"✅ Succeeded: {len(self._run_all_succeeded)} | "
            f"⚠️ Skipped: {len(self._run_all_skipped)} | "
            f"❌ Failed: {len(self._run_all_failed)}"
        )

        self._run_all_progress.setValue(step)
        self._run_all_current_step = step

    def _on_run_all_extractor_finished(self, index: int, extractor_name: str, phase: str, success: bool, message: str):
        """Handle Run All finish signal.

        Args:
            index: Extractor index in batch
            extractor_name: meta.name (unique identifier)
            phase: 'extract' or 'ingest'
            success: True if phase succeeded
            message: Error message if failed
        """
        # Get display name for result tracking
        display_name = self._run_all_name_to_display.get(extractor_name, extractor_name)

        # Track results only after ingestion phase or if extraction skipped/failed
        if phase == "ingest" or (phase == "extract" and not success):
            if success:
                if display_name not in self._run_all_succeeded:
                    self._run_all_succeeded.append(display_name)
            elif message and ("skipped" in message.lower() or "already exists" in message.lower() or "No output" in message):
                if display_name not in [name for name, _ in self._run_all_skipped]:
                    self._run_all_skipped.append((display_name, message))
            else:
                if display_name not in [name for name, _ in self._run_all_failed]:
                    self._run_all_failed.append((display_name, message))

        # Update progress
        self._run_all_progress.setValue(self._run_all_current_step + 1)

    def _on_run_all_finished(self, succeeded: list, skipped: list, failed: list, cancelled: bool):
        """Handle Run All completion signal."""
        self._run_all_progress.close()

        # Clean up worker
        self._run_all_worker.wait()
        self._run_all_worker.deleteLater()
        self._run_all_worker = None

        # Show summary
        self._show_run_all_summary(succeeded, skipped, failed, cancelled)

        # Emit data changed and refresh UI if any succeeded
        if succeeded:
            self.data_changed.emit()
            # Refresh extractor cards to show updated extraction/ingestion dates
            self._refresh_ui()

    def _show_run_all_summary(self, succeeded: list, skipped: list, failed: list, cancelled: bool):
        """Show summary dialog after Run All."""
        msg = QMessageBox(self)
        msg.setWindowTitle("Run All Complete")

        if cancelled:
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Run All was cancelled.")
        elif failed:
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Run All completed with some failures.")
        else:
            msg.setIcon(QMessageBox.Information)
            msg.setText("Run All completed successfully.")

        # Build details
        details = []

        if succeeded:
            details.append(f"✅ Succeeded ({len(succeeded)}):")
            for name in succeeded:
                details.append(f"   • {name}")
            details.append("")

        if skipped:
            details.append(f"⚠️ Skipped ({len(skipped)}):")
            for name, reason in skipped:
                details.append(f"   • {name}: {reason}")
            details.append("")

        if failed:
            details.append(f"❌ Failed ({len(failed)}):")
            for name, reason in failed:
                details.append(f"   • {name}: {reason}")

        msg.setInformativeText(
            f"Succeeded: {len(succeeded)} | Skipped: {len(skipped)} | Failed: {len(failed)}"
        )
        msg.setDetailedText("\n".join(details))
        msg.exec()

    def _show_purge_dialog(self):
        """Show purge data dialog."""
        if not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Evidence",
                "Please select an evidence before purging data."
            )
            return

        # Extract evidence details from current_evidence (dict or object)
        evidence_id = (
            self.current_evidence.get("id")
            if isinstance(self.current_evidence, dict)
            else self.current_evidence.id
        )
        evidence_label = (
            self.current_evidence.get("label")
            if isinstance(self.current_evidence, dict)
            else getattr(self.current_evidence, "label", None)
        )

        dialog = PurgeDataDialog(
            evidence_id,
            evidence_label,
            self.db_manager,
            self.audit_logger,
            self
        )

        if dialog.exec() == QDialog.Accepted and dialog.confirmed:
            # Refresh all tabs to reflect purged data
            self.data_changed.emit()
            QMessageBox.information(
                self,
                "Purge Complete",
                "All ingested data has been purged successfully.\n"
                "All tabs have been refreshed to reflect the purged data."
            )

    def _create_extractor_widget(self, extractor) -> QWidget:
        """Create widget for individual extractor."""
        # extractor is already an instance
        meta = extractor.metadata

        # Store reference
        self._extractors[meta.name] = extractor

        # Container
        widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(5, 10, 5, 10)

        # === Header Row: Name + Buttons ===
        header_layout = QHBoxLayout()

        # Name + description
        info_layout = QVBoxLayout()

        name_label = QLabel(f"<b>{meta.display_name}</b>")
        name_font = QFont()
        name_font.setPointSize(11)
        name_label.setFont(name_font)
        info_layout.addWidget(name_label)

        desc_label = QLabel(meta.description)
        desc_label.setStyleSheet("color: gray; font-size: 9pt;")
        desc_label.setWordWrap(True)
        info_layout.addWidget(desc_label)

        header_layout.addLayout(info_layout, 1)

        # Buttons
        btn_layout = QHBoxLayout()

        # Configure button (always present)
        config_btn = QPushButton("⚙️ Configure")
        config_btn.setMaximumWidth(140)
        config_btn.clicked.connect(lambda: self._show_config_dialog(extractor))
        btn_layout.addWidget(config_btn)

        # Import button (for extractors that import external data)
        if meta.name == "bulk_extractor":
            import_btn = QPushButton("📂 Import Data")
            import_btn.setMaximumWidth(140)
            import_btn.clicked.connect(lambda: self._import_bulk_extractor_data(extractor))
            btn_layout.addWidget(import_btn)
        elif meta.name in ("file_list", "file_list_importer"):
            import_btn = QPushButton("📂 Import")
            import_btn.setMaximumWidth(140)
            import_btn.clicked.connect(lambda: self._import_file_list(extractor))
            btn_layout.addWidget(import_btn)
        elif meta.name == "image_carving":
            import_btn = QPushButton("📂 Import Carved")
            import_btn.setMaximumWidth(140)
            import_btn.clicked.connect(lambda: self._import_carved_images(extractor))
            btn_layout.addWidget(import_btn)

        # Run Extraction button (if can_extract)
        if meta.can_extract:
            run_btn = QPushButton("▶️ Run Extraction")
            run_btn.setMaximumWidth(140)
            run_btn.clicked.connect(lambda: self._run_extraction(extractor))
            btn_layout.addWidget(run_btn)

        # Ingest Results button (if can_ingest)
        # Skip for file_list/file_list_importer - they write directly to DB during extraction
        if meta.can_ingest and meta.name not in ("file_list", "file_list_importer"):
            ingest_btn = QPushButton("📥 Ingest Results")
            ingest_btn.setMaximumWidth(140)
            ingest_btn.clicked.connect(lambda: self._run_ingestion(extractor))
            btn_layout.addWidget(ingest_btn)

        header_layout.addLayout(btn_layout)
        main_layout.addLayout(header_layout)

        # === Run Status Row (process_log based) ===
        # Always create and add - will be updated when set_current_case is called
        run_status_widget = self._create_run_status_widget(meta.name)
        main_layout.addWidget(run_status_widget)

        # === Status Row ===
        if self.current_case and self.current_evidence and self.evidence_conn:
            try:
                # Get evidence label and slugify for filesystem-safe folder name
                evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
                evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)
                evidence_slug = slugify_label(evidence_label, evidence_id)

                output_dir = extractor.get_output_dir(
                    Path(self.current_case.workspace_dir),
                    evidence_slug
                )
                status_widget = extractor.get_status_widget(
                    self,
                    output_dir,
                    self.evidence_conn,
                    self.evidence_id
                )
                if status_widget:
                    main_layout.addWidget(status_widget)
            except Exception as e:
                # Status widget optional - don't fail if it errors
                pass

        # === Capabilities Info ===
        caps = []
        if meta.can_extract:
            caps.append("✓ Can extract from evidence")
        if meta.can_ingest:
            caps.append("✓ Can ingest external results")

        if caps:
            caps_label = QLabel(" | ".join(caps))
            caps_label.setStyleSheet("color: #28a745; font-size: 8pt;")
            main_layout.addWidget(caps_label)

        # === Separator ===
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        main_layout.addWidget(sep)

        widget.setLayout(main_layout)
        return widget

    def _create_run_status_widget(self, extractor_name: str) -> ExtractorRunStatusWidget:
        """
        Create run status widget for extractor showing last successful runs.

        Always creates a widget and registers it for later updates when
        case context becomes available. The widget starts in "Not run yet"
        state and is updated via _refresh_ui() when set_current_case() is called.

        Args:
            extractor_name: Name of the extractor (e.g., 'browser_history').

        Returns:
            ExtractorRunStatusWidget (always returns a widget, never None).

        Note:
            Resolves DB path based on db_manager.enable_split flag:
            - If True: uses evidence-local database
            - If False: falls back to case database
        """
        # Get current status if context is available
        status = self._get_run_status_for_extractor(extractor_name)

        # Create widget (may show "Not run yet" if no context)
        widget = ExtractorRunStatusWidget(
            extraction_info=status.get("extraction") if status else None,
            ingestion_info=status.get("ingestion") if status else None,
            parent=self
        )

        # Register for future updates
        self._run_status_widgets[extractor_name] = widget

        return widget

    def _show_config_dialog(self, extractor: BaseExtractor):
        """Show configuration dialog for extractor."""
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

        layout = QVBoxLayout()

        # Info label
        info = QLabel(
            f"<p>Configure settings for <b>{extractor.metadata.display_name}</b>.</p>"
            "<p>Settings are saved when you click OK.</p>"
        )
        info.setWordWrap(True)
        info.setStyleSheet("background: #e7f3ff; padding: 10px; margin-bottom: 10px;")
        layout.addWidget(info)

        # The actual config widget
        layout.addWidget(config_widget)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)

        dialog.setLayout(layout)

        if dialog.exec() == QDialog.Accepted:
            # Save config to extractor instance
            if hasattr(config_widget, 'get_config'):
                config = config_widget.get_config()
                # Store config in extractor (add _config attribute)
                extractor._config = config

            # Also call save_config if widget has it
            if hasattr(config_widget, 'save_config'):
                config_widget.save_config()

            self.log_message.emit(self.evidence_id, f"Configuration updated for {extractor.metadata.display_name}")

    def _run_extraction(self, extractor: BaseExtractor):
        """Run extraction in background worker."""
        import logging
        import inspect
        logger = logging.getLogger(__name__)
        logger.info(f"_run_extraction called for {extractor.metadata.name}")

        # Validation
        if not self.current_case or not self.current_evidence:
            logger.warning("No case or evidence - showing warning dialog")
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        # Determine if extractor needs filesystem access by checking run_extraction signature
        # - evidence_conn: Database-based extractors (file_list_importer) - no FS needed
        # - evidence_source_path: Path-based extractors (bulk_extractor) - no FS mount needed
        # - evidence_fs: Filesystem-based extractors (browser_history, cache, etc.) - needs FS
        run_sig = inspect.signature(extractor.run_extraction)
        run_params = list(run_sig.parameters.keys())
        needs_filesystem = 'evidence_fs' in run_params

        logger.info(f"Extractor {extractor.metadata.name} run_extraction params: {run_params}, needs_filesystem={needs_filesystem}")

        # Mount evidence filesystem on-demand only if extractor needs it
        evidence_fs = self.evidence_fs
        if needs_filesystem and evidence_fs is None:
            logger.info("Evidence filesystem not mounted, mounting on-demand")
            self.log_message.emit(self.evidence_id, "Mounting evidence filesystem...")
            evidence_fs = self.mount_evidence_filesystem()
            if evidence_fs is None:
                logger.error("Failed to mount evidence filesystem")
                QMessageBox.critical(
                    self,
                    "Mount Failed",
                    "Failed to mount evidence filesystem.\n\nCannot run extraction."
                )
                return
            # Store for future use
            self.evidence_fs = evidence_fs
            logger.info(f"Evidence filesystem mounted: {type(evidence_fs).__name__}")
            self.log_message.emit(self.evidence_id, f"Evidence filesystem mounted ({type(evidence_fs).__name__})")

        # Determine what parameter the extractor's can_run_extraction expects
        sig = inspect.signature(extractor.can_run_extraction)
        params = list(sig.parameters.keys())

        # Get evidence source path
        evidence_source_path = self.current_evidence.get("source_path") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "source_path", None)
        if evidence_source_path and isinstance(evidence_source_path, str):
            evidence_source_path = Path(evidence_source_path)

        # Call can_run_extraction with appropriate parameter
        if 'evidence_source_path' in params:
            logger.info(f"Checking if {extractor.metadata.name} can run (evidence_source_path={evidence_source_path})")
            can_run, reason = extractor.can_run_extraction(evidence_source_path)
        else:
            # Default: evidence_fs (for modular extractors)
            logger.info(f"Checking if {extractor.metadata.name} can run (evidence_fs={evidence_fs})")
            can_run, reason = extractor.can_run_extraction(evidence_fs)

        if not can_run:
            logger.warning(f"Cannot run {extractor.metadata.name}: {reason}")
            QMessageBox.warning(
                self,
                "Cannot Run Extraction",
                f"{extractor.metadata.display_name} cannot run:\n\n{reason}"
            )
            return

        # Get config from extractor instance (saved by Configure dialog)
        config = getattr(extractor, '_config', {})

        # Get output directory
        # current_evidence is a dict, not an object
        evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
        evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)
        evidence_source_path = self.current_evidence.get("source_path") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "source_path", None)

        # Convert source_path to Path if it's a string
        if evidence_source_path and isinstance(evidence_source_path, str):
            evidence_source_path = Path(evidence_source_path)

        # Slugify evidence label for filesystem-safe folder name
        evidence_slug = slugify_label(evidence_label, evidence_id)

        output_dir = extractor.get_output_dir(
            Path(self.current_case.workspace_dir),
            evidence_slug
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        # Check for existing output files
        if output_dir.exists():
            # Check if extractor has existing output
            has_output = False
            try:
                has_output = extractor.has_existing_output(output_dir)
            except Exception as e:
                logger.warning(f"Error checking existing output: {e}")

            if has_output:
                # Ask user what to do
                reply = QMessageBox.question(
                    self,
                    "Output Files Exist",
                    f"{extractor.metadata.display_name} has already been run.\n\n"
                    f"Output directory: {output_dir.name}\n\n"
                    "Do you want to overwrite the existing output files?\n\n"
                    "• Yes: Delete existing output and re-run extraction\n"
                    "• No: Cancel extraction",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No  # Default to No for safety
                )

                if reply == QMessageBox.No:
                    logger.info(f"User cancelled extraction (existing output)")
                    return

                # User chose Yes - delete existing output
                try:
                    import shutil
                    logger.info(f"Deleting existing output directory: {output_dir}")
                    self.log_message.emit(self.evidence_id, f"Deleting existing output: {output_dir.name}")
                    shutil.rmtree(output_dir)
                    output_dir.mkdir(parents=True, exist_ok=True)
                    self.log_message.emit(self.evidence_id, f"Existing output deleted, starting fresh extraction")

                    # Force overwrite mode in config (in case deletion didn't fully work)
                    config = dict(config)  # Make a copy
                    config['output_reuse_policy'] = 'overwrite'

                except Exception as e:
                    logger.error(f"Failed to delete existing output: {e}")
                    QMessageBox.critical(
                        self,
                        "Error",
                        f"Failed to delete existing output:\n\n{e}\n\nCannot continue."
                    )
                    return

        # Create progress dialog
        progress = QProgressDialog(
            f"Running {extractor.metadata.display_name}...\n\nInitializing...",
            "Cancel",
            0, 0,  # Indeterminate
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.show()

        # Get evidence logger for persistent logging
        evidence_logger = None
        if self.audit_logger and self.db_manager:
            try:
                # Ensure evidence database exists with migrations (creates process_log table)
                _ = self.db_manager.get_evidence_conn(evidence_id, evidence_label)
                evidence_db_path = self.db_manager.evidence_db_path(evidence_id, evidence_label)
                evidence_logger = self.audit_logger.get_evidence_logger(evidence_id, evidence_db_path)
            except Exception as e:
                LOGGER.warning(f"Could not create evidence logger: {e}")

        # Create worker
        worker = ExtractionWorker(
            extractor,
            evidence_fs,
            output_dir,
            config,
            db_manager=self.db_manager,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
            evidence_source_path=evidence_source_path,
            evidence_logger=evidence_logger,
            parent=self
        )

        # Connect signals
        worker.callbacks.progress.connect(
            lambda cur, tot, msg: self._on_extraction_progress(progress, cur, tot, msg)
        )
        worker.callbacks.log_message.connect(
            lambda msg, level: self._on_extraction_log(progress, extractor, msg, level)
        )
        worker.callbacks.step.connect(
            lambda step: progress.setLabelText(f"{extractor.metadata.display_name}\n\n{step}")
        )
        worker.finished.connect(
            lambda success: self._on_extraction_finished(extractor, progress, worker, success)
        )
        worker.error.connect(
            lambda err: self._on_extraction_error(extractor, progress, worker, err)
        )

        # Handle cancel
        progress.canceled.connect(worker.cancel)

        # Store and start
        self._active_workers[extractor.metadata.name] = worker

        self.extraction_started.emit(extractor.metadata.display_name)
        self.log_message.emit(self.evidence_id, f"Starting extraction: {extractor.metadata.display_name}")

        worker.start()

    def _run_ingestion(self, extractor: BaseExtractor):
        """Run ingestion in background worker."""
        # Validation
        if not self.current_case or not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        if not self.db_manager:
            QMessageBox.warning(
                self,
                "No Database Manager",
                "Database manager not available. Cannot run ingestion."
            )
            return

        # Get output directory
        # current_evidence is a dict, not an object
        evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
        evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)

        # Slugify evidence label for filesystem-safe folder name
        evidence_slug = slugify_label(evidence_label, evidence_id)

        output_dir = extractor.get_output_dir(
            Path(self.current_case.workspace_dir),
            evidence_slug
        )

        # Check if can run
        can_run, reason = extractor.can_run_ingestion(output_dir)
        if not can_run:
            QMessageBox.warning(
                self,
                "Cannot Run Ingestion",
                f"{extractor.metadata.display_name} cannot ingest:\n\n{reason}"
            )
            return

        # Get config from extractor instance (saved by Configure dialog)
        config = getattr(extractor, '_config', {})

        # Check if data already exists (if extractor supports it)
        if hasattr(extractor, '_check_existing_data'):
            evidence_conn = self.db_manager.get_evidence_conn(evidence_id, evidence_label)
            try:
                artifact_types = config.get("artifact_types", ["url"])
                existing_counts = extractor._check_existing_data(
                    evidence_conn,
                    evidence_id,
                    artifact_types
                )
                total_existing = sum(existing_counts.values())

                if total_existing > 0:
                    # Data exists - ask user what to do
                    msg = QMessageBox(self)
                    msg.setIcon(QMessageBox.Question)
                    msg.setWindowTitle("Data Already Exists")
                    msg.setText(
                        f"Found {total_existing:,} existing {extractor.metadata.display_name} artifacts.\n\n"
                        "What would you like to do?"
                    )
                    msg.setInformativeText(
                        "• Overwrite: Delete existing data and import fresh\n"
                        "• Append: Keep existing and add new data\n"
                        "• Cancel: Don't import anything"
                    )

                    overwrite_btn = msg.addButton("Overwrite", QMessageBox.DestructiveRole)
                    append_btn = msg.addButton("Append", QMessageBox.AcceptRole)
                    cancel_btn = msg.addButton("Cancel", QMessageBox.RejectRole)

                    msg.exec()

                    if msg.clickedButton() == cancel_btn:
                        return
                    elif msg.clickedButton() == overwrite_btn:
                        config['overwrite_mode'] = 'overwrite'
                    else:  # append_btn
                        config['overwrite_mode'] = 'append'
                else:
                    # No existing data - default to append mode
                    config['overwrite_mode'] = 'append'
            finally:
                evidence_conn.close()

        # Create progress dialog
        progress = QProgressDialog(
            f"Ingesting {extractor.metadata.display_name}...\n\nInitializing...",
            "Cancel",
            0, 0,  # Indeterminate
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.show()

        # Get evidence logger for persistent logging
        evidence_logger = None
        if self.audit_logger and self.db_manager:
            try:
                # Ensure evidence database exists with migrations (creates process_log table)
                _ = self.db_manager.get_evidence_conn(evidence_id, evidence_label)
                evidence_db_path = self.db_manager.evidence_db_path(evidence_id, evidence_label)
                evidence_logger = self.audit_logger.get_evidence_logger(evidence_id, evidence_db_path)
            except Exception as e:
                LOGGER.warning(f"Could not create evidence logger for ingestion: {e}")

        # Create worker
        worker = IngestionWorker(
            extractor,
            output_dir,
            self.db_manager,
            evidence_id,
            evidence_label,
            config,
            evidence_logger=evidence_logger,
            parent=self
        )

        # Connect signals
        worker.callbacks.progress.connect(
            lambda cur, tot, msg: self._on_extraction_progress(progress, cur, tot, msg)
        )

        # Store reference to progress dialog for log updates
        self._current_progress_dialog = progress

        worker.callbacks.log_message.connect(
            lambda msg, level: self._on_ingestion_log(progress, extractor, msg, level)
        )
        worker.callbacks.step.connect(
            lambda step: progress.setLabelText(f"{extractor.metadata.display_name}\n\n{step}")
        )
        worker.finished.connect(
            lambda success, counts: self._on_ingestion_finished(extractor, progress, worker, success, counts)
        )
        worker.error.connect(
            lambda err: self._on_ingestion_error(extractor, progress, worker, err)
        )

        # Handle cancel
        progress.canceled.connect(worker.cancel)

        # Store and start
        self._active_workers[extractor.metadata.name] = worker

        self.log_message.emit(self.evidence_id, f"Starting ingestion: {extractor.metadata.display_name}")
        self.log_message.emit(self.evidence_id, f"Output directory: {output_dir}")
        self.log_message.emit(self.evidence_id, f"Evidence ID: {evidence_id}")

        worker.start()

    def _on_extraction_log(self, progress: QProgressDialog, extractor, msg: str, level: str):
        """Handle log messages during extraction - show in both dialog and log panel."""
        # Send to log panel
        self.log_message.emit(self.evidence_id, f"[{extractor.metadata.name}] {msg}")

        # Update progress dialog with all messages for extraction (more verbose than ingestion)
        # Get current label
        current_text = progress.labelText()
        lines = current_text.split('\n')

        # Keep header (first line with extractor name)
        if lines:
            header = lines[0]

            # Keep only last 5 detail lines to prevent dialog from growing too large
            detail_lines = [line for line in lines[1:] if line.strip()]
            detail_lines.append(msg)

            # Keep last 5 messages for extraction (more verbose)
            detail_lines = detail_lines[-5:]

            # Rebuild label
            new_text = header + '\n\n' + '\n'.join(detail_lines)
            progress.setLabelText(new_text)

    def _on_ingestion_log(self, progress: QProgressDialog, extractor, msg: str, level: str):
        """Handle log messages during ingestion - show in both dialog and log panel."""
        # Send to log panel
        self.log_message.emit(self.evidence_id, f"[{extractor.metadata.name}] {msg}")

        # Update progress dialog with important messages
        # Show progress updates, batch operations, and completion messages
        important_keywords = [
            'Processed', 'Imported', 'Inserting', 'Reading',
            'complete', 'Skipped', 'Successfully', '📄', '✓', '✅', '🎉'
        ]

        if any(keyword in msg for keyword in important_keywords):
            # Get current label
            current_text = progress.labelText()
            lines = current_text.split('\n')

            # Keep header (first line with extractor name)
            if lines:
                header = lines[0]

                # Keep only last 3 detail lines to prevent dialog from growing too large
                detail_lines = [line for line in lines[1:] if line.strip()]
                detail_lines.append(msg)

                # Keep last 4 messages (more context)
                detail_lines = detail_lines[-4:]

                # Rebuild label
                new_text = header + '\n\n' + '\n'.join(detail_lines)
                progress.setLabelText(new_text)

    def _on_extraction_progress(self, progress: QProgressDialog, current: int, total: int, message: str):
        """Update progress dialog."""
        if total > 0:
            progress.setMaximum(total)
            progress.setValue(current)

        if message:
            current_text = progress.labelText()
            # Keep extractor name on first line, update message on second line
            lines = current_text.split('\n')
            if len(lines) >= 2:
                progress.setLabelText(f"{lines[0]}\n\n{message}")
            else:
                progress.setLabelText(f"{current_text}\n{message}")

    def _on_extraction_finished(
        self,
        extractor: BaseExtractor,
        progress: QProgressDialog,
        worker: ExtractionWorker,
        success: bool
    ):
        """Handle extraction completion."""
        progress.close()

        # Remove worker
        self._active_workers.pop(extractor.metadata.name, None)
        worker.wait()

        if success:
            message = f"{extractor.metadata.display_name} extraction completed successfully."
            self.extraction_finished.emit(extractor.metadata.display_name, True, message)
            self.log_message.emit(self.evidence_id, message)

            QMessageBox.information(
                self,
                "Extraction Complete",
                f"{extractor.metadata.display_name} completed.\n\n"
                "Output files have been written. "
                "Click 'Ingest Results' to load data into the database."
            )

            # Refresh UI to show updated status
            self._refresh_ui()

            # If this extractor doesn't have a separate ingestion step, it imported data directly
            # Emit data_changed so other tabs can refresh
            if not extractor.metadata.can_ingest:
                self.data_changed.emit()
        else:
            message = f"{extractor.metadata.display_name} extraction failed."
            self.extraction_finished.emit(extractor.metadata.display_name, False, message)
            self.log_message.emit(self.evidence_id, message)

    def _on_extraction_error(
        self,
        extractor: BaseExtractor,
        progress: QProgressDialog,
        worker: ExtractionWorker,
        error: str
    ):
        """Handle extraction error."""
        progress.close()

        # Remove worker
        self._active_workers.pop(extractor.metadata.name, None)
        worker.wait()

        message = f"{extractor.metadata.display_name} extraction failed: {error}"
        self.extraction_finished.emit(extractor.metadata.display_name, False, error)
        self.log_message.emit(self.evidence_id, message)

        QMessageBox.critical(
            self,
            "Extraction Failed",
            f"{extractor.metadata.display_name} failed:\n\n{error}"
        )

    def _on_ingestion_finished(
        self,
        extractor: BaseExtractor,
        progress: QProgressDialog,
        worker: IngestionWorker,
        success: bool,
        counts: Dict[str, int]
    ):
        """Handle ingestion completion."""
        progress.close()

        # Remove worker
        self._active_workers.pop(extractor.metadata.name, None)
        worker.wait()

        if success:
            # Format counts (handle both numeric values and lists)
            formatted_counts = []
            for k, v in counts.items():
                if isinstance(v, list):
                    formatted_counts.append(f"  {k}: {len(v)}")
                elif isinstance(v, (int, float)):
                    formatted_counts.append(f"  {k}: {v:,}")
                else:
                    formatted_counts.append(f"  {k}: {v}")
            counts_str = "\n".join(formatted_counts)
            message = f"{extractor.metadata.display_name} ingestion completed:\n{counts_str}"

            self.log_message.emit(self.evidence_id, message)

            QMessageBox.information(
                self,
                "Ingestion Complete",
                f"{extractor.metadata.display_name} completed.\n\n{counts_str}"
            )

            # Refresh UI to show updated status
            self._refresh_ui()

            # Emit signal so other tabs can refresh their data
            self.data_changed.emit()
        else:
            message = f"{extractor.metadata.display_name} ingestion failed."
            self.log_message.emit(self.evidence_id, message)

    def _on_ingestion_error(
        self,
        extractor: BaseExtractor,
        progress: QProgressDialog,
        worker: IngestionWorker,
        error: str
    ):
        """Handle ingestion error."""
        progress.close()

        # Remove worker
        self._active_workers.pop(extractor.metadata.name, None)
        worker.wait()

        message = f"{extractor.metadata.display_name} ingestion failed: {error}"
        self.log_message.emit(self.evidence_id, message)

        QMessageBox.critical(
            self,
            "Ingestion Failed",
            f"{extractor.metadata.display_name} failed:\n\n{error}"
        )

    def _import_bulk_extractor_data(self, extractor: BaseExtractor):
        """
        Import existing bulk_extractor output files.

        Opens a file dialog to select folder or files, validates them,
        copies to output directory, and refreshes status.
        """
        from PySide6.QtWidgets import QFileDialog
        import shutil

        # Validation
        if not self.current_case or not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        # Get output directory
        evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
        evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)

        # Slugify evidence label for filesystem-safe folder name
        evidence_slug = slugify_label(evidence_label, evidence_id)

        output_dir = extractor.get_output_dir(
            Path(self.current_case.workspace_dir),
            evidence_slug
        )

        # Ask user what they want to import with custom button labels
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Import bulk_extractor Output")
        msg_box.setText("What would you like to import?")
        msg_box.setInformativeText(
            "• Click 'Folder' to select a FOLDER containing bulk_extractor output\n"
            "• Click 'File' to select individual FILES"
        )
        msg_box.setIcon(QMessageBox.Question)

        # Custom buttons
        folder_btn = msg_box.addButton("Folder", QMessageBox.YesRole)
        file_btn = msg_box.addButton("File", QMessageBox.NoRole)
        cancel_btn = msg_box.addButton("Cancel", QMessageBox.RejectRole)

        msg_box.setDefaultButton(folder_btn)
        msg_box.exec()

        clicked = msg_box.clickedButton()

        if clicked == cancel_btn:
            return

        # Known bulk_extractor output files
        valid_extensions = [".txt"]
        known_files = [
            "url.txt", "email.txt", "domain.txt", "ip.txt",
            "telephone.txt", "ccn.txt", "bitcoin.txt", "ether.txt"
        ]

        files_to_import = []

        if clicked == folder_btn:
            # Select folder
            folder = QFileDialog.getExistingDirectory(
                self,
                "Select Folder with bulk_extractor Output",
                "",
                QFileDialog.ShowDirsOnly
            )

            if not folder:
                return

            folder_path = Path(folder)
            txt_files = list(folder_path.glob("*.txt"))
            files_to_import.extend(txt_files)

        else:
            # Select files
            files, _ = QFileDialog.getOpenFileNames(
                self,
                "Select bulk_extractor Output Files",
                "",
                "Text Files (*.txt);;All Files (*)"
            )

            if not files:
                return

            files_to_import = [Path(f) for f in files]

        if not files_to_import:
            QMessageBox.warning(
                self,
                "No Files Found",
                "No bulk_extractor output files (.txt) found in selection."
            )
            return

        # Filter to known bulk_extractor files
        recognized = [f for f in files_to_import if f.name in known_files]

        if not recognized:
            QMessageBox.warning(
                self,
                "No Recognized Files",
                f"No recognized bulk_extractor output files found.\n\n"
                f"Expected files: {', '.join(known_files)}"
            )
            return

        # Check for existing files
        output_dir.mkdir(parents=True, exist_ok=True)
        existing = [f for f in recognized if (output_dir / f.name).exists()]

        if existing:
            # Ask for confirmation to overwrite
            existing_names = "\n".join([f"  • {f.name}" for f in existing])
            reply = QMessageBox.question(
                self,
                "Overwrite Existing Files?",
                f"The following files already exist in the output directory:\n\n{existing_names}\n\n"
                f"Do you want to overwrite them?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if reply == QMessageBox.No:
                return

        # Copy files
        try:
            copied_count = 0
            for src_file in recognized:
                dest_file = output_dir / src_file.name
                shutil.copy2(src_file, dest_file)
                copied_count += 1
                self.log_message.emit(self.evidence_id, f"Imported: {src_file.name}")

            # Show success message
            file_list = "\n".join([f"  • {f.name}" for f in recognized])
            QMessageBox.information(
                self,
                "Import Successful",
                f"Successfully imported {copied_count} file(s):\n\n{file_list}\n\n"
                f"You can now click 'Ingest Results' to import the data into the database."
            )

            # Refresh UI to update status widget
            self._refresh_ui()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Import Failed",
                f"Failed to import files:\n\n{e}"
            )
            self.log_message.emit(self.evidence_id, f"Import failed: {e}")

    def _import_file_list(self, extractor: BaseExtractor):
        """
        Import file list CSV for file_list_importer extractor.
        Prompts for CSV file and copies it to output_dir/file_list.csv.
        """
        # Validation
        if not self.current_case or not self.current_evidence:
            QMessageBox.warning(
                self,
                "No Case/Evidence",
                "Please load a case and select evidence first."
            )
            return

        # Get output directory
        evidence_id = self.current_evidence.get("id") if isinstance(self.current_evidence, dict) else self.current_evidence.id
        evidence_label = self.current_evidence.get("label") if isinstance(self.current_evidence, dict) else getattr(self.current_evidence, "label", None)

        # Slugify evidence label for filesystem-safe folder name
        evidence_slug = slugify_label(evidence_label, evidence_id)

        output_dir = extractor.get_output_dir(
            Path(self.current_case.workspace_dir),
            evidence_slug
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        dest_file = output_dir / "file_list.csv"

        # Check if file already exists
        if dest_file.exists():
            reply = QMessageBox.question(
                self,
                "File Exists",
                f"A file list CSV already exists at:\n{dest_file}\n\n"
                "Do you want to replace it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        # Open file dialog to select CSV
        csv_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select File List CSV",
            str(Path.home()),
            "CSV Files (*.csv);;All Files (*)"
        )

        if not csv_path:
            return  # User cancelled

        try:
            src_file = Path(csv_path)

            # Copy file
            shutil.copy2(src_file, dest_file)
            self.log_message.emit(self.evidence_id, f"Imported: {src_file.name} → file_list.csv")

            # Show success message
            QMessageBox.information(
                self,
                "Import Successful",
                f"Successfully imported file list CSV:\n\n"
                f"  • {src_file.name}\n\n"
                f"Saved as: file_list.csv\n\n"
                f"You can now click 'Run Extraction' to process the file list."
            )

            # Refresh UI
            self._refresh_ui()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Import Failed",
                f"Failed to import CSV file:\n\n{e}"
            )
            self.log_message.emit(self.evidence_id, f"Import failed: {e}")

    def _import_carved_images(self, extractor: BaseExtractor):
        """Legacy import handler removed (replaced by dedicated Foremost/Scalpel ingest)."""
        QMessageBox.information(
            self,
            "Importer Removed",
            "Legacy image carving importer has been replaced by dedicated Foremost and Scalpel extractors."
        )

    def _refresh_ui(self):
        """Refresh the UI to update run status widgets with current context."""
        if not self._run_status_widgets:
            return

        # Update all tracked run status widgets
        for extractor_name, widget in self._run_status_widgets.items():
            status = self._get_run_status_for_extractor(extractor_name)
            widget.update_status(
                extraction_info=status.get("extraction") if status else None,
                ingestion_info=status.get("ingestion") if status else None,
            )

        self.log_message.emit(self.evidence_id, "Run status widgets refreshed")

    def _get_run_status_for_extractor(self, extractor_name: str) -> Optional[Dict]:
        """
        Get run status for an extractor from process_log.

        Args:
            extractor_name: Name of the extractor.

        Returns:
            Dict with 'extraction' and 'ingestion' status, or None.
        """
        if not self.db_manager or not self.current_evidence:
            return None

        try:
            evidence_id = (
                self.current_evidence.get("id")
                if isinstance(self.current_evidence, dict)
                else getattr(self.current_evidence, "id", None)
            )
            evidence_label = (
                self.current_evidence.get("label")
                if isinstance(self.current_evidence, dict)
                else getattr(self.current_evidence, "label", None)
            )

            if evidence_id is None:
                return None

            # Resolve database path based on enable_split flag
            if self.db_manager.enable_split:
                db_path = self.db_manager.evidence_db_path(
                    evidence_id, evidence_label, create_dirs=False
                )
            else:
                db_path = self.db_manager.case_db_path

            if not db_path.exists():
                return None

            return get_extractor_run_status(db_path, extractor_name, evidence_id)
        except Exception as e:
            LOGGER.debug("Failed to get run status for %s: %s", extractor_name, e)
            return None

    def set_current_case(self, case, evidence=None, evidence_fs=None, db_manager=None, audit_logger=None):
        """
        Update current case and evidence context.

        Args:
            case: Case object
            evidence: Evidence object (dict with 'source_path', 'partition_index', etc.)
            evidence_fs: Evidence filesystem (pytsk3 or mounted) - optional, can be None
            db_manager: DatabaseManager for creating thread-local connections
            audit_logger: AuditLogger for persistent forensic logging

        Note:
            evidence_fs is optional. Extractors that need filesystem access
            will mount on-demand using mount_evidence_filesystem().
        """
        self.current_case = case
        self.current_evidence = evidence
        self.db_manager = db_manager
        self.evidence_fs = evidence_fs
        self.audit_logger = audit_logger

        # Get evidence logger from audit logger if available
        if audit_logger and evidence and db_manager:
            evidence_id = evidence.get("id") if isinstance(evidence, dict) else getattr(evidence, "id", None)
            evidence_label = evidence.get("label") if isinstance(evidence, dict) else getattr(evidence, "label", None)
            if evidence_id is not None and evidence_label is not None:
                try:
                    # Ensure evidence database exists with migrations applied (creates process_log table)
                    # This must be done BEFORE getting the evidence logger, which writes to process_log
                    _ = db_manager.get_evidence_conn(evidence_id, evidence_label)
                    evidence_db_path = db_manager.evidence_db_path(evidence_id, evidence_label)
                    self.evidence_logger = audit_logger.get_evidence_logger(evidence_id, evidence_db_path)
                except Exception as e:
                    LOGGER.warning(f"Could not get evidence logger: {e}")
                    self.evidence_logger = None
            else:
                self.evidence_logger = None
        else:
            self.evidence_logger = None

        # Test log emission to verify connection
        self.log_message.emit(self.evidence_id, "ExtractionTab: Case context updated")

        # Enable/disable based on context
        self.setEnabled(
            case is not None and
            evidence is not None and
            (evidence_fs is not None or db_manager is not None)
        )

        # Refresh UI with new context
        if case and evidence:
            self._refresh_ui()

    def mount_evidence_filesystem(self):
        """
        Mount evidence filesystem on-demand (E01 or directory).

        Called by extractors that need filesystem access.
        Uses self.current_evidence to get source_path and partition_index.

        Returns:
            EvidenceFS instance (PyEwfTskFS or MountedFS) or None on failure

        Example:
            # In extractor's extract() method:
            fs = self.parent_tab.mount_evidence_filesystem()
            if fs:
                # Now can scan filesystem
                for path in fs.iter_paths("Users/*/AppData/Local/Chrome/*/History"):
                    ...
        """
        if self.current_evidence is None:
            self.log_message.emit(
                self.evidence_id,
                "⚠️ Cannot mount evidence: no evidence context set"
            )
            return None

        from pathlib import Path
        from core.evidence_fs import PyEwfTskFS, MountedFS, find_ewf_segments

        # Helper to handle dict/object attribute access
        def get_attr(obj, key, default=None):
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        source_path_str = get_attr(self.current_evidence, 'source_path', '')
        if not source_path_str:
            self.log_message.emit(
                self.evidence_id,
                "⚠️ Evidence has no source_path - cannot mount"
            )
            return None

        source_path = Path(source_path_str)

        # Check existence - may fail if file is on encrypted filesystem without key
        try:
            exists = source_path.exists()
        except OSError as e:
            # errno 126 = "Required key not available" (encrypted filesystem)
            if e.errno == 126:
                self.log_message.emit(
                    self.evidence_id,
                    f"❌ Cannot access evidence - file is on encrypted filesystem and key not loaded: {source_path}"
                )
                LOGGER.error(
                    "Encrypted filesystem access denied (errno 126): %s. "
                    "Unlock the encrypted directory or mount it with proper credentials.",
                    source_path
                )
            else:
                self.log_message.emit(
                    self.evidence_id,
                    f"❌ Cannot access evidence file: {e}"
                )
                LOGGER.error("OSError accessing evidence path %s: %s", source_path, e)
            return None

        if not exists:
            self.log_message.emit(
                self.evidence_id,
                f"⚠️ Evidence source not found: {source_path}"
            )
            return None

        # Case 1: Directory - use MountedFS
        if source_path.is_dir():
            try:
                fs = MountedFS(source_path)
                self.log_message.emit(
                    self.evidence_id,
                    f"✅ Mounted directory: {source_path}"
                )
                return fs
            except Exception as e:
                self.log_message.emit(
                    self.evidence_id,
                    f"❌ Failed to mount directory {source_path}: {e}"
                )
                return None

        # Case 2: E01 image - use PyEwfTskFS
        if source_path.suffix.lower() in ['.e01', '.e02', '.e03']:
            try:
                # Discover all segments (.E01, .E02, .E03, ...)
                segments = find_ewf_segments(source_path)
                if not segments:
                    self.log_message.emit(
                        self.evidence_id,
                        f"❌ No E01 segments found for {source_path}"
                    )
                    return None

                # Get partition index if specified (None = auto-detect)
                partition_index = get_attr(self.current_evidence, 'partition_index')
                if partition_index is None:
                    partition_index = -1  # Auto-detect

                # Mount filesystem
                fs = PyEwfTskFS(segments, partition_index=partition_index)

                # Log success with partition info
                partition_msg = f" (partition {partition_index})" if partition_index != -1 else " (auto-detected)"
                self.log_message.emit(
                    self.evidence_id,
                    f"✅ Mounted E01: {source_path.name}{partition_msg} - {fs.fs_type}"
                )
                return fs

            except Exception as e:
                import traceback
                self.log_message.emit(
                    self.evidence_id,
                    f"❌ Failed to mount E01 {source_path.name}: {e}"
                )
                # Log full traceback for debugging
                LOGGER.error("E01 mount error details:\n%s", traceback.format_exc())
                return None

        # Unknown format
        self.log_message.emit(
            self.evidence_id,
            f"⚠️ Unsupported evidence format: {source_path.suffix}"
        )
        return None

    def _load_evidence_data(self):
        """Load evidence data (kept for compatibility)."""
        # In modular architecture, this is handled via set_current_case
        pass
