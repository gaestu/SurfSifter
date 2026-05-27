"""Reports tab shim - integrates self-contained reports module into main app."""
from __future__ import annotations

import logging
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING, Union

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QMessageBox,
    QPushButton,
    QWidget,
    QVBoxLayout,
)

from app.common.dialogs.tag_match_summary import TagMatchSummaryDialog
from reports.tag_summary_service import (
    TagSummaryExportError,
    export_tag_summary,
)
from reports.ui import ReportTabWidget

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class ReportsTab(QWidget):
    """Thin shim that wraps the self-contained ReportTabWidget.

    This class exists only to integrate the reports module with the main app.
    All actual logic lives in src/reports/.
    """

    manage_text_blocks_requested = Signal()

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        # State passed from main.py
        self._case_data: Optional[Union[CaseDataAccess, dict]] = None
        self._evidence_id: Optional[int] = None
        self._evidence_label: Optional[str] = None
        self._db_manager = None

        self._summary_dialog: Optional[TagMatchSummaryDialog] = None

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Setup the shim UI - just embed the report widget."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Toolbar row with summary button (right-aligned)
        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(8, 4, 8, 0)
        toolbar.addStretch()

        self._summary_btn = QPushButton("📊 Tag && Match Summary")
        self._summary_btn.setToolTip(
            "Open a floating panel showing tagged artifacts and reference-list matches"
        )
        self._summary_btn.clicked.connect(self._open_summary)
        toolbar.addWidget(self._summary_btn)

        self._export_btn = QPushButton("📝 Export Tag Summary")
        self._export_btn.setToolTip(
            "Export a working summary of tagged artifacts (XLSX) for prosecutor review"
        )
        self._export_btn.setEnabled(False)
        self._export_btn.clicked.connect(self._on_export_tag_summary)
        toolbar.addWidget(self._export_btn)
        layout.addLayout(toolbar)

        # The actual report UI from src/reports/
        self._report_widget = ReportTabWidget(parent=self)
        self._report_widget.manage_text_blocks_requested.connect(
            self.manage_text_blocks_requested.emit
        )
        layout.addWidget(self._report_widget)

    # --- Compatibility methods called from main.py ---

    def set_default_settings(
        self,
        report_settings: Any,
        config_dir: Optional[Path] = None,
    ) -> None:
        """Set global default settings from app preferences.

        Args:
            report_settings: ReportSettings dataclass from app settings
            config_dir: Path to config directory for resolving logo path
        """
        # Convert dataclass to dict for the report widget
        defaults: Dict[str, Any] = asdict(report_settings) if hasattr(report_settings, '__dataclass_fields__') else {}
        self._report_widget.set_default_settings(defaults, config_dir)

    def set_database_manager(self, db_manager, case_number: Optional[str] = None) -> None:
        """Set database manager for section persistence.

        Args:
            db_manager: DatabaseManager instance
            case_number: Case number string
        """
        self._db_manager = db_manager

        # Set workspace path for PDF save location
        if db_manager is not None and hasattr(db_manager, 'case_folder'):
            self._report_widget.set_workspace_path(Path(db_manager.case_folder))
        else:
            self._report_widget.set_workspace_path(None)

    def set_case_data(self, case_data: Optional[Union[CaseDataAccess, dict]]) -> None:
        """Set case metadata.

        Args:
            case_data: CaseDataAccess instance or dictionary with case metadata
        """
        self._case_data = case_data
        self._report_widget.set_case_data(case_data)

        # Extract investigator from case data if available
        if case_data is not None:
            investigator = None
            if hasattr(case_data, 'get_case_metadata'):
                metadata = case_data.get_case_metadata()
                investigator = metadata.get("investigator")
            elif isinstance(case_data, dict):
                investigator = case_data.get("investigator")
            self._report_widget.set_investigator(investigator)

    def set_evidence(self, evidence_id: Optional[int], evidence_label: Optional[str] = None) -> None:
        """Set the current evidence.

        Args:
            evidence_id: Evidence ID
            evidence_label: Human-readable evidence label
        """
        self._evidence_id = evidence_id
        self._evidence_label = evidence_label

        # Pass the evidence database connection to the report widget
        if self._db_manager is not None and evidence_id is not None:
            conn = self._db_manager.get_evidence_conn(evidence_id, evidence_label)
            self._report_widget.set_db_connection(conn)
        else:
            conn = None
            self._report_widget.set_db_connection(None)

        self._report_widget.set_evidence(evidence_id, evidence_label)
        # Toggle export button to match selection state.
        self._export_btn.setEnabled(
            self._db_manager is not None and evidence_id is not None
        )
        # Keep summary dialog in sync if it exists
        if self._summary_dialog is not None:
            self._summary_dialog.set_db_connection(conn, evidence_id)

    # --- Summary dialog ---

    def _open_summary(self) -> None:
        """Open (or raise) the floating Tag & Match Summary panel."""
        if self._summary_dialog is None:
            self._summary_dialog = TagMatchSummaryDialog(parent=self.window())

        # Ensure connection is current
        conn = None
        if self._db_manager is not None and self._evidence_id is not None:
            conn = self._db_manager.get_evidence_conn(
                self._evidence_id, self._evidence_label
            )
        self._summary_dialog.set_db_connection(conn, self._evidence_id)
        self._summary_dialog.show()

    # --- Export tag summary (XLSX working document) ---

    @staticmethod
    def _slugify(value: Optional[str]) -> str:
        """Make a filesystem-safe slug from an evidence label."""
        if not value:
            return "evidence"
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
        return slug or "evidence"

    def _default_export_path(self) -> Optional[Path]:
        """Build the default ``reports/`` save path for this evidence.

        Returns ``None`` when no case workspace is bound — the export
        button is then refused so the writer cannot fall back outside
        the case folder (forensic output isolation rule).
        """
        case_folder = self._case_folder()
        if case_folder is None:
            return None
        slug = self._slugify(self._evidence_label)
        filename = f"tag_summary_{slug}.xlsx"
        return case_folder / "reports" / filename

    def _case_folder(self) -> Optional[Path]:
        """Resolve the case workspace folder, or ``None`` if not bound."""
        if self._db_manager is None:
            return None
        case_folder = getattr(self._db_manager, "case_folder", None)
        if case_folder is None:
            return None
        try:
            return Path(case_folder).resolve()
        except (OSError, RuntimeError):
            return None

    @staticmethod
    def _is_within(child: Path, parent: Path) -> bool:
        try:
            child.resolve().relative_to(parent)
        except (OSError, ValueError):
            return False
        return True

    def _on_export_tag_summary(self) -> None:
        """Handle the 📝 Export Tag Summary button click.

        UI concerns only — path/dialog handling and user feedback. All
        export pipeline logic (path validation, data composition,
        workbook write, forensic audit, rollback) lives in
        :func:`reports.tag_summary_service.export_tag_summary`.
        """
        if self._db_manager is None or self._evidence_id is None:
            QMessageBox.warning(
                self,
                "Export Tag Summary",
                "Select an evidence before exporting the tag summary.",
            )
            return

        case_folder = self._case_folder()
        if case_folder is None:
            QMessageBox.warning(
                self,
                "Export Tag Summary",
                "No case workspace is bound — cannot export.\n\n"
                "Tag summary exports are restricted to the case workspace "
                "to preserve forensic output isolation.",
            )
            return

        default_path = self._default_export_path()
        assert default_path is not None  # case_folder check above
        default_path.parent.mkdir(parents=True, exist_ok=True)

        chosen, _ = QFileDialog.getSaveFileName(
            self,
            "Export Tag Summary",
            str(default_path),
            "Excel Workbook (*.xlsx)",
        )
        if not chosen:
            return

        output_path = Path(chosen)
        if output_path.suffix.lower() != ".xlsx":
            output_path = output_path.with_suffix(".xlsx")

        conn = self._db_manager.get_evidence_conn(
            self._evidence_id, self._evidence_label
        )

        finished_at_iso = (
            datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
        )

        try:
            result = export_tag_summary(
                conn=conn,
                evidence_id=self._evidence_id,
                evidence_label=self._evidence_label
                or f"evidence #{self._evidence_id}",
                case_folder=case_folder,
                output_path=output_path,
                exported_at_iso="see case audit log",
                finished_at_iso=finished_at_iso,
                top_n=10,
            )
        except TagSummaryExportError as exc:
            if exc.stage == "path":
                QMessageBox.warning(
                    self,
                    "Export Tag Summary",
                    f"{exc}\n\nCase folder: {case_folder}\n\n"
                    "Please save the export inside the case workspace.",
                )
            else:
                QMessageBox.critical(self, "Export Tag Summary", str(exc))
            return

        QMessageBox.information(
            self,
            "Export Tag Summary",
            f"Tag summary exported to:\n{result.output_path}",
        )
