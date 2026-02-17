"""Reports tab shim - integrates self-contained reports module into main app."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING, Union

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QWidget, QVBoxLayout

from reports.ui import ReportTabWidget

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

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Setup the shim UI - just embed the report widget."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

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
            self._report_widget.set_db_connection(None)

        self._report_widget.set_evidence(evidence_id, evidence_label)
