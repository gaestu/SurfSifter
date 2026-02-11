"""
Qt model for browser_cache_inventory table.

Displays all discovered browser artifacts with extraction/ingestion status.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QIcon

from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class BrowserInventoryModel(QAbstractTableModel):
    """
    Qt model for browser_cache_inventory table.

    Displays all discovered browser artifacts with extraction/ingestion status,
    allowing investigators to track which browser artifacts have been extracted
    and ingested from evidence images.
    """

    # Column definitions
    COLUMNS = [
        "browser",
        "artifact_type",
        "profile",
        "logical_path",
        "extraction_status",
        "ingestion_status",
        "urls_parsed",
        "records_parsed",
    ]

    HEADERS = [
        "Browser",
        "Type",
        "Profile",
        "Path",
        "Extraction",
        "Ingestion",
        "URLs",
        "Records",
    ]

    # Column indexes for easy reference
    COL_BROWSER = 0
    COL_TYPE = 1
    COL_PROFILE = 2
    COL_PATH = 3
    COL_EXTRACTION_STATUS = 4
    COL_INGESTION_STATUS = 5
    COL_URLS = 6
    COL_RECORDS = 7

    # Status icons (unicode glyphs for MVP, should be QIcon resources in production)
    STATUS_ICONS = {
        "ok": "✓",
        "partial": "⚠",
        "error": "✗",
        "failed": "✗",
        "skipped": "⊝",
        "pending": "⊙",
    }

    def __init__(self, case_folder: str, evidence_id: int, case_db_path: Path, parent=None):
        """
        Initialize browser inventory model.

        Args:
            case_folder: Path to case folder
            evidence_id: Evidence ID
            case_db_path: Path to case database file
            parent: Parent widget
        """
        super().__init__(parent)
        self.case_folder = Path(case_folder)
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.db_manager = DatabaseManager(self.case_folder, case_db_path=case_db_path)

        # Cache evidence label to avoid repeated queries
        self._evidence_label_cache: Optional[str] = None

        # Data storage
        self._rows: List[Dict[str, Any]] = []

        # Filters
        self._filters = {
            "browser": "",  # All browsers (or specific: chrome, firefox, edge, safari)
            "artifact_type": "",  # All types (or specific: history, cache_simple, cache_firefox)
            "status": "",  # All statuses (or specific: ok, partial, error, pending)
        }

        self._load_data()

    def _get_evidence_label(self) -> str:
        """
        Retrieve the evidence label from the database.

        Returns:
            Evidence label from database, or fallback format if not found.
        """
        if self._evidence_label_cache is not None:
            return self._evidence_label_cache

        with sqlite3.connect(self.case_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.evidence_id,),
            ).fetchone()
            if row and row["label"]:
                self._evidence_label_cache = row["label"]
            else:
                # Fallback (should not happen with  auto-derived labels)
                self._evidence_label_cache = f"EV-{self.evidence_id:03d}"
                logger.warning(
                    "Evidence %d has no label in database, using fallback: %s",
                    self.evidence_id,
                    self._evidence_label_cache
                )

        return self._evidence_label_cache

    def _load_data(self) -> None:
        """Load browser_cache_inventory data from evidence database."""
        logger.debug(f"Loading browser inventory for evidence_id={self.evidence_id}")

        try:
            # Get evidence database path (not connection!)
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self._get_evidence_label()
            )

            # Build filter clauses
            filter_clauses = []
            params = [self.evidence_id]

            if self._filters["browser"]:
                filter_clauses.append("browser = ?")
                params.append(self._filters["browser"])

            if self._filters["artifact_type"]:
                filter_clauses.append("artifact_type = ?")
                params.append(self._filters["artifact_type"])

            if self._filters["status"]:
                # Filter by ingestion_status
                filter_clauses.append("ingestion_status = ?")
                params.append(self._filters["status"])

            # Build WHERE clause
            where_clause = "WHERE evidence_id = ?"
            if filter_clauses:
                where_clause += " AND " + " AND ".join(filter_clauses)

            # Query browser_cache_inventory table
            query = f"""
                SELECT
                    id,
                    browser,
                    artifact_type,
                    profile,
                    logical_path,
                    extraction_status,
                    ingestion_status,
                    urls_parsed,
                    records_parsed,
                    run_id,
                    extracted_path,
                    partition_index,
                    fs_type,
                    forensic_path,
                    file_size_bytes,
                    file_md5,
                    file_sha256,
                    extraction_timestamp_utc,
                    ingestion_timestamp_utc,
                    extraction_tool,
                    extraction_notes,
                    ingestion_notes
                FROM browser_cache_inventory
                {where_clause}
                ORDER BY browser, artifact_type, profile
            """

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                self._rows = [dict(row) for row in cursor.fetchall()]

            logger.debug(f"Loaded {len(self._rows)} browser inventory entries")

        except Exception as e:
            logger.error(f"Failed to load browser inventory: {e}", exc_info=True)
            self._rows = []

    def refresh(self) -> None:
        """Reload data from database (called after extraction/ingestion)."""
        self.beginResetModel()
        self._load_data()
        self.endResetModel()

    def set_filters(
        self,
        browser: str = "",
        artifact_type: str = "",
        status: str = "",
    ) -> None:
        """
        Update filters and reload data.

        Args:
            browser: Browser filter (empty = all)
            artifact_type: Artifact type filter (empty = all)
            status: Status filter (empty = all)
        """
        self._filters["browser"] = browser
        self._filters["artifact_type"] = artifact_type
        self._filters["status"] = status
        self.refresh()

    # Qt interface methods

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of rows."""
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of columns."""
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for given index and role."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row_data = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == self.COL_BROWSER:
                return row_data.get("browser", "")
            elif col == self.COL_TYPE:
                return row_data.get("artifact_type", "")
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_PATH:
                # Truncate long paths
                path = row_data.get("logical_path", "")
                if len(path) > 50:
                    return "..." + path[-47:]
                return path
            elif col == self.COL_EXTRACTION_STATUS:
                status = row_data.get("extraction_status", "")
                icon = self.STATUS_ICONS.get(status, "")
                return f"{icon} {status}" if status else ""
            elif col == self.COL_INGESTION_STATUS:
                status = row_data.get("ingestion_status") or "pending"
                icon = self.STATUS_ICONS.get(status, "")
                return f"{icon} {status}"
            elif col == self.COL_URLS:
                return row_data.get("urls_parsed") or 0
            elif col == self.COL_RECORDS:
                return row_data.get("records_parsed") or 0

        elif role == Qt.ToolTipRole:
            if col == self.COL_PATH:
                # Full path in tooltip
                return row_data.get("logical_path", "")
            elif col == self.COL_EXTRACTION_STATUS:
                # Show extraction notes if any
                notes = row_data.get("extraction_notes")
                if notes:
                    return f"Status: {row_data.get('extraction_status')}\n\n{notes}"
                return row_data.get("extraction_status", "")
            elif col == self.COL_INGESTION_STATUS:
                # Show ingestion notes if any
                notes = row_data.get("ingestion_notes")
                if notes:
                    status = row_data.get("ingestion_status") or "pending"
                    return f"Status: {status}\n\n{notes}"
                return row_data.get("ingestion_status") or "pending"

        elif role == Qt.TextAlignmentRole:
            # Right-align numeric columns
            if col in (self.COL_URLS, self.COL_RECORDS):
                return Qt.AlignRight | Qt.AlignVCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """
        Get full row data for context menu actions.

        Args:
            index: Model index

        Returns:
            Dictionary with all row data, or empty dict if invalid
        """
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    def get_available_browsers(self) -> List[str]:
        """
        Get list of unique browsers in inventory.

        Returns:
            Sorted list of browser names
        """
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self._get_evidence_label()
            )
            with sqlite3.connect(evidence_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT browser
                    FROM browser_cache_inventory
                    WHERE evidence_id = ?
                    ORDER BY browser
                """, (self.evidence_id,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get browsers: {e}", exc_info=True)
            return []

    def get_available_types(self) -> List[str]:
        """
        Get list of unique artifact types in inventory.

        Returns:
            Sorted list of artifact type names
        """
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self._get_evidence_label()
            )
            with sqlite3.connect(evidence_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT artifact_type
                    FROM browser_cache_inventory
                    WHERE evidence_id = ?
                    ORDER BY artifact_type
                """, (self.evidence_id,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get artifact types: {e}", exc_info=True)
            return []
