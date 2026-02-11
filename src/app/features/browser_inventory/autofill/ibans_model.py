"""
Qt model for autofill_ibans table.

Displays Chromium/Edge IBAN artifacts parsed from Web Data local_ibans and
masked_ibans tables.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_autofill_ibans

logger = logging.getLogger(__name__)


class AutofillIbansTableModel(QAbstractTableModel):
    """Qt model for autofill IBAN entries."""

    COLUMNS = [
        "source_table",
        "nickname",
        "value",
        "prefix",
        "suffix",
        "browser",
        "profile",
        "use_date_utc",
        "tags",
    ]

    HEADERS = [
        "Source",
        "Nickname",
        "Value",
        "Prefix",
        "Suffix",
        "Browser",
        "Profile",
        "Last Used",
        "Tags",
    ]

    COL_SOURCE = 0
    COL_NICKNAME = 1
    COL_VALUE = 2
    COL_PREFIX = 3
    COL_SUFFIX = 4
    COL_BROWSER = 5
    COL_PROFILE = 6
    COL_LAST_USED = 7
    COL_TAGS = 8

    ARTIFACT_TYPE = "autofill_iban"

    def __init__(self, db_manager, evidence_id: int, evidence_label: str, case_data=None, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        self._rows: List[Dict[str, Any]] = []
        self._tag_map: Dict[int, str] = {}

    def load(self, browser_filter: str = "", source_filter: str = "", term_filter: str = "") -> None:
        """Load IBAN rows from DB with optional filters."""
        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(self.evidence_id, label=self.evidence_label)
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_autofill_ibans(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    source_table=source_filter or None,
                    limit=5000,
                )

                if term_filter:
                    term_lower = term_filter.lower()
                    self._rows = [
                        row for row in self._rows
                        if term_lower in (row.get("nickname") or "").lower()
                        or term_lower in (row.get("prefix") or "").lower()
                        or term_lower in (row.get("suffix") or "").lower()
                        or term_lower in (row.get("value") or "").lower()
                    ]

                self._refresh_tags()

            logger.debug("Loaded %d autofill IBAN entries", len(self._rows))
        except Exception as e:
            logger.error("Failed to load autofill IBANs: %s", e, exc_info=True)
            self._rows = []
            self._tag_map = {}
        self.endResetModel()

    def _refresh_tags(self) -> None:
        """Refresh tag strings for current rows."""
        if not self.case_data:
            self._tag_map = {}
            return
        ids = [row.get("id") for row in self._rows if row.get("id") is not None]
        self._tag_map = self.case_data.get_tag_strings_for_artifacts(
            self.evidence_id,
            self.ARTIFACT_TYPE,
            ids,
        )

    def get_available_browsers(self) -> List[str]:
        """Get list of browsers with autofill IBAN entries."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(self.evidence_id, label=self.evidence_label)
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM autofill_ibans WHERE evidence_id = ?",
                    (self.evidence_id,),
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error("Failed to get autofill IBAN browsers: %s", e, exc_info=True)
            return []

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for the given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row_data = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == self.COL_SOURCE:
                return row_data.get("source_table", "")
            if col == self.COL_NICKNAME:
                return row_data.get("nickname") or ""
            if col == self.COL_VALUE:
                value = row_data.get("value") or ""
                return value if len(value) <= 50 else value[:47] + "..."
            if col == self.COL_PREFIX:
                return row_data.get("prefix") or ""
            if col == self.COL_SUFFIX:
                return row_data.get("suffix") or ""
            if col == self.COL_BROWSER:
                return (row_data.get("browser") or "").capitalize()
            if col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            if col == self.COL_LAST_USED:
                value = row_data.get("use_date_utc") or ""
                return value[:19] if len(value) > 19 else value
            if col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        if role == Qt.ToolTipRole:
            if col in (self.COL_VALUE, self.COL_LAST_USED):
                return row_data.get(self.COLUMNS[col], "") or ""
            if col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
