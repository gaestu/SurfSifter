"""
Qt model for credit_cards table.

Displays saved payment cards extracted from Chromium Web Data.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_credit_cards

logger = logging.getLogger(__name__)


class CreditCardsTableModel(QAbstractTableModel):
    """Qt model for saved credit card entries."""

    COLUMNS = [
        "name_on_card",
        "nickname",
        "card_number_last_four",
        "expiration_month",
        "expiration_year",
        "use_count",
        "use_date_utc",
        "browser",
        "profile",
        "tags",
    ]

    HEADERS = [
        "Name On Card",
        "Nickname",
        "Last 4",
        "Exp. Month",
        "Exp. Year",
        "Use Count",
        "Last Used",
        "Browser",
        "Profile",
        "Tags",
    ]

    COL_NAME = 0
    COL_NICKNAME = 1
    COL_LAST4 = 2
    COL_EXP_MONTH = 3
    COL_EXP_YEAR = 4
    COL_USE_COUNT = 5
    COL_LAST_USED = 6
    COL_BROWSER = 7
    COL_PROFILE = 8
    COL_TAGS = 9

    ARTIFACT_TYPE = "credit_card"

    def __init__(self, db_manager, evidence_id: int, evidence_label: str, case_data=None, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        self._rows: List[Dict[str, Any]] = []
        self._tag_map: Dict[int, str] = {}

    def load(self, browser_filter: str = "", name_filter: str = "") -> None:
        """Load credit card rows from DB with optional filters."""
        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(self.evidence_id, label=self.evidence_label)
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_credit_cards(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )

                if name_filter:
                    term = name_filter.lower()
                    self._rows = [
                        row for row in self._rows
                        if term in (row.get("name_on_card") or "").lower()
                        or term in (row.get("nickname") or "").lower()
                        or term in (row.get("card_number_last_four") or "").lower()
                    ]

                self._refresh_tags()

            logger.debug("Loaded %d credit card entries", len(self._rows))
        except Exception as e:
            logger.error("Failed to load credit cards: %s", e, exc_info=True)
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
        """Get list of browsers with credit card entries."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(self.evidence_id, label=self.evidence_label)
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM credit_cards WHERE evidence_id = ?",
                    (self.evidence_id,),
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error("Failed to get credit card browsers: %s", e, exc_info=True)
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
            if col == self.COL_NAME:
                return row_data.get("name_on_card") or ""
            if col == self.COL_NICKNAME:
                return row_data.get("nickname") or ""
            if col == self.COL_LAST4:
                return row_data.get("card_number_last_four") or ""
            if col == self.COL_EXP_MONTH:
                month = row_data.get("expiration_month")
                return f"{int(month):02d}" if month else ""
            if col == self.COL_EXP_YEAR:
                year = row_data.get("expiration_year")
                return str(year) if year else ""
            if col == self.COL_USE_COUNT:
                return str(row_data.get("use_count") or 0)
            if col == self.COL_LAST_USED:
                value = row_data.get("use_date_utc") or ""
                return value[:19] if len(value) > 19 else value
            if col == self.COL_BROWSER:
                return (row_data.get("browser") or "").capitalize()
            if col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            if col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        if role == Qt.ToolTipRole:
            if col in (self.COL_NAME, self.COL_NICKNAME, self.COL_LAST_USED):
                return row_data.get(self.COLUMNS[col], "") or ""
            if col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        if role == Qt.TextAlignmentRole and col in (self.COL_EXP_MONTH, self.COL_EXP_YEAR, self.COL_USE_COUNT):
            return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
