"""
Qt model for auth tokens (storage_tokens table)

Displays authentication tokens extracted from browser web storage.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from core.database import get_storage_tokens, get_storage_token_stats

logger = logging.getLogger(__name__)


class AuthTokensTableModel(QAbstractTableModel):
    """
    Qt model for storage_tokens table.

    Displays OAuth, JWT, and session tokens extracted from browser web storage
    (LocalStorage, SessionStorage, IndexedDB).
    """

    # Column definitions - DB column names
    COLUMNS = [
        "token_type",
        "origin",
        "storage_key",
        "associated_email",
        "expires_at_utc",
        "is_expired",
        "risk_level",
        "browser",
        "profile",
        "storage_type",
    ]

    HEADERS = [
        "Type",
        "Origin",
        "Key",
        "Email",
        "Expires",
        "Expired",
        "Risk",
        "Browser",
        "Profile",
        "Storage",
    ]

    # Column indexes
    COL_TYPE = 0
    COL_ORIGIN = 1
    COL_KEY = 2
    COL_EMAIL = 3
    COL_EXPIRES = 4
    COL_EXPIRED = 5
    COL_RISK = 6
    COL_BROWSER = 7
    COL_PROFILE = 8
    COL_STORAGE_TYPE = 9

    ARTIFACT_TYPE = "auth_token"

    # Risk level colors
    RISK_COLORS = {
        "high": QColor(255, 200, 200),    # Light red
        "medium": QColor(255, 255, 200),  # Light yellow
        "low": QColor(200, 255, 200),     # Light green
    }

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize auth tokens model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tags
            parent: Parent widget
        """
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        # Data storage
        self._rows: List[Dict[str, Any]] = []
        self._stats: Dict[str, Any] = {}

        # Filters
        self._token_type_filter: str = ""
        self._origin_filter: str = ""
        self._risk_filter: str = ""
        self._include_expired: bool = True

    def load(
        self,
        token_type_filter: str = "",
        origin_filter: str = "",
        risk_filter: str = "",
        include_expired: bool = True,
    ) -> None:
        """
        Load auth tokens data from database with optional filters.

        Args:
            token_type_filter: Token type filter (empty = all)
            origin_filter: Origin substring filter
            risk_filter: Risk level filter (empty = all)
            include_expired: Whether to include expired tokens
        """
        self._token_type_filter = token_type_filter
        self._origin_filter = origin_filter
        self._risk_filter = risk_filter
        self._include_expired = include_expired

        self.beginResetModel()
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            self._rows = get_storage_tokens(
                conn,
                self.evidence_id,
                token_type=token_type_filter or None,
                origin=origin_filter or None,
            )

            self._stats = get_storage_token_stats(conn, self.evidence_id)

            logger.debug(
                "Loaded %d auth tokens for evidence %d",
                len(self._rows),
                self.evidence_id,
            )
        except Exception as e:
            logger.error("Failed to load auth tokens: %s", e)
            self._rows = []
            self._stats = {}
        finally:
            self.endResetModel()

    def get_stats(self) -> Dict[str, Any]:
        """Return current statistics."""
        return self._stats

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of rows."""
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of columns."""
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for the given index and role."""
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        record = self._rows[row]

        if role == Qt.DisplayRole:
            col_name = self.COLUMNS[col]
            value = record.get(col_name)

            # Format boolean columns
            if col == self.COL_EXPIRED:
                return "Yes" if value else "No"

            # Format timestamps
            if col == self.COL_EXPIRES and value:
                # Show date only for cleaner display
                return value[:10] if len(value) >= 10 else value

            # Format token type
            if col == self.COL_TYPE:
                return (value or "unknown").replace("_", " ").title()

            # Format risk level
            if col == self.COL_RISK:
                return (value or "medium").upper()

            return value or ""

        elif role == Qt.BackgroundRole:
            # Highlight by risk level
            risk = record.get("risk_level", "medium")
            if risk in self.RISK_COLORS:
                return self.RISK_COLORS[risk]

            # Dim expired tokens
            if record.get("is_expired"):
                return QColor(220, 220, 220)

        elif role == Qt.ToolTipRole:
            # Show full token value on hover
            if col == self.COL_KEY:
                token_value = record.get("token_value", "")
                if len(token_value) > 100:
                    return f"{token_value[:100]}..."
                return token_value

            # Show issuer/subject for JWT
            if col == self.COL_TYPE:
                issuer = record.get("issuer")
                subject = record.get("subject")
                if issuer or subject:
                    parts = []
                    if issuer:
                        parts.append(f"Issuer: {issuer}")
                    if subject:
                        parts.append(f"Subject: {subject}")
                    return "\n".join(parts)

        elif role == Qt.UserRole:
            # Return full record for detail dialogs
            return record

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ) -> Any:
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def get_record_by_row(self, row: int) -> Optional[Dict[str, Any]]:
        """Get the full record for a given row."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_distinct_token_types(self) -> List[str]:
        """Get list of distinct token types for filter dropdown."""
        types = set()
        for row in self._rows:
            t = row.get("token_type")
            if t:
                types.add(t)
        return sorted(types)

    def get_distinct_risk_levels(self) -> List[str]:
        """Get list of distinct risk levels for filter dropdown."""
        return ["high", "medium", "low"]
