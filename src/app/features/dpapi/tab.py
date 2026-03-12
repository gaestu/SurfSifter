"""DPAPI Decrypt feature tab.

Displays Windows user accounts, DPAPI master keys, Chromium
application-bound encryption keys, and an overall decryption summary.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, Optional

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QSplitter,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)


class DPAPITab(QWidget):
    """Feature tab showing DPAPI decryption results.

    Three subtabs:
    1. Users & Keys — Windows users with their master keys
    2. Chromium Keys — application-bound encryption blobs from Local State
    3. Decrypt Summary — dashboard of decryption outcomes
    """

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._db_manager = None
        self.case_data = None
        self.evidence_id: Optional[int] = None
        self._evidence_label: str = ""

        # Lazy loading state
        self._data_loaded = False
        self._load_pending = False
        self._data_stale = False

        self._setup_ui()

    # -- Public API -----------------------------------------------------------

    def set_db_manager(self, db_manager) -> None:
        """Set the database manager for evidence DB access."""
        self._db_manager = db_manager

    def set_case_data(self, case_data, defer_load: bool = False) -> None:
        self.case_data = case_data
        if defer_load:
            self._load_pending = True

    def set_evidence(
        self,
        evidence_id: int,
        evidence_label: str = "",
        defer_load: bool = False,
    ) -> None:
        self.evidence_id = evidence_id
        self._evidence_label = evidence_label
        self._data_loaded = False
        if defer_load:
            self._load_pending = True
        else:
            self._load_data()

    def refresh(self) -> None:
        """Reload all data from the database."""
        self._load_data()

    def mark_stale(self) -> None:
        """Mark data as stale for lazy refresh on next showEvent."""
        self._data_stale = True

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._load_pending and not self._data_loaded:
            QTimer.singleShot(10, self._perform_deferred_load)
        elif self._data_stale and self._data_loaded:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    # -- UI setup -------------------------------------------------------------

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        self._tabs = QTabWidget()

        # --- Subtab 1: Users & Keys ---
        self._tabs.addTab(self._build_users_keys_tab(), "Users && Keys")

        # --- Subtab 2: Chromium Keys ---
        self._tabs.addTab(self._build_chromium_keys_tab(), "Chromium Keys")

        # --- Subtab 3: Decrypt Summary ---
        self._tabs.addTab(self._build_summary_tab(), "Decrypt Summary")

        layout.addWidget(self._tabs)

    def _build_users_keys_tab(self) -> QWidget:
        from .models.users_model import WindowsUsersModel
        from .models.master_keys_model import MasterKeysModel

        widget = QWidget()
        layout = QVBoxLayout(widget)

        splitter = QSplitter(Qt.Orientation.Vertical)

        # Top: users table
        users_widget = QWidget()
        users_layout = QVBoxLayout(users_widget)
        users_layout.setContentsMargins(0, 0, 0, 0)
        users_layout.addWidget(QLabel("Windows Users"))

        self._users_model = WindowsUsersModel(self)
        self._users_table = QTableView()
        self._users_table.setModel(self._users_model)
        self._users_table.horizontalHeader().setStretchLastSection(True)
        self._users_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._users_table.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self._users_table.setSortingEnabled(True)
        self._users_table.selectionModel().selectionChanged.connect(self._on_user_selected)
        users_layout.addWidget(self._users_table)

        self._users_summary = QLabel("No Windows users loaded.")
        users_layout.addWidget(self._users_summary)

        splitter.addWidget(users_widget)

        # Bottom: master keys for selected user
        keys_widget = QWidget()
        keys_layout = QVBoxLayout(keys_widget)
        keys_layout.setContentsMargins(0, 0, 0, 0)
        self._keys_label = QLabel("Master Keys")
        keys_layout.addWidget(self._keys_label)

        self._keys_model = MasterKeysModel(self)
        self._keys_table = QTableView()
        self._keys_table.setModel(self._keys_model)
        self._keys_table.horizontalHeader().setStretchLastSection(True)
        self._keys_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._keys_table.setSortingEnabled(True)
        keys_layout.addWidget(self._keys_table)

        self._keys_summary = QLabel("Select a user to view master keys.")
        keys_layout.addWidget(self._keys_summary)

        splitter.addWidget(keys_widget)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)

        layout.addWidget(splitter)
        return widget

    def _build_chromium_keys_tab(self) -> QWidget:
        from .models.app_keys_model import ChromiumAppKeysModel

        widget = QWidget()
        layout = QVBoxLayout(widget)

        self._app_keys_model = ChromiumAppKeysModel(self)
        self._app_keys_table = QTableView()
        self._app_keys_table.setModel(self._app_keys_model)
        self._app_keys_table.horizontalHeader().setStretchLastSection(True)
        self._app_keys_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._app_keys_table.setSortingEnabled(True)
        layout.addWidget(self._app_keys_table)

        self._app_keys_summary = QLabel("No Chromium app keys loaded.")
        layout.addWidget(self._app_keys_summary)

        return widget

    def _build_summary_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Summary cards (grid)
        cards_frame = QFrame()
        cards_frame.setFrameShape(QFrame.Shape.StyledPanel)
        cards_layout = QGridLayout(cards_frame)

        self._lbl_total = self._make_stat_label("0")
        self._lbl_decrypted = self._make_stat_label("0")
        self._lbl_failed = self._make_stat_label("0")
        self._lbl_no_key = self._make_stat_label("0")

        cards_layout.addWidget(QLabel("Total Records"), 0, 0, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(self._lbl_total, 1, 0, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(QLabel("Decrypted"), 0, 1, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(self._lbl_decrypted, 1, 1, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(QLabel("Failed"), 0, 2, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(self._lbl_failed, 1, 2, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(QLabel("No Key"), 0, 3, alignment=Qt.AlignmentFlag.AlignCenter)
        cards_layout.addWidget(self._lbl_no_key, 1, 3, alignment=Qt.AlignmentFlag.AlignCenter)

        layout.addWidget(cards_frame)

        # Per-table breakdown
        layout.addWidget(QLabel("Breakdown by Artifact Type"))
        self._breakdown_table = QTableView()
        self._breakdown_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._breakdown_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._breakdown_table)

        self._summary_note = QLabel("Run a DPAPI decryption extraction to populate this tab.")
        layout.addWidget(self._summary_note)

        return widget

    @staticmethod
    def _make_stat_label(text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet("font-size: 24px; font-weight: bold;")
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        return lbl

    # -- Data loading ---------------------------------------------------------

    def _perform_deferred_load(self) -> None:
        if self._data_loaded:
            return
        self._data_loaded = True
        self._load_pending = False
        self._load_data()

    def _load_data(self) -> None:
        """Fetch all DPAPI-related data and populate models."""
        if not self._db_manager or self.evidence_id is None or not self._evidence_label:
            self._clear_all()
            return

        try:
            conn = self._db_manager.get_evidence_conn(
                self.evidence_id, self._evidence_label
            )
            conn.row_factory = sqlite3.Row
            self._load_users_keys(conn)
            self._load_app_keys(conn)
            self._load_summary(conn)

        except Exception:
            logger.exception("Failed to load DPAPI data for evidence %s", self.evidence_id)
            self._clear_all()

    def _load_users_keys(self, conn: sqlite3.Connection) -> None:
        from core.database.helpers.dpapi import get_windows_users, get_dpapi_master_keys

        users = get_windows_users(conn, self.evidence_id)
        self._users_model.load(users)

        # Resize columns
        header = self._users_table.horizontalHeader()
        for i in range(self._users_model.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        if self._users_model.columnCount() > 0:
            header.setSectionResizeMode(
                self._users_model.columnCount() - 1,
                QHeaderView.ResizeMode.Stretch,
            )

        count = len(users)
        self._users_summary.setText(
            f"{count} Windows user(s) found." if count else "No Windows users found."
        )

        # Load all master keys initially (filtered on user selection)
        all_keys = get_dpapi_master_keys(conn, self.evidence_id)
        self._all_master_keys = all_keys
        self._keys_model.load(all_keys)
        self._keys_label.setText(f"Master Keys (all users — {len(all_keys)})")
        self._keys_summary.setText(
            f"{len(all_keys)} master key(s) total."
            if all_keys
            else "No master keys found."
        )

    def _load_app_keys(self, conn: sqlite3.Connection) -> None:
        from core.database.helpers.dpapi import get_chromium_app_keys

        keys = get_chromium_app_keys(conn, self.evidence_id)
        self._app_keys_model.load(keys)

        header = self._app_keys_table.horizontalHeader()
        for i in range(self._app_keys_model.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        if self._app_keys_model.columnCount() > 0:
            header.setSectionResizeMode(
                self._app_keys_model.columnCount() - 1,
                QHeaderView.ResizeMode.Stretch,
            )

        count = len(keys)
        self._app_keys_summary.setText(
            f"{count} Chromium app key(s) found." if count else "No Chromium app keys found."
        )

    def _load_summary(self, conn: sqlite3.Connection) -> None:
        from core.database.helpers.dpapi import get_decrypt_summary

        summary = get_decrypt_summary(conn, self.evidence_id)

        self._lbl_total.setText(str(summary.get("total", 0)))
        self._lbl_decrypted.setText(str(summary.get("decrypted", 0)))
        self._lbl_failed.setText(str(summary.get("failed", 0)))
        self._lbl_no_key.setText(str(summary.get("no_key", 0)))

        # Populate per-table breakdown using a simple model
        by_table: Dict[str, Dict[str, Any]] = summary.get("by_table", {})
        self._populate_breakdown(by_table)

        if summary.get("total", 0) > 0:
            self._summary_note.setText("")
        else:
            self._summary_note.setText(
                "Run a DPAPI decryption extraction to populate this tab."
            )

    def _populate_breakdown(self, by_table: Dict[str, Dict[str, Any]]) -> None:
        """Build a simple QAbstractTableModel for the breakdown table."""
        from PySide6.QtCore import QAbstractTableModel, QModelIndex

        _FRIENDLY = {
            "credentials": "Saved Credentials",
            "cookies": "Cookies",
            "credit_cards": "Credit Cards",
        }

        class _BreakdownModel(QAbstractTableModel):
            _HEADERS = ["Artifact", "Total", "Decrypted", "Failed", "No Key"]

            def __init__(self, rows, parent=None):
                super().__init__(parent)
                self._rows = rows

            def rowCount(self, parent=QModelIndex()):
                return len(self._rows) if not parent.isValid() else 0

            def columnCount(self, parent=QModelIndex()):
                return len(self._HEADERS)

            def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
                if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
                    return self._HEADERS[section]
                return None

            def data(self, index, role=Qt.ItemDataRole.DisplayRole):
                if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
                    return None
                r = self._rows[index.row()]
                return str(r[index.column()])

        rows = []
        for table_name, stats in sorted(by_table.items()):
            rows.append([
                _FRIENDLY.get(table_name, table_name),
                stats.get("total", 0),
                stats.get("decrypted", 0),
                stats.get("failed", 0),
                stats.get("no_key", 0),
            ])

        model = _BreakdownModel(rows, self)
        self._breakdown_table.setModel(model)
        header = self._breakdown_table.horizontalHeader()
        for i in range(model.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        if model.columnCount() > 0:
            header.setSectionResizeMode(
                model.columnCount() - 1,
                QHeaderView.ResizeMode.Stretch,
            )

    def _clear_all(self) -> None:
        self._users_model.clear()
        self._keys_model.clear()
        self._app_keys_model.clear()
        self._users_summary.setText("No Windows users loaded.")
        self._keys_summary.setText("Select a user to view master keys.")
        self._keys_label.setText("Master Keys")
        self._app_keys_summary.setText("No Chromium app keys loaded.")
        self._lbl_total.setText("0")
        self._lbl_decrypted.setText("0")
        self._lbl_failed.setText("0")
        self._lbl_no_key.setText("0")
        self._summary_note.setText("Run a DPAPI decryption extraction to populate this tab.")
        self._all_master_keys = []

    # -- Slots ----------------------------------------------------------------

    def _on_user_selected(self) -> None:
        indexes = self._users_table.selectionModel().selectedRows()
        if not indexes:
            self._keys_model.load(self._all_master_keys)
            self._keys_label.setText(f"Master Keys (all users — {len(self._all_master_keys)})")
            self._keys_summary.setText(
                f"{len(self._all_master_keys)} master key(s) total."
                if self._all_master_keys
                else "No master keys found."
            )
            return

        user = self._users_model.get_row_data(indexes[0])
        sid = user.get("sid", "")
        username = user.get("username", "")

        filtered = [k for k in self._all_master_keys if k.get("sid") == sid]
        self._keys_model.load(filtered)
        self._keys_label.setText(f"Master Keys for {username} ({sid}) — {len(filtered)}")
        self._keys_summary.setText(
            f"{len(filtered)} master key(s) for this user."
            if filtered
            else "No master keys for this user."
        )
