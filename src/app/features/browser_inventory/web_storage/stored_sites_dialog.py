"""Stored site details dialog."""
from __future__ import annotations

from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QDesktopServices, QFont
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)


class StoredSiteDetailsDialog(QDialog):
    """Dialog showing storage summary and key previews for a site.

    Enhanced to show actual storage keys with forensic highlighting.
    """

    # Signal emitted when user wants to view all keys in Storage Keys tab
    view_all_keys_requested = Signal(str)  # origin

    # Keys that often contain forensically interesting data
    INTERESTING_KEYS = {
        "username", "user", "userid", "user_id", "email", "login",
        "session", "sessionid", "session_id", "token", "auth",
        "password", "pwd", "credential", "key", "secret",
        "account", "name", "id", "uid",
    }

    # Max keys to show per storage type
    MAX_PREVIEW_KEYS = 25

    def __init__(
        self,
        row_data: dict,
        db_manager=None,
        evidence_id: int = None,
        evidence_label: str = None,
        parent=None
    ):
        """
        Initialize stored site details dialog.

        Args:
            row_data: Site summary data dictionary
            db_manager: Database manager for fetching keys (optional)
            evidence_id: Evidence ID for fetching keys (optional)
            evidence_label: Evidence label for DB path (optional)
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self._origin = row_data.get("origin", "")

        self.setWindowTitle("Stored Site Details")
        self.setModal(True)
        self.resize(700, 550)

        self._setup_ui()

        # Load keys if we have database access
        if self.db_manager and self.evidence_id and self.evidence_label:
            self._load_keys()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Site origin header
        origin = self._origin or "Unknown"
        origin_label = QLabel(f"<b style='font-size: 14px;'>{origin}</b>")
        origin_label.setWordWrap(True)
        layout.addWidget(origin_label)

        # Summary section
        summary_group = QGroupBox("Storage Summary")
        summary_layout = QFormLayout(summary_group)

        # Storage counts with clickable numbers
        local_count = self.row_data.get("local_storage_count", 0)
        session_count = self.row_data.get("session_storage_count", 0)
        indexeddb_count = self.row_data.get("indexeddb_count", 0)
        cookie_count = self.row_data.get("cookie_count", 0)

        summary_layout.addRow("Local Storage:", QLabel(str(local_count)))
        summary_layout.addRow("Session Storage:", QLabel(str(session_count)))
        summary_layout.addRow("IndexedDB:", QLabel(str(indexeddb_count)))
        summary_layout.addRow("Cookies:", QLabel(str(cookie_count)))

        # Forensically interesting data
        tokens = self.row_data.get("token_count", 0)
        identifiers = self.row_data.get("identifier_count", 0)

        token_label = QLabel(str(tokens))
        if tokens > 0:
            token_label.setStyleSheet("color: red; font-weight: bold;")
        summary_layout.addRow("Auth Tokens:", token_label)

        id_label = QLabel(str(identifiers))
        if identifiers >= 5:
            id_label.setStyleSheet("color: orange; font-weight: bold;")
        summary_layout.addRow("Identifiers:", id_label)

        # Total
        total = self.row_data.get("total_keys", 0)
        total_label = QLabel(f"<b>{total}</b>")
        summary_layout.addRow("Total Keys:", total_label)

        layout.addWidget(summary_group)

        # Keys preview section (will be populated by _load_keys)
        self._keys_group = QGroupBox("Storage Keys Preview")
        keys_layout = QVBoxLayout(self._keys_group)

        # Local Storage table
        self._local_table = self._create_keys_table("Local Storage")
        keys_layout.addWidget(QLabel("<b>Local Storage</b>"))
        keys_layout.addWidget(self._local_table)

        # Session Storage table
        self._session_table = self._create_keys_table("Session Storage")
        keys_layout.addWidget(QLabel("<b>Session Storage</b>"))
        keys_layout.addWidget(self._session_table)

        # Placeholder if no DB access
        self._no_data_label = QLabel(
            "<i>Database access not available - showing counts only</i>"
        )
        self._no_data_label.setStyleSheet("color: gray;")
        keys_layout.addWidget(self._no_data_label)

        layout.addWidget(self._keys_group)

        # Buttons
        button_layout = QHBoxLayout()

        copy_btn = QPushButton("📋 Copy Origin")
        copy_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(origin)
        )
        button_layout.addWidget(copy_btn)

        open_btn = QPushButton("🌐 Open in Browser")
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(origin)))
        button_layout.addWidget(open_btn)

        # View All button (only if we have data)
        self._view_all_btn = QPushButton("📊 View All in Storage Keys Tab")
        self._view_all_btn.setToolTip("Open Storage Keys tab filtered to this origin")
        self._view_all_btn.clicked.connect(self._on_view_all_clicked)
        self._view_all_btn.setEnabled(False)  # Enable when keys loaded
        button_layout.addWidget(self._view_all_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)

    def _create_keys_table(self, name: str) -> QTableWidget:
        """Create a table widget for displaying keys."""
        table = QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(["Key", "Value", "Type", "Size"])
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setMaximumHeight(150)

        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Interactive)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        table.setColumnWidth(0, 180)

        table.hide()  # Hidden until data loaded
        return table

    def _load_keys(self) -> None:
        """Load storage keys from database."""
        try:
            from core.database import get_local_storage, get_session_storage

            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            # Exact origin match for filtering
            origin = self._origin

            # Load local storage keys
            local_rows = get_local_storage(
                conn, self.evidence_id,
                origin=origin,
                limit=self.MAX_PREVIEW_KEYS + 1,  # +1 to detect if more exist
            )
            # Filter for exact origin match (the helper uses LIKE)
            local_rows = [r for r in local_rows if r.get("origin") == origin]

            # Load session storage keys
            session_rows = get_session_storage(
                conn, self.evidence_id,
                origin=origin,
                limit=self.MAX_PREVIEW_KEYS + 1,
            )
            session_rows = [r for r in session_rows if r.get("origin") == origin]

            # Populate tables
            has_local = self._populate_table(
                self._local_table, local_rows[:self.MAX_PREVIEW_KEYS],
                len(local_rows) > self.MAX_PREVIEW_KEYS
            )
            has_session = self._populate_table(
                self._session_table, session_rows[:self.MAX_PREVIEW_KEYS],
                len(session_rows) > self.MAX_PREVIEW_KEYS
            )

            # Hide no-data label if we have keys
            if has_local or has_session:
                self._no_data_label.hide()
                self._view_all_btn.setEnabled(True)
            else:
                self._no_data_label.setText(
                    "<i>No storage keys found for this origin</i>"
                )

            # Show/hide tables based on content
            if has_local:
                self._local_table.show()
            if has_session:
                self._session_table.show()

        except Exception as e:
            self._no_data_label.setText(f"<i>Error loading keys: {e}</i>")

    def _populate_table(
        self,
        table: QTableWidget,
        rows: list,
        has_more: bool
    ) -> bool:
        """Populate a table with key-value rows.

        Returns True if any rows were added.
        """
        if not rows:
            return False

        # Sort by forensic interest (interesting keys first)
        def sort_key(r):
            key = (r.get("key") or "").lower()
            is_interesting = any(ik in key for ik in self.INTERESTING_KEYS)
            return (0 if is_interesting else 1, key)

        rows = sorted(rows, key=sort_key)

        table.setRowCount(len(rows))

        for i, row in enumerate(rows):
            key = row.get("key") or ""
            value = row.get("value") or ""
            value_type = row.get("value_type") or ""
            value_size = row.get("value_size") or len(value)

            # Truncate value for display
            display_value = value[:100] + "..." if len(value) > 100 else value

            # Check if forensically interesting
            is_interesting = any(
                ik in key.lower() for ik in self.INTERESTING_KEYS
            )

            # Create items
            key_item = QTableWidgetItem(key)
            value_item = QTableWidgetItem(display_value)
            type_item = QTableWidgetItem(value_type)
            size_item = QTableWidgetItem(str(value_size))

            # Highlight interesting keys
            if is_interesting:
                bold_font = QFont()
                bold_font.setBold(True)
                key_item.setFont(bold_font)
                key_item.setBackground(Qt.yellow)
                value_item.setBackground(Qt.yellow)

            # Set tooltip for full value
            if len(value) > 100:
                value_item.setToolTip(value[:500] + ("..." if len(value) > 500 else ""))

            table.setItem(i, 0, key_item)
            table.setItem(i, 1, value_item)
            table.setItem(i, 2, type_item)
            table.setItem(i, 3, size_item)

        return True

    def _on_view_all_clicked(self) -> None:
        """Handle View All button click."""
        self.view_all_keys_requested.emit(self._origin)
        self.accept()  # Close dialog
