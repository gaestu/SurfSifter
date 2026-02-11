"""Session tab details dialog."""
from __future__ import annotations

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
)


class SessionDetailsDialog(QDialog):
    """Dialog showing full details for a session tab with navigation history."""

    def __init__(
        self,
        row_data: dict,
        db_manager=None,
        evidence_id: int = None,
        evidence_label: str = None,
        parent=None
    ):
        super().__init__(parent)
        self.row_data = row_data
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        # Data loaded from DB
        self._history_entries = []
        self._window_info = None

        self.setWindowTitle("Session Tab Details")
        self.setModal(True)
        self.resize(700, 550)

        # Load additional data from database
        self._load_additional_data()

        self._setup_ui()

    def _load_additional_data(self) -> None:
        """Load navigation history and window info from database."""
        if not self.db_manager or not self.evidence_id or not self.evidence_label:
            return

        import sqlite3
        from core.database import get_session_tab_history, get_session_window_by_id

        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Load navigation history for this tab
                tab_id = self.row_data.get("id")
                if tab_id:
                    self._history_entries = get_session_tab_history(
                        conn, self.evidence_id, tab_id=tab_id, limit=500
                    )

                # Load window info
                window_id = self.row_data.get("window_id")
                if window_id:
                    self._window_info = get_session_window_by_id(
                        conn, self.evidence_id, window_id
                    )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to load session details: {e}")

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Use tab widget if we have history or window info
        from PySide6.QtWidgets import QTabWidget, QWidget

        tabs = QTabWidget()

        # Tab 1: Basic Details
        details_widget = QWidget()
        details_layout = QVBoxLayout(details_widget)

        form = QFormLayout()

        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))
        window_id = self.row_data.get("window_id")
        tab_index = self.row_data.get("tab_index")
        form.addRow("Window ID:", QLabel("N/A" if window_id is None else str(window_id)))
        form.addRow("Tab Index:", QLabel("N/A" if tab_index is None else str(tab_index)))
        form.addRow("Pinned:", QLabel("Yes" if self.row_data.get("pinned") else "No"))
        group_id = self.row_data.get("group_id")
        form.addRow("Tab Group:", QLabel(str(group_id) if group_id else "None"))
        form.addRow("", QLabel(""))
        form.addRow("Navigation History:", QLabel(f"{len(self._history_entries)} entries"))
        form.addRow("Last Accessed:", QLabel(self.row_data.get("last_accessed_utc") or "N/A"))

        details_layout.addLayout(form)

        details_layout.addWidget(QLabel("Title:"))
        title_text = QTextEdit()
        title_text.setReadOnly(True)
        title_text.setMaximumHeight(40)
        title_text.setPlainText(self.row_data.get("title", ""))
        details_layout.addWidget(title_text)

        details_layout.addWidget(QLabel("URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(60)
        url_text.setPlainText(self.row_data.get("url", ""))
        details_layout.addWidget(url_text)

        # Source info
        source_path = self.row_data.get("source_path") or self.row_data.get("logical_path")
        if source_path:
            details_layout.addWidget(QLabel("Source File:"))
            source_text = QTextEdit()
            source_text.setReadOnly(True)
            source_text.setMaximumHeight(40)
            source_text.setPlainText(source_path)
            details_layout.addWidget(source_text)

        tabs.addTab(details_widget, "Details")

        # Tab 2: Navigation History
        if self._history_entries:
            history_widget = self._build_history_tab()
            tabs.addTab(history_widget, f"History ({len(self._history_entries)})")

        # Tab 3: Window Context
        if self._window_info:
            window_widget = self._build_window_tab()
            tabs.addTab(window_widget, "Window")

        layout.addWidget(tabs)

        # Buttons
        button_layout = QHBoxLayout()
        url = self.row_data.get("url", "")
        open_btn = QPushButton("Open URL")
        open_btn.setEnabled(bool(url))
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(url)))
        button_layout.addWidget(open_btn)
        copy_btn = QPushButton("Copy URL")
        copy_btn.setEnabled(bool(url))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(url))
        button_layout.addWidget(copy_btn)
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)

    def _build_history_tab(self) -> "QWidget":
        """Build navigation history table tab."""
        from PySide6.QtWidgets import QWidget

        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(4, 4, 4, 4)

        # Info label
        info_label = QLabel(
            "Navigation history shows all pages visited in this tab (back/forward navigation).\n"
            "Transition types indicate how each page was reached."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; font-size: 11px; margin-bottom: 8px;")
        layout.addWidget(info_label)

        # History table
        table = QTableWidget()
        table.setColumnCount(7)
        table.setHorizontalHeaderLabels([
            "Index", "URL", "Title", "Transition", "Timestamp", "Referrer", "POST"
        ])
        table.setRowCount(len(self._history_entries))
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setEditTriggers(QTableWidget.NoEditTriggers)

        for row_idx, entry in enumerate(self._history_entries):
            # Index
            nav_index = entry.get("nav_index", row_idx)
            item = QTableWidgetItem(str(nav_index))
            item.setTextAlignment(Qt.AlignCenter)
            table.setItem(row_idx, 0, item)

            # URL (full, no truncation)
            url = entry.get("url", "")
            url_item = QTableWidgetItem(url)
            url_item.setToolTip(url)
            table.setItem(row_idx, 1, url_item)

            # Title (full, no truncation)
            title = entry.get("title", "")
            title_item = QTableWidgetItem(title)
            title_item.setToolTip(title)
            table.setItem(row_idx, 2, title_item)

            # Transition type
            transition = entry.get("transition_type", "")
            trans_item = QTableWidgetItem(transition)
            # Color-code transition types
            if transition == "typed":
                trans_item.setForeground(Qt.darkGreen)
            elif transition == "link":
                trans_item.setForeground(Qt.darkBlue)
            elif transition == "form_submit":
                trans_item.setForeground(Qt.darkMagenta)
            elif transition == "reload":
                trans_item.setForeground(Qt.darkYellow)
            table.setItem(row_idx, 3, trans_item)

            # Timestamp
            timestamp = entry.get("timestamp_utc", "")
            if timestamp:
                timestamp = timestamp[:19]  # Trim to YYYY-MM-DDTHH:MM:SS
            table.setItem(row_idx, 4, QTableWidgetItem(timestamp))

            # Referrer URL (full, no truncation)
            referrer = entry.get("referrer_url", "")
            ref_item = QTableWidgetItem(referrer)
            ref_item.setToolTip(referrer)
            table.setItem(row_idx, 5, ref_item)

            # POST data indicator
            has_post = entry.get("has_post_data", 0)
            post_item = QTableWidgetItem("Yes" if has_post else "")
            if has_post:
                post_item.setForeground(Qt.darkRed)
                post_item.setToolTip("This navigation included form POST data")
            table.setItem(row_idx, 6, post_item)

        # All columns user-resizable (Interactive mode)
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(False)

        # Set reasonable initial widths
        table.setColumnWidth(0, 50)    # Index
        table.setColumnWidth(1, 350)   # URL
        table.setColumnWidth(2, 150)   # Title
        table.setColumnWidth(3, 90)    # Transition
        table.setColumnWidth(4, 145)   # Timestamp
        table.setColumnWidth(5, 300)   # Referrer
        table.setColumnWidth(6, 45)    # POST

        # Enable horizontal scrolling for long URLs
        table.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)

        layout.addWidget(table)

        # Copy selected URL button
        copy_layout = QHBoxLayout()
        copy_url_btn = QPushButton("Copy Selected URL")
        copy_url_btn.clicked.connect(lambda: self._copy_history_url(table))
        copy_layout.addWidget(copy_url_btn)
        copy_layout.addStretch()
        layout.addLayout(copy_layout)

        return widget

    def _copy_history_url(self, table: QTableWidget) -> None:
        """Copy selected history URL to clipboard."""
        rows = table.selectedItems()
        if rows:
            row_idx = rows[0].row()
            if row_idx < len(self._history_entries):
                url = self._history_entries[row_idx].get("url", "")
                QApplication.clipboard().setText(url)

    def _build_window_tab(self) -> "QWidget":
        """Build window context info tab."""
        from PySide6.QtWidgets import QWidget

        widget = QWidget()
        layout = QVBoxLayout(widget)

        form = QFormLayout()

        # Window ID
        form.addRow("Window ID:", QLabel(str(self._window_info.get("window_id", "N/A"))))

        # Window type
        window_type = self._window_info.get("window_type", "normal")
        type_label = QLabel(window_type.capitalize() if window_type else "Normal")
        if window_type == "popup":
            type_label.setStyleSheet("color: orange; font-weight: bold;")
        elif window_type == "devtools":
            type_label.setStyleSheet("color: purple; font-weight: bold;")
        form.addRow("Window Type:", type_label)

        # Show state
        show_state = self._window_info.get("show_state", "")
        form.addRow("Show State:", QLabel(show_state.capitalize() if show_state else "Normal"))

        # Session type
        session_type = self._window_info.get("session_type", "")
        session_label = QLabel(session_type.replace("_", " ").title() if session_type else "Current")
        if session_type == "last":
            session_label.setStyleSheet("color: #666; font-style: italic;")
        form.addRow("Session Type:", session_label)

        form.addRow("", QLabel(""))

        # Selected tab
        selected_tab = self._window_info.get("selected_tab_index")
        form.addRow("Selected Tab Index:", QLabel(str(selected_tab) if selected_tab is not None else "N/A"))

        form.addRow("", QLabel(""))

        # Window bounds
        bounds_x = self._window_info.get("bounds_x")
        bounds_y = self._window_info.get("bounds_y")
        bounds_w = self._window_info.get("bounds_width")
        bounds_h = self._window_info.get("bounds_height")

        if any(v is not None for v in [bounds_x, bounds_y, bounds_w, bounds_h]):
            form.addRow(QLabel("<b>Window Position & Size:</b>"), QLabel(""))
            form.addRow("Position (X, Y):", QLabel(f"{bounds_x or 0}, {bounds_y or 0}"))
            form.addRow("Size (W × H):", QLabel(f"{bounds_w or 0} × {bounds_h or 0}"))

        form.addRow("", QLabel(""))

        # Source info
        source_path = self._window_info.get("source_path") or self._window_info.get("logical_path")
        if source_path:
            form.addRow("Source File:", QLabel(""))

        layout.addLayout(form)

        if source_path:
            source_text = QTextEdit()
            source_text.setReadOnly(True)
            source_text.setMaximumHeight(50)
            source_text.setPlainText(source_path)
            layout.addWidget(source_text)

        layout.addStretch()

        return widget
