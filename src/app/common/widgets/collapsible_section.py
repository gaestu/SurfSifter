"""
Collapsible Section Widget for Extractors Tab.

Provides a collapsible section with:
- Header with icon, title, count, and Run All button
- Show/hide toggle for section content
- Vertical layout for extractor widgets
"""

from typing import Optional, Callable
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QFrame,
    QSizePolicy,
)
from PySide6.QtGui import QFont


class CollapsibleSection(QWidget):
    """
    A collapsible section widget with header and content area.

    Features:
    - Clickable header to expand/collapse
    - Icon and title display
    - Extractor count in parentheses
    - "Run All" button in header
    - Smooth show/hide of content

    Signals:
        run_all_clicked: Emitted when Run All button is clicked
        toggled(bool): Emitted when section is expanded/collapsed (True=expanded)
    """

    run_all_clicked = Signal()
    toggled = Signal(bool)

    def __init__(
        self,
        title: str,
        icon: str = "",
        count: int = 0,
        collapsed: bool = True,
        show_run_all: bool = True,
        parent: Optional[QWidget] = None,
    ):
        """
        Initialize the collapsible section.

        Args:
            title: Section title text.
            icon: Emoji icon to display before title.
            count: Number of items in section (shown in parentheses).
            collapsed: Initial collapsed state (True = collapsed).
            show_run_all: Whether to show the Run All button.
            parent: Parent widget.
        """
        super().__init__(parent)

        self._title = title
        self._icon = icon
        self._count = count
        self._collapsed = collapsed
        self._show_run_all = show_run_all

        self._setup_ui()
        self._update_toggle_button()

        # Set initial state
        self._content_widget.setVisible(not collapsed)

    def _setup_ui(self):
        """Build the widget UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # === Header Frame ===
        self._header_frame = QFrame()
        self._header_frame.setFrameShape(QFrame.StyledPanel)
        self._header_frame.setStyleSheet("""
            QFrame {
                background-color: palette(alternate-base);
                border: 1px solid palette(mid);
                border-radius: 4px;
            }
            QFrame:hover {
                background-color: palette(midlight);
            }
        """)
        self._header_frame.setCursor(Qt.PointingHandCursor)

        header_layout = QHBoxLayout(self._header_frame)
        header_layout.setContentsMargins(10, 8, 10, 8)

        # Toggle button (arrow)
        self._toggle_btn = QPushButton()
        self._toggle_btn.setFlat(True)
        self._toggle_btn.setFixedSize(24, 24)
        self._toggle_btn.setStyleSheet("QPushButton { border: none; font-size: 12pt; }")
        self._toggle_btn.clicked.connect(self.toggle)
        header_layout.addWidget(self._toggle_btn)

        # Title with icon and count
        title_text = f"{self._icon} {self._title}" if self._icon else self._title
        if self._count > 0:
            title_text += f" ({self._count})"

        self._title_label = QLabel(title_text)
        title_font = QFont()
        title_font.setBold(True)
        title_font.setPointSize(10)
        self._title_label.setFont(title_font)
        self._title_label.setCursor(Qt.PointingHandCursor)
        header_layout.addWidget(self._title_label, 1)

        # Run All button
        if self._show_run_all:
            self._run_all_btn = QPushButton("▶️ Run All")
            self._run_all_btn.setToolTip(f"Extract and ingest all extractors in {self._title}")
            self._run_all_btn.setMaximumWidth(100)
            self._run_all_btn.setStyleSheet("""
                QPushButton {
                    background-color: #4CAF50;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    padding: 4px 12px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #45a049;
                }
                QPushButton:pressed {
                    background-color: #3d8b40;
                }
                QPushButton:disabled {
                    background-color: #cccccc;
                    color: #666666;
                }
            """)
            self._run_all_btn.clicked.connect(self._on_run_all_clicked)
            header_layout.addWidget(self._run_all_btn)
        else:
            self._run_all_btn = None

        layout.addWidget(self._header_frame)

        # === Content Widget ===
        self._content_widget = QWidget()
        self._content_layout = QVBoxLayout(self._content_widget)
        self._content_layout.setContentsMargins(20, 5, 5, 5)  # Indent content
        self._content_layout.setSpacing(0)

        layout.addWidget(self._content_widget)

        # Make header clickable
        self._header_frame.mousePressEvent = self._on_header_clicked
        self._title_label.mousePressEvent = self._on_header_clicked

    def _update_toggle_button(self):
        """Update toggle button arrow based on collapsed state."""
        if self._collapsed:
            self._toggle_btn.setText("▶")
        else:
            self._toggle_btn.setText("▼")

    def _on_header_clicked(self, event):
        """Handle header click - toggle section."""
        self.toggle()

    def _on_run_all_clicked(self):
        """Handle Run All button click."""
        self.run_all_clicked.emit()

    def toggle(self):
        """Toggle collapsed/expanded state."""
        self._collapsed = not self._collapsed
        self._content_widget.setVisible(not self._collapsed)
        self._update_toggle_button()
        self.toggled.emit(not self._collapsed)

    def set_collapsed(self, collapsed: bool):
        """Set collapsed state explicitly."""
        if self._collapsed != collapsed:
            self._collapsed = collapsed
            self._content_widget.setVisible(not collapsed)
            self._update_toggle_button()
            self.toggled.emit(not collapsed)

    def is_collapsed(self) -> bool:
        """Return current collapsed state."""
        return self._collapsed

    def add_widget(self, widget: QWidget):
        """Add a widget to the content area."""
        self._content_layout.addWidget(widget)

    def content_layout(self) -> QVBoxLayout:
        """Return the content layout for adding widgets."""
        return self._content_layout

    def set_count(self, count: int):
        """Update the item count in the header."""
        self._count = count
        title_text = f"{self._icon} {self._title}" if self._icon else self._title
        if count > 0:
            title_text += f" ({count})"
        self._title_label.setText(title_text)

    def set_run_all_enabled(self, enabled: bool):
        """Enable or disable the Run All button."""
        if self._run_all_btn:
            self._run_all_btn.setEnabled(enabled)

    def set_empty_state(self, is_empty: bool, message: str = "No extractors in this section"):
        """
        Set empty state for the section.

        When empty, shows a grayed-out message and disables Run All.

        Args:
            is_empty: Whether the section is empty.
            message: Message to show when empty.
        """
        if is_empty:
            # Clear content and add grayed message
            while self._content_layout.count():
                item = self._content_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

            empty_label = QLabel(message)
            empty_label.setStyleSheet("color: gray; font-style: italic; padding: 10px;")
            self._content_layout.addWidget(empty_label)

            if self._run_all_btn:
                self._run_all_btn.setEnabled(False)
        else:
            if self._run_all_btn:
                self._run_all_btn.setEnabled(True)
