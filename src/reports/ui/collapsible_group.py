"""Collapsible group box widget for report sections."""

from typing import Optional

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QFrame,
    QSizePolicy,
)
from PySide6.QtCore import Signal, Qt, QPropertyAnimation, QEasingCurve


class CollapsibleGroupBox(QFrame):
    """A group box that can be collapsed/expanded by clicking the header.

    When collapsed, only the header is visible.
    When expanded, the content area is shown.

    Signals:
        collapsed_changed: Emitted when collapse state changes (bool: is_collapsed)
    """

    collapsed_changed = Signal(bool)

    def __init__(
        self,
        title: str,
        parent: Optional[QWidget] = None,
        *,
        collapsed: bool = False,
    ):
        """Initialize the collapsible group box.

        Args:
            title: The group title shown in the header
            parent: Parent widget
            collapsed: Initial collapsed state
        """
        super().__init__(parent)

        self._title = title
        self._is_collapsed = collapsed
        self._content_widget: Optional[QWidget] = None

        self._setup_ui()

        # Apply initial state without animation
        if collapsed:
            self._content_container.setVisible(False)
            self._update_indicator()

    def _setup_ui(self) -> None:
        """Setup the widget UI."""
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet("""
            CollapsibleGroupBox {
                background-color: palette(window);
                border: 1px solid palette(mid);
                border-radius: 4px;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Header (clickable)
        self._header = QFrame()
        self._header.setStyleSheet("""
            QFrame {
                background-color: palette(button);
                border: none;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                padding: 6px 12px;
            }
            QFrame:hover {
                background-color: palette(midlight);
            }
        """)
        self._header.setCursor(Qt.PointingHandCursor)
        self._header.mousePressEvent = self._on_header_clicked

        header_layout = QHBoxLayout(self._header)
        header_layout.setContentsMargins(8, 6, 8, 6)
        header_layout.setSpacing(8)

        # Collapse indicator
        self._indicator = QLabel("▼")
        self._indicator.setFixedWidth(16)
        self._indicator.setStyleSheet("font-size: 10px; color: palette(text);")
        header_layout.addWidget(self._indicator)

        # Title
        self._title_label = QLabel(self._title)
        self._title_label.setStyleSheet("font-weight: bold;")
        header_layout.addWidget(self._title_label)

        header_layout.addStretch()

        layout.addWidget(self._header)

        # Content container
        self._content_container = QFrame()
        self._content_container.setStyleSheet("""
            QFrame {
                border: none;
                background-color: transparent;
            }
        """)
        self._content_layout = QVBoxLayout(self._content_container)
        self._content_layout.setContentsMargins(12, 12, 12, 12)
        self._content_layout.setSpacing(8)

        layout.addWidget(self._content_container)

    def _on_header_clicked(self, event) -> None:
        """Handle header click to toggle collapse state."""
        self.set_collapsed(not self._is_collapsed)

    def _update_indicator(self) -> None:
        """Update the collapse indicator arrow."""
        self._indicator.setText("▶" if self._is_collapsed else "▼")

    def set_collapsed(self, collapsed: bool) -> None:
        """Set the collapsed state.

        Args:
            collapsed: True to collapse, False to expand
        """
        if collapsed == self._is_collapsed:
            return

        self._is_collapsed = collapsed
        self._update_indicator()
        self._content_container.setVisible(not collapsed)
        self.collapsed_changed.emit(collapsed)

    def is_collapsed(self) -> bool:
        """Get the current collapsed state."""
        return self._is_collapsed

    def set_content_widget(self, widget: QWidget) -> None:
        """Set the content widget.

        Args:
            widget: The widget to show in the content area
        """
        # Remove old content if any
        if self._content_widget is not None:
            self._content_layout.removeWidget(self._content_widget)
            self._content_widget.setParent(None)

        self._content_widget = widget
        self._content_layout.addWidget(widget)

    def content_layout(self) -> QVBoxLayout:
        """Get the content layout to add widgets directly.

        Returns:
            The QVBoxLayout for the content area
        """
        return self._content_layout

    def set_title(self, title: str) -> None:
        """Update the group title.

        Args:
            title: New title text
        """
        self._title = title
        self._title_label.setText(title)

    def title(self) -> str:
        """Get the current title."""
        return self._title
