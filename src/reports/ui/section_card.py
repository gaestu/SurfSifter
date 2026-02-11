"""Section card widget for displaying custom report sections."""

from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import (
    QFrame,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QWidget,
    QTextBrowser,
    QSizePolicy,
)
from PySide6.QtCore import Signal, Qt

from ..modules import ModuleRegistry


class SectionCard(QFrame):
    """Card widget displaying a single custom report section.

    Shows title, preview of content, modules, and action buttons (edit, delete, move up/down).

    Signals:
        edit_requested: Emitted when edit button is clicked
        delete_requested: Emitted when delete button is clicked
        move_up_requested: Emitted when move up button is clicked
        move_down_requested: Emitted when move down button is clicked
    """

    edit_requested = Signal(int)  # section_id
    delete_requested = Signal(int)  # section_id
    move_up_requested = Signal(int)  # section_id
    move_down_requested = Signal(int)  # section_id

    def __init__(
        self,
        section_id: int,
        title: str,
        content: str = "",
        modules: Optional[List[Dict[str, Any]]] = None,
        *,
        is_first: bool = False,
        is_last: bool = False,
        parent: Optional[QWidget] = None,
    ):
        """Initialize the section card.

        Args:
            section_id: Database ID of the section
            title: Section title
            content: Section content (HTML)
            modules: List of module data dicts with module_id and config
            is_first: True if this is the first section (disables move up)
            is_last: True if this is the last section (disables move down)
            parent: Parent widget
        """
        super().__init__(parent)

        self._section_id = section_id
        self._title = title
        self._content = content
        self._modules = modules or []
        self._is_first = is_first
        self._is_last = is_last

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Setup the card UI."""
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Raised)
        self.setStyleSheet("""
            SectionCard {
                background-color: palette(base);
                border: 1px solid palette(mid);
                border-radius: 6px;
                padding: 8px;
            }
            SectionCard:hover {
                border-color: palette(highlight);
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # Header row: title + action buttons
        header_layout = QHBoxLayout()
        header_layout.setSpacing(8)

        # Title label
        self._title_label = QLabel(self._title)
        self._title_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        self._title_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        header_layout.addWidget(self._title_label)

        # Move up button
        self._move_up_btn = QPushButton("↑")
        self._move_up_btn.setToolTip("Move Up")
        self._move_up_btn.setFixedSize(28, 28)
        self._move_up_btn.setEnabled(not self._is_first)
        self._move_up_btn.clicked.connect(lambda: self.move_up_requested.emit(self._section_id))
        header_layout.addWidget(self._move_up_btn)

        # Move down button
        self._move_down_btn = QPushButton("↓")
        self._move_down_btn.setToolTip("Move Down")
        self._move_down_btn.setFixedSize(28, 28)
        self._move_down_btn.setEnabled(not self._is_last)
        self._move_down_btn.clicked.connect(lambda: self.move_down_requested.emit(self._section_id))
        header_layout.addWidget(self._move_down_btn)

        # Edit button
        edit_btn = QPushButton("✏️ Edit")
        edit_btn.setToolTip("Edit Section")
        edit_btn.clicked.connect(lambda: self.edit_requested.emit(self._section_id))
        header_layout.addWidget(edit_btn)

        # Delete button
        delete_btn = QPushButton("🗑️")
        delete_btn.setToolTip("Delete Section")
        delete_btn.setFixedSize(28, 28)
        delete_btn.clicked.connect(lambda: self.delete_requested.emit(self._section_id))
        header_layout.addWidget(delete_btn)

        layout.addLayout(header_layout)

        # Content preview (read-only)
        if self._content:
            self._content_browser = QTextBrowser()
            self._content_browser.setHtml(self._content)
            self._content_browser.setReadOnly(True)
            self._content_browser.setMaximumHeight(100)
            self._content_browser.setStyleSheet("""
                QTextBrowser {
                    background-color: palette(window);
                    border: 1px solid palette(mid);
                    border-radius: 4px;
                }
            """)
            layout.addWidget(self._content_browser)
        elif not self._modules:
            # Show placeholder only if no content AND no modules
            empty_label = QLabel("(No content)")
            empty_label.setStyleSheet("color: palette(mid); font-style: italic;")
            layout.addWidget(empty_label)

        # Modules preview
        if self._modules:
            modules_container = QFrame()
            modules_container.setStyleSheet("""
                QFrame {
                    background-color: palette(window);
                    border: 1px solid palette(mid);
                    border-radius: 4px;
                    padding: 4px;
                }
            """)
            modules_layout = QVBoxLayout(modules_container)
            modules_layout.setContentsMargins(8, 6, 8, 6)
            modules_layout.setSpacing(4)

            modules_header = QLabel(f"📦 Modules ({len(self._modules)})")
            modules_header.setStyleSheet("font-weight: bold; color: #e67e22;")
            modules_layout.addWidget(modules_header)

            registry = ModuleRegistry()
            for mod_data in self._modules:
                module_id = mod_data.get("module_id", "")
                config = mod_data.get("config", {})

                module = registry.get_module(module_id)
                if module:
                    name = f"{module.metadata.icon} {module.metadata.name}"
                    summary = module.format_config_summary(config)
                else:
                    name = f"❓ {module_id}"
                    summary = "(unknown module)"

                mod_label = QLabel(f"  {name}: {summary}")
                mod_label.setStyleSheet("color: palette(text); font-size: 11px;")
                mod_label.setWordWrap(True)
                modules_layout.addWidget(mod_label)

            layout.addWidget(modules_container)

    @property
    def section_id(self) -> int:
        """Get the section ID."""
        return self._section_id

    def update_position(self, is_first: bool, is_last: bool) -> None:
        """Update the move button states based on position.

        Args:
            is_first: True if this is now the first section
            is_last: True if this is now the last section
        """
        self._is_first = is_first
        self._is_last = is_last
        self._move_up_btn.setEnabled(not is_first)
        self._move_down_btn.setEnabled(not is_last)
