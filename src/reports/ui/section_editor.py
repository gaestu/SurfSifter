"""Section editor dialog for creating/editing custom report sections."""

import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QComboBox,
    QLabel,
    QLineEdit,
    QTextEdit,
    QPushButton,
    QToolBar,
    QWidget,
    QMessageBox,
    QGroupBox,
    QListWidget,
    QListWidgetItem,
    QFrame,
)
from PySide6.QtGui import QFont, QTextCharFormat, QAction, QTextListFormat
from PySide6.QtCore import Qt, Signal

from .module_picker import ModulePickerDialog
from ..modules import ModuleRegistry


class SectionEditorDialog(QDialog):
    """Dialog for creating or editing a custom report section.

    Supports basic text formatting: bold, italic, underline.
    Allows adding report modules with configurable filters.
    """

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        *,
        title: str = "",
        content: str = "",
        modules: Optional[List[Dict[str, Any]]] = None,
        text_blocks: Optional[List[Dict[str, Any]]] = None,
        edit_mode: bool = False,
        db_conn: Optional[sqlite3.Connection] = None,
    ):
        """Initialize the section editor dialog.

        Args:
            parent: Parent widget
            title: Initial title text
            content: Initial content (HTML)
            modules: List of module configs [{"module_id": str, "config": dict}, ...]
            text_blocks: Available reusable text blocks [{"title": str, "content": str}, ...]
            edit_mode: True if editing existing section, False for new section
            db_conn: Optional SQLite connection for dynamic module filter options
        """
        super().__init__(parent)

        self._edit_mode = edit_mode
        self._modules: List[Dict[str, Any]] = modules or []
        self._text_blocks: List[Dict[str, Any]] = text_blocks or []
        self._registry = ModuleRegistry()
        self._db_conn = db_conn

        self.setWindowTitle("Edit Section" if edit_mode else "Add Section")
        self.setMinimumSize(550, 550)
        self.resize(650, 600)

        self._setup_ui()

        # Set initial values
        self._title_input.setText(title)
        if content:
            self._content_edit.setHtml(content)

        # Populate modules list
        self._refresh_modules_list()

    def _setup_ui(self) -> None:
        """Setup the dialog UI."""
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        # Optional reusable text blocks selector
        text_block_layout = QHBoxLayout()
        text_block_label = QLabel("From Text Block:")
        text_block_label.setFixedWidth(95)
        text_block_layout.addWidget(text_block_label)

        self._text_block_combo = QComboBox()
        self._text_block_combo.addItem("Select text block...", None)
        for block in self._text_blocks:
            title = str(block.get("title", "")).strip()
            if not title:
                continue
            self._text_block_combo.addItem(title, block)
        self._text_block_combo.currentIndexChanged.connect(self._on_text_block_selected)
        self._text_block_combo.setEnabled(bool(self._text_blocks))
        text_block_layout.addWidget(self._text_block_combo)
        layout.addLayout(text_block_layout)

        # Title section
        title_layout = QHBoxLayout()
        title_label = QLabel("Title:")
        title_label.setFixedWidth(60)
        title_layout.addWidget(title_label)

        self._title_input = QLineEdit()
        self._title_input.setPlaceholderText("Enter section title (required)...")
        title_layout.addWidget(self._title_input)
        layout.addLayout(title_layout)

        # Content section with formatting toolbar
        content_label = QLabel("Content (optional):")
        layout.addWidget(content_label)

        # Rich text editor (create BEFORE toolbar so toolbar can connect signals)
        self._content_edit = QTextEdit()
        self._content_edit.setPlaceholderText("Enter section content...")
        self._content_edit.setMinimumHeight(150)

        # Formatting toolbar
        toolbar = self._build_formatting_toolbar()
        layout.addWidget(toolbar)

        # Add text editor after toolbar
        layout.addWidget(self._content_edit)

        # Modules section
        modules_group = self._build_modules_section()
        layout.addWidget(modules_group)

        # Button row
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)

        save_btn = QPushButton("Save")
        save_btn.setDefault(True)
        save_btn.clicked.connect(self._on_save)
        button_layout.addWidget(save_btn)

        layout.addLayout(button_layout)

    def _build_modules_section(self) -> QGroupBox:
        """Build the modules management section."""
        group = QGroupBox("Modules")
        group_layout = QVBoxLayout(group)
        group_layout.setContentsMargins(8, 12, 8, 8)
        group_layout.setSpacing(8)

        # Module list
        self._modules_list = QListWidget()
        self._modules_list.setMinimumHeight(80)
        self._modules_list.setMaximumHeight(150)
        self._modules_list.itemDoubleClicked.connect(self._on_edit_module)
        self._modules_list.currentRowChanged.connect(self._on_module_selection_changed)
        group_layout.addWidget(self._modules_list)

        # Module buttons row
        btn_layout = QHBoxLayout()

        add_module_btn = QPushButton("+ Add Module")
        add_module_btn.setStyleSheet("color: #e67e22; font-weight: bold;")
        add_module_btn.clicked.connect(self._on_add_module)
        btn_layout.addWidget(add_module_btn)

        btn_layout.addStretch()

        self._move_up_btn = QPushButton("↑")
        self._move_up_btn.setFixedWidth(30)
        self._move_up_btn.setToolTip("Move module up")
        self._move_up_btn.clicked.connect(self._on_move_module_up)
        btn_layout.addWidget(self._move_up_btn)

        self._move_down_btn = QPushButton("↓")
        self._move_down_btn.setFixedWidth(30)
        self._move_down_btn.setToolTip("Move module down")
        self._move_down_btn.clicked.connect(self._on_move_module_down)
        btn_layout.addWidget(self._move_down_btn)

        self._edit_module_btn = QPushButton("Edit")
        self._edit_module_btn.clicked.connect(self._on_edit_module)
        btn_layout.addWidget(self._edit_module_btn)

        self._remove_module_btn = QPushButton("Remove")
        self._remove_module_btn.clicked.connect(self._on_remove_module)
        btn_layout.addWidget(self._remove_module_btn)

        group_layout.addLayout(btn_layout)

        return group

    def _refresh_modules_list(self) -> None:
        """Refresh the modules list widget from internal state."""
        self._modules_list.clear()

        for i, mod_data in enumerate(self._modules):
            module_id = mod_data.get("module_id", "")
            config = mod_data.get("config", {})

            # Get module metadata
            module = self._registry.get_module(module_id)
            if module:
                name = f"{module.metadata.icon} {module.metadata.name}"
                summary = module.format_config_summary(config)
            else:
                name = f"❓ {module_id}"
                summary = "(unknown module)"

            item = QListWidgetItem(f"{name}\n    {summary}")
            item.setData(Qt.UserRole, i)  # Store index
            self._modules_list.addItem(item)

        # Update button states
        has_selection = self._modules_list.currentRow() >= 0
        has_modules = len(self._modules) > 0
        self._edit_module_btn.setEnabled(has_selection)
        self._remove_module_btn.setEnabled(has_selection)
        self._move_up_btn.setEnabled(has_selection and self._modules_list.currentRow() > 0)
        self._move_down_btn.setEnabled(has_selection and self._modules_list.currentRow() < len(self._modules) - 1)

    def _on_module_selection_changed(self, current_row: int) -> None:
        """Handle module list selection change - update button states."""
        has_selection = current_row >= 0
        self._edit_module_btn.setEnabled(has_selection)
        self._remove_module_btn.setEnabled(has_selection)
        self._move_up_btn.setEnabled(has_selection and current_row > 0)
        self._move_down_btn.setEnabled(has_selection and current_row < len(self._modules) - 1)

    def _on_add_module(self) -> None:
        """Handle Add Module button click."""
        dialog = ModulePickerDialog(self, edit_mode=False, db_conn=self._db_conn)
        if dialog.exec() == QDialog.Accepted:
            module_id = dialog.get_module_id()
            config = dialog.get_config()

            if module_id:
                self._modules.append({
                    "module_id": module_id,
                    "config": config,
                })
                self._refresh_modules_list()

    def _on_edit_module(self) -> None:
        """Handle Edit Module button click or double-click."""
        row = self._modules_list.currentRow()
        if row < 0 or row >= len(self._modules):
            return

        mod_data = self._modules[row]
        dialog = ModulePickerDialog(
            self,
            module_id=mod_data.get("module_id"),
            config=mod_data.get("config", {}),
            edit_mode=True,
            db_conn=self._db_conn,
        )

        if dialog.exec() == QDialog.Accepted:
            self._modules[row] = {
                "module_id": dialog.get_module_id(),
                "config": dialog.get_config(),
            }
            self._refresh_modules_list()

    def _on_remove_module(self) -> None:
        """Handle Remove Module button click."""
        row = self._modules_list.currentRow()
        if row < 0 or row >= len(self._modules):
            return

        reply = QMessageBox.question(
            self,
            "Remove Module",
            "Are you sure you want to remove this module?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            del self._modules[row]
            self._refresh_modules_list()

    def _on_move_module_up(self) -> None:
        """Move selected module up in the list."""
        row = self._modules_list.currentRow()
        if row <= 0:
            return

        self._modules[row], self._modules[row - 1] = self._modules[row - 1], self._modules[row]
        self._refresh_modules_list()
        self._modules_list.setCurrentRow(row - 1)

    def _on_move_module_down(self) -> None:
        """Move selected module down in the list."""
        row = self._modules_list.currentRow()
        if row < 0 or row >= len(self._modules) - 1:
            return

        self._modules[row], self._modules[row + 1] = self._modules[row + 1], self._modules[row]
        self._refresh_modules_list()
        self._modules_list.setCurrentRow(row + 1)

    def _on_text_block_selected(self, index: int) -> None:
        """Apply selected text block content to title/body fields."""
        if index <= 0:
            return

        block = self._text_block_combo.currentData()
        if not isinstance(block, dict):
            return

        block_title = str(block.get("title", "")).strip()
        block_content = str(block.get("content", "")).strip()
        if not block_title and not block_content:
            self._text_block_combo.setCurrentIndex(0)
            return

        existing_title = self._title_input.text().strip()
        existing_content = self._content_edit.toPlainText().strip()

        if existing_title and existing_content:
            reply = QMessageBox.question(
                self,
                "Replace Existing Content?",
                "Title and content already contain text. Replace both with this text block?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply != QMessageBox.Yes:
                self._text_block_combo.setCurrentIndex(0)
                return
            self._title_input.setText(block_title)
            self._content_edit.setPlainText(block_content)
            self._text_block_combo.setCurrentIndex(0)
            return

        if not existing_title and block_title:
            self._title_input.setText(block_title)
        if not existing_content and block_content:
            self._content_edit.setPlainText(block_content)

        self._text_block_combo.setCurrentIndex(0)

    def _build_formatting_toolbar(self) -> QToolBar:
        """Build the text formatting toolbar."""
        toolbar = QToolBar()
        toolbar.setMovable(False)

        # Bold action
        bold_action = QAction("B", self)
        bold_action.setToolTip("Bold (Ctrl+B)")
        bold_action.setShortcut("Ctrl+B")
        bold_action.setCheckable(True)
        font = bold_action.font()
        font.setBold(True)
        bold_action.setFont(font)
        bold_action.triggered.connect(self._toggle_bold)
        toolbar.addAction(bold_action)
        self._bold_action = bold_action

        # Italic action
        italic_action = QAction("I", self)
        italic_action.setToolTip("Italic (Ctrl+I)")
        italic_action.setShortcut("Ctrl+I")
        italic_action.setCheckable(True)
        font = italic_action.font()
        font.setItalic(True)
        italic_action.setFont(font)
        italic_action.triggered.connect(self._toggle_italic)
        toolbar.addAction(italic_action)
        self._italic_action = italic_action

        # Underline action
        underline_action = QAction("U", self)
        underline_action.setToolTip("Underline (Ctrl+U)")
        underline_action.setShortcut("Ctrl+U")
        underline_action.setCheckable(True)
        font = underline_action.font()
        font.setUnderline(True)
        underline_action.setFont(font)
        underline_action.triggered.connect(self._toggle_underline)
        toolbar.addAction(underline_action)
        self._underline_action = underline_action

        toolbar.addSeparator()

        # Bullet list action
        bullet_action = QAction("• List", self)
        bullet_action.setToolTip("Bullet List")
        bullet_action.triggered.connect(self._insert_bullet_list)
        toolbar.addAction(bullet_action)

        # Update toolbar state when cursor moves
        self._content_edit.cursorPositionChanged.connect(self._update_format_actions)

        return toolbar

    def _toggle_bold(self) -> None:
        """Toggle bold formatting on selection."""
        fmt = QTextCharFormat()
        if self._content_edit.fontWeight() == QFont.Weight.Bold:
            fmt.setFontWeight(QFont.Weight.Normal)
        else:
            fmt.setFontWeight(QFont.Weight.Bold)
        self._merge_format(fmt)

    def _toggle_italic(self) -> None:
        """Toggle italic formatting on selection."""
        fmt = QTextCharFormat()
        fmt.setFontItalic(not self._content_edit.fontItalic())
        self._merge_format(fmt)

    def _toggle_underline(self) -> None:
        """Toggle underline formatting on selection."""
        fmt = QTextCharFormat()
        fmt.setFontUnderline(not self._content_edit.fontUnderline())
        self._merge_format(fmt)

    def _insert_bullet_list(self) -> None:
        """Insert or toggle bullet list at cursor."""
        cursor = self._content_edit.textCursor()
        current_list = cursor.currentList()

        if current_list:
            # Remove from list
            block_fmt = cursor.blockFormat()
            block_fmt.setIndent(0)
            cursor.setBlockFormat(block_fmt)
        else:
            # Create bullet list
            cursor.createList(QTextListFormat.ListDisc)

    def _merge_format(self, fmt: QTextCharFormat) -> None:
        """Apply format to current selection or cursor position."""
        cursor = self._content_edit.textCursor()
        if not cursor.hasSelection():
            cursor.select(cursor.WordUnderCursor)
        cursor.mergeCharFormat(fmt)
        self._content_edit.mergeCurrentCharFormat(fmt)

    def _update_format_actions(self) -> None:
        """Update toolbar button states based on current cursor format."""
        self._bold_action.setChecked(
            self._content_edit.fontWeight() == QFont.Weight.Bold
        )
        self._italic_action.setChecked(self._content_edit.fontItalic())
        self._underline_action.setChecked(self._content_edit.fontUnderline())

    def _on_save(self) -> None:
        """Validate and accept the dialog."""
        title = self.get_title()
        if not title:
            QMessageBox.warning(
                self,
                "Title Required",
                "Please enter a title for the section."
            )
            self._title_input.setFocus()
            return

        self.accept()

    def get_title(self) -> str:
        """Get the section title."""
        return self._title_input.text().strip()

    def get_content(self) -> str:
        """Get the section content as HTML."""
        return self._content_edit.toHtml()

    def get_plain_content(self) -> str:
        """Get the section content as plain text."""
        return self._content_edit.toPlainText()

    def get_modules(self) -> List[Dict[str, Any]]:
        """Get the configured modules list.

        Returns:
            List of module dictionaries with module_id and config keys
        """
        return self._modules.copy()
