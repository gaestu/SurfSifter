from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QLineEdit,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.config.text_blocks import TextBlock, TextBlockImportResult, TextBlockStore


class TextBlocksTab(QWidget):
    """Preferences tab for managing reusable report text blocks."""

    def __init__(self, config_dir: Optional[Path], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._config_dir = Path(config_dir) if config_dir else None
        self._store: Optional[TextBlockStore] = TextBlockStore(self._config_dir)

        self._all_blocks: List[TextBlock] = []
        self._current_block_id: Optional[str] = None

        self._setup_ui()
        self._reload_blocks()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        info_label = QLabel(
            "Manage reusable plain-text snippets for report custom sections. "
            f"Stored in: {self._store.path}"
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: palette(mid);")
        layout.addWidget(info_label)

        header_layout = QHBoxLayout()
        header_layout.addStretch()

        self._import_btn = QPushButton("Import")
        self._import_btn.clicked.connect(self._on_import)
        header_layout.addWidget(self._import_btn)

        self._export_btn = QPushButton("Export")
        self._export_btn.clicked.connect(self._on_export)
        header_layout.addWidget(self._export_btn)

        layout.addLayout(header_layout)

        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Filter:"))
        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Search title, content, tags...")
        self._search_input.textChanged.connect(self._refresh_list)
        filter_layout.addWidget(self._search_input, 1)

        filter_layout.addWidget(QLabel("Tag:"))
        self._tag_combo = QComboBox()
        self._tag_combo.currentIndexChanged.connect(self._refresh_list)
        filter_layout.addWidget(self._tag_combo)

        layout.addLayout(filter_layout)

        content_layout = QHBoxLayout()
        content_layout.setSpacing(10)

        list_pane = QWidget()
        list_layout = QVBoxLayout(list_pane)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setSpacing(6)

        self._list_widget = QListWidget()
        self._list_widget.setSelectionMode(QListWidget.ExtendedSelection)
        self._list_widget.currentItemChanged.connect(self._on_selection_changed)
        self._list_widget.setMinimumWidth(260)
        list_layout.addWidget(self._list_widget, 1)

        self._empty_hint_label = QLabel(
            "No text blocks yet. Create your first text block to reuse section text."
        )
        self._empty_hint_label.setWordWrap(True)
        self._empty_hint_label.setStyleSheet("color: palette(mid);")
        list_layout.addWidget(self._empty_hint_label)

        self._create_first_btn = QPushButton("Create Your First Text Block")
        self._create_first_btn.clicked.connect(self._on_new)
        list_layout.addWidget(self._create_first_btn)

        content_layout.addWidget(list_pane, 1)

        editor_widget = QWidget()
        editor_form = QFormLayout(editor_widget)
        editor_form.setContentsMargins(0, 0, 0, 0)
        editor_form.setSpacing(8)

        self._title_input = QLineEdit()
        self._title_input.setPlaceholderText("Block title")
        editor_form.addRow("Title:", self._title_input)

        self._tags_input = QLineEdit()
        self._tags_input.setPlaceholderText("Comma-separated tags")
        editor_form.addRow("Tags:", self._tags_input)

        self._content_input = QTextEdit()
        self._content_input.setPlaceholderText("Block content (plain text)...")
        self._content_input.setMinimumHeight(220)
        editor_form.addRow("Content:", self._content_input)

        content_layout.addWidget(editor_widget, 2)
        layout.addLayout(content_layout, 1)

        action_layout = QHBoxLayout()

        self._new_btn = QPushButton("+ New Block")
        self._new_btn.clicked.connect(self._on_new)
        action_layout.addWidget(self._new_btn)

        self._delete_btn = QPushButton("Delete")
        self._delete_btn.clicked.connect(self._on_delete)
        self._delete_btn.setEnabled(False)
        action_layout.addWidget(self._delete_btn)

        action_layout.addStretch()

        self._save_btn = QPushButton("Save Changes")
        self._save_btn.clicked.connect(self._on_save)
        action_layout.addWidget(self._save_btn)

        layout.addLayout(action_layout)

    def _reload_blocks(self) -> None:
        if self._store is None:
            return

        try:
            self._all_blocks = self._store.load_blocks()
        except Exception as exc:
            self._all_blocks = []
            QMessageBox.warning(self, "Text Blocks", f"Failed to load text blocks: {exc}")

        self._refresh_tag_filter()
        self._refresh_list()
        self._update_empty_state()

    def _refresh_tag_filter(self) -> None:
        current_tag = self._tag_combo.currentData() if hasattr(self, "_tag_combo") else None

        self._tag_combo.blockSignals(True)
        self._tag_combo.clear()
        self._tag_combo.addItem("All Tags", "all")
        for tag in TextBlockStore.all_tags(self._all_blocks):
            self._tag_combo.addItem(tag, tag)

        index = self._tag_combo.findData(current_tag)
        self._tag_combo.setCurrentIndex(index if index >= 0 else 0)
        self._tag_combo.blockSignals(False)

    def _refresh_list(self) -> None:
        if self._store is None:
            return

        search = self._search_input.text().strip()
        selected_tag = self._tag_combo.currentData()
        previous_id = self._current_block_id

        filtered = TextBlockStore.filter_blocks(self._all_blocks, search=search, tag=selected_tag)

        self._list_widget.blockSignals(True)
        self._list_widget.clear()
        for block in filtered:
            subtitle = f"[{', '.join(block.tags)}]" if block.tags else ""
            item = QListWidgetItem(f"{block.title}\n{subtitle}".strip())
            item.setData(Qt.UserRole, block.id)
            self._list_widget.addItem(item)
        self._list_widget.blockSignals(False)

        restored = False
        if previous_id:
            for row in range(self._list_widget.count()):
                item = self._list_widget.item(row)
                if item.data(Qt.UserRole) == previous_id:
                    self._list_widget.setCurrentRow(row)
                    restored = True
                    break

        if not restored:
            if self._list_widget.count() > 0:
                self._list_widget.setCurrentRow(0)
            else:
                self._set_editor_state(None)
        self._update_empty_state()

    def _update_empty_state(self) -> None:
        is_empty = not self._all_blocks
        self._empty_hint_label.setVisible(is_empty)
        self._create_first_btn.setVisible(is_empty)

    def _set_editor_state(self, block: Optional[TextBlock]) -> None:
        self._current_block_id = block.id if block else None
        self._delete_btn.setEnabled(block is not None)

        if block is None:
            self._title_input.clear()
            self._tags_input.clear()
            self._content_input.clear()
            return

        self._title_input.setText(block.title)
        self._tags_input.setText(", ".join(block.tags))
        self._content_input.setPlainText(block.content)

    def _on_selection_changed(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:  # noqa: ARG002
        if current is None:
            self._set_editor_state(None)
            return

        block_id = current.data(Qt.UserRole)
        selected = next((block for block in self._all_blocks if block.id == block_id), None)
        self._set_editor_state(selected)

    def _collect_tags(self) -> List[str]:
        return [tag.strip() for tag in self._tags_input.text().split(",") if tag.strip()]

    def _on_new(self) -> None:
        self._list_widget.clearSelection()
        self._set_editor_state(None)
        self._title_input.setFocus()

    def _on_save(self) -> None:
        if self._store is None:
            return

        title = self._title_input.text().strip()
        content = self._content_input.toPlainText().strip()
        tags = self._collect_tags()

        if not title:
            QMessageBox.warning(self, "Text Blocks", "Title is required.")
            self._title_input.setFocus()
            return

        if not content:
            QMessageBox.warning(self, "Text Blocks", "Content is required.")
            self._content_input.setFocus()
            return

        saved_id: Optional[str] = None

        if self._current_block_id:
            updated = self._store.update_block(
                self._current_block_id,
                title=title,
                content=content,
                tags=tags,
            )
            if updated is None:
                QMessageBox.warning(self, "Text Blocks", "Selected block no longer exists.")
            else:
                saved_id = updated.id
        else:
            created = self._store.add_block(title=title, content=content, tags=tags)
            saved_id = created.id

        self._reload_blocks()
        if saved_id:
            self._select_block(saved_id)

    def _on_delete(self) -> None:
        if self._store is None or not self._current_block_id:
            return

        reply = QMessageBox.question(
            self,
            "Delete Text Block",
            "Are you sure you want to delete this text block?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self._store.delete_block(self._current_block_id)
        self._reload_blocks()

    def _on_export(self) -> None:
        if self._store is None:
            return

        if not self._all_blocks:
            QMessageBox.information(self, "Export Text Blocks", "No text blocks available to export.")
            return

        selected_ids = [
            item.data(Qt.UserRole)
            for item in self._list_widget.selectedItems()
            if item.data(Qt.UserRole)
        ]

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Text Blocks",
            "text_blocks_export.json",
            "JSON Files (*.json);;All Files (*)",
        )
        if not path:
            return

        export_path = Path(path)
        if export_path.suffix.lower() != ".json":
            export_path = export_path.with_suffix(".json")

        count = self._store.export_blocks(export_path, block_ids=selected_ids or None)
        scope = "selected" if selected_ids else "all"
        QMessageBox.information(
            self,
            "Export Text Blocks",
            f"Exported {count} {scope} text block(s) to:\n{export_path}",
        )

    def _on_import(self) -> None:
        if self._store is None:
            return

        path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Text Blocks",
            "",
            "JSON Files (*.json);;All Files (*)",
        )
        if not path:
            return

        duplicate_strategy = self._ask_duplicate_strategy()
        if duplicate_strategy is None:
            return

        try:
            result = self._store.import_blocks(Path(path), duplicate_strategy=duplicate_strategy)
        except Exception as exc:
            QMessageBox.warning(self, "Import Text Blocks", f"Import failed: {exc}")
            return

        self._reload_blocks()
        summary = self._format_import_summary(result)
        if result.skipped_invalid > 0:
            QMessageBox.warning(self, "Import Text Blocks", summary)
        else:
            QMessageBox.information(self, "Import Text Blocks", summary)

    def _ask_duplicate_strategy(self) -> Optional[str]:
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Question)
        dialog.setWindowTitle("Duplicate Titles")
        dialog.setText("How should duplicate titles be handled during import?")

        skip_btn = dialog.addButton("Skip", QMessageBox.AcceptRole)
        rename_btn = dialog.addButton("Rename", QMessageBox.ActionRole)
        overwrite_btn = dialog.addButton("Overwrite", QMessageBox.DestructiveRole)
        dialog.addButton(QMessageBox.Cancel)

        dialog.exec()
        clicked = dialog.clickedButton()

        if clicked == skip_btn:
            return "skip"
        if clicked == rename_btn:
            return "rename"
        if clicked == overwrite_btn:
            return "overwrite"
        return None

    def _format_import_summary(self, result: TextBlockImportResult) -> str:
        return (
            "Import completed.\n\n"
            f"Processed: {result.total_processed}\n"
            f"Imported: {result.imported}\n"
            f"Overwritten: {result.overwritten}\n"
            f"Renamed: {result.renamed}\n"
            f"Skipped duplicates: {result.skipped_duplicates}\n"
            f"Skipped invalid: {result.skipped_invalid}"
        )

    def _select_block(self, block_id: str) -> None:
        for row in range(self._list_widget.count()):
            item = self._list_widget.item(row)
            if item.data(Qt.UserRole) == block_id:
                self._list_widget.setCurrentRow(row)
                return
