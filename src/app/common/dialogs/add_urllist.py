"""
Dialog for adding URL lists with metadata configuration.

Allows users to upload simple pattern files and configure metadata
(name, category, description, pattern type) before saving.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QButtonGroup,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QRadioButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class AddUrlListDialog(QDialog):
    """Dialog for configuring URL list metadata before import."""

    # Predefined categories for URL lists
    PREDEFINED_CATEGORIES = [
        "Gambling",
        "Gaming",
        "Social Media",
        "Adult Content",
        "Malicious",
        "Excluded",
        "Custom"
    ]

    def __init__(
        self,
        patterns: List[str],
        suggested_name: str = "",
        parent: Optional[QWidget] = None
    ):
        """
        Initialize dialog.

        Args:
            patterns: List of URL patterns from uploaded file
            suggested_name: Suggested name (from filename)
            parent: Parent widget
        """
        super().__init__(parent)
        self.patterns = patterns
        self.suggested_name = suggested_name

        # Result properties
        self.name = ""
        self.category = ""
        self.description = ""
        self.is_regex = False

        self.setWindowTitle("Add URL List")
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)

        self._build_ui()

    def _build_ui(self) -> None:
        """Build the dialog UI."""
        layout = QVBoxLayout()

        # Info label
        info_label = QLabel(
            "Configure metadata for the URL list. "
            "All fields are required."
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # Metadata form
        form_layout = QFormLayout()

        # Name field
        self.name_edit = QLineEdit()
        self.name_edit.setText(self.suggested_name)
        self.name_edit.setPlaceholderText("Enter a unique name (without .txt)")
        form_layout.addRow("List Name:", self.name_edit)

        # Category dropdown
        category_layout = QHBoxLayout()
        self.category_combo = QComboBox()
        self.category_combo.addItems(self.PREDEFINED_CATEGORIES)
        self.category_combo.setCurrentText("Custom")  # Default category
        self.category_combo.currentTextChanged.connect(self._on_category_changed)
        category_layout.addWidget(self.category_combo)

        # Custom category field (hidden by default)
        self.custom_category_edit = QLineEdit()
        self.custom_category_edit.setPlaceholderText("Enter custom category")
        self.custom_category_edit.setVisible(False)
        category_layout.addWidget(self.custom_category_edit)

        form_layout.addRow("Category:", category_layout)

        # Description field
        self.description_edit = QTextEdit()
        self.description_edit.setPlaceholderText(
            "Enter a description of what this URL list matches"
        )
        self.description_edit.setMaximumHeight(80)
        form_layout.addRow("Description:", self.description_edit)

        layout.addLayout(form_layout)

        # Pattern type group
        pattern_group = QGroupBox("Pattern Type")
        pattern_layout = QVBoxLayout()

        self.wildcard_radio = QRadioButton(
            "Wildcard Patterns (* and ? supported)"
        )
        self.wildcard_radio.setChecked(True)  # Default to wildcard
        pattern_layout.addWidget(self.wildcard_radio)

        self.regex_radio = QRadioButton("Regular Expressions")
        pattern_layout.addWidget(self.regex_radio)

        # Button group for radio buttons
        self.pattern_button_group = QButtonGroup(self)
        self.pattern_button_group.addButton(self.wildcard_radio, 0)
        self.pattern_button_group.addButton(self.regex_radio, 1)

        pattern_group.setLayout(pattern_layout)
        layout.addWidget(pattern_group)

        # Pattern preview
        preview_label = QLabel(
            f"Pattern Preview ({len(self.patterns)} patterns found):"
        )
        layout.addWidget(preview_label)

        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setMaximumHeight(150)

        # Show first 15 patterns
        preview_patterns = self.patterns[:15]
        preview_text = "\n".join(preview_patterns)
        if len(self.patterns) > 15:
            preview_text += f"\n... and {len(self.patterns) - 15} more patterns"
        self.preview_text.setPlainText(preview_text)

        layout.addWidget(self.preview_text)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.setLayout(layout)

    def _on_category_changed(self, category: str) -> None:
        """Handle category selection change."""
        is_custom = category == "Custom"
        self.custom_category_edit.setVisible(is_custom)
        if is_custom:
            self.custom_category_edit.setFocus()

    def _on_accept(self) -> None:
        """Validate and accept dialog."""
        # Validate name
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(
                self,
                "Validation Error",
                "List name is required."
            )
            self.name_edit.setFocus()
            return

        # Validate name doesn't contain invalid characters
        if any(c in name for c in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']):
            QMessageBox.warning(
                self,
                "Validation Error",
                "List name contains invalid characters."
            )
            self.name_edit.setFocus()
            return

        # Validate category
        category = self.category_combo.currentText()
        if category == "Custom":
            category = self.custom_category_edit.text().strip()
            if not category:
                QMessageBox.warning(
                    self,
                    "Validation Error",
                    "Custom category is required."
                )
                self.custom_category_edit.setFocus()
                return

        # Validate description
        description = self.description_edit.toPlainText().strip()
        if not description:
            QMessageBox.warning(
                self,
                "Validation Error",
                "Description is required."
            )
            self.description_edit.setFocus()
            return

        # Store results
        self.name = name
        self.category = category
        self.description = description
        self.is_regex = self.regex_radio.isChecked()

        self.accept()
