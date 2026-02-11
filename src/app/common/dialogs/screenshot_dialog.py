"""
Screenshot capture dialog for forensic documentation.

This dialog allows investigators to add metadata (title, caption, notes)
before saving a screenshot captured from the sandbox browser.

Initial implementation.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QSizePolicy,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from core.database.helpers import get_sequences, insert_screenshot
from app.features.screenshots.storage import ScreenshotMetadata, save_screenshot

logger = logging.getLogger(__name__)

__all__ = ["ScreenshotCaptureDialog"]


class ScreenshotCaptureDialog(QDialog):
    """
    Dialog for capturing screenshots with investigator annotations.

    Shows a preview of the captured image and allows the investigator
    to add title, caption, notes, and optionally assign to a sequence.
    """

    def __init__(
        self,
        pixmap: QPixmap,
        captured_url: str,
        forensic_context,  # ForensicContext - avoid circular import
        parent: Optional[QWidget] = None,
        *,
        existing_screenshot: Optional[dict] = None,
    ):
        """
        Initialize the screenshot capture dialog.

        Args:
            pixmap: The captured screenshot as QPixmap
            captured_url: URL that was displayed when screenshot was taken
            forensic_context: ForensicContext with evidence info and db connection
            parent: Parent widget
            existing_screenshot: For edit mode, the existing screenshot record
        """
        super().__init__(parent)

        self.pixmap = pixmap
        self.captured_url = captured_url
        self.forensic_context = forensic_context
        self.existing_screenshot = existing_screenshot
        self._saved_screenshot_id: Optional[int] = None

        self._is_edit_mode = existing_screenshot is not None

        self.setWindowTitle("📷 Edit Screenshot" if self._is_edit_mode else "📷 Save Screenshot")
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)

        self._init_ui()
        self._load_sequences()

        if self._is_edit_mode:
            self._populate_from_existing()

    def _init_ui(self) -> None:
        """Initialize the user interface."""
        layout = QVBoxLayout(self)

        # Preview section
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout(preview_group)

        self.preview_label = QLabel()
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_label.setMinimumHeight(200)
        self.preview_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        # Scale pixmap to fit preview area while maintaining aspect ratio
        if not self.pixmap.isNull():
            scaled = self.pixmap.scaled(
                550, 300,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )
            self.preview_label.setPixmap(scaled)

        preview_layout.addWidget(self.preview_label)

        # Show dimensions
        if not self.pixmap.isNull():
            dims_label = QLabel(f"Size: {self.pixmap.width()} × {self.pixmap.height()} pixels")
            dims_label.setStyleSheet("color: gray; font-size: 11px;")
            dims_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            preview_layout.addWidget(dims_label)

        layout.addWidget(preview_group)

        # URL display
        if self.captured_url:
            url_layout = QHBoxLayout()
            url_label = QLabel("URL:")
            url_label.setStyleSheet("font-weight: bold;")
            self.url_display = QLineEdit(self.captured_url)
            self.url_display.setReadOnly(True)
            self.url_display.setStyleSheet("background-color: #f5f5f5;")
            url_layout.addWidget(url_label)
            url_layout.addWidget(self.url_display)
            layout.addLayout(url_layout)

        # Metadata form
        form_group = QGroupBox("Metadata")
        form_layout = QFormLayout(form_group)

        # Title
        self.title_edit = QLineEdit()
        self.title_edit.setPlaceholderText("Short title for report headers (e.g., 'Login page')")
        self.title_edit.setMaxLength(200)
        form_layout.addRow("Title:", self.title_edit)

        # Caption
        self.caption_edit = QPlainTextEdit()
        self.caption_edit.setPlaceholderText(
            "Description shown under image in report.\n"
            "Describe what the screenshot shows and why it's significant."
        )
        self.caption_edit.setMaximumHeight(80)
        form_layout.addRow("Caption:", self.caption_edit)

        # Notes
        self.notes_edit = QPlainTextEdit()
        self.notes_edit.setPlaceholderText(
            "Internal notes (not shown in report).\n"
            "Add any observations or details for your reference."
        )
        self.notes_edit.setMaximumHeight(60)
        form_layout.addRow("Notes:", self.notes_edit)

        layout.addWidget(form_group)

        # Sequence section
        seq_group = QGroupBox("Sequence (Optional)")
        seq_layout = QFormLayout(seq_group)

        # Sequence combo
        seq_row = QHBoxLayout()
        self.sequence_combo = QComboBox()
        self.sequence_combo.setMinimumWidth(200)
        self.sequence_combo.setEditable(True)
        self.sequence_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.sequence_combo.lineEdit().setPlaceholderText("Select or type new sequence name")
        seq_row.addWidget(self.sequence_combo)

        # Sequence order
        seq_row.addWidget(QLabel("Order:"))
        self.sequence_order_spin = QSpinBox()
        self.sequence_order_spin.setMinimum(0)
        self.sequence_order_spin.setMaximum(9999)
        self.sequence_order_spin.setValue(0)
        self.sequence_order_spin.setToolTip("Position within the sequence (0 = first)")
        seq_row.addWidget(self.sequence_order_spin)
        seq_row.addStretch()

        seq_layout.addRow("Sequence:", seq_row)

        help_label = QLabel(
            "Group related screenshots (e.g., 'login_flow', 'payment_process') "
            "for organized report sections."
        )
        help_label.setStyleSheet("color: gray; font-size: 11px;")
        help_label.setWordWrap(True)
        seq_layout.addRow("", help_label)

        layout.addWidget(seq_group)

        # Buttons
        self.button_box = QDialogButtonBox()
        self.save_btn = self.button_box.addButton(
            "💾 Save Changes" if self._is_edit_mode else "💾 Save Screenshot",
            QDialogButtonBox.ButtonRole.AcceptRole
        )
        self.cancel_btn = self.button_box.addButton(QDialogButtonBox.StandardButton.Cancel)

        self.button_box.accepted.connect(self._on_save)
        self.button_box.rejected.connect(self.reject)

        layout.addWidget(self.button_box)

    def _load_sequences(self) -> None:
        """Load existing sequences from database."""
        try:
            sequences = get_sequences(
                self.forensic_context.db_conn,
                self.forensic_context.evidence_id
            )
            self.sequence_combo.clear()
            self.sequence_combo.addItem("")  # Empty option for no sequence
            for seq in sequences:
                self.sequence_combo.addItem(seq)
        except Exception as e:
            logger.warning("Failed to load sequences: %s", e)

    def _populate_from_existing(self) -> None:
        """Populate form fields from existing screenshot (edit mode)."""
        if not self.existing_screenshot:
            return

        self.title_edit.setText(self.existing_screenshot.get("title") or "")
        self.caption_edit.setPlainText(self.existing_screenshot.get("caption") or "")
        self.notes_edit.setPlainText(self.existing_screenshot.get("notes") or "")

        seq_name = self.existing_screenshot.get("sequence_name") or ""
        idx = self.sequence_combo.findText(seq_name)
        if idx >= 0:
            self.sequence_combo.setCurrentIndex(idx)
        else:
            self.sequence_combo.setCurrentText(seq_name)

        self.sequence_order_spin.setValue(self.existing_screenshot.get("sequence_order", 0))

    def _on_save(self) -> None:
        """Handle save button click."""
        title = self.title_edit.text().strip()
        caption = self.caption_edit.toPlainText().strip()
        notes = self.notes_edit.toPlainText().strip()
        sequence_name = self.sequence_combo.currentText().strip() or None
        sequence_order = self.sequence_order_spin.value()

        # Title is required
        if not title:
            QMessageBox.warning(
                self,
                "Title Required",
                "Please enter a title for the screenshot.\n"
                "This is used as the heading in reports."
            )
            self.title_edit.setFocus()
            return

        # Caption is required
        if not caption:
            QMessageBox.warning(
                self,
                "Caption Required",
                "Please enter a caption for the screenshot.\n"
                "This describes what the screenshot shows."
            )
            self.caption_edit.setFocus()
            return

        if self._is_edit_mode:
            self._save_edit(title, caption, notes, sequence_name, sequence_order)
        else:
            self._save_new(title, caption, notes, sequence_name, sequence_order)

    def _save_new(
        self,
        title: str,
        caption: str,
        notes: str,
        sequence_name: Optional[str],
        sequence_order: int,
    ) -> None:
        """Save a new screenshot."""
        try:
            # Save pixmap to disk
            metadata = save_screenshot(
                self.pixmap,
                self.forensic_context.workspace_path,
                self.forensic_context.evidence_label,
                self.forensic_context.evidence_id,
            )

            # Insert database record
            now_utc = datetime.now(timezone.utc).isoformat()
            screenshot_id = insert_screenshot(
                self.forensic_context.db_conn,
                self.forensic_context.evidence_id,
                metadata.dest_path,
                metadata.filename,
                captured_url=self.captured_url,
                size_bytes=metadata.size_bytes,
                width=metadata.width,
                height=metadata.height,
                md5=metadata.md5,
                sha256=metadata.sha256,
                title=title,
                caption=caption,
                notes=notes or None,
                sequence_name=sequence_name,
                sequence_order=sequence_order,
                source="sandbox",
                captured_at_utc=now_utc,
            )

            self._saved_screenshot_id = screenshot_id
            logger.info("Saved screenshot %d: %s", screenshot_id, metadata.dest_path)

            self.accept()

        except Exception as e:
            logger.error("Failed to save screenshot: %s", e)
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to save screenshot:\n{e}"
            )

    def _save_edit(
        self,
        title: str,
        caption: str,
        notes: str,
        sequence_name: Optional[str],
        sequence_order: int,
    ) -> None:
        """Save changes to existing screenshot."""
        try:
            from core.database.helpers import update_screenshot

            success = update_screenshot(
                self.forensic_context.db_conn,
                self.forensic_context.evidence_id,
                self.existing_screenshot["id"],
                title=title,
                caption=caption,
                notes=notes or None,
                sequence_name=sequence_name,
                sequence_order=sequence_order,
            )

            if success:
                logger.info("Updated screenshot %d", self.existing_screenshot["id"])
                self.accept()
            else:
                QMessageBox.warning(
                    self,
                    "Update Failed",
                    "Failed to update screenshot. It may have been deleted."
                )

        except Exception as e:
            logger.error("Failed to update screenshot: %s", e)
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to update screenshot:\n{e}"
            )

    @property
    def saved_screenshot_id(self) -> Optional[int]:
        """Get the ID of the saved screenshot (if save was successful)."""
        return self._saved_screenshot_id
