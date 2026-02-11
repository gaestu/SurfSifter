"""
Screenshots Tab Widget

Top-level tab for managing investigator-captured screenshots for forensic documentation.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QModelIndex, QSize
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.data.case_data import CaseDataAccess
from app.features.screenshots.models import ScreenshotsTableModel
from core.database.helpers import (
    delete_screenshot,
    get_screenshot,
    get_screenshot_count,
    get_screenshots,
    get_sequences,
    insert_screenshot,
)
from core.database.manager import slugify_label, DatabaseManager
from app.features.screenshots.storage import import_screenshot

logger = logging.getLogger(__name__)


class ScreenshotsTab(QWidget):
    """
    Screenshots management tab for forensic documentation.

    Allows viewing, uploading, editing, and deleting screenshots
    captured during investigation.
    """

    def __init__(
        self,
        case_data: Optional[CaseDataAccess] = None,
        case_folder: Optional[Path] = None,
        db_manager: Optional[DatabaseManager] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self.case_data = case_data
        self.case_folder = case_folder
        self._db_manager = db_manager
        self.evidence_id: Optional[int] = None
        self.evidence_label: Optional[str] = None

        self._init_ui()

    def _init_ui(self) -> None:
        """Initialize the user interface."""
        layout = QVBoxLayout(self)

        # Header with count and actions
        header_layout = QHBoxLayout()

        self.count_label = QLabel("📷 Screenshots (0)")
        self.count_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        header_layout.addWidget(self.count_label)

        header_layout.addStretch()

        # Upload button
        self.upload_btn = QPushButton("📤 Upload")
        self.upload_btn.setToolTip("Upload external screenshot (from phone, other browser, etc.)")
        self.upload_btn.clicked.connect(self._upload_screenshot)
        header_layout.addWidget(self.upload_btn)

        # Delete button
        self.delete_btn = QPushButton("🗑️ Delete Selected")
        self.delete_btn.setToolTip("Delete selected screenshots")
        self.delete_btn.clicked.connect(self._delete_selected)
        self.delete_btn.setEnabled(False)
        header_layout.addWidget(self.delete_btn)

        layout.addLayout(header_layout)

        # Filters row
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Sequence:"))
        self.sequence_combo = QComboBox()
        self.sequence_combo.setMinimumWidth(150)
        self.sequence_combo.addItem("All", None)
        self.sequence_combo.currentIndexChanged.connect(self._apply_filters)
        filter_layout.addWidget(self.sequence_combo)

        filter_layout.addWidget(QLabel("Source:"))
        self.source_combo = QComboBox()
        self.source_combo.addItem("All", None)
        self.source_combo.addItem("Sandbox", "sandbox")
        self.source_combo.addItem("Upload", "upload")
        self.source_combo.currentIndexChanged.connect(self._apply_filters)
        filter_layout.addWidget(self.source_combo)

        filter_layout.addStretch()

        # Selection actions
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all)
        filter_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("Deselect All")
        self.deselect_all_btn.clicked.connect(self._deselect_all)
        filter_layout.addWidget(self.deselect_all_btn)

        layout.addLayout(filter_layout)

        # Table view
        self.model = ScreenshotsTableModel(self)
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(False)  # Sorting handled by query
        self.table_view.verticalHeader().setVisible(False)

        # Column sizes
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)  # Checkbox
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)  # Thumbnail
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)  # Title
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)  # Caption
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Interactive)  # Sequence
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)  # Source
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.Interactive)  # Date

        self.table_view.setColumnWidth(0, 30)  # Checkbox
        self.table_view.setColumnWidth(1, 80)  # Thumbnail
        self.table_view.setColumnWidth(2, 200)  # Title
        self.table_view.setColumnWidth(5, 70)  # Source
        self.table_view.setColumnWidth(6, 140)  # Date

        self.table_view.setIconSize(QSize(60, 60))
        self.table_view.verticalHeader().setDefaultSectionSize(70)

        self.table_view.doubleClicked.connect(self._on_double_click)

        # Connect model changes to update delete button
        self.model.dataChanged.connect(self._update_delete_button)

        layout.addWidget(self.table_view)

        # Status bar
        self.status_label = QLabel("Select an evidence to view screenshots")
        self.status_label.setStyleSheet("color: gray; padding: 5px;")
        layout.addWidget(self.status_label)

    def set_case_data(self, case_data: CaseDataAccess) -> None:
        """Set the case data access object."""
        self.case_data = case_data

    def set_case_folder(self, case_folder: Path) -> None:
        """Set the case folder path."""
        self.case_folder = case_folder

    def set_database_manager(self, db_manager: DatabaseManager) -> None:
        """Set the database manager."""
        self._db_manager = db_manager

    def _get_conn(self):
        """Get the evidence database connection."""
        if not self._db_manager or not self.evidence_id:
            return None
        return self._db_manager.get_evidence_conn(self.evidence_id, self.evidence_label)

    def set_evidence(self, evidence_id: int, evidence_label: str) -> None:
        """Set the current evidence."""
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        # Update model paths for thumbnail loading
        if self.case_folder and evidence_label and evidence_id:
            self.model.set_paths(self.case_folder, evidence_label, evidence_id)
        self._refresh()

    def _refresh(self) -> None:
        """Refresh the screenshots list."""
        if not self.evidence_id or not self._db_manager:
            self.model.load_data([])
            self.count_label.setText("📷 Screenshots (0)")
            return

        try:
            conn = self._get_conn()
            if not conn:
                logger.warning("No evidence database connection for evidence %d", self.evidence_id)
                return

            # Ensure model has paths for thumbnail loading
            if self.case_folder and self.evidence_label:
                self.model.set_paths(self.case_folder, self.evidence_label, self.evidence_id)

            # Load sequences for filter
            self._load_sequences(conn)

            # Get filter values
            sequence_name = self.sequence_combo.currentData()
            source = self.source_combo.currentData()

            # Load screenshots
            screenshots = get_screenshots(
                conn,
                self.evidence_id,
                sequence_name=sequence_name,
                source=source,
                limit=10000,
            )

            self.model.load_data(screenshots)

            # Update count
            total = get_screenshot_count(conn, self.evidence_id)
            displayed = len(screenshots)
            if displayed < total:
                self.count_label.setText(f"📷 Screenshots ({displayed} of {total})")
            else:
                self.count_label.setText(f"📷 Screenshots ({total})")

            self.status_label.setText(f"Showing {displayed} screenshot(s)")

        except Exception as e:
            logger.error("Failed to load screenshots: %s", e)
            self.status_label.setText(f"Error loading screenshots: {e}")

    def _load_sequences(self, conn) -> None:
        """Load sequence filter options."""
        current_seq = self.sequence_combo.currentData()

        self.sequence_combo.blockSignals(True)
        self.sequence_combo.clear()
        self.sequence_combo.addItem("All", None)

        try:
            sequences = get_sequences(conn, self.evidence_id)
            for seq in sequences:
                self.sequence_combo.addItem(seq, seq)

            # Restore selection if still valid
            if current_seq:
                idx = self.sequence_combo.findData(current_seq)
                if idx >= 0:
                    self.sequence_combo.setCurrentIndex(idx)
        except Exception as e:
            logger.warning("Failed to load sequences: %s", e)
        finally:
            self.sequence_combo.blockSignals(False)

    def _apply_filters(self) -> None:
        """Apply filter changes."""
        self._refresh()

    def _update_delete_button(self) -> None:
        """Update delete button enabled state."""
        count = self.model.get_checked_count()
        self.delete_btn.setEnabled(count > 0)
        if count > 0:
            self.delete_btn.setText(f"🗑️ Delete ({count})")
        else:
            self.delete_btn.setText("🗑️ Delete Selected")

    def _select_all(self) -> None:
        """Select all screenshots."""
        self.model.select_all()
        self._update_delete_button()

    def _deselect_all(self) -> None:
        """Deselect all screenshots."""
        self.model.deselect_all()
        self._update_delete_button()

    def _on_double_click(self, index: QModelIndex) -> None:
        """Handle double-click to edit screenshot."""
        screenshot = self.model.get_screenshot(index)
        if not screenshot:
            return

        self._edit_screenshot(screenshot)

    def _edit_screenshot(self, screenshot: Dict[str, Any]) -> None:
        """Open edit dialog for a screenshot."""
        if not self._db_manager or not self.case_folder or not self.evidence_id:
            return

        from app.common.dialogs import ScreenshotCaptureDialog
        from app.common import ForensicContext

        conn = self._get_conn()
        if not conn:
            return

        # Load the image from disk
        slug = slugify_label(self.evidence_label, self.evidence_id)
        image_path = self.case_folder / "evidences" / slug / screenshot["dest_path"]

        if not image_path.exists():
            QMessageBox.warning(
                self,
                "File Not Found",
                f"Screenshot file not found:\n{image_path}"
            )
            return

        pixmap = QPixmap(str(image_path))
        if pixmap.isNull():
            QMessageBox.warning(
                self,
                "Load Error",
                f"Could not load image:\n{image_path}"
            )
            return

        forensic_context = ForensicContext(
            evidence_id=self.evidence_id,
            evidence_label=self.evidence_label,
            workspace_path=self.case_folder,
            db_conn=conn,
        )

        dialog = ScreenshotCaptureDialog(
            pixmap,
            screenshot.get("captured_url") or "",
            forensic_context,
            parent=self,
            existing_screenshot=screenshot,
        )

        if dialog.exec() == ScreenshotCaptureDialog.DialogCode.Accepted:
            self._refresh()

    def _upload_screenshot(self) -> None:
        """Upload an external screenshot."""
        if not self._db_manager or not self.case_folder or not self.evidence_id:
            QMessageBox.warning(
                self,
                "No Evidence Selected",
                "Please select an evidence before uploading screenshots."
            )
            return

        # File picker
        file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Screenshots to Upload",
            str(Path.home()),
            "Images (*.png *.jpg *.jpeg *.gif *.webp *.bmp);;All Files (*)",
        )

        if not file_paths:
            return

        conn = self._get_conn()
        if not conn:
            QMessageBox.critical(
                self,
                "Database Error",
                "Could not access evidence database."
            )
            return

        from app.common.dialogs import ScreenshotCaptureDialog
        from app.common import ForensicContext

        forensic_context = ForensicContext(
            evidence_id=self.evidence_id,
            evidence_label=self.evidence_label,
            workspace_path=self.case_folder,
            db_conn=conn,
        )

        uploaded = 0
        for file_path in file_paths:
            path = Path(file_path)

            # Load image
            pixmap = QPixmap(str(path))
            if pixmap.isNull():
                QMessageBox.warning(
                    self,
                    "Load Error",
                    f"Could not load image:\n{path}\n\nSkipping this file."
                )
                continue

            # For uploads, we need a custom dialog flow
            # Since the file isn't saved yet, we show dialog first
            dialog = UploadScreenshotDialog(
                pixmap,
                path.name,
                forensic_context,
                parent=self,
            )

            if dialog.exec() == UploadScreenshotDialog.DialogCode.Accepted:
                uploaded += 1

        if uploaded > 0:
            self.status_label.setText(f"Uploaded {uploaded} screenshot(s)")
            self._refresh()

    def _delete_selected(self) -> None:
        """Delete selected screenshots."""
        if not self.case_data or not self.evidence_id:
            return

        checked = self.model.get_checked_screenshots()
        if not checked:
            return

        # Confirm deletion
        result = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Delete {len(checked)} selected screenshot(s)?\n\n"
            "This will remove the files and database records.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if result != QMessageBox.StandardButton.Yes:
            return

        conn = self._get_conn()
        if not conn:
            return

        deleted = 0
        errors = []

        for screenshot in checked:
            try:
                # Delete file
                if self.case_folder and self.evidence_label:
                    slug = slugify_label(self.evidence_label, self.evidence_id)
                    file_path = self.case_folder / "evidences" / slug / screenshot["dest_path"]
                    if file_path.exists():
                        file_path.unlink()

                # Delete database record
                delete_screenshot(conn, self.evidence_id, screenshot["id"])
                deleted += 1

            except Exception as e:
                errors.append(f"ID {screenshot['id']}: {e}")
                logger.error("Failed to delete screenshot %d: %s", screenshot["id"], e)

        if errors:
            QMessageBox.warning(
                self,
                "Partial Delete",
                f"Deleted {deleted} screenshots.\n\n"
                f"Failed to delete {len(errors)} screenshots:\n" +
                "\n".join(errors[:5])
            )
        else:
            self.status_label.setText(f"Deleted {deleted} screenshot(s)")

        self._refresh()

    def showEvent(self, event) -> None:
        """Refresh when tab becomes visible."""
        super().showEvent(event)
        self._refresh()


class UploadScreenshotDialog(QWidget):
    """
    Dialog for uploading external screenshots with metadata.

    Similar to ScreenshotCaptureDialog but handles the import workflow.
    """

    DialogCode = type('DialogCode', (), {'Accepted': 1, 'Rejected': 0})

    def __init__(
        self,
        pixmap: QPixmap,
        original_filename: str,
        forensic_context,
        parent: Optional[QWidget] = None,
    ):
        # Actually use QDialog
        from PySide6.QtWidgets import QDialog
        self._dialog = QDialog(parent)
        self._dialog.setWindowTitle("📤 Upload Screenshot")
        self._dialog.setMinimumWidth(600)

        self.pixmap = pixmap
        self.original_filename = original_filename
        self.forensic_context = forensic_context
        self._result = 0

        self._init_ui()

    def _init_ui(self) -> None:
        from PySide6.QtWidgets import (
            QDialogButtonBox, QFormLayout, QGroupBox, QLineEdit,
            QPlainTextEdit, QSpinBox, QComboBox
        )

        layout = QVBoxLayout(self._dialog)

        # Preview
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout(preview_group)

        preview_label = QLabel()
        preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        if not self.pixmap.isNull():
            scaled = self.pixmap.scaled(
                400, 250,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )
            preview_label.setPixmap(scaled)
        preview_layout.addWidget(preview_label)

        info_label = QLabel(
            f"Original: {self.original_filename} | "
            f"Size: {self.pixmap.width()} × {self.pixmap.height()} pixels"
        )
        info_label.setStyleSheet("color: gray; font-size: 11px;")
        info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        preview_layout.addWidget(info_label)

        layout.addWidget(preview_group)

        # Form
        form_group = QGroupBox("Metadata")
        form_layout = QFormLayout(form_group)

        self.title_edit = QLineEdit()
        self.title_edit.setPlaceholderText("Short title for report headers")
        form_layout.addRow("Title:", self.title_edit)

        self.caption_edit = QPlainTextEdit()
        self.caption_edit.setPlaceholderText("Description shown in report")
        self.caption_edit.setMaximumHeight(80)
        form_layout.addRow("Caption:", self.caption_edit)

        self.notes_edit = QPlainTextEdit()
        self.notes_edit.setPlaceholderText("Internal notes (not in report)")
        self.notes_edit.setMaximumHeight(60)
        form_layout.addRow("Notes:", self.notes_edit)

        # Sequence
        seq_row = QHBoxLayout()
        self.sequence_combo = QComboBox()
        self.sequence_combo.setEditable(True)
        self.sequence_combo.lineEdit().setPlaceholderText("Optional sequence name")
        self._load_sequences()
        seq_row.addWidget(self.sequence_combo)

        seq_row.addWidget(QLabel("Order:"))
        self.order_spin = QSpinBox()
        self.order_spin.setMinimum(0)
        self.order_spin.setMaximum(9999)
        seq_row.addWidget(self.order_spin)
        seq_row.addStretch()
        form_layout.addRow("Sequence:", seq_row)

        layout.addWidget(form_group)

        # Buttons
        button_box = QDialogButtonBox()
        save_btn = button_box.addButton("📤 Upload", QDialogButtonBox.ButtonRole.AcceptRole)
        button_box.addButton(QDialogButtonBox.StandardButton.Cancel)

        button_box.accepted.connect(self._on_save)
        button_box.rejected.connect(self._dialog.reject)

        layout.addWidget(button_box)

    def _load_sequences(self) -> None:
        from core.database.helpers import get_sequences
        try:
            sequences = get_sequences(
                self.forensic_context.db_conn,
                self.forensic_context.evidence_id
            )
            self.sequence_combo.addItem("")
            for seq in sequences:
                self.sequence_combo.addItem(seq)
        except Exception:
            pass

    def _on_save(self) -> None:
        title = self.title_edit.text().strip()
        caption = self.caption_edit.toPlainText().strip()

        if not title:
            QMessageBox.warning(
                self._dialog,
                "Title Required",
                "Please enter a title for the screenshot."
            )
            return

        if not caption:
            QMessageBox.warning(
                self._dialog,
                "Caption Required",
                "Please enter a caption for the screenshot."
            )
            return

        try:
            # Save the pixmap
            from app.features.screenshots.storage import save_screenshot

            metadata = save_screenshot(
                self.pixmap,
                self.forensic_context.workspace_path,
                self.forensic_context.evidence_label,
                self.forensic_context.evidence_id,
                prefix="upload",
            )

            # Insert record
            now_utc = datetime.now(timezone.utc).isoformat()
            insert_screenshot(
                self.forensic_context.db_conn,
                self.forensic_context.evidence_id,
                metadata.dest_path,
                metadata.filename,
                captured_url=None,  # External uploads don't have URLs
                size_bytes=metadata.size_bytes,
                width=metadata.width,
                height=metadata.height,
                md5=metadata.md5,
                sha256=metadata.sha256,
                title=title,
                caption=caption,
                notes=self.notes_edit.toPlainText().strip() or None,
                sequence_name=self.sequence_combo.currentText().strip() or None,
                sequence_order=self.order_spin.value(),
                source="upload",
                captured_at_utc=now_utc,
            )

            self._result = 1
            self._dialog.accept()

        except Exception as e:
            logger.error("Failed to upload screenshot: %s", e)
            QMessageBox.critical(
                self._dialog,
                "Upload Error",
                f"Failed to upload screenshot:\n{e}"
            )

    def exec(self) -> int:
        self._dialog.exec()
        return self._result
