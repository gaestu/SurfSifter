"""Details dialog for a single Firefox cache index entry."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
)


class CacheIndexDetailsDialog(QDialog):
    """Dialog showing full details for a Firefox cache index entry."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data
        self.setWindowTitle("Cache Index Entry Details")
        self.setModal(True)
        self.resize(600, 550)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Entry hash header
        entry_hash = self.row_data.get("entry_hash", "Unknown")
        hash_label = QLabel(f"<b>Entry Hash:</b> <code>{entry_hash}</code>")
        hash_label.setTextFormat(hash_label.RichText)
        hash_label.setWordWrap(True)
        layout.addWidget(hash_label)

        # URL (if resolved)
        url = self.row_data.get("url")
        if url:
            url_display = url[:80] + "..." if len(url) > 80 else url
            url_label = QLabel(f"<b>URL:</b> {url_display}")
            url_label.setWordWrap(True)
            url_label.setToolTip(url)
            layout.addWidget(url_label)

        layout.addWidget(QLabel(""))  # Spacer

        form = QFormLayout()

        # ── Content Info ──────────────────────────────────────────────
        content_type_name = self.row_data.get("content_type_name", "N/A")
        content_type_raw = self.row_data.get("content_type")
        ct_text = content_type_name or "N/A"
        if content_type_raw is not None:
            ct_text += f"  (enum: {content_type_raw})"
        form.addRow("Content Type:", QLabel(ct_text))

        frecency = self.row_data.get("frecency")
        form.addRow("Frecency:", QLabel(str(frecency) if frecency is not None else "N/A"))

        file_size = self.row_data.get("file_size_kb")
        form.addRow("File Size:", QLabel(f"{file_size} KB" if file_size is not None else "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # ── Flags ──────────────────────────────────────────────────────
        form.addRow(QLabel("<b>Flags:</b>"), QLabel(""))

        flag_fields = [
            ("is_initialized", "Initialized"),
            ("is_anonymous", "Anonymous"),
            ("is_removed", "Removed"),
            ("is_pinned", "Pinned"),
            ("has_alt_data", "Has Alt Data"),
        ]
        for field, label_text in flag_fields:
            cb = QCheckBox(label_text)
            cb.setChecked(bool(self.row_data.get(field, False)))
            cb.setEnabled(False)  # Read-only
            form.addRow("", cb)

        raw_flags = self.row_data.get("raw_flags")
        if raw_flags is not None:
            form.addRow("Raw Flags:", QLabel(f"0x{raw_flags:08X}"))

        form.addRow("", QLabel(""))  # Spacer

        # ── Origin & Timing ──────────────────────────────────────────
        origin_hash = self.row_data.get("origin_attrs_hash")
        form.addRow(
            "Origin Attrs Hash:",
            QLabel(str(origin_hash) if origin_hash is not None else "N/A"),
        )

        on_start = self.row_data.get("on_start_time")
        on_stop = self.row_data.get("on_stop_time")
        form.addRow(
            "on_start_time:",
            QLabel(f"{on_start} ms" if on_start is not None else "N/A"),
        )
        form.addRow(
            "on_stop_time:",
            QLabel(f"{on_stop} ms" if on_stop is not None else "N/A"),
        )

        form.addRow("", QLabel(""))  # Spacer

        # ── Index Metadata ──────────────────────────────────────────
        idx_version = self.row_data.get("index_version")
        idx_timestamp = self.row_data.get("index_timestamp")
        idx_dirty = self.row_data.get("index_dirty", False)

        form.addRow(
            "Index Version:",
            QLabel(str(idx_version) if idx_version is not None else "N/A"),
        )
        form.addRow(
            "Index Timestamp:",
            QLabel(str(idx_timestamp) if idx_timestamp is not None else "N/A"),
        )
        dirty_label = QLabel("Yes" if idx_dirty else "No")
        if idx_dirty:
            dirty_label.setStyleSheet("color: red; font-weight: bold;")
        form.addRow("Index Dirty:", dirty_label)

        form.addRow("", QLabel(""))  # Spacer

        # ── Recovery Status ──────────────────────────────────────────
        has_file = self.row_data.get("has_entry_file", False)
        has_file_label = QLabel("Yes" if has_file else "No")
        if not has_file:
            has_file_label.setStyleSheet("color: gray; font-style: italic;")
        form.addRow("Has Entry File:", has_file_label)

        entry_source = self.row_data.get("entry_source") or "index_only"
        source_label = QLabel(entry_source)
        if entry_source in ("doomed", "trash"):
            source_label.setStyleSheet("color: orange; font-weight: bold;")
        form.addRow("Entry Source:", source_label)

        # Profile path
        profile = self.row_data.get("profile_path")
        if profile:
            profile_label = QLabel(profile)
            profile_label.setWordWrap(True)
            form.addRow("Profile Path:", profile_label)

        # Run ID
        run_id = self.row_data.get("run_id")
        if run_id:
            form.addRow("", QLabel(""))
            run_label = QLabel(str(run_id))
            run_label.setStyleSheet("color: gray; font-size: 10px;")
            form.addRow("Run ID:", run_label)

        layout.addLayout(form)
        layout.addStretch()

        # ── Buttons ──────────────────────────────────────────────────
        button_layout = QHBoxLayout()

        copy_hash_btn = QPushButton("Copy Hash")
        copy_hash_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(entry_hash),
        )
        button_layout.addWidget(copy_hash_btn)

        if url:
            copy_url_btn = QPushButton("Copy URL")
            copy_url_btn.clicked.connect(
                lambda: QApplication.clipboard().setText(url),
            )
            button_layout.addWidget(copy_url_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
