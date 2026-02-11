"""Site engagement details dialog."""
from __future__ import annotations

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)


class SiteEngagementDetailsDialog(QDialog):
    """Dialog showing full details for a site engagement record."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        engagement_type = row_data.get("engagement_type", "site_engagement")
        title = "Site Engagement Details" if engagement_type == "site_engagement" else "Media Engagement Details"
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(550, 450)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # Basic info
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))

        # Engagement type
        engagement_type = self.row_data.get("engagement_type", "")
        type_label = "Site Engagement" if engagement_type == "site_engagement" else "Media Engagement"
        form.addRow("Type:", QLabel(type_label))
        form.addRow("", QLabel(""))

        # Site engagement specific
        if engagement_type == "site_engagement":
            score = self.row_data.get("raw_score")
            score_str = f"{score:.4f}" if score is not None else "N/A"
            form.addRow("Engagement Score:", QLabel(score_str))

            points = self.row_data.get("points_added_today")
            points_str = f"{points:.2f}" if points is not None else "N/A"
            form.addRow("Points Added Today:", QLabel(points_str))

            has_high = self.row_data.get("has_high_score")
            high_str = "Yes" if has_high else "No" if has_high is not None else "N/A"
            form.addRow("Has High Score:", QLabel(high_str))

            form.addRow("Last Engagement:", QLabel(self.row_data.get("last_engagement_time_utc") or "N/A"))
            form.addRow("Last Shortcut Launch:", QLabel(self.row_data.get("last_shortcut_launch_time_utc") or "N/A"))

        # Media engagement specific
        else:
            visits = self.row_data.get("visits")
            form.addRow("Visits:", QLabel(str(visits) if visits is not None else "N/A"))

            playbacks = self.row_data.get("media_playbacks")
            form.addRow("Media Playbacks:", QLabel(str(playbacks) if playbacks is not None else "N/A"))

            has_high = self.row_data.get("has_high_score")
            high_str = "Yes" if has_high else "No" if has_high is not None else "N/A"
            form.addRow("Has High Score:", QLabel(high_str))

            form.addRow("Last Playback:", QLabel(self.row_data.get("last_media_playback_time_utc") or "N/A"))

        form.addRow("", QLabel(""))

        # Common metadata
        form.addRow("Last Modified:", QLabel(str(self.row_data.get("last_modified_webkit") or "N/A")))
        form.addRow("Expiration:", QLabel(self.row_data.get("expiration") or "N/A"))
        form.addRow("Model:", QLabel(str(self.row_data.get("model") or "N/A")))

        layout.addLayout(form)

        # Origin
        layout.addWidget(QLabel("Origin:"))
        origin_text = QTextEdit()
        origin_text.setReadOnly(True)
        origin_text.setMaximumHeight(60)
        origin_text.setPlainText(self.row_data.get("origin", ""))
        layout.addWidget(origin_text)

        # Forensic info
        layout.addWidget(QLabel("Source Path:"))
        source_text = QTextEdit()
        source_text.setReadOnly(True)
        source_text.setMaximumHeight(60)
        source_text.setPlainText(self.row_data.get("source_path") or self.row_data.get("logical_path") or "N/A")
        layout.addWidget(source_text)

        # Buttons
        button_layout = QHBoxLayout()

        origin = self.row_data.get("origin", "")
        # Clean origin for URL opening (remove trailing comma patterns)
        clean_origin = origin.split(",")[0] if "," in origin else origin

        open_btn = QPushButton("Open Origin")
        open_btn.setEnabled(clean_origin.startswith(("http://", "https://")))
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(clean_origin)))
        button_layout.addWidget(open_btn)

        copy_btn = QPushButton("Copy Origin")
        copy_btn.setEnabled(bool(origin))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(origin))
        button_layout.addWidget(copy_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
