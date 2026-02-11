"""Browser inventory details dialog."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QTextEdit,
    QVBoxLayout,
)


class BrowserInventoryDetailsDialog(QDialog):
    """Dialog showing full details for a browser artifact."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize details dialog.

        Args:
            row_data: Row data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Browser Artifact Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for key fields
        form = QFormLayout()

        # Basic info
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A")))
        form.addRow("Artifact Type:", QLabel(self.row_data.get("artifact_type", "N/A")))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Paths
        form.addRow("Logical Path:", QLabel(self.row_data.get("logical_path", "N/A")))
        form.addRow("Extracted Path:", QLabel(self.row_data.get("extracted_path", "N/A")))
        form.addRow("Forensic Path:", QLabel(self.row_data.get("forensic_path") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # E01 context
        partition = self.row_data.get("partition_index")
        partition_str = str(partition) if partition is not None else "N/A"
        form.addRow("Partition Index:", QLabel(partition_str))
        form.addRow("Filesystem:", QLabel(self.row_data.get("fs_type") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Extraction info
        form.addRow("Run ID:", QLabel(self.row_data.get("run_id", "N/A")))
        form.addRow("Extraction Status:", QLabel(self.row_data.get("extraction_status", "N/A")))
        form.addRow("Extraction Time:", QLabel(self.row_data.get("extraction_timestamp_utc") or "N/A"))
        form.addRow("Extraction Tool:", QLabel(self.row_data.get("extraction_tool") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Ingestion info
        ingestion_status = self.row_data.get("ingestion_status") or "pending"
        form.addRow("Ingestion Status:", QLabel(ingestion_status))
        form.addRow("Ingestion Time:", QLabel(self.row_data.get("ingestion_timestamp_utc") or "N/A"))
        form.addRow("URLs Parsed:", QLabel(str(self.row_data.get("urls_parsed") or 0)))
        form.addRow("Records Parsed:", QLabel(str(self.row_data.get("records_parsed") or 0)))

        form.addRow("", QLabel(""))  # Spacer

        # File metadata
        file_size = self.row_data.get("file_size_bytes")
        file_size_str = f"{file_size:,} bytes" if file_size is not None else "N/A"
        form.addRow("File Size:", QLabel(file_size_str))
        form.addRow("MD5:", QLabel(self.row_data.get("file_md5") or "N/A"))
        form.addRow("SHA-256:", QLabel(self.row_data.get("file_sha256") or "N/A"))

        layout.addLayout(form)

        # Notes section
        extraction_notes = self.row_data.get("extraction_notes")
        ingestion_notes = self.row_data.get("ingestion_notes")

        if extraction_notes or ingestion_notes:
            layout.addWidget(QLabel("Notes:"))

            notes_text = QTextEdit()
            notes_text.setReadOnly(True)
            notes_text.setMaximumHeight(150)

            notes_content = ""
            if extraction_notes:
                notes_content += f"Extraction:\n{extraction_notes}\n\n"
            if ingestion_notes:
                notes_content += f"Ingestion:\n{ingestion_notes}"

            notes_text.setPlainText(notes_content.strip())
            layout.addWidget(notes_text)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
