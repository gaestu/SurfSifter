"""
Registry extractor UI components.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QLabel,
    QCheckBox,
    QGroupBox,
)


class RegistryConfigWidget(QWidget):
    """
    Configuration widget for Registry extractor.

    Allows user to:
    - Select which registry hives to scan (SYSTEM, SOFTWARE, SAM, SECURITY)
    """

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        """Build the configuration UI."""
        layout = QVBoxLayout(self)

        # Header
        header = QLabel("<b>Registry Extraction Configuration</b>")
        layout.addWidget(header)

        # Hive selection info
        hive_group = QGroupBox("Registry Hives")
        hive_layout = QVBoxLayout(hive_group)

        info_label = QLabel(
            "All standard machine hives (SYSTEM, SOFTWARE, SAM, SECURITY) "
            "and all discovered user hives (NTUSER.DAT, UsrClass.dat) "
            "will be exported automatically."
        )
        info_label.setWordWrap(True)
        hive_layout.addWidget(info_label)

        layout.addWidget(hive_group)

        # Ingestion options
        ingest_group = QGroupBox("Ingestion Options")
        ingest_layout = QVBoxLayout(ingest_group)

        self.purge_existing_checkbox = QCheckBox(
            "Purge existing registry indicators before ingest (destructive)"
        )
        self.purge_existing_checkbox.setToolTip(
            "Deletes existing os_indicators rows for this evidence before ingest."
        )
        ingest_layout.addWidget(self.purge_existing_checkbox)

        layout.addWidget(ingest_group)

        layout.addStretch()

    def get_config(self) -> dict:
        """
        Get configuration from widget.

        Returns:
            Dict with enabled ingestion options.
        """
        config = {}

        if self.purge_existing_checkbox.isChecked():
            config["purge_existing"] = True

        return config


class RegistryStatusWidget(QWidget):
    """
    Status widget showing registry extraction results.
    """

    def __init__(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.output_dir = output_dir
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self._setup_ui()

    def _setup_ui(self):
        """Build status display."""
        layout = QVBoxLayout(self)

        # Check for manifest
        manifest = self.output_dir / "manifest.json"
        if manifest.exists():
            import json
            try:
                data = json.loads(manifest.read_text())

                extracted_count = len(data.get("extracted_hives", []))
                run_id = data.get("run_id", "N/A")
                timestamp = data.get("timestamp", "")

                # Format timestamp nicely if possible
                try:
                    from datetime import datetime
                    ts = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M")
                except (ValueError, ImportError):
                    ts = timestamp

                status_label = QLabel(
                    f"<b>Registry Extraction Complete</b><br>"
                    f"Hives Exported: {extracted_count}<br>"
                    f"Last Run: {ts}<br>"
                    f"Run ID: {run_id}"
                )
            except Exception:
                status_label = QLabel("<b>Registry Extraction</b><br>Error reading manifest")
        else:
            status_label = QLabel(
                "<b>Registry Extraction</b><br>"
                "No extraction run yet.<br>"
                "Click 'Run Extraction' to scan registry hives."
            )
            status_label.setStyleSheet("color: gray;")

        layout.addWidget(status_label)
        layout.addStretch()
