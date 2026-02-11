"""
Registry extractor UI components.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QCheckBox,
    QGroupBox,
    QFileDialog,
    QPushButton,
)

from core.logging import get_logger

LOGGER = get_logger("extractors.system.registry.ui")


class RegistryConfigWidget(QWidget):
    """
    Configuration widget for Registry extractor.

    Allows user to:
    - Select which registry hives to scan (SYSTEM, SOFTWARE, SAM, SECURITY)
    - Choose custom detector rules file (optional)
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

        # Rules file selection
        rules_group = QGroupBox("Detector Rules (Optional)")
        rules_layout = QVBoxLayout(rules_group)

        rules_info = QLabel(
            "Built-in detection rules are used by default.\n"
            "Use custom rules to detect additional registry indicators."
        )
        rules_info.setWordWrap(True)
        rules_info.setStyleSheet("color: gray; font-size: 9pt;")
        rules_layout.addWidget(rules_info)

        rules_button_layout = QHBoxLayout()
        self.rules_button = QPushButton("📁 Select Custom Rules")
        self.rules_button.clicked.connect(self._select_rules_file)
        rules_button_layout.addWidget(self.rules_button)
        rules_button_layout.addStretch()
        rules_layout.addLayout(rules_button_layout)

        self.rules_label = QLabel("Using default rules")
        self.rules_label.setStyleSheet("color: green; font-size: 9pt;")
        rules_layout.addWidget(self.rules_label)

        layout.addWidget(rules_group)

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

        # Store custom rules path
        self.custom_rules_path: Optional[Path] = None

    def _select_rules_file(self):
        """Open file dialog to select custom rules YAML."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Detector Rules File",
            str(Path.home()),
            "YAML Files (*.yml *.yaml);;All Files (*)"
        )

        if file_path:
            self.custom_rules_path = Path(file_path)
            self.rules_label.setText(f"Using: {self.custom_rules_path.name}")
            self.rules_label.setStyleSheet("color: blue; font-size: 9pt;")
            LOGGER.info("Selected custom rules: %s", self.custom_rules_path)

    def get_config(self) -> dict:
        """
        Get configuration from widget.

        Returns:
            Dict with rules_path (if selected)
        """
        config = {}

        if self.custom_rules_path:
            config["rules_path"] = str(self.custom_rules_path)

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
