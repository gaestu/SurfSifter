"""bulk_extractor status widget."""

from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QGroupBox, QCheckBox,
    QHBoxLayout
)
from PySide6.QtCore import Signal


class BulkExtractorStatusWidget(QWidget):
    """
    Status widget for bulk_extractor showing:
    - Extraction status (files generated, last run time)
    - Ingestion options (which artifacts to import)
    """

    configChanged = Signal(dict)

    def __init__(self, parent, output_dir: Path, evidence_conn, evidence_id: int):
        super().__init__(parent)
        self.output_dir = output_dir
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self._setup_ui()
        self._update_status()

    def _setup_ui(self):
        """Create UI elements."""
        layout = QVBoxLayout(self)

        # Extraction status
        status_group = QGroupBox("Extraction Status")
        status_layout = QVBoxLayout()

        self.status_label = QLabel("No output files found")
        status_layout.addWidget(self.status_label)

        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # Ingestion options
        ingest_group = QGroupBox("Artifacts to Import")
        ingest_layout = QVBoxLayout()

        self.url_cb = QCheckBox("URLs")
        self.url_cb.setChecked(True)
        self.url_cb.setToolTip("Import URL artifacts from url.txt")
        ingest_layout.addWidget(self.url_cb)

        self.email_cb = QCheckBox("Emails")
        self.email_cb.setToolTip("Import email addresses from email.txt")
        ingest_layout.addWidget(self.email_cb)

        self.domain_cb = QCheckBox("Domains")
        self.domain_cb.setToolTip("Import domain names from domain.txt")
        ingest_layout.addWidget(self.domain_cb)

        self.ip_cb = QCheckBox("IP Addresses")
        self.ip_cb.setToolTip("Import IP addresses from ip.txt")
        ingest_layout.addWidget(self.ip_cb)

        self.telephone_cb = QCheckBox("Telephone Numbers")
        self.telephone_cb.setToolTip("Import phone numbers from telephone.txt")
        ingest_layout.addWidget(self.telephone_cb)

        self.bitcoin_cb = QCheckBox("Bitcoin Addresses")
        self.bitcoin_cb.setToolTip("Import Bitcoin addresses from bitcoin.txt")
        ingest_layout.addWidget(self.bitcoin_cb)

        self.ether_cb = QCheckBox("Ethereum Addresses")
        self.ether_cb.setToolTip("Import Ethereum addresses from ether.txt")
        ingest_layout.addWidget(self.ether_cb)

        ingest_group.setLayout(ingest_layout)
        layout.addWidget(ingest_group)

        layout.addStretch()

        # Connect signals
        self.url_cb.toggled.connect(self._emit_config)
        self.email_cb.toggled.connect(self._emit_config)
        self.domain_cb.toggled.connect(self._emit_config)
        self.ip_cb.toggled.connect(self._emit_config)
        self.telephone_cb.toggled.connect(self._emit_config)
        self.bitcoin_cb.toggled.connect(self._emit_config)
        self.ether_cb.toggled.connect(self._emit_config)

    def _update_status(self):
        """Update status display based on output directory."""
        if not self.output_dir.exists():
            self.status_label.setText("No output files found")
            return

        # Count output files
        output_files = {
            "url.txt": "URLs",
            "email.txt": "Emails",
            "domain.txt": "Domains",
            "ip.txt": "IPs",
            "telephone.txt": "Phone Numbers",
            "bitcoin.txt": "Bitcoin",
            "ether.txt": "Ethereum"
        }

        found_files = []
        for filename, label in output_files.items():
            file_path = self.output_dir / filename
            if file_path.exists():
                # Count lines (quick estimate)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        count = sum(1 for line in f if line.strip() and not line.startswith('#'))
                    found_files.append(f"{label}: {count:,}")
                except Exception:
                    found_files.append(label)

        carved_count = self._detect_carved_images()

        lines = []
        if found_files:
            lines.append("✓ Output files found:")
            lines.extend(found_files)
        else:
            lines.append("Output directory exists but no recognized files found")

        if carved_count:
            lines.append(f"Images carved (jpeg_carve): {carved_count}")
        else:
            lines.append("Images: none detected. Enable \"Carve images (jpeg_carve)\" and rerun extraction.")

        self.status_label.setText("\n".join(lines))

    def _emit_config(self):
        """Emit current configuration."""
        self.configChanged.emit(self.get_config())

    def get_config(self) -> dict:
        """Get current configuration (which artifacts to import)."""
        artifact_types = []

        if self.url_cb.isChecked():
            artifact_types.append("url")
        if self.email_cb.isChecked():
            artifact_types.append("email")
        if self.domain_cb.isChecked():
            artifact_types.append("domain")
        if self.ip_cb.isChecked():
            artifact_types.append("ip")
        if self.telephone_cb.isChecked():
            artifact_types.append("telephone")
        if self.bitcoin_cb.isChecked():
            artifact_types.append("bitcoin")
        if self.ether_cb.isChecked():
            artifact_types.append("ether")

        return {"artifact_types": artifact_types}

    def set_config(self, config: dict):
        """Set configuration from dict."""
        artifact_types = config.get("artifact_types", ["url"])

        self.url_cb.setChecked("url" in artifact_types)
        self.email_cb.setChecked("email" in artifact_types)
        self.domain_cb.setChecked("domain" in artifact_types)
        self.ip_cb.setChecked("ip" in artifact_types)
        self.telephone_cb.setChecked("telephone" in artifact_types)
        self.bitcoin_cb.setChecked("bitcoin" in artifact_types)
        self.ether_cb.setChecked("ether" in artifact_types)

    def refresh(self):
        """Refresh status display."""
        self._update_status()

    def _detect_carved_images(self) -> int:
        """Detect carved images under bulk_extractor output."""
        carved_dirs = ["jpeg_carved", "jpeg", "images"]
        count = 0
        for name in carved_dirs:
            root = self.output_dir / name
            if root.exists():
                count += sum(
                    1 for p in root.rglob("*") if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".gif"}
                )
        return count
