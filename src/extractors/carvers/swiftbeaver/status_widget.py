"""SwiftBeaver status widget."""

import json
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QGroupBox, QCheckBox,
)
from PySide6.QtCore import Signal

from .extractor import (
    METADATA_DIR,
    CARVED_FILES_JSONL,
    STRING_ARTEFACTS_JSONL,
    RUN_SUMMARY_JSONL,
    find_latest_run_dir,
)


class SwiftbeaverStatusWidget(QWidget):
    """
    Status widget for SwiftBeaver showing:
    - Extraction status (run directory, carved image counts, string artefacts)
    - Ingestion options (which artifact types to import)
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

        self.status_label = QLabel("No output found")
        self.status_label.setWordWrap(True)
        status_layout.addWidget(self.status_label)

        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # Import options
        import_group = QGroupBox("Import Options")
        import_layout = QVBoxLayout()

        self._import_images_cb = QCheckBox("Import carved images")
        self._import_images_cb.setChecked(True)
        self._import_images_cb.setToolTip("Import carved images into the images database")
        self._import_images_cb.toggled.connect(self._emit_config)
        import_layout.addWidget(self._import_images_cb)

        self._import_urls_cb = QCheckBox("Import URLs")
        self._import_urls_cb.setChecked(True)
        self._import_urls_cb.setToolTip("Import extracted URLs into the URLs database")
        self._import_urls_cb.toggled.connect(self._emit_config)
        import_layout.addWidget(self._import_urls_cb)

        import_group.setLayout(import_layout)
        layout.addWidget(import_group)

        layout.addStretch()

    def _find_latest_run_dir(self) -> Path | None:
        """Find the latest SwiftBeaver run directory."""
        return find_latest_run_dir(self.output_dir)

    def _update_status(self):
        """Update status display based on output directory."""
        if not self.output_dir.exists():
            self.status_label.setText("No output found")
            return

        run_dir = self._find_latest_run_dir()
        if not run_dir:
            self.status_label.setText("Output directory exists but no run directories found")
            return

        lines = [f"✓ Run directory: {run_dir.name}"]
        metadata_dir = run_dir / METADATA_DIR

        # Count carved images by type
        carved_jsonl = metadata_dir / CARVED_FILES_JSONL
        if carved_jsonl.exists():
            type_counts: dict[str, int] = {}
            total = 0
            try:
                with open(carved_jsonl, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            ftype = entry.get("file_type", "unknown")
                            type_counts[ftype] = type_counts.get(ftype, 0) + 1
                            total += 1
                        except (json.JSONDecodeError, KeyError):
                            continue
            except Exception:
                total = 0

            if total > 0:
                lines.append(f"Carved images: {total:,}")
                for ftype, count in sorted(type_counts.items()):
                    lines.append(f"  {ftype}: {count:,}")
            else:
                lines.append("No carved images found")
        else:
            lines.append("No carved_files.jsonl found")

        # Count string artefacts
        strings_jsonl = metadata_dir / STRING_ARTEFACTS_JSONL
        if strings_jsonl.exists():
            url_count = 0
            try:
                with open(strings_jsonl, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            if entry.get("artefact_kind", "").lower() == "url":
                                url_count += 1
                        except (json.JSONDecodeError, KeyError):
                            continue
            except Exception:
                url_count = 0

            lines.append(f"URLs found: {url_count:,}")
        else:
            lines.append("No string_artefacts.jsonl found")

        # Show run summary if available
        summary_jsonl = metadata_dir / RUN_SUMMARY_JSONL
        if summary_jsonl.exists():
            try:
                with open(summary_jsonl, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            summary = json.loads(line)
                            if "duration_secs" in summary:
                                lines.append(f"Run duration: {summary['duration_secs']:.1f}s")
                            if "bytes_processed" in summary:
                                mb = summary["bytes_processed"] / (1024 * 1024)
                                lines.append(f"Bytes processed: {mb:.1f} MB")
                        except (json.JSONDecodeError, KeyError):
                            continue
            except Exception:
                pass

        self.status_label.setText("\n".join(lines))

    def _emit_config(self):
        """Emit current configuration."""
        self.configChanged.emit(self.get_config())

    def get_config(self) -> dict:
        """Get current configuration (which artifacts to import)."""
        return {
            "import_images": self._import_images_cb.isChecked(),
            "import_urls": self._import_urls_cb.isChecked(),
        }

    def set_config(self, config: dict):
        """Set configuration from dict."""
        if "import_images" in config:
            self._import_images_cb.setChecked(config["import_images"])
        if "import_urls" in config:
            self._import_urls_cb.setChecked(config["import_urls"])

    def refresh(self):
        """Refresh status display."""
        self._update_status()
