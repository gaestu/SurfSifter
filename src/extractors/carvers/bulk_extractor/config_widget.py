"""bulk_extractor configuration widget."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QCheckBox, QSpinBox,
    QLabel, QHBoxLayout, QRadioButton, QButtonGroup
)
from PySide6.QtCore import Signal


class BulkExtractorConfigWidget(QWidget):
    """
    Configuration widget for bulk_extractor.

    Allows user to configure:
    - Scanners to enable (email, accts, etc.)
    - Optional image carving (jpeg_carve scanner)
    - Thread count
    - Output reuse policy
    """

    configChanged = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        """Create UI elements."""
        layout = QVBoxLayout(self)

        # Scanner selection
        scanner_group = QGroupBox("Scanners")
        scanner_layout = QVBoxLayout()

        self.email_scanner_cb = QCheckBox("email (URLs, emails, domains)")
        self.email_scanner_cb.setChecked(True)
        self.email_scanner_cb.setToolTip("Extract URLs, email addresses, and domains")
        scanner_layout.addWidget(self.email_scanner_cb)

        self.accts_scanner_cb = QCheckBox("accts (Phone numbers, credit cards)")
        self.accts_scanner_cb.setChecked(True)
        self.accts_scanner_cb.setToolTip("Extract telephone numbers and credit card numbers")
        scanner_layout.addWidget(self.accts_scanner_cb)

        self.images_scanner_cb = QCheckBox("Carve images (jpeg_carve)")
        self.images_scanner_cb.setChecked(True)
        self.images_scanner_cb.setToolTip("Enable bulk_extractor jpeg_carve scanner to carve images (outputs jpeg_carved/)")
        scanner_layout.addWidget(self.images_scanner_cb)

        info_label = QLabel("Note: More scanners slow down extraction significantly")
        info_label.setStyleSheet("color: gray; font-size: 10px;")
        scanner_layout.addWidget(info_label)

        scanner_group.setLayout(scanner_layout)
        layout.addWidget(scanner_group)

        # Thread count
        thread_group = QGroupBox("Performance")
        thread_layout = QHBoxLayout()

        thread_layout.addWidget(QLabel("Threads:"))

        self.thread_spinner = QSpinBox()
        self.thread_spinner.setMinimum(1)
        self.thread_spinner.setMaximum(32)
        import os
        cpu_count = os.cpu_count() or 4
        default_threads = max(1, min(cpu_count - 2, 16))
        self.thread_spinner.setValue(default_threads)
        self.thread_spinner.setToolTip(
            f"Recommended: {default_threads} (CPU cores: {cpu_count})"
        )
        thread_layout.addWidget(self.thread_spinner)

        thread_layout.addStretch()
        thread_group.setLayout(thread_layout)
        layout.addWidget(thread_group)

        # Output reuse policy
        reuse_group = QGroupBox("Output Handling")
        reuse_layout = QVBoxLayout()

        self.reuse_button_group = QButtonGroup(self)

        self.reuse_radio = QRadioButton("Reuse existing output if available")
        self.reuse_radio.setChecked(True)
        self.reuse_radio.setToolTip("Skip extraction if output files already exist")
        self.reuse_button_group.addButton(self.reuse_radio, 0)
        reuse_layout.addWidget(self.reuse_radio)

        self.overwrite_radio = QRadioButton("Always overwrite (re-run extraction)")
        self.overwrite_radio.setToolTip("Delete existing output and run extraction again")
        self.reuse_button_group.addButton(self.overwrite_radio, 1)
        reuse_layout.addWidget(self.overwrite_radio)

        reuse_group.setLayout(reuse_layout)
        layout.addWidget(reuse_group)

        layout.addStretch()

        # Connect signals
        self.email_scanner_cb.toggled.connect(self._emit_config)
        self.accts_scanner_cb.toggled.connect(self._emit_config)
        self.thread_spinner.valueChanged.connect(self._emit_config)
        self.reuse_button_group.buttonClicked.connect(self._emit_config)
        self.images_scanner_cb.toggled.connect(self._emit_config)

    def _emit_config(self):
        """Emit current configuration."""
        self.configChanged.emit(self.get_config())

    def get_config(self) -> dict:
        """Get current configuration as dict."""
        scanners = []
        if self.email_scanner_cb.isChecked():
            scanners.append("email")
        if self.accts_scanner_cb.isChecked():
            scanners.append("accts")
        carve_images = self.images_scanner_cb.isChecked()

        reuse_policy = "reuse" if self.reuse_radio.isChecked() else "overwrite"

        return {
            "scanners": scanners,
            "num_threads": self.thread_spinner.value(),
            "output_reuse_policy": reuse_policy,
            "carve_images": carve_images,
        }

    def set_config(self, config: dict):
        """Set configuration from dict."""
        scanners = config.get("scanners", ["email", "accts"])

        self.email_scanner_cb.setChecked("email" in scanners)
        self.accts_scanner_cb.setChecked("accts" in scanners)
        self.images_scanner_cb.setChecked(config.get("carve_images", True))

        if "num_threads" in config:
            self.thread_spinner.setValue(config["num_threads"])

        reuse_policy = config.get("output_reuse_policy", "reuse")
        if reuse_policy == "reuse":
            self.reuse_radio.setChecked(True)
        else:
            self.overwrite_radio.setChecked(True)
