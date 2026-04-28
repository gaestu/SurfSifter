"""SwiftBeaver configuration widget."""

import os

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QCheckBox, QSpinBox,
    QLabel, QHBoxLayout, QRadioButton, QButtonGroup, QComboBox
)
from PySide6.QtCore import Signal


class SwiftbeaverConfigWidget(QWidget):
    """
    Configuration widget for SwiftBeaver.

    Allows user to configure:
    - Image types to carve
    - String scanning (URLs)
    - Worker thread count
    - Minimum image size filter
    - Output reuse policy
    """

    configChanged = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        """Create UI elements."""
        layout = QVBoxLayout(self)

        # --- Image Types ---
        types_group = QGroupBox("Image Types")
        types_layout = QVBoxLayout()

        self._type_checkboxes: dict[str, QCheckBox] = {}
        image_types = [
            ("jpeg", "JPEG"),
            ("png", "PNG"),
            ("gif", "GIF"),
            ("webp", "WebP"),
            ("bmp", "BMP"),
            ("tiff", "TIFF"),
            ("heic", "HEIC"),
            ("ico", "ICO"),
        ]
        for type_id, label in image_types:
            cb = QCheckBox(label)
            cb.setChecked(True)
            cb.toggled.connect(self._emit_config)
            types_layout.addWidget(cb)
            self._type_checkboxes[type_id] = cb

        types_group.setLayout(types_layout)
        layout.addWidget(types_group)

        # --- String Scanning ---
        strings_group = QGroupBox("String Scanning")
        strings_layout = QVBoxLayout()

        self._scan_urls_cb = QCheckBox("Scan for URLs")
        self._scan_urls_cb.setChecked(True)
        self._scan_urls_cb.setToolTip("Extract URLs from evidence using string scanning")
        self._scan_urls_cb.toggled.connect(self._emit_config)
        strings_layout.addWidget(self._scan_urls_cb)

        strings_group.setLayout(strings_layout)
        layout.addWidget(strings_group)

        # --- Performance ---
        perf_group = QGroupBox("Performance")
        perf_layout = QHBoxLayout()

        perf_layout.addWidget(QLabel("Worker threads:"))

        cpu_count = os.cpu_count() or 4
        default_workers = max(1, min(cpu_count - 2, 16))

        self._workers_spin = QSpinBox()
        self._workers_spin.setMinimum(1)
        self._workers_spin.setMaximum(32)
        self._workers_spin.setValue(default_workers)
        self._workers_spin.setToolTip(
            f"Recommended: {default_workers} (CPU cores: {cpu_count})"
        )
        self._workers_spin.valueChanged.connect(self._emit_config)
        perf_layout.addWidget(self._workers_spin)

        perf_layout.addStretch()
        perf_group.setLayout(perf_layout)
        layout.addWidget(perf_group)

        # --- Image Filtering ---
        filter_group = QGroupBox("Image Filtering")
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Min image size:"))

        self._min_size_spin = QSpinBox()
        self._min_size_spin.setRange(0, 999999)
        self._min_size_spin.setValue(4)  # Default 4 KB
        self._min_size_spin.valueChanged.connect(self._emit_config)
        filter_layout.addWidget(self._min_size_spin)

        self._min_size_unit = QComboBox()
        self._min_size_unit.addItems(["KB", "MB"])
        self._min_size_unit.currentIndexChanged.connect(self._emit_config)
        filter_layout.addWidget(self._min_size_unit)

        filter_layout.addStretch()
        filter_group.setLayout(filter_layout)
        layout.addWidget(filter_group)

        # --- Output Handling ---
        reuse_group = QGroupBox("Output Handling")
        reuse_layout = QVBoxLayout()

        self._reuse_button_group = QButtonGroup(self)

        self._reuse_radio = QRadioButton("Reuse existing output if available")
        self._reuse_radio.setChecked(True)
        self._reuse_radio.setToolTip("Skip extraction if output files already exist")
        self._reuse_button_group.addButton(self._reuse_radio, 0)
        reuse_layout.addWidget(self._reuse_radio)

        self._overwrite_radio = QRadioButton("Always overwrite (re-run extraction)")
        self._overwrite_radio.setToolTip("Delete existing output and run extraction again")
        self._reuse_button_group.addButton(self._overwrite_radio, 1)
        reuse_layout.addWidget(self._overwrite_radio)

        reuse_group.setLayout(reuse_layout)
        layout.addWidget(reuse_group)

        layout.addStretch()

        # Connect button group signal
        self._reuse_button_group.buttonClicked.connect(self._emit_config)

    def _emit_config(self):
        """Emit current configuration."""
        self.configChanged.emit(self.get_config())

    def _min_size_to_bytes(self) -> int:
        """Convert spin + unit to bytes."""
        value = self._min_size_spin.value()
        unit = self._min_size_unit.currentText()
        if unit == "MB":
            return value * 1024 * 1024
        return value * 1024  # KB

    def get_config(self) -> dict:
        """Get current configuration as dict."""
        image_types = [
            type_id for type_id, cb in self._type_checkboxes.items() if cb.isChecked()
        ]

        reuse_policy = "reuse" if self._reuse_radio.isChecked() else "overwrite"

        return {
            "image_types": image_types,
            "scan_urls": self._scan_urls_cb.isChecked(),
            "num_workers": self._workers_spin.value(),
            "min_size_bytes": self._min_size_to_bytes(),
            "output_reuse_policy": reuse_policy,
        }

    def set_config(self, config: dict):
        """Set configuration from dict."""
        if "image_types" in config:
            selected = config["image_types"]
            for type_id, cb in self._type_checkboxes.items():
                cb.setChecked(type_id in selected)

        if "scan_urls" in config:
            self._scan_urls_cb.setChecked(config["scan_urls"])

        if "num_workers" in config:
            self._workers_spin.setValue(config["num_workers"])

        if "min_size_bytes" in config:
            size_bytes = config["min_size_bytes"]
            if size_bytes >= 1024 * 1024 and size_bytes % (1024 * 1024) == 0:
                self._min_size_unit.setCurrentText("MB")
                self._min_size_spin.setValue(size_bytes // (1024 * 1024))
            else:
                self._min_size_unit.setCurrentText("KB")
                self._min_size_spin.setValue(size_bytes // 1024)

        if "output_reuse_policy" in config:
            if config["output_reuse_policy"] == "reuse":
                self._reuse_radio.setChecked(True)
            else:
                self._overwrite_radio.setChecked(True)
