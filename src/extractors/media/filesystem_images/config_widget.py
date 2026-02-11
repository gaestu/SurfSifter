"""
Filesystem Images Configuration Widget

Configuration UI for the filesystem images extractor.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Set

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QSpinBox,
    QComboBox,
    QCheckBox,
    QPushButton,
    QGridLayout,
)

from extractors.image_signatures import SUPPORTED_IMAGE_EXTENSIONS
from .parallel_extractor import MAX_WORKERS_CAP


class FilesystemImagesConfigWidget(QWidget):
    """
    Configuration widget for filesystem images extractor.

    Allows user to configure:
    - Include/exclude patterns
    - Size range filtering
    - Extension selection
    - Signature detection toggle
    - Folder structure preservation
    """

    configChanged = Signal()

    def __init__(self, parent: QWidget = None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Path Patterns Group
        patterns_group = QGroupBox("Path Patterns")
        patterns_layout = QVBoxLayout(patterns_group)

        # Include patterns
        include_label = QLabel("Include Patterns (one per line, leave empty for all files):")
        self._include_edit = QPlainTextEdit()
        self._include_edit.setPlaceholderText(
            "Examples:\n"
            "Users/*/Pictures/**\n"
            "Users/*/Downloads/**\n"
            "Users/*/AppData/Local/Temp/**"
        )
        self._include_edit.setMaximumHeight(80)
        # textChanged has no arguments, safe to connect directly
        self._include_edit.textChanged.connect(lambda: self.configChanged.emit())

        # Exclude patterns
        exclude_label = QLabel("Exclude Patterns (one per line):")
        self._exclude_edit = QPlainTextEdit()
        self._exclude_edit.setPlaceholderText(
            "Examples:\n"
            "Windows/**\n"
            "Program Files/**\n"
            "*.dll"
        )
        self._exclude_edit.setMaximumHeight(80)
        # Pre-fill with common excludes
        self._exclude_edit.setPlainText(
            "Windows/**\n"
            "Program Files/**\n"
            "Program Files (x86)/**\n"
            "$Recycle.Bin/**"
        )
        # textChanged has no arguments, safe to connect directly
        self._exclude_edit.textChanged.connect(lambda: self.configChanged.emit())

        patterns_layout.addWidget(include_label)
        patterns_layout.addWidget(self._include_edit)
        patterns_layout.addWidget(exclude_label)
        patterns_layout.addWidget(self._exclude_edit)

        # Size Filter Group
        size_group = QGroupBox("Size Filters")
        size_layout = QHBoxLayout(size_group)

        # Minimum size
        size_layout.addWidget(QLabel("Min:"))
        self._min_size_spin = QSpinBox()
        self._min_size_spin.setRange(0, 999999)
        self._min_size_spin.setValue(1)  # Default: 1 KB (skip zero-byte OneDrive placeholders)
        # valueChanged emits int, use lambda to ignore
        self._min_size_spin.valueChanged.connect(lambda _: self.configChanged.emit())
        size_layout.addWidget(self._min_size_spin)

        self._min_size_unit = QComboBox()
        self._min_size_unit.addItems(["KB", "MB"])
        # currentIndexChanged emits int, use lambda to ignore
        self._min_size_unit.currentIndexChanged.connect(lambda _: self.configChanged.emit())
        size_layout.addWidget(self._min_size_unit)

        size_layout.addSpacing(20)

        # Maximum size
        size_layout.addWidget(QLabel("Max:"))
        self._max_size_spin = QSpinBox()
        self._max_size_spin.setRange(0, 999999)
        self._max_size_spin.setValue(0)  # Default: 0 (no limit)
        self._max_size_spin.setSpecialValueText("No limit")
        # valueChanged emits int, use lambda to ignore
        self._max_size_spin.valueChanged.connect(lambda _: self.configChanged.emit())
        size_layout.addWidget(self._max_size_spin)

        self._max_size_unit = QComboBox()
        self._max_size_unit.addItems(["KB", "MB"])
        self._max_size_unit.setCurrentIndex(1)  # MB
        # currentIndexChanged emits int, use lambda to ignore
        self._max_size_unit.currentIndexChanged.connect(lambda _: self.configChanged.emit())
        size_layout.addWidget(self._max_size_unit)

        size_layout.addStretch()

        # Extensions Group
        ext_group = QGroupBox("Image Formats")
        ext_layout = QGridLayout(ext_group)

        # Common formats
        # stateChanged emits int, use lambda to ignore
        ext_layout.addWidget(QLabel("Common:"), 0, 0)
        self._ext_jpg = QCheckBox("JPG/JPEG")
        self._ext_jpg.setChecked(True)
        self._ext_jpg.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_jpg, 0, 1)

        self._ext_png = QCheckBox("PNG")
        self._ext_png.setChecked(True)
        self._ext_png.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_png, 0, 2)

        self._ext_gif = QCheckBox("GIF")
        self._ext_gif.setChecked(True)
        self._ext_gif.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_gif, 0, 3)

        self._ext_bmp = QCheckBox("BMP")
        self._ext_bmp.setChecked(True)
        self._ext_bmp.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_bmp, 0, 4)

        # Web formats
        ext_layout.addWidget(QLabel("Web:"), 1, 0)
        self._ext_webp = QCheckBox("WebP")
        self._ext_webp.setChecked(True)
        self._ext_webp.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_webp, 1, 1)

        self._ext_avif = QCheckBox("AVIF")
        self._ext_avif.setChecked(True)
        self._ext_avif.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_avif, 1, 2)

        self._ext_heic = QCheckBox("HEIC")
        self._ext_heic.setChecked(True)
        self._ext_heic.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_heic, 1, 3)

        # Other formats
        ext_layout.addWidget(QLabel("Other:"), 2, 0)
        self._ext_tiff = QCheckBox("TIFF")
        self._ext_tiff.setChecked(False)
        self._ext_tiff.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_tiff, 2, 1)

        self._ext_ico = QCheckBox("ICO")
        self._ext_ico.setChecked(False)
        self._ext_ico.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_ico, 2, 2)

        self._ext_svg = QCheckBox("SVG")
        self._ext_svg.setChecked(False)
        self._ext_svg.stateChanged.connect(lambda _: self.configChanged.emit())
        ext_layout.addWidget(self._ext_svg, 2, 3)

        # Select All / Deselect All buttons
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self._select_all_extensions)
        btn_layout.addWidget(select_all_btn)

        deselect_all_btn = QPushButton("Deselect All")
        deselect_all_btn.clicked.connect(self._deselect_all_extensions)
        btn_layout.addWidget(deselect_all_btn)
        btn_layout.addStretch()

        ext_layout.addLayout(btn_layout, 3, 0, 1, 5)

        # Options Group
        options_group = QGroupBox("Options")
        options_layout = QVBoxLayout(options_group)

        self._use_signatures = QCheckBox("Verify signatures during extraction")
        self._use_signatures.setChecked(True)
        self._use_signatures.setToolTip(
            "Check magic bytes during extraction to verify file type matches extension.\n"
            "Mismatches are flagged in the manifest (forensically interesting).\n"
            "Note: Discovery is always fast (extension-based). This only affects extraction."
        )
        # stateChanged emits int, use lambda to ignore
        self._use_signatures.stateChanged.connect(lambda _: self.configChanged.emit())
        options_layout.addWidget(self._use_signatures)

        self._preserve_structure = QCheckBox("Preserve folder structure")
        self._preserve_structure.setChecked(False)
        self._preserve_structure.setToolTip(
            "Keep original directory structure in extracted files.\n"
            "When unchecked, files are extracted flat with inode in filename\n"
            "to avoid collisions from duplicate filenames."
        )
        # stateChanged emits int, use lambda to ignore
        self._preserve_structure.stateChanged.connect(lambda _: self.configChanged.emit())
        options_layout.addWidget(self._preserve_structure)

        # Parallel Workers Group
        parallel_group = QGroupBox("Performance")
        parallel_layout = QHBoxLayout(parallel_group)

        parallel_layout.addWidget(QLabel("Parallel Workers:"))
        self._parallel_workers = QSpinBox()
        self._parallel_workers.setRange(0, MAX_WORKERS_CAP)
        self._parallel_workers.setValue(0)  # 0 = auto
        self._parallel_workers.setSpecialValueText("Auto")
        self._parallel_workers.setToolTip(
            f"Number of parallel extraction workers (max {MAX_WORKERS_CAP}).\n"
            "Auto (0) uses CPU count minus 2.\n"
            "More workers = faster extraction from E01 images.\n"
            "Each worker opens its own copy of the E01."
        )
        # Lambda ignores the int argument from valueChanged
        self._parallel_workers.valueChanged.connect(lambda _: self.configChanged.emit())
        parallel_layout.addWidget(self._parallel_workers)

        # Add detected CPU count hint
        cpu_count = os.cpu_count() or 4
        parallel_layout.addWidget(
            QLabel(f"(detected: {cpu_count} CPUs, auto={MAX_WORKERS_CAP})")
        )
        parallel_layout.addStretch()

        # Add all groups to main layout
        layout.addWidget(patterns_group)
        layout.addWidget(size_group)
        layout.addWidget(ext_group)
        layout.addWidget(options_group)
        layout.addWidget(parallel_group)
        layout.addStretch()

    def _select_all_extensions(self) -> None:
        """Select all extension checkboxes."""
        for cb in self._get_extension_checkboxes():
            cb.setChecked(True)

    def _deselect_all_extensions(self) -> None:
        """Deselect all extension checkboxes."""
        for cb in self._get_extension_checkboxes():
            cb.setChecked(False)

    def _get_extension_checkboxes(self) -> list:
        """Get all extension checkbox widgets."""
        return [
            self._ext_jpg, self._ext_png, self._ext_gif, self._ext_bmp,
            self._ext_webp, self._ext_avif, self._ext_heic,
            self._ext_tiff, self._ext_ico, self._ext_svg,
        ]

    def get_config(self) -> Dict[str, Any]:
        """
        Get current configuration values.

        Returns:
            Configuration dictionary
        """
        # Parse patterns
        include_text = self._include_edit.toPlainText().strip()
        include_patterns = [p.strip() for p in include_text.split("\n") if p.strip()]

        exclude_text = self._exclude_edit.toPlainText().strip()
        exclude_patterns = [p.strip() for p in exclude_text.split("\n") if p.strip()]

        # Calculate size in bytes
        min_size_value = self._min_size_spin.value()
        min_size_unit = self._min_size_unit.currentText()
        if min_size_unit == "MB":
            min_size_bytes = min_size_value * 1024 * 1024
        else:
            min_size_bytes = min_size_value * 1024

        max_size_value = self._max_size_spin.value()
        max_size_unit = self._max_size_unit.currentText()
        if max_size_value == 0:
            max_size_bytes = None  # No limit
        elif max_size_unit == "MB":
            max_size_bytes = max_size_value * 1024 * 1024
        else:
            max_size_bytes = max_size_value * 1024

        # Build extension config
        extensions = {
            ".jpg": self._ext_jpg.isChecked(),
            ".jpeg": self._ext_jpg.isChecked(),
            ".png": self._ext_png.isChecked(),
            ".gif": self._ext_gif.isChecked(),
            ".bmp": self._ext_bmp.isChecked(),
            ".webp": self._ext_webp.isChecked(),
            ".avif": self._ext_avif.isChecked(),
            ".heic": self._ext_heic.isChecked(),
            ".heif": self._ext_heic.isChecked(),
            ".tiff": self._ext_tiff.isChecked(),
            ".tif": self._ext_tiff.isChecked(),
            ".ico": self._ext_ico.isChecked(),
            ".svg": self._ext_svg.isChecked(),
        }

        return {
            "include_patterns": include_patterns,
            "exclude_patterns": exclude_patterns,
            "min_size_bytes": min_size_bytes,
            "max_size_bytes": max_size_bytes,
            "extensions": extensions,
            "use_signature_detection": self._use_signatures.isChecked(),
            "preserve_folder_structure": self._preserve_structure.isChecked(),
            "parallel_workers": self._parallel_workers.value() or None,  # 0 = auto (None)
        }
