"""
Configuration widget for Chromium Browser Storage Extractor.

Provides UI for selecting storage types and extraction options.

Extracted from extractor.py for maintainability
"""
from __future__ import annotations

from typing import Dict, Any, List, Optional

from PySide6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QCheckBox, QSpinBox,
    QHBoxLayout, QGroupBox
)

from ....widgets import BrowserConfigWidget
from .._patterns import CHROMIUM_BROWSERS


class ChromiumStorageWidget(QWidget):
    """Widget for selecting storage types and options."""

    def __init__(self, parent: Optional[QWidget] = None, supported_browsers: Optional[List[str]] = None):
        super().__init__(parent)

        layout = QVBoxLayout(self)

        # Browser selection with multi-partition support
        if supported_browsers is None:
            supported_browsers = list(CHROMIUM_BROWSERS.keys())
        self._browser_widget = BrowserConfigWidget(self, supported_browsers=supported_browsers)
        layout.addWidget(self._browser_widget)

        # Storage type selection
        storage_group = QGroupBox("Storage Types")
        storage_layout = QVBoxLayout(storage_group)

        self._local_storage_cb = QCheckBox("Local Storage - Key-value pairs persisted per site")
        self._local_storage_cb.setChecked(True)
        storage_layout.addWidget(self._local_storage_cb)

        self._session_storage_cb = QCheckBox("Session Storage - Per-tab temporary storage")
        self._session_storage_cb.setChecked(True)
        storage_layout.addWidget(self._session_storage_cb)

        self._indexeddb_cb = QCheckBox("IndexedDB - Structured databases for web apps")
        self._indexeddb_cb.setChecked(True)
        storage_layout.addWidget(self._indexeddb_cb)

        layout.addWidget(storage_group)

        # Options
        options_group = QGroupBox("Options")
        options_layout = QVBoxLayout(options_group)

        # Value excerpt size
        excerpt_layout = QHBoxLayout()
        excerpt_layout.addWidget(QLabel("Value excerpt size:"))
        self._excerpt_size = QSpinBox()
        self._excerpt_size.setRange(256, 65536)
        self._excerpt_size.setValue(4096)
        self._excerpt_size.setSuffix(" bytes")
        excerpt_layout.addWidget(self._excerpt_size)
        excerpt_layout.addStretch()
        options_layout.addLayout(excerpt_layout)

        self._deleted_cb = QCheckBox("Include deleted/historical records")
        self._deleted_cb.setChecked(True)
        options_layout.addWidget(self._deleted_cb)

        self._extract_images_cb = QCheckBox("Extract images from IndexedDB blobs")
        self._extract_images_cb.setChecked(True)
        self._extract_images_cb.setToolTip("Detect and extract image data from IndexedDB blob values")
        options_layout.addWidget(self._extract_images_cb)

        layout.addWidget(options_group)
        layout.addStretch()

    def get_config(self) -> Dict[str, Any]:
        """Get current configuration."""
        browser_config = self._browser_widget.get_config()
        return {
            "browsers": browser_config.get("browsers") or browser_config.get("selected_browsers", []),
            "local_storage": self._local_storage_cb.isChecked(),
            "session_storage": self._session_storage_cb.isChecked(),
            "indexeddb": self._indexeddb_cb.isChecked(),
            "excerpt_size": self._excerpt_size.value(),
            "include_deleted": self._deleted_cb.isChecked(),
            "extract_images": self._extract_images_cb.isChecked(),
        }
