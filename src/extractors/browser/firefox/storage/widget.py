"""
Firefox Browser Storage Widget.

Configuration widget for Firefox storage extraction settings.

Features:
- Browser selection with multi-partition support
- Storage type selection (Local Storage, IndexedDB)
- Value analysis options (URLs, emails, tokens, identifiers)
- Configurable excerpt size

Extracted from extractor.py
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import (
    QWidget,
    QLabel,
    QVBoxLayout,
    QCheckBox,
    QSpinBox,
    QHBoxLayout,
    QGroupBox,
)

from extractors.widgets import BrowserConfigWidget
from .._patterns import FIREFOX_BROWSERS


class FirefoxStorageWidget(QWidget):
    """Widget for selecting storage types and options."""

    def __init__(self, parent: QWidget = None, supported_browsers: Optional[List[str]] = None):
        super().__init__(parent)

        layout = QVBoxLayout(self)

        # Browser selection with multi-partition support
        if supported_browsers is None:
            supported_browsers = list(FIREFOX_BROWSERS.keys())
        self._browser_widget = BrowserConfigWidget(self, supported_browsers=supported_browsers)
        layout.addWidget(self._browser_widget)

        # Storage type selection
        storage_group = QGroupBox("Storage Types")
        storage_layout = QVBoxLayout(storage_group)

        self._local_storage_cb = QCheckBox("Local Storage - Key-value pairs persisted per site")
        self._local_storage_cb.setChecked(True)
        storage_layout.addWidget(self._local_storage_cb)

        self._indexeddb_cb = QCheckBox("IndexedDB - Structured databases for web apps")
        self._indexeddb_cb.setChecked(True)
        storage_layout.addWidget(self._indexeddb_cb)

        layout.addWidget(storage_group)

        # Analysis options
        analysis_group = QGroupBox("Value Analysis")
        analysis_layout = QVBoxLayout(analysis_group)

        self._analyze_values_cb = QCheckBox("Analyze storage values for forensic artifacts")
        self._analyze_values_cb.setChecked(True)
        self._analyze_values_cb.setToolTip(
            "Parse storage values to extract URLs, emails, tokens, and identifiers"
        )
        analysis_layout.addWidget(self._analyze_values_cb)

        # Sub-options (indented)
        sub_layout = QVBoxLayout()
        sub_layout.setContentsMargins(20, 0, 0, 0)

        self._extract_urls_cb = QCheckBox("Extract URLs with timestamps")
        self._extract_urls_cb.setChecked(True)
        self._extract_urls_cb.setToolTip("Find URLs in JSON values and plain text")
        sub_layout.addWidget(self._extract_urls_cb)

        self._extract_emails_cb = QCheckBox("Extract email addresses")
        self._extract_emails_cb.setChecked(True)
        self._extract_emails_cb.setToolTip("Find email addresses in storage values")
        sub_layout.addWidget(self._extract_emails_cb)

        self._detect_tokens_cb = QCheckBox("Detect authentication tokens (JWT, OAuth)")
        self._detect_tokens_cb.setChecked(True)
        self._detect_tokens_cb.setToolTip("Find and classify auth tokens (HIGH forensic value)")
        sub_layout.addWidget(self._detect_tokens_cb)

        self._extract_identifiers_cb = QCheckBox("Extract user/tracking identifiers")
        self._extract_identifiers_cb.setChecked(True)
        self._extract_identifiers_cb.setToolTip("Find user IDs, device IDs, tracking IDs")
        sub_layout.addWidget(self._extract_identifiers_cb)

        analysis_layout.addLayout(sub_layout)

        # Connect master checkbox to enable/disable sub-options
        self._analyze_values_cb.toggled.connect(self._on_analyze_toggled)

        layout.addWidget(analysis_group)

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

        layout.addWidget(options_group)
        layout.addStretch()

    def _on_analyze_toggled(self, checked: bool) -> None:
        """Enable/disable sub-options when master checkbox toggled."""
        self._extract_urls_cb.setEnabled(checked)
        self._extract_emails_cb.setEnabled(checked)
        self._detect_tokens_cb.setEnabled(checked)
        self._extract_identifiers_cb.setEnabled(checked)

    def get_config(self) -> Dict[str, Any]:
        """Get current configuration."""
        browser_config = self._browser_widget.get_config()
        return {
            "browsers": browser_config.get("browsers") or browser_config.get("selected_browsers", []),
            "local_storage": self._local_storage_cb.isChecked(),
            "indexeddb": self._indexeddb_cb.isChecked(),
            "excerpt_size": self._excerpt_size.value(),
            "analyze_values": self._analyze_values_cb.isChecked(),
            "extract_urls": self._extract_urls_cb.isChecked(),
            "extract_emails": self._extract_emails_cb.isChecked(),
            "detect_tokens": self._detect_tokens_cb.isChecked(),
            "extract_identifiers": self._extract_identifiers_cb.isChecked(),
        }
