"""
Browser Selection Widget

Configuration widgets for browser artifact extractors.
Provides checkboxes for selecting which browsers to extract from.

Added MultiPartitionWidget for multi-partition discovery configuration.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QCheckBox,
    QGroupBox,
    QLabel,
    QPushButton,
)
from PySide6.QtCore import Signal

from .browser_patterns import BROWSER_PATTERNS, get_browser_display_name


class BrowserSelectionWidget(QWidget):
    """
    Widget for selecting browsers to extract artifacts from.

    Provides:
    - Checkboxes for each supported browser
    - Select All / Deselect All buttons
    - get_config() method for integration with extraction UI

    Usage:
        widget = BrowserSelectionWidget(parent)
        # ... user interacts with checkboxes ...
        config = widget.get_config()
        # config = {"browsers": ["chrome", "edge", "firefox"]}

    For cookies extractor (limits to browsers with cookie patterns):
        widget = BrowserSelectionWidget(parent, supported_browsers=["chrome", "edge", "firefox"])
    """

    # Signal emitted when selection changes
    selection_changed = Signal()

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        default_browsers: Optional[List[str]] = None,
        show_all_buttons: bool = True,
        supported_browsers: Optional[List[str]] = None,
    ):
        """
        Initialize the browser selection widget.

        Args:
            parent: Parent widget
            default_browsers: List of browser keys to select by default (None = all supported)
            show_all_buttons: Whether to show Select All / Deselect All buttons
            supported_browsers: List of browser keys to show (None = all from BROWSER_PATTERNS)
        """
        super().__init__(parent)

        self._checkboxes: Dict[str, QCheckBox] = {}
        self._supported_browsers = supported_browsers or list(BROWSER_PATTERNS.keys())
        self._default_browsers = default_browsers or self._supported_browsers

        self._setup_ui(show_all_buttons)

    def _setup_ui(self, show_all_buttons: bool) -> None:
        """Set up the widget UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Browser selection group
        group = QGroupBox("Browsers to Extract")
        group_layout = QVBoxLayout(group)

        # Browser checkboxes (in two columns)
        browsers_layout = QHBoxLayout()

        # Left column
        left_col = QVBoxLayout()
        # Right column
        right_col = QVBoxLayout()

        # Only show supported browsers
        browser_keys = [b for b in BROWSER_PATTERNS.keys() if b in self._supported_browsers]
        mid = (len(browser_keys) + 1) // 2

        for i, browser_key in enumerate(browser_keys):
            display_name = get_browser_display_name(browser_key)
            engine = BROWSER_PATTERNS[browser_key]["engine"]

            checkbox = QCheckBox(f"{display_name} ({engine})")
            checkbox.setObjectName(f"browser_{browser_key}")
            checkbox.setChecked(browser_key in self._default_browsers)
            checkbox.stateChanged.connect(self._on_selection_changed)

            self._checkboxes[browser_key] = checkbox

            if i < mid:
                left_col.addWidget(checkbox)
            else:
                right_col.addWidget(checkbox)

        browsers_layout.addLayout(left_col)
        browsers_layout.addLayout(right_col)
        group_layout.addLayout(browsers_layout)

        # Select All / Deselect All buttons
        if show_all_buttons:
            buttons_layout = QHBoxLayout()

            select_all_btn = QPushButton("Select All")
            select_all_btn.clicked.connect(self._select_all)
            buttons_layout.addWidget(select_all_btn)

            deselect_all_btn = QPushButton("Deselect All")
            deselect_all_btn.clicked.connect(self._deselect_all)
            buttons_layout.addWidget(deselect_all_btn)

            buttons_layout.addStretch()
            group_layout.addLayout(buttons_layout)

        layout.addWidget(group)

        # Info label
        info_label = QLabel(
            "<small>Select which browsers to scan for artifacts. "
            "Chromium-based browsers (Chrome, Edge, Opera, Brave) share similar formats.</small>"
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; margin-top: 5px;")
        layout.addWidget(info_label)

    def _on_selection_changed(self) -> None:
        """Handle checkbox state change."""
        self.selection_changed.emit()

    def _select_all(self) -> None:
        """Select all browsers."""
        for checkbox in self._checkboxes.values():
            checkbox.setChecked(True)

    def _deselect_all(self) -> None:
        """Deselect all browsers."""
        for checkbox in self._checkboxes.values():
            checkbox.setChecked(False)

    def get_config(self) -> Dict[str, Any]:
        """
        Get current configuration.

        Returns:
            Dict with "browsers" key containing list of selected browser keys
        """
        selected = [
            browser_key
            for browser_key, checkbox in self._checkboxes.items()
            if checkbox.isChecked()
        ]
        return {"browsers": selected}

    def set_config(self, config: Dict[str, Any]) -> None:
        """
        Set configuration from dict.

        Args:
            config: Dict with "browsers" key (or "selected_browsers" for backward compat)
        """
        # Accept both "browsers" and "selected_browsers" keys
        browsers = config.get("browsers") or config.get("selected_browsers", [])

        for browser_key, checkbox in self._checkboxes.items():
            checkbox.setChecked(browser_key in browsers)

    def get_selected_browsers(self) -> List[str]:
        """
        Get list of selected browser keys.

        Returns:
            List of browser keys that are checked
        """
        return self.get_config()["browsers"]

    def set_selected_browsers(self, browsers: List[str]) -> None:
        """
        Set selected browsers.

        Args:
            browsers: List of browser keys to select
        """
        self.set_config({"browsers": browsers})


class MultiPartitionWidget(QWidget):
    """
    Widget for configuring multi-partition discovery.

    Provides a checkbox to enable/disable scanning all partitions
    (vs. only the auto-selected main partition).

    Usage:
        widget = MultiPartitionWidget(parent)
        config = widget.get_config()
        # config = {"scan_all_partitions": True}

    Combine with BrowserSelectionWidget:
        browser_widget = BrowserSelectionWidget(parent)
        partition_widget = MultiPartitionWidget(parent)
        # ...
        config = {**browser_widget.get_config(), **partition_widget.get_config()}
    """

    # Signal emitted when configuration changes
    config_changed = Signal()

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        default_enabled: bool = True,
        show_info: bool = True,
    ):
        """
        Initialize the multi-partition configuration widget.

        Args:
            parent: Parent widget
            default_enabled: Whether multi-partition scanning is enabled by default
            show_info: Whether to show explanatory text
        """
        super().__init__(parent)

        self._default_enabled = default_enabled
        self._setup_ui(show_info)

    def _setup_ui(self, show_info: bool) -> None:
        """Set up the widget UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Multi-partition checkbox
        self._checkbox = QCheckBox("Scan all partitions (recommended)")
        self._checkbox.setObjectName("scan_all_partitions")
        self._checkbox.setChecked(self._default_enabled)
        self._checkbox.setToolTip(
            "When enabled, searches for browser artifacts across all partitions.\n"
            "This finds artifacts in dual-boot systems, portable apps, and old OS installations.\n"
            "Requires file_list to be generated first (via File List extractor)."
        )
        self._checkbox.stateChanged.connect(self._on_config_changed)
        layout.addWidget(self._checkbox)

        # Info label
        if show_info:
            info_label = QLabel(
                "<small>💡 <b>Multi-partition discovery</b>: Scans all partitions for browser artifacts, "
                "not just the main Windows partition. Useful for dual-boot systems, portable apps, "
                "and forensic recovery of old installations.</small>"
            )
            info_label.setWordWrap(True)
            info_label.setStyleSheet("color: #666; margin-top: 3px; margin-left: 20px;")
            layout.addWidget(info_label)

    def _on_config_changed(self) -> None:
        """Handle configuration change."""
        self.config_changed.emit()

    def get_config(self) -> Dict[str, Any]:
        """
        Get current configuration.

        Returns:
            Dict with "scan_all_partitions" key
        """
        return {"scan_all_partitions": self._checkbox.isChecked()}

    def set_config(self, config: Dict[str, Any]) -> None:
        """
        Set configuration from dict.

        Args:
            config: Dict with "scan_all_partitions" key
        """
        if "scan_all_partitions" in config:
            self._checkbox.setChecked(bool(config["scan_all_partitions"]))

    def is_enabled(self) -> bool:
        """Return True if multi-partition scanning is enabled."""
        return self._checkbox.isChecked()

    def set_enabled(self, enabled: bool) -> None:
        """Set whether multi-partition scanning is enabled."""
        self._checkbox.setChecked(enabled)


class BrowserConfigWidget(QWidget):
    """
    Combined widget for browser extraction configuration.

    Combines BrowserSelectionWidget and MultiPartitionWidget into
    a single widget for browser artifact extractors.

    Usage:
        widget = BrowserConfigWidget(parent)
        config = widget.get_config()
        # config = {"browsers": ["chrome", "firefox"], "scan_all_partitions": True}
    """

    config_changed = Signal()

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        default_browsers: Optional[List[str]] = None,
        supported_browsers: Optional[List[str]] = None,
        default_scan_all_partitions: bool = True,
    ):
        """
        Initialize the combined browser config widget.

        Args:
            parent: Parent widget
            default_browsers: List of browser keys to select by default
            supported_browsers: List of browser keys to show
            default_scan_all_partitions: Whether to scan all partitions by default
        """
        super().__init__(parent)

        self._setup_ui(
            default_browsers,
            supported_browsers,
            default_scan_all_partitions,
        )

    def _setup_ui(
        self,
        default_browsers: Optional[List[str]],
        supported_browsers: Optional[List[str]],
        default_scan_all_partitions: bool,
    ) -> None:
        """Set up the widget UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Browser selection
        self._browser_widget = BrowserSelectionWidget(
            parent=self,
            default_browsers=default_browsers,
            supported_browsers=supported_browsers,
        )
        self._browser_widget.selection_changed.connect(self._on_config_changed)
        layout.addWidget(self._browser_widget)

        # Multi-partition option
        self._partition_widget = MultiPartitionWidget(
            parent=self,
            default_enabled=default_scan_all_partitions,
        )
        self._partition_widget.config_changed.connect(self._on_config_changed)
        layout.addWidget(self._partition_widget)

    def _on_config_changed(self) -> None:
        """Handle configuration change."""
        self.config_changed.emit()

    def get_config(self) -> Dict[str, Any]:
        """
        Get combined configuration.

        Returns:
            Dict with "browsers" and "scan_all_partitions" keys
        """
        config = self._browser_widget.get_config()
        config.update(self._partition_widget.get_config())
        return config

    def set_config(self, config: Dict[str, Any]) -> None:
        """
        Set configuration from dict.

        Args:
            config: Dict with "browsers" and/or "scan_all_partitions" keys
        """
        self._browser_widget.set_config(config)
        self._partition_widget.set_config(config)

    def get_selected_browsers(self) -> List[str]:
        """Get list of selected browser keys."""
        return self._browser_widget.get_selected_browsers()

    def is_multi_partition_enabled(self) -> bool:
        """Return True if multi-partition scanning is enabled."""
        return self._partition_widget.is_enabled()


class ESEStatusWidget(QWidget):
    """
    Widget showing ESE library status for IE/Edge extractors.

    Displays whether the ESE library is installed and available
    for parsing WebCacheV01.dat files.

    Usage:
        widget = ESEStatusWidget(parent)
        config = widget.get_config()  # Always returns {}
    """

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        show_install_hint: bool = True,
    ):
        """
        Initialize the ESE status widget.

        Args:
            parent: Parent widget
            show_install_hint: Whether to show installation instructions
        """
        super().__init__(parent)
        self._show_install_hint = show_install_hint
        self._setup_ui()

    def _setup_ui(self) -> None:
        """Set up the widget UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Check ESE availability
        try:
            from .browser.ie_legacy._ese_reader import check_ese_available, ESE_LIBRARY
            ese_ok, ese_info = check_ese_available()
        except ImportError:
            ese_ok = False
            ese_info = "ESE module not found"

        # Status group
        group = QGroupBox("ESE Library Status")
        group_layout = QVBoxLayout(group)

        if ese_ok:
            status_label = QLabel(f"✅ ESE library available: <b>{ese_info}</b>")
            status_label.setStyleSheet("color: #2e7d32;")  # Green
        else:
            status_label = QLabel("⚠️ ESE library <b>not installed</b>")
            status_label.setStyleSheet("color: #d84315;")  # Orange-red

        group_layout.addWidget(status_label)

        if not ese_ok and self._show_install_hint:
            hint_label = QLabel(
                "<small>Install with: <code>pip install libesedb-python</code><br>"
                "Or: <code>pip install dissect.esedb</code></small>"
            )
            hint_label.setStyleSheet("color: #666; margin-top: 5px;")
            hint_label.setWordWrap(True)
            group_layout.addWidget(hint_label)

        # Info about what ESE is used for
        info_label = QLabel(
            "<small>The ESE library is required to parse Internet Explorer and "
            "Legacy Edge WebCacheV01.dat database files.</small>"
        )
        info_label.setStyleSheet("color: #666; margin-top: 5px;")
        info_label.setWordWrap(True)
        group_layout.addWidget(info_label)

        layout.addWidget(group)

    def get_config(self) -> Dict[str, Any]:
        """
        Get configuration (empty for status widget).

        Returns:
            Empty dict (no configuration)
        """
        return {}

    def set_config(self, config: Dict[str, Any]) -> None:
        """Set configuration (no-op for status widget)."""
        pass

    def is_ese_available(self) -> bool:
        """Check if ESE library is available."""
        try:
            from .browser.ie_legacy._ese_reader import check_ese_available
            ese_ok, _ = check_ese_available()
            return ese_ok
        except ImportError:
            return False
