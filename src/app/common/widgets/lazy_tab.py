"""
Lazy loading support for tab widgets.

Phase 3 implementation: Defers data loading until a tab becomes visible,
preventing UI blocking when opening cases with many artifacts.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QLabel,
    QProgressBar,
    QStackedWidget,
)

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class LoadingOverlay(QWidget):
    """
    A loading overlay widget shown while tab data is being loaded.
    """

    def __init__(self, message: str = "Loading...", parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignCenter)

        self.message_label = QLabel(message)
        self.message_label.setAlignment(Qt.AlignCenter)
        self.message_label.setStyleSheet("font-size: 14px; color: #666;")

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.progress_bar.setMaximumWidth(300)
        self.progress_bar.setTextVisible(False)

        layout.addStretch()
        layout.addWidget(self.message_label)
        layout.addWidget(self.progress_bar, alignment=Qt.AlignCenter)
        layout.addStretch()

        self.setLayout(layout)

    def set_message(self, message: str) -> None:
        """Update the loading message."""
        self.message_label.setText(message)


class LazyLoadMixin:
    """
    Mixin class that adds lazy loading capability to tab widgets.

    Usage:
        class MyTab(QWidget, LazyLoadMixin):
            def __init__(self):
                QWidget.__init__(self)
                LazyLoadMixin.__init__(self)
                self._setup_lazy_loading()

            def _do_load_data(self):
                # Override to load actual data
                self.model.reload()
                self._populate_filters()

    The mixin intercepts set_evidence() calls and defers the actual data
    loading until the tab becomes visible.
    """

    # Signal emitted when data loading is complete
    dataLoaded = Signal()

    def __init__(self) -> None:
        # Lazy loading state
        self._lazy_initialized = False
        self._lazy_loaded = False
        self._lazy_pending_evidence_id: Optional[int] = None
        self._lazy_pending_case_data: Optional["CaseDataAccess"] = None
        self._lazy_content_widget: Optional[QWidget] = None
        self._lazy_loading_widget: Optional[LoadingOverlay] = None
        self._lazy_stacked: Optional[QStackedWidget] = None

    def _setup_lazy_loading(self, content_widget: QWidget) -> QStackedWidget:
        """
        Set up lazy loading infrastructure.

        Args:
            content_widget: The main content widget to show after loading

        Returns:
            A QStackedWidget that should be used as the main layout widget
        """
        self._lazy_content_widget = content_widget
        self._lazy_loading_widget = LoadingOverlay("Loading data...")

        self._lazy_stacked = QStackedWidget()
        self._lazy_stacked.addWidget(self._lazy_loading_widget)
        self._lazy_stacked.addWidget(content_widget)
        self._lazy_stacked.setCurrentIndex(0)  # Show loading initially

        self._lazy_initialized = True
        return self._lazy_stacked

    def _set_evidence_lazy(self, evidence_id: Optional[int]) -> None:
        """
        Called by set_evidence() - stores evidence ID but defers loading.

        If the tab is already visible, loads immediately.
        Otherwise, loading is deferred until showEvent.
        """
        self._lazy_pending_evidence_id = evidence_id
        self._lazy_loaded = False

        # If we're already visible, load now
        if hasattr(self, 'isVisible') and self.isVisible():
            self._trigger_lazy_load()
        else:
            # Show loading overlay
            if self._lazy_stacked:
                self._lazy_stacked.setCurrentIndex(0)

    def _set_case_data_lazy(self, case_data: Optional["CaseDataAccess"]) -> None:
        """Store case data for lazy loading."""
        self._lazy_pending_case_data = case_data
        self._lazy_loaded = False

    def showEvent(self, event) -> None:
        """Override showEvent to trigger lazy loading when tab becomes visible."""
        # Call parent showEvent
        super().showEvent(event)

        # Trigger lazy load if we have pending data and haven't loaded yet
        if self._lazy_initialized and not self._lazy_loaded and self._lazy_pending_evidence_id is not None:
            # Use a short timer to let the UI paint first
            QTimer.singleShot(10, self._trigger_lazy_load)

    def _trigger_lazy_load(self) -> None:
        """Trigger the actual data loading."""
        if self._lazy_loaded:
            return

        self._lazy_loaded = True

        # Show loading state
        if self._lazy_stacked and self._lazy_loading_widget:
            self._lazy_loading_widget.set_message("Loading data...")
            self._lazy_stacked.setCurrentIndex(0)

        # Perform the actual loading (deferred slightly to allow UI update)
        QTimer.singleShot(50, self._perform_lazy_load)

    def _perform_lazy_load(self) -> None:
        """
        Perform the actual data loading.

        Override _do_load_data() in subclasses to customize loading behavior.
        """
        try:
            self._do_load_data()
        finally:
            # Show content
            if self._lazy_stacked:
                self._lazy_stacked.setCurrentIndex(1)

            # Emit signal
            if hasattr(self, 'dataLoaded'):
                try:
                    self.dataLoaded.emit()
                except (RuntimeError, AttributeError):
                    pass  # Signal not connected or widget deleted

    def _do_load_data(self) -> None:
        """
        Override this method in subclasses to perform actual data loading.

        This is called when the tab becomes visible for the first time
        after set_evidence() was called.
        """
        pass

    def _reset_lazy_state(self) -> None:
        """Reset lazy loading state (call when evidence changes)."""
        self._lazy_loaded = False
        if self._lazy_stacked:
            self._lazy_stacked.setCurrentIndex(0)

    def force_reload(self) -> None:
        """Force a reload of data even if already loaded."""
        self._lazy_loaded = False
        if hasattr(self, 'isVisible') and self.isVisible():
            self._trigger_lazy_load()
