"""Helpers that keep the report UI usable on small screens (issue #77).

The reports package does not depend on ``app.common``, which has the
application's own helpers for this (``app.common.widgets.scroll_area`` and
``app.common.screen_fit``).
"""

from typing import Optional

from PySide6.QtCore import QEvent, QMargins, QObject, QTimer
from PySide6.QtGui import QGuiApplication, QScreen
from PySide6.QtWidgets import QAbstractSpinBox, QComboBox, QDialog, QWidget


class WheelGuard(QObject):
    """Makes combo boxes, spin boxes and date fields ignore the mouse wheel.

    The wheel then scrolls the surrounding scroll area instead of changing a
    value.
    """

    def protect(self, content: QWidget) -> None:
        """Protect the fields that ``content`` contains now."""
        for widget in content.findChildren(QComboBox) + content.findChildren(QAbstractSpinBox):
            widget.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if event.type() == QEvent.Type.Wheel:
            # An ignored wheel event propagates to the parent, up to the scroll area.
            event.ignore()
            return True
        return False


class ScreenLimitedDialog(QDialog):
    """Dialog that never opens larger than the available area of its screen."""

    _limit_pending = False

    def showEvent(self, event) -> None:
        if not event.spontaneous():
            # Before QDialog places itself, so it uses the limited size.
            parent = self.parentWidget()
            screen = self._screen_of(parent.window() if parent is not None else self)
            if screen is not None:
                self.resize(self.size().boundedTo(screen.availableGeometry().size()))
            self._limit_pending = True
            QTimer.singleShot(0, self, self._limit_to_screen)
        super().showEvent(event)

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() == QEvent.Type.ActivationChange and self._limit_pending and self.isActiveWindow():
            # X11 reports the frame only once the window manager has mapped the window.
            self._limit_pending = False
            QTimer.singleShot(0, self, self._limit_to_screen)

    @staticmethod
    def _screen_of(window: QWidget) -> Optional[QScreen]:
        # Not QWidget.screen(): PySide ties the returned QScreen to the widget
        # and can delete it when the widget is garbage-collected.
        return QGuiApplication.screenAt(window.geometry().center()) or QGuiApplication.primaryScreen()

    def _frame_margins(self) -> QMargins:
        handle = self.windowHandle()
        return handle.frameMargins() if handle is not None else QMargins()

    def _limit_to_screen(self) -> None:
        """Keep the dialog, including its frame, inside the available area."""
        screen = self._screen_of(self)
        if screen is None or not self.isVisible() or self.isMaximized():
            return
        area = screen.availableGeometry().marginsRemoved(self._frame_margins())
        geometry = self.geometry()
        geometry.setSize(geometry.size().boundedTo(area.size()))
        geometry.moveLeft(max(area.left(), min(geometry.left(), area.left() + area.width() - geometry.width())))
        geometry.moveTop(max(area.top(), min(geometry.top(), area.top() + area.height() - geometry.height())))
        if geometry != self.geometry():
            self.setGeometry(geometry)
