"""Keep windows and dialogs inside the available area of their screen (issue #77)."""
from __future__ import annotations

from typing import Optional, Sequence

from PySide6.QtCore import QEvent, QMargins, QObject, QRect, QSize, Qt, QTimer
from PySide6.QtGui import QGuiApplication, QScreen
from PySide6.QtWidgets import QDialog, QWidget

from app.common.widgets.scroll_area import ContentScrollArea

# Stands in for the frame of a window that is restored maximized: the frame of
# its normal state is not known then. Larger than usual window decorations.
ASSUMED_FRAME_MARGINS = QMargins(8, 40, 8, 8)


def fit_rect(rect: QRect, available: QRect, margins: QMargins) -> QRect:
    """Limit a window's client rectangle to an area.

    Args:
        rect: Client rectangle, without the window frame.
        available: Area that must contain the window including its frame.
        margins: Frame margins around the client rectangle.
    """
    area = available.marginsRemoved(margins)
    width = min(rect.width(), area.width())
    height = min(rect.height(), area.height())
    x = min(max(rect.x(), area.left()), area.left() + area.width() - width)
    y = min(max(rect.y(), area.top()), area.top() + area.height() - height)
    return QRect(x, y, width, height)


def find_screen(rect: QRect, screens: Sequence[QScreen]) -> Optional[QScreen]:
    """The screen that shows ``rect``, or None if no screen shows any part of it.

    That is the screen with the rectangle's center; if the center is on no
    screen, the one with the largest overlap.
    """
    best, best_area = None, 0
    for screen in screens:
        geometry = screen.geometry()
        if geometry.contains(rect.center()):
            return screen
        overlap = geometry.intersected(rect)
        area = overlap.width() * overlap.height() if overlap.isValid() else 0
        if area > best_area:
            best, best_area = screen, area
    return best


def _screen_of(window: QWidget) -> Optional[QScreen]:
    """The screen that shows a top-level window.

    Not ``QWidget.screen()``: PySide ties the returned QScreen to the widget and
    can delete it, and with it crash Qt, when the widget is garbage-collected.
    """
    return find_screen(window.geometry(), QGuiApplication.screens()) or QGuiApplication.primaryScreen()


def _opener(dialog: QDialog) -> QWidget:
    """The window that decides on which screen a dialog opens."""
    parent = dialog.parentWidget()
    return parent.window() if parent is not None else dialog


def _frame_margins(window: QWidget) -> QMargins:
    handle = window.windowHandle()
    return handle.frameMargins() if handle is not None else QMargins()


class _ScreenFit(QObject):
    """Event filter that keeps a top-level window inside its screen when shown."""

    def __init__(self, window: QWidget) -> None:
        super().__init__(window)
        self._pending = False
        window.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        kind = event.type()
        if kind == QEvent.Type.Show and not event.spontaneous():
            self._pending = True
            if isinstance(watched, QDialog):
                # Only the size: QDialog places itself after this event.
                self._limit_size(watched)
            QTimer.singleShot(0, self, self._fit)
        elif kind == QEvent.Type.WindowActivate and self._pending:
            # X11 reports the frame only once the window manager has mapped the window.
            self._pending = False
            QTimer.singleShot(0, self, self._fit)
        return False

    @staticmethod
    def _limit_size(dialog: QDialog) -> None:
        screen = _screen_of(_opener(dialog))
        if screen is not None:
            dialog.resize(dialog.size().boundedTo(screen.availableGeometry().size()))

    def _fit(self) -> None:
        window = self.parent()
        screen = _screen_of(window)
        if screen is None or not window.isVisible():
            return
        if window.windowState() & (Qt.WindowState.WindowMaximized | Qt.WindowState.WindowFullScreen):
            return
        geometry = window.geometry()
        fitted = fit_rect(geometry, screen.availableGeometry(), _frame_margins(window))
        if fitted != geometry:
            window.setGeometry(fitted)


def keep_on_screen(window: QWidget) -> None:
    """Keep a top-level window, including its frame, on its screen when it is shown."""
    _ScreenFit(window)


def limit_to_screen(dialog: QDialog, width: int, height: int) -> None:
    """Set a dialog's opening size, limited to the available area of its screen.

    Call it after the dialog's content has been added.

    Args:
        dialog: The dialog.
        width: Preferred width.
        height: Preferred height. A dialog whose scrolling content needs more
            space opens larger, as far as the screen allows.
    """
    dialog.resize(_opening_size(dialog, width, height))
    keep_on_screen(dialog)


def _opening_size(dialog: QDialog, width: int, height: int) -> QSize:
    """The preferred size, or the size that shows the scrolling content completely."""
    layout = dialog.layout()
    margins = layout.contentsMargins().left() + layout.contentsMargins().right()
    areas = dialog.findChildren(ContentScrollArea, options=Qt.FindChildOption.FindDirectChildrenOnly)
    minimum = layout.totalMinimumSize()
    width = max([width, minimum.width()] + [area.content_minimum_width() + margins for area in areas])

    needed = minimum.height()
    if layout.hasHeightForWidth():
        needed = max(needed, layout.totalMinimumHeightForWidth(width))
    screen = _screen_of(_opener(dialog))
    for area in areas:
        needed += max(0, area.content_minimum_height(width - margins) - area.minimumSizeHint().height())
        scrolls_sideways = area.horizontalScrollBarPolicy() != Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        if scrolls_sideways and screen is not None and width > screen.availableGeometry().width():
            needed += area.horizontalScrollBar().sizeHint().height()
    return QSize(width, max(height, needed))
