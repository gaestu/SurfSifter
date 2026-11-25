"""Scroll areas that keep tabs and dialogs usable on small screens (issue #77)."""
from __future__ import annotations

from typing import Optional

from PySide6.QtCore import QEvent, QObject, QSize, Qt
from PySide6.QtWidgets import (
    QAbstractSpinBox,
    QComboBox,
    QDialog,
    QFrame,
    QScrollArea,
    QStyle,
    QVBoxLayout,
    QWidget,
)


class _WheelGuard(QObject):
    """Event filter: the mouse wheel scrolls the page instead of changing a value."""

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if event.type() == QEvent.Type.Wheel:
            # An ignored wheel event propagates to the parent, up to the scroll area.
            event.ignore()
            return True
        return False


class _ScrollBody(QWidget):
    """Content of a ContentScrollArea; its height depends on the visible height.

    While the content's minimum height fits, it gets exactly the visible
    height, so it looks as if there were no scroll area. Once it does not fit,
    it gets its preferred height and scrolls.
    """

    visible_height = 0  # set by the scroll area

    def _fits(self, height: int) -> bool:
        return height <= self.visible_height

    def minimumSizeHint(self) -> QSize:
        layout = self.layout()
        if layout is None:
            return super().minimumSizeHint()
        minimum = layout.totalMinimumSize()
        if self._fits(minimum.height()):
            return minimum
        return QSize(minimum.width(), max(minimum.height(), layout.totalSizeHint().height()))

    def heightForWidth(self, width: int) -> int:
        layout = self.layout()
        if layout is None or not layout.hasHeightForWidth():
            return super().heightForWidth(width)
        minimum = layout.totalMinimumHeightForWidth(width)
        if self._fits(minimum):
            return minimum
        return max(minimum, layout.totalHeightForWidth(width))


class ContentScrollArea(QScrollArea):
    """Vertical scroll area whose content follows the width.

    The size hint is the content's, so opening sizes stay the same, but the
    minimum height is small. Combo boxes, spin boxes and date fields inside
    ignore the mouse wheel (see :class:`_WheelGuard`).

    Args:
        read_only: The content only shows data. It then also scrolls
            horizontally, so long values do not widen the window, and the area
            takes the focus, so the keyboard scrolls it.
    """

    def __init__(self, parent: Optional[QWidget] = None, *, read_only: bool = False) -> None:
        super().__init__(parent)
        self._read_only = read_only
        self._wheel_guard = _WheelGuard(self)
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        if read_only:
            self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        else:
            # Do not take the initial focus from the first input field.
            self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._body = _ScrollBody()
        self._body_layout = QVBoxLayout(self._body)
        self.setWidget(self._body)
        # Keep the background of the tab or dialog.
        self._body.setAutoFillBackground(False)
        self.viewport().setAutoFillBackground(False)

    def body_layout(self) -> QVBoxLayout:
        """The layout to add the content to."""
        return self._body_layout

    def content_minimum_width(self) -> int:
        """Minimum width of the content."""
        return self._body_layout.totalMinimumSize().width()

    def content_minimum_height(self, width: int) -> int:
        """Minimum height that shows the whole content at ``width`` without scrolling."""
        minimum = self._body_layout.totalMinimumSize().height()
        if self._body_layout.hasHeightForWidth():
            minimum = max(minimum, self._body_layout.totalMinimumHeightForWidth(width))
        return minimum

    def sizeHint(self) -> QSize:
        return self._body_layout.totalSizeHint()

    def minimumSizeHint(self) -> QSize:
        minimum = super().minimumSizeHint()
        if self._read_only:
            return minimum
        bar = self.style().pixelMetric(QStyle.PixelMetric.PM_ScrollBarExtent, None, self)
        return QSize(max(minimum.width(), self.content_minimum_width() + bar), minimum.height())

    def event(self, event: QEvent) -> bool:
        if event.type() == QEvent.Type.LayoutRequest:
            self.updateGeometry()  # the size hints come from the content
        return super().event(event)

    def resizeEvent(self, event) -> None:
        self._body.visible_height = event.size().height()  # of the viewport
        super().resizeEvent(event)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        for widget in self._body.findChildren(QComboBox) + self._body.findChildren(QAbstractSpinBox):
            widget.installEventFilter(self._wheel_guard)


def add_scroll_body(layout: QVBoxLayout, *, read_only: bool = False) -> QVBoxLayout:
    """Add a scroll area to ``layout`` and return the layout for its content."""
    area = ContentScrollArea(read_only=read_only)
    layout.addWidget(area, 1)
    return area.body_layout()


def add_dialog_body(dialog: QDialog, *, read_only: bool = False) -> QVBoxLayout:
    """Give ``dialog`` a layout with a scrolling body and return the body layout.

    Add the button row to ``dialog.layout()``: it stays pinned below the body.
    """
    outer = QVBoxLayout(dialog)
    body = add_scroll_body(outer, read_only=read_only)
    body.setContentsMargins(0, 0, 0, 0)
    return body


def wrap_in_scroll_area(widget: QWidget) -> ContentScrollArea:
    """Return a scroll area that shows ``widget``."""
    area = ContentScrollArea()
    area.body_layout().setContentsMargins(0, 0, 0, 0)
    area.body_layout().addWidget(widget)
    return area
