"""Geometry functions that keep windows on their screen (issue #77)."""
from __future__ import annotations

import pytest
from PySide6.QtCore import QMargins, QRect

from app.common.screen_fit import find_screen, fit_rect

AVAILABLE = QRect(0, 30, 1280, 650)  # 1280x720 with a top panel and a taskbar
FRAME = QMargins(8, 31, 8, 8)
NO_FRAME = QMargins()


class FakeScreen:
    def __init__(self, x: int, y: int, width: int, height: int) -> None:
        self._geometry = QRect(x, y, width, height)

    def geometry(self) -> QRect:
        return self._geometry


# ---------------------------------------------------------------------------
# fit_rect
# ---------------------------------------------------------------------------


def test_fitting_rect_is_unchanged():
    rect = QRect(100, 100, 800, 500)
    assert fit_rect(rect, AVAILABLE, FRAME) == rect
    exact = AVAILABLE.marginsRemoved(FRAME)
    assert fit_rect(exact, AVAILABLE, FRAME) == exact


def test_rect_larger_than_the_screen_fills_the_available_area():
    rect = QRect(-200, -50, 3000, 2000)
    assert fit_rect(rect, AVAILABLE, NO_FRAME) == AVAILABLE


def test_frame_including_title_bar_stays_inside():
    fitted = fit_rect(QRect(0, 30, 1280, 650), AVAILABLE, FRAME)
    assert fitted == QRect(8, 61, 1264, 611)
    assert fitted.marginsAdded(FRAME) == AVAILABLE


@pytest.mark.parametrize(
    "rect",
    [
        QRect(-500, 100, 800, 500),
        QRect(1000, 100, 800, 500),
        QRect(100, -300, 800, 500),
        QRect(100, 600, 800, 500),
    ],
    ids=["left", "right", "top", "bottom"],
)
def test_rect_outside_is_moved_but_not_resized(rect):
    fitted = fit_rect(rect, AVAILABLE, FRAME)
    assert fitted.size() == rect.size()
    assert AVAILABLE.contains(fitted.marginsAdded(FRAME))


def test_screen_with_negative_coordinates():
    available = QRect(-1920, -200, 1920, 1040)
    fitted = fit_rect(QRect(-1930, -230, 2500, 900), available, FRAME)
    assert fitted == QRect(-1912, -169, 1904, 900)


# ---------------------------------------------------------------------------
# find_screen
# ---------------------------------------------------------------------------


def test_screen_that_contains_the_center_is_used():
    primary = FakeScreen(0, 0, 1920, 1080)
    second = FakeScreen(1920, 0, 1280, 720)
    # Overlaps the primary screen more than the second one does not matter:
    # the center decides.
    assert find_screen(QRect(2000, 50, 900, 600), [primary, second]) is second
    assert find_screen(QRect(100, 50, 900, 600), [primary, second]) is primary


def test_screen_left_of_the_primary_screen():
    primary = FakeScreen(0, 0, 1920, 1080)
    left = FakeScreen(-1280, 0, 1280, 720)
    assert find_screen(QRect(-1200, 40, 1000, 600), [primary, left]) is left


def test_mostly_hidden_window_uses_the_largest_overlap():
    primary = FakeScreen(0, 0, 1920, 1080)
    second = FakeScreen(1920, 0, 1280, 720)
    # Center below the second screen, upper part still on it.
    assert find_screen(QRect(2000, 600, 900, 600), [primary, second]) is second


def test_missing_screen_is_not_found():
    primary = FakeScreen(0, 0, 1920, 1080)
    assert find_screen(QRect(2500, 100, 900, 600), [primary]) is None
    assert find_screen(QRect(0, 0, 900, 600), []) is None
