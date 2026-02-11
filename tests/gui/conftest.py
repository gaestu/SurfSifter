"""
Pytest configuration and fixtures for the test suite.

Provides Qt application setup and common fixtures for GUI tests.
"""
import os
import sys
import pytest

# Ensure offscreen rendering for GUI tests by default
os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

# Import Qt before any application code to set platform
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt


@pytest.fixture(scope='session')
def qapp():
    """
    Session-wide QApplication instance.

    Creates a single QApplication for all GUI tests to share,
    preventing "QApplication already exists" errors.
    """
    # Check if QApplication already exists
    app = QApplication.instance()
    if app is None:
        # Create new QApplication with offscreen platform
        app = QApplication(sys.argv)
        # Set AA_ShareOpenGLContexts to prevent OpenGL crashes
        app.setAttribute(Qt.AA_ShareOpenGLContexts, True)

    yield app

    # Note: We don't call app.quit() because pytest-qt manages the lifecycle


@pytest.fixture(scope='function')
def qtbot(qapp, request):
    """
    Function-scoped qtbot fixture that works with our session qapp.

    This ensures each test gets a fresh qtbot but shares the QApplication.
    """
    from pytestqt.qtbot import QtBot
    from PySide6.QtWidgets import QWidget

    bot = QtBot(request)
    yield bot

    # Aggressive cleanup: close and delete all test widgets
    # QtBot stores widgets internally, access them properly
    if hasattr(bot, '_widgets'):
        for widget in bot._widgets:
            try:
                if isinstance(widget, QWidget) and not widget.isHidden():
                    widget.close()
                widget.deleteLater()
            except RuntimeError:
                # Widget already deleted
                pass

    # Process events to ensure cleanup happens
    qapp.processEvents()


# Configure pytest-qt to not exit on widget close
def pytest_configure(config):
    """Configure pytest for Qt testing."""
    config.addinivalue_line(
        "markers",
        "gui_offscreen: GUI tests that run with offscreen platform"
    )
    config.addinivalue_line(
        "markers",
        "gui_live: GUI tests that require a live display"
    )
    # PyEWF/PyTSK3 markers for test selection
    config.addinivalue_line(
        "markers",
        "e01: tests requiring E01 image files (may segfault on Python 3.13)"
    )
    config.addinivalue_line(
        "markers",
        "pyewf: tests using pyewf library (may have threading issues)"
    )
    config.addinivalue_line(
        "markers",
        "slow: tests that take a long time to run"
    )


def pytest_collection_modifyitems(config, items):
    """Default all GUI tests to gui_offscreen unless explicitly marked."""
    for item in items:
        if "tests/gui" not in str(item.fspath):
            continue
        if item.get_closest_marker("gui_live") or item.get_closest_marker("gui_offscreen"):
            continue
        item.add_marker(pytest.mark.gui_offscreen)
