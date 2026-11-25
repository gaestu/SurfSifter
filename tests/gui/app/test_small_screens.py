"""The main window and its tabs on small screens (issue #77).

Opens a synthetic case with two evidences in a real ``MainWindow`` (offscreen).
"""
from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QCoreApplication, QDate, QEvent, QMargins, QPoint, QPointF, QRect, Qt, QThreadPool
from PySide6.QtGui import QShowEvent
from PySide6.QtTest import QTest
from PySide6.QtWidgets import (
    QAbstractSpinBox,
    QApplication,
    QComboBox,
    QDateEdit,
    QDialog,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

import app.main as main_module
from app.common import screen_fit
from app.common.widgets import ContentScrollArea, add_dialog_body, add_scroll_body
from app.features.downloads.subtab_available import AvailableDownloadsPanel
from app.features.urls.tab import UrlsTab
from reports.database import insert_custom_section, insert_section_module
from reports.ui import ReportTabWidget
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 15000
CODE_ROOT = Path(__file__).resolve().parents[3]
MAX_HEIGHT = 600
SMALL_WIDTH = 1280


def _settle() -> None:
    for _ in range(5):
        QCoreApplication.processEvents()


def _wheel(widget: QWidget, notches: int = -1) -> None:
    """Turn the mouse wheel over a visible ``widget`` (negative: scroll down).

    The event goes through the window: Qt does not propagate synthetic wheel
    events (QApplication.sendEvent) from a widget to its parents.
    """
    window = widget.window()
    center = widget.mapTo(window, widget.rect().center())
    assert widget.visibleRegion().contains(widget.rect().center())
    QTest.wheelEvent(window.windowHandle(), QPointF(center), QPoint(0, 120 * notches))
    _settle()


def _partitions(count: int) -> list:
    return [
        {
            "index": index,
            "length": 10**9 * (index + 1),
            "offset": 10**6,
            "description": "NTFS / exFAT (0x07)",
            "filesystem_readable": True,
            "root_file_count": 10,
        }
        for index in range(count)
    ]


@pytest.fixture
def make_window(qtbot, tmp_path, monkeypatch):
    """Factory for a MainWindow without a case, using the shipped config."""
    import core.tool_discovery as tool_discovery

    # No search index build and no per-extractor-card tool version probes.
    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)
    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)
    # A closed Download tab can start its worker again (debounce timer), which
    # then reports to deleted widgets.
    monkeypatch.setattr(AvailableDownloadsPanel, "_apply_filters", lambda self: None)

    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))

    base_dir = tmp_path / "base_dir"
    (base_dir / "config").mkdir(parents=True)
    for name in ("config.yml", "settings.defaults.json"):
        source = CODE_ROOT / "config" / name
        if source.exists():
            shutil.copy2(source, base_dir / "config" / name)

    windows = []

    def _make():
        window = main_module.MainWindow(base_dir)
        windows.append(window)
        window.show()
        return window

    yield _make
    for window in windows:
        window.close()
        QThreadPool.globalInstance().waitForDone(10000)
        QCoreApplication.processEvents()
        window.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


@pytest.fixture
def case_window(qtbot, tmp_path, make_window):
    """A shown MainWindow with a case of two evidences."""
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=20, evidence_count=2, tag_every=3, domain_count=4)
    window = make_window()
    window._open_case(case.case_folder, case.case_db_path)
    qtbot.waitUntil(lambda: window.main_tabs.count() == 3, timeout=TIMEOUT_MS)
    _settle()
    window.case_fixture = case
    return window


def _minimum_height(window: QWidget) -> int:
    _settle()
    return max(window.minimumHeight(), window.minimumSizeHint().height())


def _evidence_tabs(window) -> list:
    return [window.main_tabs.widget(index) for index in range(1, window.main_tabs.count())]


def _report_widget(evidence_tabs) -> ReportTabWidget:
    return evidence_tabs.property("reports_tab")._report_widget


def _visit_all_tabs(window, check) -> None:
    """Show the Case tab and every sub-tab of every evidence; ``check`` each."""
    window.main_tabs.setCurrentIndex(0)
    check("Case")
    for evidence_tabs in _evidence_tabs(window):
        window.main_tabs.setCurrentWidget(evidence_tabs)
        for index in range(evidence_tabs.count()):
            evidence_tabs.setCurrentIndex(index)
            check(evidence_tabs.tabText(index))


def _show_small(window, evidence_tabs=None, page=None) -> None:
    """Show a page in a window of the smallest supported size."""
    if evidence_tabs is not None:
        window.main_tabs.setCurrentWidget(evidence_tabs)
        evidence_tabs.setCurrentWidget(page)
    else:
        window.main_tabs.setCurrentIndex(0)
    _settle()
    window.resize(SMALL_WIDTH, MAX_HEIGHT)
    _settle()
    assert window.height() == MAX_HEIGHT


def _assert_reachable(area: QScrollArea) -> None:
    """The whole content can be scrolled into view, nothing is cut off."""
    body = area.widget()
    viewport = area.viewport()
    bar = area.verticalScrollBar()
    assert body.width() == viewport.width(), "content does not follow the width"
    assert body.height() == viewport.height() + bar.maximum()
    assert body.height() >= body.layout().totalMinimumSize().height()
    bar.setValue(bar.maximum())
    _settle()
    for child in body.findChildren(QWidget, options=Qt.FindChildOption.FindDirectChildrenOnly):
        if child.isVisible():
            assert body.rect().contains(child.geometry()), child
    last = body.layout().itemAt(body.layout().count() - 1)
    if last.widget() is not None:
        bottom = last.widget().mapTo(viewport, QPoint(0, last.widget().height() - 1)).y()
        assert 0 <= bottom < viewport.height()


# ---------------------------------------------------------------------------
# Minimum height of the main window
# ---------------------------------------------------------------------------


def test_window_without_case_fits(make_window):
    assert _minimum_height(make_window()) <= MAX_HEIGHT


def test_window_fits_on_every_tab_in_every_state(case_window):
    window = case_window
    heights = {}

    def check(state):
        def record(tab):
            heights[(state, tab)] = max(_minimum_height(window), heights.get((state, tab), 0))

        return record

    _visit_all_tabs(window, check("default"))

    for evidence_tabs in _evidence_tabs(window):
        report = _report_widget(evidence_tabs)
        report._settings_group.set_collapsed(False)
        report._appendix_group.set_collapsed(False)
        for index in range(5):
            insert_custom_section(report._db_conn, report._evidence_id, f"Section {index}", "<p>text</p>" * 20)
        report._load_sections()
    _visit_all_tabs(window, check("reports expanded"))

    for evidence_tabs in _evidence_tabs(window):
        evidence_tabs.property("overview_widget").property("disk_tab_widget").set_partitions(_partitions(4), "ev")
    _visit_all_tabs(window, check("four partitions"))

    case = window.case_fixture
    added = window._build_evidence_tab(case.evidence_ids[0], case.labels[case.evidence_ids[0]])
    window.main_tabs.addTab(added, "added")
    _visit_all_tabs(window, check("evidence added"))
    window._on_close_evidence_tab(window.main_tabs.indexOf(added))
    window._remove_evidence(case.evidence_ids[1], case.labels[case.evidence_ids[1]])
    _visit_all_tabs(window, check("evidence removed"))

    assert len(heights) == 5 * 14  # Case and 13 sub-tabs; both evidences have the same names
    too_high = {key: height for key, height in heights.items() if height > MAX_HEIGHT}
    assert not too_high


# ---------------------------------------------------------------------------
# Tabs at 600 px
# ---------------------------------------------------------------------------


def test_reports_buttons_stay_visible(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    _show_small(window, evidence_tabs, evidence_tabs.property("reports_tab"))
    report = _report_widget(evidence_tabs)
    report._settings_group.set_collapsed(False)
    report._appendix_group.set_collapsed(False)
    _settle()
    assert window.height() == MAX_HEIGHT

    reports_tab = evidence_tabs.property("reports_tab")
    buttons = [
        report._preview_btn,
        report._preview_appendix_btn,
        report._preview_complete_btn,
        report._create_report_pdf_btn,
        report._create_appendix_pdf_btn,
        report._create_complete_pdf_btn,
        reports_tab._summary_btn,
        reports_tab._export_btn,
    ]
    bar = report._scroll_area.verticalScrollBar()
    assert bar.maximum() > 0
    for value in (0, bar.maximum()):
        bar.setValue(value)
        _settle()
        for button in buttons:
            area = QRect(button.mapTo(window, QPoint(0, 0)), button.size())
            assert window.rect().contains(area), button.text()
            assert button.visibleRegion().boundingRect() == button.rect(), button.text()


@pytest.mark.parametrize("modules", [True, False], ids=["modules", "text only"])
def test_report_sections_scroll_together(case_window, modules):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    report = _report_widget(evidence_tabs)
    _show_small(window, evidence_tabs, evidence_tabs.property("reports_tab"))
    for index in range(4):
        section_id = insert_custom_section(report._db_conn, report._evidence_id, f"Section {index}", "<p>text</p>" * 20)
        if modules:  # their word-wrapped labels change how Qt sizes the content
            insert_section_module(report._db_conn, section_id, "unknown_module", {})
    report._load_sections()
    report._appendix_group.set_collapsed(False)
    _settle()

    # One scroll area; the cards and lists scroll on their own only as list/text views.
    assert report.findChildren(QScrollArea) == [report._scroll_area]
    content = report._scroll_area.widget()
    for widget in (report._settings_group, report._sections_container, report._appendix_group):
        assert content.isAncestorOf(widget)
    _assert_reachable(report._scroll_area)
    for card in report._section_cards:
        assert card._content_browser.height() == card._content_browser.maximumHeight()
        assert report._sections_container.rect().contains(card.geometry())
        assert card.height() >= card.minimumSizeHint().height()
    assert report._appendix_list.height() == report._appendix_list.maximumHeight()


def test_long_section_text_does_not_widen_the_page(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    _show_small(window, evidence_tabs, evidence_tabs.property("reports_tab"))
    report = _report_widget(evidence_tabs)
    for title in ("long title " * 30, "x" * 400):
        section_id = insert_custom_section(report._db_conn, report._evidence_id, title, "")
        insert_section_module(report._db_conn, section_id, "y" * 400, {})
    report._load_sections()
    report._appendix_group.set_collapsed(False)
    _settle()
    _assert_reachable(report._scroll_area)
    viewport = report._scroll_area.viewport()
    for widget in (
        report._add_section_btn,
        report._branding_logo_clear_btn,
        report._appendix_remove_btn,
        *[card._move_down_btn for card in report._section_cards],
    ):
        right = widget.mapTo(viewport, QPoint(widget.width() - 1, 0)).x()
        assert 0 < right < viewport.width()
    content = report._scroll_area.widget()
    needed = content.layout().totalHeightForWidth(viewport.width())
    assert content.height() == max(needed, viewport.height())  # no blank space
    wrapped = report._section_cards[0]._title_label
    assert wrapped.height() >= wrapped.heightForWidth(wrapped.width()) > wrapped.fontMetrics().height() * 2


def test_moving_a_section_keeps_the_scroll_position(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    report = _report_widget(evidence_tabs)
    _show_small(window, evidence_tabs, evidence_tabs.property("reports_tab"))
    for index in range(4):
        insert_custom_section(report._db_conn, report._evidence_id, f"Section {index}", "<p>text</p>" * 20)
    report._load_sections()
    _settle()
    bar = report._scroll_area.verticalScrollBar()
    bar.setValue(bar.maximum())
    _settle()
    before = bar.value()
    card = report._section_cards[-1]
    card._move_up_btn.setFocus()
    card._move_up_btn.click()
    _settle()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)
    _settle()
    assert bar.value() == before


def test_saved_collapse_states_are_restored(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    report = _report_widget(evidence_tabs)
    window.main_tabs.setCurrentWidget(evidence_tabs)
    evidence_tabs.setCurrentWidget(evidence_tabs.property("reports_tab"))
    _settle()
    assert not report._settings_group.is_collapsed()
    assert report._appendix_group.is_collapsed()
    report._settings_group.set_collapsed(True)
    report._appendix_group.set_collapsed(False)

    other = ReportTabWidget()
    other.set_db_connection(report._db_conn)
    other.set_evidence(report._evidence_id, report._evidence_label)
    assert other._settings_group.is_collapsed()
    assert not other._appendix_group.is_collapsed()
    other.deleteLater()


def test_overview_is_reachable(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    overview = evidence_tabs.property("overview_widget")
    assert evidence_tabs.widget(0) is overview
    overview.property("disk_tab_widget").set_partitions(_partitions(4), "ev")
    _show_small(window, evidence_tabs, overview)
    area = overview.findChild(ContentScrollArea)
    assert area.verticalScrollBar().maximum() > 0
    _assert_reachable(area)
    table = overview.property("disk_tab_widget").partition_table
    assert table.height() == table.sizeHint().height()


def test_case_tab_is_reachable(case_window):
    window = case_window
    _show_small(window)
    area = window.case_info_tab.findChild(ContentScrollArea)
    assert area.verticalScrollBar().maximum() > 0
    assert window.case_info_tab.evidence_group.isVisible()
    _assert_reachable(area)


def test_tabs_do_not_scroll_where_the_content_fits(case_window):
    """On a large screen the content gets exactly the tab's height, as before."""
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    overview = evidence_tabs.property("overview_widget")
    overview.property("disk_tab_widget").set_partitions(_partitions(4), "ev")
    window.resize(1900, 1170)
    report = _report_widget(evidence_tabs)
    pages = [
        (window.main_tabs.widget(0), window.case_info_tab, window.case_info_tab.findChild(ContentScrollArea)),
        (evidence_tabs, overview, overview.findChild(ContentScrollArea)),
        (evidence_tabs, evidence_tabs.property("reports_tab"), report._scroll_area),
    ]
    for main_page, page, area in pages:
        window.main_tabs.setCurrentWidget(main_page)
        if main_page is evidence_tabs:
            evidence_tabs.setCurrentWidget(page)
        _settle()
        assert area.verticalScrollBar().maximum() == 0, page
        assert area.widget().height() == area.viewport().height()
    for page in (window.case_info_tab, overview):
        assert page.findChild(ContentScrollArea).size() == page.size()


# ---------------------------------------------------------------------------
# ContentScrollArea
# ---------------------------------------------------------------------------


def test_content_is_squeezed_while_it_fits_and_scrolls_at_its_preferred_height(qtbot):
    page = QWidget()
    qtbot.addWidget(page)
    outer = QVBoxLayout(page)
    outer.setContentsMargins(0, 0, 0, 0)
    body = add_scroll_body(outer)
    label = QLabel("word " * 60)
    label.setWordWrap(True)
    body.addWidget(label)
    for _ in range(3):
        body.addWidget(QTextEdit())
    area = page.findChild(ContentScrollArea)
    page.resize(500, 800)
    page.show()
    _settle()
    minimum = body.totalMinimumHeightForWidth(area.viewport().width())
    preferred = body.totalHeightForWidth(area.viewport().width())
    assert minimum < preferred

    page.resize(500, minimum)
    _settle()
    assert area.verticalScrollBar().maximum() == 0
    assert area.widget().height() == minimum

    page.resize(500, minimum - 1)
    _settle()
    preferred = body.totalHeightForWidth(area.viewport().width())
    assert area.widget().height() == preferred
    assert label.height() == label.heightForWidth(label.width())
    _assert_reachable(area)


def test_size_hints(qtbot):
    page = QWidget()
    qtbot.addWidget(page)
    outer = QVBoxLayout(page)
    body = add_scroll_body(outer)
    wide = QPushButton("wide")
    wide.setMinimumSize(700, 900)
    body.addWidget(wide)
    area = page.findChild(ContentScrollArea)
    assert area.sizeHint().height() >= 900
    assert area.minimumSizeHint().height() < 200
    assert area.minimumSizeHint().width() >= 700  # content is never cut off at the side
    page.show()
    _settle()
    wide.setMinimumWidth(1000)
    _settle()
    assert page.minimumSizeHint().width() >= 1000


def test_forms_keep_the_initial_focus_on_the_first_field(qtbot):
    dialog = QDialog()
    qtbot.addWidget(dialog)
    body = add_dialog_body(dialog)
    field = QLineEdit()
    body.addWidget(field)
    dialog.layout().addWidget(QPushButton("OK"))
    dialog.show()
    dialog.activateWindow()
    _settle()
    assert dialog.focusWidget() is field


# ---------------------------------------------------------------------------
# Mouse wheel
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("focused", [False, True])
def test_wheel_scrolls_instead_of_changing_values(qtbot, focused):
    page = QWidget()
    qtbot.addWidget(page)
    outer = QVBoxLayout(page)
    form = QFormLayout()
    add_scroll_body(outer).addLayout(form)
    combo, editable, spin, date = QComboBox(), QComboBox(), QSpinBox(), QDateEdit(QDate(2024, 5, 6))
    for box in (combo, editable):
        box.addItems(["a", "b", "c"])
        box.setCurrentIndex(1)
    editable.setEditable(True)
    spin.setValue(5)
    fields = [combo, editable, spin, date]
    for widget in fields:
        form.addRow("label", widget)
    form.addRow(QTextEdit())
    form.addRow(QTextEdit())
    area = page.findChild(ContentScrollArea)
    page.resize(400, 120)
    page.show()
    page.activateWindow()
    _settle()
    bar = area.verticalScrollBar()

    targets = fields + [editable.lineEdit(), spin.lineEdit(), date.lineEdit()]
    for target in targets:
        if focused:
            target.setFocus()
        area.ensureWidgetVisible(target, 0, 0)
        bar.setValue(max(1, min(bar.value(), bar.maximum() - 1)))
        before = bar.value()
        _wheel(target)
        assert bar.value() > before, target
    assert combo.currentIndex() == 1
    assert editable.currentIndex() == 1
    assert spin.value() == 5
    assert date.date() == QDate(2024, 5, 6)


def test_wheel_does_not_change_report_settings(case_window):
    window = case_window
    evidence_tabs = window.main_tabs.widget(1)
    _show_small(window, evidence_tabs, evidence_tabs.property("reports_tab"))
    report = _report_widget(evidence_tabs)
    combos = report.findChildren(QComboBox)
    spins = report.findChildren(QAbstractSpinBox)
    assert report._author_date_input in spins
    assert report._locale_combo in combos and report._date_format_combo in combos
    before = [combo.currentIndex() for combo in combos], [spin.text() for spin in spins]
    bar = report._scroll_area.verticalScrollBar()
    for widget in combos + spins:
        for notches in (-1, 1):
            report._scroll_area.ensureWidgetVisible(widget, 0, 40)
            bar.setValue(max(1, min(bar.value(), bar.maximum() - 1)))
            before_wheel = bar.value()
            _wheel(widget, notches)
            assert (bar.value() - before_wheel) * notches < 0, widget
    assert ([combo.currentIndex() for combo in combos], [spin.text() for spin in spins]) == before


# ---------------------------------------------------------------------------
# Window geometry
# ---------------------------------------------------------------------------


class FakeScreen:
    def __init__(self, geometry: QRect, available: QRect) -> None:
        self._geometry, self._available = geometry, available

    def geometry(self) -> QRect:
        return self._geometry

    def availableGeometry(self) -> QRect:
        return self._available


PRIMARY = FakeScreen(QRect(0, 0, 1920, 1080), QRect(0, 30, 1920, 1010))
SECOND = FakeScreen(QRect(-1280, 0, 1280, 720), QRect(-1280, 30, 1280, 650))


class FakeWindow:
    """Just what MainWindow's geometry methods use."""

    _default_window_geometry = main_module.MainWindow._default_window_geometry

    def __init__(self, **window) -> None:
        values = {"x": None, "y": None, "width": None, "height": None, "maximized": False}
        values.update(window)
        self.settings = SimpleNamespace(window=SimpleNamespace(**values))
        self.geometry = None
        self.state = Qt.WindowState.WindowNoState

    def setGeometry(self, geometry: QRect) -> None:
        self.geometry = geometry

    def windowState(self):
        return self.state

    def setWindowState(self, state) -> None:
        self.state = state


@pytest.fixture
def restore(monkeypatch):
    """Restore the geometry of a fake window on given screens (primary first)."""
    monkeypatch.setattr(main_module, "keep_on_screen", lambda window: None)

    def _restore(screens, **window):
        fake_app = SimpleNamespace(screens=lambda: list(screens), primaryScreen=lambda: screens[0] if screens else None)
        monkeypatch.setattr(main_module, "QApplication", fake_app)
        fake = FakeWindow(**window)
        main_module.MainWindow._restore_window_geometry(fake)
        return fake

    return _restore


def test_restored_on_the_saved_screen(restore):
    saved = {"x": -1200, "y": 60, "width": 1000, "height": 560}
    assert restore([PRIMARY, SECOND], **saved).geometry == QRect(-1200, 60, 1000, 560)


def test_restored_on_the_primary_screen_if_the_saved_one_is_missing(restore):
    saved = {"x": -1200, "y": 60, "width": 1000, "height": 560}
    assert restore([PRIMARY], **saved).geometry == QRect(0, 60, 1000, 560)


def test_restored_window_larger_than_its_screen(restore):
    saved = {"x": -1280, "y": -10, "width": 1500, "height": 1100}
    # The center is on neither screen; most of the window is on the second one.
    assert restore([PRIMARY, SECOND], **saved).geometry == QRect(-1280, 30, 1280, 650)


def test_default_geometry_is_on_the_primary_screen(restore):
    for screens in ([PRIMARY, SECOND], [SECOND, PRIMARY]):
        geometry = restore(screens).geometry
        assert screens[0].availableGeometry().contains(geometry)


def test_without_screens(restore):
    assert restore([]).geometry == QRect(80, 60, 1500, 960)


@pytest.mark.parametrize("value", [2**32, -(2**40), float("inf"), 2**31 - 1, 1e300])
@pytest.mark.parametrize("name", ["x", "y", "width", "height"])
def test_restored_with_values_out_of_range(restore, name, value):
    """settings.json is not validated; Qt accepts only 32-bit values."""
    saved = {"x": 100, "y": 100, "width": 800, "height": 600, name: value}
    geometry = restore([PRIMARY, SECOND], **saved).geometry
    assert PRIMARY.availableGeometry().intersects(geometry) or name in ("width", "height") and value < 0


def test_restored_maximized_reserves_space_for_the_title_bar(restore):
    fake = restore([PRIMARY, SECOND], x=-1280, y=30, width=1280, height=650, maximized=True)
    assert fake.state & Qt.WindowState.WindowMaximized
    margins = screen_fit.ASSUMED_FRAME_MARGINS
    assert SECOND.availableGeometry().contains(fake.geometry.marginsAdded(margins))


def test_shown_window_fits_including_its_frame(make_window):
    window = make_window()
    _settle()
    available = window.screen().availableGeometry()
    margins = window.windowHandle().frameMargins()
    assert margins != QMargins(), "the offscreen platform has frame margins"
    assert available.contains(window.geometry().marginsAdded(margins))


def test_open_case_does_not_enlarge_the_window(case_window):
    available = case_window.screen().availableGeometry()
    assert case_window.frameGeometry().height() <= available.height()


class RecordingWindow(QWidget):
    """Records the geometry set from Python (by the code under test)."""

    def __init__(self) -> None:
        super().__init__()
        self.fitted = []

    def setGeometry(self, *args) -> None:
        self.fitted.append(args[0])
        super().setGeometry(*args)


@pytest.fixture
def frame(monkeypatch):
    """Settable frame margins, as a window manager would report them."""
    margins = SimpleNamespace(value=QMargins())
    monkeypatch.setattr(screen_fit, "_frame_margins", lambda window: margins.value)
    return margins


def test_window_is_fitted_when_shown(qtbot):
    window = QWidget()
    qtbot.addWidget(window)
    screen_fit.keep_on_screen(window)
    available = QApplication.primaryScreen().availableGeometry()
    window.show()
    _settle()
    window.setGeometry(available)
    QCoreApplication.sendEvent(window, QShowEvent())  # not activated again
    _settle()
    margins = window.windowHandle().frameMargins()
    assert window.geometry() == available.marginsRemoved(margins)


def test_window_is_fitted_once_more_when_the_frame_is_known(qtbot, frame):
    """X11 reports the frame after mapping; later the user may move the window."""
    window = RecordingWindow()
    qtbot.addWidget(window)
    screen_fit.keep_on_screen(window)
    available = QApplication.primaryScreen().availableGeometry()
    window.show()
    _settle()
    QWidget.setGeometry(window, available)
    # The offscreen platform activates a window while it is shown; X11 does it later.
    QCoreApplication.sendEvent(window, QShowEvent())
    _settle()
    window.fitted.clear()

    frame.value = QMargins(1, 37, 1, 1)
    QCoreApplication.sendEvent(window, QEvent(QEvent.Type.WindowActivate))
    _settle()
    assert window.fitted == [available.marginsRemoved(frame.value)]

    window.fitted.clear()
    frame.value = QMargins(50, 50, 50, 50)
    QCoreApplication.sendEvent(window, QEvent(QEvent.Type.WindowActivate))
    _settle()
    assert window.fitted == []


@pytest.mark.parametrize("state", [Qt.WindowState.WindowMaximized, Qt.WindowState.WindowFullScreen])
def test_maximized_window_is_not_changed(qtbot, frame, state):
    window = RecordingWindow()
    qtbot.addWidget(window)
    screen_fit.keep_on_screen(window)
    frame.value = QMargins(30, 30, 30, 30)
    QWidget.setGeometry(window, QRect(100, 100, 400, 300))
    window.setWindowState(state)
    window.show()
    window.activateWindow()
    _settle()
    assert window.windowState() & state
    assert window.fitted == []


def test_dialog_size_is_limited_before_the_dialog_is_placed(qtbot):
    """QDialog centers itself while it is shown, so the size must be final then."""

    class Dialog(QDialog):
        def showEvent(self, event) -> None:
            self.size_when_shown = self.size()
            super().showEvent(event)

    available = QApplication.primaryScreen().availableGeometry()
    dialog = Dialog()
    qtbot.addWidget(dialog)
    QVBoxLayout(dialog)
    screen_fit.limit_to_screen(dialog, 300, available.height() + 500)
    dialog.show()
    assert dialog.size_when_shown.height() == available.height()


def test_widget_screen_is_not_used(qtbot, monkeypatch):
    """PySide can delete the QScreen that QWidget.screen() returned."""

    def fail(widget):
        raise AssertionError("QWidget.screen() called")

    from reports.ui.small_screens import ScreenLimitedDialog

    for cls in (QWidget, QDialog):
        monkeypatch.setattr(cls, "screen", fail)
    parent = QWidget()
    qtbot.addWidget(parent)
    parent.show()
    for dialog in (QDialog(parent), ScreenLimitedDialog(parent)):
        QVBoxLayout(dialog)
        if type(dialog) is QDialog:
            screen_fit.limit_to_screen(dialog, 300, 300)
        dialog.show()
        dialog.activateWindow()
        _settle()
        dialog.hide()
