"""Every dialog class fits a small screen, with its buttons visible (issue #77)."""
from __future__ import annotations

import ast
import importlib
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import QCoreApplication, QDate, QEvent, QMargins, QPoint, QPointF, QRect
from PySide6.QtGui import QPixmap, QShowEvent
from PySide6.QtTest import QTest
from PySide6.QtWidgets import (
    QAbstractButton,
    QApplication,
    QComboBox,
    QDateEdit,
    QDialog,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTableWidget,
    QWidget,
)

from app.common.dialogs.extraction import CaseWideExtractIngestDialog
from app.common import screen_fit
from app.common.screen_fit import limit_to_screen
from app.common.widgets import ContentScrollArea, add_dialog_body
from app.config.settings import AppSettings
from app.features.extraction import ExtractionTab
from core.tool_registry import ToolRegistry
from extractors import ExtractorRegistry

pytestmark = pytest.mark.gui_offscreen

SRC = Path(__file__).resolve().parents[3] / "src"
MAX_HEIGHT = 600
MAX_WIDTH = 1280
LONG_TEXT = "Lorem ipsum dolor sit amet consectetur adipiscing elit " * 4
LONG_URL = "https://example.com/" + "a" * 300


def _settle() -> None:
    for _ in range(5):
        QCoreApplication.processEvents()


@pytest.fixture(autouse=True)
def no_side_effects(monkeypatch):
    import core.tool_discovery as tool_discovery

    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)

    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))
    monkeypatch.setattr(QMessageBox, "exec", unexpected_box)  # would block the run


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def _pinned_buttons(dialog: QDialog) -> List[QAbstractButton]:
    """Visible push buttons that are not part of a scrolling body or a page."""
    buttons = []
    for button in dialog.findChildren(QPushButton):
        areas = [area for area in dialog.findChildren(QScrollArea) if area.isAncestorOf(button)]
        if button.isVisible() and not areas:
            buttons.append(button)
    return buttons


def _inside(dialog: QDialog, widget: QAbstractButton) -> bool:
    return dialog.rect().contains(QRect(widget.mapTo(dialog, QPoint(0, 0)), widget.size()))


def _check_dialog(dialog: QDialog, *, buttons: bool = True, wide: bool = False) -> None:
    dialog.show()
    _settle()

    # It opens inside its screen (800x800 on the offscreen platform).
    available = QApplication.primaryScreen().availableGeometry()
    assert dialog.frameGeometry().height() <= available.height()
    if not wide:
        assert dialog.frameGeometry().width() <= available.width()
        assert available.contains(dialog.frameGeometry())
    assert dialog.minimumWidth() <= MAX_WIDTH

    # Its minimum height, and what its layout needs at least.
    heights = {
        "minimumHeight": dialog.minimumHeight(),
        "minimumSizeHint": dialog.minimumSizeHint().height(),
        "layout": dialog.layout().totalMinimumSize().height(),
    }
    assert max(heights.values()) <= MAX_HEIGHT, heights

    # At that height the buttons are visible.
    dialog.resize(dialog.width(), min(dialog.height(), MAX_HEIGHT))
    _settle()
    assert dialog.height() <= MAX_HEIGHT
    pinned = _pinned_buttons(dialog)
    assert pinned or not buttons
    bottoms = {button: button.mapTo(dialog, QPoint(0, button.height())).y() for button in pinned}
    for button in pinned:
        assert _inside(dialog, button), button.text()
        if bottoms[button] == max(bottoms.values()):  # the button row
            assert button.height() >= button.minimumSizeHint().height(), button.text()

    # Nothing of the scrolling content is cut off.
    for area in dialog.findChildren(ContentScrollArea):
        if area.isVisible():
            body = area.widget()
            assert body.height() >= body.layout().totalMinimumSize().height()
            assert body.width() >= body.layout().totalMinimumSize().width()


# ---------------------------------------------------------------------------
# Record details: every field filled
# ---------------------------------------------------------------------------


class FullRow(dict):
    """A record with a plausible, long value for every key."""

    VALUES = [
        (r"^state$", "complete"),
        (r"danger_type", "dangerous_file"),
        (r"engagement_type", "site_engagement"),
        (r"raw_flags|from_visit|points|_seconds|raw_score|daily|frecency|_kb$|score|visit_id|opener", 7),
        (r"enabled|from_webstore|deleted|encrypted$", 1),
        (r"encrypted_value|_blob$|encrypted_data", b"\x00" * 200),
        (r"_utc$|timestamp|_time$|date", "2024-01-02T03:04:05Z"),
        (
            r"count$|size|_ms$|^id$|_bytes$|length|^is_|index|port|month|year|duration|position"
            r"|status_code|response_code|height|width|state$|type_int|transition",
            5,
        ),
        (r"url|path|origin", LONG_URL),
    ]

    def get(self, key, default=None):
        for pattern, value in self.VALUES:
            if re.search(pattern, key):
                return value
        if isinstance(default, bool):
            return True
        if isinstance(default, int):
            return 5
        return LONG_TEXT

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return True

    def __bool__(self):
        return True


DETAILS = {
    "app.features.browser_inventory.autofill.credentials_dialog": ["CredentialsDetailsDialog"],
    "app.features.browser_inventory.autofill.credit_card_dialog": ["CreditCardDetailsDialog"],
    "app.features.browser_inventory.autofill.dialog": ["AutofillDetailsDialog"],
    "app.features.browser_inventory.bookmarks.dialog": ["BookmarkDetailsDialog"],
    "app.features.browser_inventory.browser_config.dialog": ["BrowserConfigDetailsDialog"],
    "app.features.browser_inventory.cache.dialog": ["CacheEntryDetailsDialog"],
    "app.features.browser_inventory.cache.index_dialog": ["CacheIndexDetailsDialog"],
    "app.features.browser_inventory.cookies.dialog": ["CookieDetailsDialog"],
    "app.features.browser_inventory.downloads.dialog": ["BrowserDownloadDetailsDialog"],
    "app.features.browser_inventory.extensions.dialog": ["ExtensionDetailsDialog"],
    "app.features.browser_inventory.history.dialog": ["HistoryDetailsDialog"],
    "app.features.browser_inventory.inventory.dialog": ["BrowserInventoryDetailsDialog"],
    "app.features.browser_inventory.media.dialog": ["MediaDetailsDialog"],
    "app.features.browser_inventory.permissions.dialog": ["PermissionDetailsDialog"],
    "app.features.browser_inventory.search_terms.dialog": ["SearchTermsDetailsDialog"],
    "app.features.browser_inventory.sessions.closed_tabs_dialog": ["ClosedTabDetailsDialog"],
    "app.features.browser_inventory.sessions.dialog": ["SessionDetailsDialog"],
    "app.features.browser_inventory.sessions.form_data_dialog": ["SessionFormDataDetailsDialog"],
    "app.features.browser_inventory.site_engagement.dialog": ["SiteEngagementDetailsDialog"],
    "app.features.browser_inventory.web_storage.auth_tokens_dialog": ["StorageTokenDetailsDialog"],
    "app.features.browser_inventory.web_storage.identifiers_dialog": ["StorageIdentifierDetailsDialog"],
    "app.features.browser_inventory.web_storage.storage_keys_dialog": ["StorageKeyDetailsDialog"],
    "app.features.browser_inventory.web_storage.stored_sites_dialog": ["StoredSiteDetailsDialog"],
    "app.features.os_artifacts.dialogs": [
        "AppExecutionDetailsDialog",
        "JumpListDetailsDialog",
        "SoftwareDetailsDialog",
        "UserActivityDetailsDialog",
    ],
}
DETAILS_CLASSES = [(module, name) for module, names in DETAILS.items() for name in names]


@pytest.mark.parametrize("module,name", DETAILS_CLASSES, ids=[name for _module, name in DETAILS_CLASSES])
def test_details_dialog_with_long_values(qtbot, module, name):
    dialog = getattr(importlib.import_module(module), name)(FullRow())
    qtbot.addWidget(dialog)
    for table in dialog.findChildren(QTableWidget):
        table.setVisible(True)  # shown by the dialog once its data is loaded
    _check_dialog(dialog)

    close = [button for button in _pinned_buttons(dialog) if "Close" in button.text()]
    assert close, "the Close button must stay pinned"
    # Long values are reachable by scrolling sideways.
    area = dialog.findChild(ContentScrollArea)
    assert area.widget().width() == area.viewport().width() + area.horizontalScrollBar().maximum()


# ---------------------------------------------------------------------------
# All other dialogs
# ---------------------------------------------------------------------------


def _pixmap(width: int, height: int) -> QPixmap:
    pixmap = QPixmap(width, height)
    pixmap.fill()
    return pixmap


def _related():
    from app.features.browser_inventory.web_storage.related_dialog import RelatedUrlsEmailsDialog

    return RelatedUrlsEmailsDialog([FullRow()] * 30, [FullRow()] * 30, LONG_TEXT)


def _add_list(module: str, name: str):
    dialog = getattr(importlib.import_module(module), name)(["pattern"] * 40, "name")
    dialog.category_combo.setCurrentText("Custom")
    return dialog


def _batch(name: str):
    import app.common.dialogs.batch_import as module

    return getattr(module, name)([Path(f"/lists/list{index}.txt") for index in range(30)], {"list1"})


def _batch_progress():
    from app.common.dialogs.batch_import import BatchImportProgressDialog

    return BatchImportProgressDialog(30)


def _purge():
    from app.features.extraction import dialogs

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("core.database.get_evidence_table_counts", lambda conn, eid: {f"t{i}": 5 for i in range(60)})
        return dialogs.PurgeDataDialog(1, LONG_TEXT, MagicMock())


def _extract_and_ingest():
    from app.features.extraction.dialogs import ExtractAndIngestDialog

    return ExtractAndIngestDialog(ExtractorRegistry().get_all())


def _case_wide():
    return CaseWideExtractIngestDialog([{"id": index, "label": f"ev{index}"} for index in range(20)])


def _image_preview():
    from app.common.dialogs.image_preview import ImagePreviewDialog

    return ImagePreviewDialog({"filename": "image.jpg"}, None, None)


def _screenshot(width: int, height: int):
    from app.common.dialogs.screenshot_dialog import ScreenshotCaptureDialog

    return ScreenshotCaptureDialog(_pixmap(width, height), LONG_URL, MagicMock())


def _upload_screenshot():
    from app.features.screenshots.tab import UploadScreenshotDialog

    return UploadScreenshotDialog(_pixmap(600, 1900), "image.png", MagicMock())._dialog


def _sandbox():
    pytest.importorskip("PySide6.QtWebEngineWidgets")
    from app.common.dialogs.sandbox_browser import SandboxBrowserDialog

    return SandboxBrowserDialog("about:blank")


def _create_case():
    from app.common.dialogs.case import CreateCaseDialog

    return CreateCaseDialog()


def _export():
    from app.common.dialogs.import_export import ExportDialog

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ExportDialog, "_recalculate_size_estimate", lambda self: None)  # reads the case in a thread
        return ExportDialog(Path("/nonexistent"), "case")


def _import():
    from app.common.dialogs.import_export import ImportDialog

    dialog = ImportDialog(Path("/nonexistent"))
    dialog.validation_details.setPlainText("line\n" * 30)
    dialog.validation_details.setVisible(True)
    return dialog


def _remove_evidence():
    from app.common.dialogs.remove_evidence import RemoveEvidenceDialog

    tables = ("urls", "images", "browser_history", "cookies", "bookmarks", "browser_downloads", "file_list",
              "os_indicators", "timeline", "other")
    evidences = [{"id": index, "label": f"ev{index}", "source_path": "/path"} for index in range(30)]
    dialog = RemoveEvidenceDialog(evidences, lambda evidence_id: dict.fromkeys(tables, 5))
    dialog.radio_buttons[0].click()
    return dialog


def _deduplicate():
    from app.common.dialogs.deduplicate_urls import DeduplicateUrlsDialog

    case_data = MagicMock()
    case_data.list_url_sources.return_value = [f"source{index}" for index in range(30)]
    dialog = DeduplicateUrlsDialog(case_data, 1)
    dialog.preview_label.setText("<br>".join(["line"] * 8))
    dialog.progress_bar.setVisible(True)
    return dialog


def _tagging():
    from app.common.dialogs.tagging import TagArtifactsDialog

    case_data = MagicMock()
    case_data.list_tags.return_value = []
    return TagArtifactsDialog(case_data, 1, "url", [1])


def _tag_match_summary():
    from app.common.dialogs.tag_match_summary import TagMatchSummaryDialog

    return TagMatchSummaryDialog()


def _reference_list():
    from app.common.dialogs.reference_lists import ReferenceListSelectorDialog

    return ReferenceListSelectorDialog([f"list{index}" for index in range(30)])


def _hash_list():
    from app.common.dialogs.reference_lists import HashListSelectorDialog

    return HashListSelectorDialog([f"list{index}" for index in range(30)])


def _validation():
    from app.common.dialogs.progress import ValidationDialog
    from core.validation import ValidationReport

    return ValidationDialog(
        ValidationReport(case_folder=Path("/case"), timestamp=datetime(2024, 1, 1), results=[], duration_seconds=1.0)
    )


def _download_manager():
    from app.features.downloads.legacy import DownloadManagerDialog

    return DownloadManagerDialog()


def _tag_dialog(name: str):
    import app.features.tags.tab as module

    argument: Any = {
        "CreateTagDialog": (),
        "RenameTagDialog": ("tag",),
        "MergeTagsDialog": ([{"id": index, "name": f"t{index}", "usage_count": 1} for index in range(40)],),
    }[name]
    return getattr(module, name)(*argument)


def _preferences(registry):
    from app.features.settings import PreferencesDialog

    return PreferencesDialog(AppSettings(), None, registry)


def _section_editor():
    from reports.ui.section_editor import SectionEditorDialog

    return SectionEditorDialog(modules=[{"module_id": "unknown", "config": {}}] * 8)


def _module_picker():
    from reports.modules import ModuleRegistry
    from reports.ui.module_picker import ModulePickerDialog

    modules = [meta for group in ModuleRegistry().list_modules_by_category().values() for meta in group]
    fields = {meta.module_id: len(ModuleRegistry().get_module(meta.module_id).get_filter_fields()) for meta in modules}
    return ModulePickerDialog(module_id=max(fields, key=fields.get), show_title_input=True, edit_mode=True)


DIALOGS: Dict[str, Callable[[], QDialog]] = {
    "AddFileListDialog": lambda: _add_list("app.common.dialogs.add_filelist", "AddFileListDialog"),
    "AddUrlListDialog": lambda: _add_list("app.common.dialogs.add_urllist", "AddUrlListDialog"),
    "BatchHashListImportDialog": lambda: _batch("BatchHashListImportDialog"),
    "BatchUrlListImportDialog": lambda: _batch("BatchUrlListImportDialog"),
    "BatchImportProgressDialog": _batch_progress,
    "CaseWideExtractIngestDialog": _case_wide,
    "CreateCaseDialog": _create_case,
    "CreateTagDialog": lambda: _tag_dialog("CreateTagDialog"),
    "DeduplicateUrlsDialog": _deduplicate,
    "DownloadManagerDialog": _download_manager,
    "ExportDialog": _export,
    "ExtractAndIngestDialog": _extract_and_ingest,
    "HashListSelectorDialog": _hash_list,
    "ImagePreviewDialog": _image_preview,
    "ImportDialog": _import,
    "MergeTagsDialog": lambda: _tag_dialog("MergeTagsDialog"),
    "ModulePickerDialog": _module_picker,
    "PreferencesDialog": lambda: _preferences(ToolRegistry()),
    "PreferencesDialog without tool registry": lambda: _preferences(None),
    "PurgeDataDialog": _purge,
    "ReferenceListSelectorDialog": _reference_list,
    "RelatedUrlsEmailsDialog": _related,
    "RemoveEvidenceDialog": _remove_evidence,
    "RenameTagDialog": lambda: _tag_dialog("RenameTagDialog"),
    "SandboxBrowserDialog": _sandbox,
    "ScreenshotCaptureDialog": lambda: _screenshot(1600, 900),
    "ScreenshotCaptureDialog portrait": lambda: _screenshot(600, 1900),
    "SectionEditorDialog": _section_editor,
    "TagArtifactsDialog": _tagging,
    "TagMatchSummaryDialog": _tag_match_summary,
    "UploadScreenshotDialog": _upload_screenshot,
    "ValidationDialog": _validation,
}
# Their buttons are in a toolbar at the top, or they have none.
WITHOUT_BUTTON_ROW = {"SandboxBrowserDialog", "TagMatchSummaryDialog"}
# Minimum width 900 px, wider than the offscreen screen.
WIDE = {"ImagePreviewDialog", "SandboxBrowserDialog"}
# Never created by the application.
UNUSED = {"BulkExtractorReuseDialog", "EnhancedProgressDialog", "PartitionSelectionDialog"}
# Base class, not a dialog of its own.
BASE_CLASSES = {"ScreenLimitedDialog"}


@pytest.mark.parametrize("name", sorted(DIALOGS))
def test_dialog_fits_a_small_screen(qtbot, name):
    dialog = DIALOGS[name]()
    qtbot.addWidget(dialog)
    _check_dialog(dialog, buttons=name not in WITHOUT_BUTTON_ROW, wide=name in WIDE)


def test_every_dialog_class_is_tested():
    """A new QDialog subclass needs an entry in DIALOGS or DETAILS.

    Plain ``QDialog(...)`` objects are not found; the extractor configuration
    is tested below.
    """
    bases: Dict[str, List[str]] = {}
    for path in [*(SRC / "app").rglob("*.py"), *(SRC / "reports").rglob("*.py"), *(SRC / "extractors").rglob("*.py")]:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ClassDef):
                bases[node.name] = [getattr(base, "id", getattr(base, "attr", "")) for base in node.bases]
    dialogs = {"QDialog"}
    while True:
        found = {name for name, names in bases.items() if dialogs & set(names)} - dialogs
        if not found:
            break
        dialogs |= found
    tested = {name.split(" ")[0] for name in DIALOGS} | {name for _module, name in DETAILS_CLASSES}
    assert dialogs - {"QDialog"} - UNUSED - BASE_CLASSES - tested == set()


# ---------------------------------------------------------------------------
# Extractor configuration
# ---------------------------------------------------------------------------


@pytest.fixture
def shown_dialogs(monkeypatch):
    """Show modal dialogs without blocking and check them."""
    checked = []

    def fake_exec(dialog):
        _check_dialog(dialog)
        checked.append(dialog.windowTitle())
        dialog.hide()
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    monkeypatch.setattr(QMessageBox, "information", staticmethod(lambda *args: None))  # no options
    return checked


def test_configuration_dialogs_from_the_extractors_tab(qtbot, shown_dialogs):
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    extractors = ExtractorRegistry().get_all()
    for extractor in extractors:
        tab._show_config_dialog(extractor)
    assert len(shown_dialogs) >= 60
    assert len(set(shown_dialogs)) == len(shown_dialogs)


def test_configuration_dialogs_from_the_case_wide_dialog(qtbot, shown_dialogs):
    dialog = _case_wide()
    qtbot.addWidget(dialog)
    for row in range(dialog.extractor_list.count()):
        dialog.extractor_list.setCurrentRow(row)
        dialog._configure_selected_extractor()
    assert len(shown_dialogs) >= 55  # only extractors that can extract


def test_configuration_is_read_after_ok(qtbot, monkeypatch):
    """Wrapping the configuration widget must not break reading it."""
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    extractor = ExtractorRegistry().get("filesystem_images")

    def fake_exec(dialog):
        dialog.show()
        dialog.findChildren(QSpinBox)[0].setValue(17)
        dialog.hide()
        return QDialog.Accepted

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    tab._show_config_dialog(extractor)
    assert 17 in extractor._config.values() or 17 * 1024 in extractor._config.values()


# ---------------------------------------------------------------------------
# Opening size
# ---------------------------------------------------------------------------


def _sized_dialog(content_height: int) -> QDialog:
    dialog = QDialog()
    body = add_dialog_body(dialog)
    content = QPushButton("content")
    content.setMinimumSize(300, content_height)
    body.addWidget(content)
    dialog.layout().addWidget(QPushButton("OK"))
    return dialog


@pytest.mark.parametrize("content_height", [200, 500, 2000])
def test_preferred_size_or_larger_if_the_content_needs_it(qtbot, content_height):
    dialog = _sized_dialog(content_height)
    qtbot.addWidget(dialog)
    limit_to_screen(dialog, 320, 400)
    layout = dialog.layout()
    margins = layout.contentsMargins()
    ok_button = dialog.findChildren(QPushButton)[-1]
    needed = margins.top() + content_height + layout.spacing() + ok_button.sizeHint().height() + margins.bottom()
    dialog.show()
    _settle()
    available = QApplication.primaryScreen().availableGeometry()
    frame = dialog.windowHandle().frameMargins()
    assert available.contains(dialog.frameGeometry())
    assert dialog.height() == min(max(400, needed), available.height() - frame.top() - frame.bottom())
    assert _inside(dialog, ok_button)


def test_dialog_is_limited_to_the_screen_of_its_parent(qtbot, monkeypatch):
    window = QWidget()
    qtbot.addWidget(window)
    parent = QPushButton("parent", window)
    window.show()
    small = MagicMock()
    small.availableGeometry.return_value = QRect(0, 0, 500, 300)
    primary = QApplication.primaryScreen()
    monkeypatch.setattr(screen_fit, "_screen_of", lambda shown: small if shown is window else primary)
    dialog = _sized_dialog(700)
    dialog.setParent(parent, dialog.windowFlags())
    limit_to_screen(dialog, 600, 400)
    screen_fit._ScreenFit._limit_size(dialog)
    assert dialog.height() == 300


# ---------------------------------------------------------------------------
# Opening size on a screen that is large enough
# ---------------------------------------------------------------------------


@pytest.fixture
def large_screen(monkeypatch):
    """Dialogs are not limited, as on a screen that is large enough."""
    monkeypatch.setattr(screen_fit._ScreenFit, "_fit", lambda self: None)
    monkeypatch.setattr(screen_fit._ScreenFit, "_limit_size", staticmethod(lambda dialog: None))


def _assert_opens_without_scrolling(dialog: QDialog) -> None:
    dialog.show()
    _settle()
    for area in dialog.findChildren(ContentScrollArea):
        if area.isVisible():
            assert area.verticalScrollBar().maximum() == 0, dialog.windowTitle()


@pytest.mark.parametrize("name", sorted(set(DIALOGS) - {"RemoveEvidenceDialog", "DeduplicateUrlsDialog"}))
def test_dialog_opens_without_scrolling(qtbot, large_screen, name):
    dialog = DIALOGS[name]()
    qtbot.addWidget(dialog)
    _assert_opens_without_scrolling(dialog)


def test_dialogs_with_results_have_room_for_them(qtbot, large_screen):
    """Their factories add the results after the opening size is set."""
    for factory in (_remove_evidence, _deduplicate):
        dialog = factory()
        qtbot.addWidget(dialog)
        _assert_opens_without_scrolling(dialog)


@pytest.mark.parametrize("module,name", DETAILS_CLASSES, ids=[name for _module, name in DETAILS_CLASSES])
def test_details_dialog_opens_without_scrolling(qtbot, large_screen, monkeypatch, module, name):
    huge = MagicMock()
    huge.availableGeometry.return_value = QRect(0, 0, 10000, 10000)
    monkeypatch.setattr(screen_fit, "_screen_of", lambda window: huge)
    dialog = getattr(importlib.import_module(module), name)(FullRow())
    qtbot.addWidget(dialog)
    _assert_opens_without_scrolling(dialog)
    for area in dialog.findChildren(ContentScrollArea):
        assert area.horizontalScrollBar().maximum() == 0


@pytest.mark.parametrize("module,name", DETAILS_CLASSES, ids=[name for _module, name in DETAILS_CLASSES])
def test_details_dialog_has_room_for_the_horizontal_scroll_bar(qtbot, monkeypatch, module, name):
    """Long values on a screen that is high, but not wide enough."""
    screen = MagicMock()
    screen.availableGeometry.return_value = QRect(0, 0, 800, 10000)
    monkeypatch.setattr(screen_fit, "_screen_of", lambda window: screen)
    dialog = getattr(importlib.import_module(module), name)(FullRow())
    qtbot.addWidget(dialog)
    _assert_opens_without_scrolling(dialog)


def test_configuration_dialogs_open_without_scrolling(qtbot, large_screen, monkeypatch):
    def fake_exec(dialog):
        _assert_opens_without_scrolling(dialog)
        dialog.hide()
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    monkeypatch.setattr(QMessageBox, "information", staticmethod(lambda *args: None))  # no options
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    for extractor in ExtractorRegistry().get_all():
        tab._show_config_dialog(extractor)


@pytest.mark.parametrize(
    "name,width,height",
    [
        ("AddFileListDialog", 600, 500),
        ("AddUrlListDialog", 600, 500),
        ("BatchHashListImportDialog", 600, 500),
        ("CaseWideExtractIngestDialog", 700, 600),
        ("ExtractAndIngestDialog", 800, 600),
        ("ImagePreviewDialog", 1100, 750),
        ("ModulePickerDialog", 700, 500),
        ("PurgeDataDialog", 700, 600),
        ("SandboxBrowserDialog", 1200, 800),
        ("SectionEditorDialog", 650, 600),
        ("ValidationDialog", 900, 600),
    ],
)
def test_preferred_opening_sizes_are_kept(qtbot, name, width, height):
    dialog = DIALOGS[name]()
    qtbot.addWidget(dialog)
    assert (dialog.width(), dialog.height()) == (width, height)


def test_dialogs_without_a_fixed_size_open_at_their_size_hint(qtbot, large_screen):
    for name in ("BatchUrlListImportDialog", "ScreenshotCaptureDialog", "UploadScreenshotDialog", "PreferencesDialog"):
        dialog = DIALOGS[name]()
        qtbot.addWidget(dialog)
        assert dialog.height() == dialog.sizeHint().height(), name


def test_remove_evidence_shows_long_source_paths(qtbot, large_screen):
    from app.common.dialogs.remove_evidence import RemoveEvidenceDialog

    path = "/mnt/very/long/path/to/evidence/images/case-2025/4Dell_Latitude_CPi.E01"
    dialog = RemoveEvidenceDialog([{"id": 1, "label": "ev", "source_path": path}], lambda evidence_id: {})
    qtbot.addWidget(dialog)
    dialog.show()
    _settle()
    radio = dialog.radio_buttons[1]
    assert radio.visibleRegion().boundingRect().width() >= radio.fontMetrics().horizontalAdvance(f"    Source: {path}")


def test_deduplicate_keeps_result_and_warning_visible(qtbot):
    dialog = _deduplicate()
    qtbot.addWidget(dialog)
    dialog.show()
    dialog.resize(dialog.width(), 500)
    _settle()
    assert dialog.height() == 500
    for widget in (dialog.preview_label, dialog.progress_bar, dialog.deduplicate_button):
        assert _inside(dialog, widget)
        assert widget.visibleRegion().boundingRect() == widget.rect()


def test_report_dialogs_are_limited_to_the_screen(qtbot):
    dialog = _section_editor()
    qtbot.addWidget(dialog)
    available = QApplication.primaryScreen().availableGeometry()
    dialog.resize(700, available.height() + 500)
    dialog.show()
    _settle()
    assert available.contains(dialog.frameGeometry())

    dialog.resize(700, available.height())
    QCoreApplication.sendEvent(dialog, QShowEvent())  # not activated again
    _settle()
    assert available.contains(dialog.frameGeometry())


def test_report_dialog_is_fitted_again_when_the_frame_is_known(qtbot, monkeypatch):
    from reports.ui.small_screens import ScreenLimitedDialog

    frame = [QMargins()]
    monkeypatch.setattr(ScreenLimitedDialog, "_frame_margins", lambda dialog: frame[0])
    dialog = ScreenLimitedDialog()
    qtbot.addWidget(dialog)
    available = QApplication.primaryScreen().availableGeometry()
    dialog.resize(300, available.height() + 100)
    dialog.show()
    dialog.activateWindow()
    _settle()
    assert dialog.height() == available.height()
    # The offscreen platform activates a window while it is shown; X11 does it later.
    QCoreApplication.sendEvent(dialog, QShowEvent())
    _settle()

    frame[0] = QMargins(1, 37, 1, 1)
    assert dialog.isActiveWindow()
    QCoreApplication.sendEvent(dialog, QEvent(QEvent.Type.ActivationChange))
    _settle()
    assert dialog.height() == available.height() - 38

    dialog.resize(300, available.height())  # by the user
    QCoreApplication.sendEvent(dialog, QEvent(QEvent.Type.ActivationChange))
    _settle()
    assert dialog.height() == available.height()


def test_report_dialog_is_not_limited_again_after_it_was_minimized(qtbot, monkeypatch):
    from reports.ui.small_screens import ScreenLimitedDialog

    dialog = ScreenLimitedDialog()
    qtbot.addWidget(dialog)
    dialog.resize(600, 500)
    dialog.show()
    _settle()
    small = MagicMock()
    small.availableGeometry.return_value = QRect(0, 0, 400, 300)
    monkeypatch.setattr(ScreenLimitedDialog, "_screen_of", staticmethod(lambda window: small))
    dialog.showMinimized()
    _settle()
    dialog.showNormal()
    _settle()
    assert (dialog.width(), dialog.height()) == (600, 500)


# ---------------------------------------------------------------------------
# Mouse wheel in scrolling dialogs
# ---------------------------------------------------------------------------


def _values(dialog: QDialog) -> list:
    return [combo.currentIndex() for combo in dialog.findChildren(QComboBox)] + [
        field.text() for field in dialog.findChildren(QSpinBox) + dialog.findChildren(QDateEdit)
    ]


def _wheel_everywhere(dialog: QDialog) -> int:
    """Turn the wheel over every visible combo, spin and date field.

    No value may change. Returns the number of fields.
    """
    count = 0
    before = _values(dialog)
    fields = dialog.findChildren(QComboBox) + dialog.findChildren(QSpinBox) + dialog.findChildren(QDateEdit)
    for field in fields:
        for area in dialog.findChildren(QScrollArea):
            if area.isVisible() and area.isAncestorOf(field):
                area.ensureWidgetVisible(field)
        _settle()
        center = field.rect().center()
        if not field.isVisible() or not field.visibleRegion().contains(center):
            continue
        for notches in (-1, 1):
            QTest.wheelEvent(dialog.windowHandle(), QPointF(field.mapTo(dialog, center)), QPoint(0, 120 * notches))
            _settle()
            assert _values(dialog) == before, (dialog.windowTitle(), field)
        count += 1
    return count


def test_wheel_does_not_change_preferences(qtbot):
    dialog = _preferences(ToolRegistry())
    qtbot.addWidget(dialog)
    dialog.show()
    wheeled = 0
    for index in range(dialog.tabs.count()):
        dialog.tabs.setCurrentIndex(index)
        _settle()
        wheeled += _wheel_everywhere(dialog)
    assert wheeled >= 4


def test_wheel_does_not_change_module_filters(qtbot):
    from reports.modules import ModuleRegistry
    from reports.ui.module_picker import ModulePickerDialog

    wheeled = 0
    for group in ModuleRegistry().list_modules_by_category().values():
        for meta in group:
            dialog = ModulePickerDialog()
            qtbot.addWidget(dialog)
            dialog.show()
            dialog._select_module_by_id(meta.module_id)
            for date in dialog.findChildren(QDateEdit):
                date.setDate(QDate(2024, 5, 6))
            _settle()
            wheeled += _wheel_everywhere(dialog)
            dialog.hide()
    assert wheeled > 10


def test_wheel_does_not_change_extractor_configuration(qtbot, monkeypatch):
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    wheeled = []

    def fake_exec(dialog):
        dialog.show()
        _settle()
        wheeled.append(_wheel_everywhere(dialog))
        dialog.hide()
        return QDialog.Rejected

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    for name in ("filesystem_images", "swiftbeaver", "cache_simple", "browser_carver", "file_list"):
        tab._show_config_dialog(ExtractorRegistry().get(name))
    assert len(wheeled) == 5 and all(wheeled)
