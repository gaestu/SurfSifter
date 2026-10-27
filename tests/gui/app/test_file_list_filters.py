"""File List tab: Exclude field and the Tags dropdown's fixed entries (issue #73).

Covers Exclude being applied only on Enter/Search, the fixed "All", "Any Tag"
and "Untagged" entries (distinct from tags with those names), selections
surviving filter-list reloads, the loading and error states and tagging, and
CSV export of the filtered rows.
"""
from __future__ import annotations

import csv
from dataclasses import replace
from typing import Any, List

import pytest
from PySide6.QtCore import Qt

import app.features.file_list.tab as tab_module
from app.data import CaseDataAccess, FileListFilterSpec, discard_case_stats_cache
from app.features.file_list import FileListTab
from core.database import DatabaseManager
from tests.fixtures.file_list_fixtures import build_file_list_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000


@pytest.fixture
def fl_case(tmp_path):
    case = build_file_list_fixture_case(tmp_path / "case", rows=80, tag_every=4, match_every=5)
    manager = DatabaseManager(case.case_folder, case_db_path=case.case_db_path)
    conn = manager.get_evidence_conn(case.evidence_id, case.label)
    with conn:
        # Real tags named like the fixed entries, on files 2 and 3
        for name, file_id in (("Untagged", 2), ("any", 3)):
            tag_id = conn.execute(
                "INSERT INTO tags(evidence_id, name, name_normalized) VALUES (?, ?, ?)",
                (case.evidence_id, name, name.lower()),
            ).lastrowid
            conn.execute(
                "INSERT INTO tag_associations(tag_id, evidence_id, artifact_type, artifact_id) "
                "VALUES (?, ?, 'file_list', ?)",
                (tag_id, case.evidence_id, file_id),
            )
        # Match lists are listed from the cache that matching fills
        conn.executemany(
            "INSERT INTO file_list_filter_cache(evidence_id, filter_type, filter_value, count, last_updated) "
            "VALUES (?, 'match', ?, 1, '2025-01-01T00:00:00Z')",
            [(case.evidence_id, name) for name in ("list0", "list1", "list2")],
        )
    manager.close_all()
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()
    discard_case_stats_cache(case.case_db_path)


@pytest.fixture
def make_tab(qtbot):
    created: List[FileListTab] = []

    def _make(case, data):
        tab = FileListTab(case.case_folder, case.evidence_id, case.case_db_path, defer_load=True)
        qtbot.addWidget(tab)
        created.append(tab)
        tab.set_case_data(data)
        tab._perform_deferred_load()
        _wait_lists(qtbot, tab)
        return tab

    yield _make
    for tab in created:
        tab.shutdown()


def _wait_lists(qtbot, tab) -> None:
    qtbot.waitUntil(lambda: tab._filter_worker is None and tab.tags_combo.isEnabled(), timeout=TIMEOUT_MS)


def _shown_ids(tab) -> List[int]:
    return [row["id"] for row in tab.model._rows]


def _expected_ids(data, case, spec: FileListFilterSpec) -> List[int]:
    return [row["id"] for row in data.list_file_list_rows(case.evidence_id, spec)]


def _select(combo, value: Any) -> None:
    index = combo.findData(value)
    assert index >= 0, value
    combo.setCurrentIndex(index)


def test_tags_dropdown_starts_with_fixed_entries(make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    combo = tab.tags_combo
    texts = [combo.itemText(i) for i in range(combo.count())]
    assert texts[:3] == ["All", "Any Tag", "Untagged"]
    assert combo.itemData(1) is tab_module._ANY_TAG and combo.itemData(2) is tab_module._UNTAGGED_TAG
    assert {"Relevant", "Review", "Untagged", "any"} <= set(texts[3:])
    assert all(isinstance(combo.itemData(i), str) for i in range(3, combo.count()))


def test_exclude_applies_on_enter_and_search_only(qtbot, make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    total = tab.model._total_rows

    qtbot.keyClicks(tab.exclude_edit, "winsxs")
    assert tab.model.filters.exclude is None and tab.model._total_rows == total

    qtbot.keyClick(tab.exclude_edit, Qt.Key_Return)
    assert tab.model.filters.exclude == "winsxs"
    assert _shown_ids(tab) == _expected_ids(data, case, FileListFilterSpec(exclude="winsxs"))
    assert tab.model._rows and all("winsxs" not in row["file_path"].lower() for row in tab.model._rows)
    assert tab.summary_label.text().startswith(f"{tab.model._total_rows:,} files")

    # Search + Exclude of the same text is empty; the counts add up to all
    excluded = tab.model._total_rows
    tab.search_edit.setText("WinSxS")
    tab.search_btn.click()
    assert tab.model._total_rows == 0
    tab.exclude_edit.clear()
    tab.search_btn.click()
    assert tab.model._total_rows + excluded == total

    # Cleared fields (and whitespace) restore the results
    tab.search_edit.setText("   ")
    tab.search_btn.click()
    assert tab.model.filters == FileListFilterSpec() and tab.model._total_rows == total


def test_untagged_and_any_tag_filter_and_complement(make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    total = tab.model._total_rows

    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    assert tab.model.filters.untagged and tab.model.filters.tag_name is None
    untagged = _shown_ids(tab)
    assert untagged == _expected_ids(data, case, FileListFilterSpec(untagged=True))
    assert all(tab.model._get_tags(file_id) == "" for file_id in untagged)
    assert "🏷️ filtered" in tab.summary_label.text()

    _select(tab.tags_combo, tab_module._ANY_TAG)
    assert tab.model.filters.any_tag and not tab.model.filters.untagged
    assert all(tab.model._get_tags(file_id) for file_id in _shown_ids(tab))
    assert tab.model._total_rows + len(untagged) == total

    # Combines with the other filters
    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    _select(tab.extension_combo, "dll")
    tab.exclude_edit.setText("System32")
    tab.search_btn.click()
    spec = FileListFilterSpec(extension="dll", untagged=True, exclude="System32")
    assert tab.model.filters == spec
    assert _shown_ids(tab) == _expected_ids(data, case, spec)
    assert tab.model._total_rows == data.count_file_list(case.evidence_id, spec)


def test_tags_named_untagged_and_any_are_normal_tags(make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)

    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    untagged_total = tab.model._total_rows
    # Same text as the fixed entry: still applied (the combo filters by index)
    _select(tab.tags_combo, "Untagged")
    assert tab.model.filters == FileListFilterSpec(tag_name="Untagged")
    assert _shown_ids(tab) == [2]
    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    assert tab.model._total_rows == untagged_total

    _select(tab.tags_combo, "any")
    assert tab.model.filters == FileListFilterSpec(tag_name="any")
    assert _shown_ids(tab) == [3]


def test_selections_survive_reload_loading_and_error(qtbot, make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    _select(tab.extension_combo, "dll")
    _select(tab.matches_combo, "list1")
    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    applied = tab.model.filters
    assert applied == FileListFilterSpec(extension="dll", match_list="list1", untagged=True)
    rows = _shown_ids(tab)

    def assert_combos_show_applied():
        assert tab.extension_combo.currentData() == "dll"
        assert tab.matches_combo.currentData() == "list1"
        assert tab.tags_combo.currentData() is tab_module._UNTAGGED_TAG

    # Loading state: Search keeps the applied dropdown values
    tab._set_filters_loading()
    tab.exclude_edit.setText("WinSxS")
    tab._on_search_clicked()
    assert tab.model.filters == replace(applied, exclude="WinSxS")
    tab.exclude_edit.clear()
    tab._on_search_clicked()
    assert tab.model.filters == applied and _shown_ids(tab) == rows

    # Error state: rows keep the applied filters, and so does Search
    tab._on_filters_load_error("boom")
    assert tab.model.filters == applied and _shown_ids(tab) == rows
    tab._on_search_clicked()
    assert tab.model.filters == applied

    # Reload
    tab._load_data()
    _wait_lists(qtbot, tab)
    assert_combos_show_applied()
    assert tab.model.filters == applied and _shown_ids(tab) == rows

    # _load_tags (after tagging) keeps the selection too
    tab._load_tags()
    assert tab.tags_combo.currentData() is tab_module._UNTAGGED_TAG
    assert [tab.tags_combo.itemText(i) for i in range(3)] == ["All", "Any Tag", "Untagged"]


def test_applied_value_missing_from_new_lists_is_inserted(qtbot, make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    tab.model.apply_filters(FileListFilterSpec(extension="zzz", tag_name="Gone"))
    tab._load_data()
    _wait_lists(qtbot, tab)
    assert tab.extension_combo.currentData() == "zzz" and tab.extension_combo.currentText() == "zzz"
    assert tab.tags_combo.currentData() == "Gone" and tab.tags_combo.currentIndex() == 3
    assert tab.model.filters == FileListFilterSpec(extension="zzz", tag_name="Gone")


class _FakeTagDialog:
    """Tags the selected files with "Relevant" and accepts, like the real dialog."""

    def __init__(self, case_data, evidence_id, artifact_type, artifact_ids, parent=None):
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.artifact_type = artifact_type
        self.artifact_ids = artifact_ids
        self._slots = []
        self.tags_changed = self
        tagged.append(list(artifact_ids))

    def connect(self, slot):
        self._slots.append(slot)

    def exec(self):
        for artifact_id in self.artifact_ids:
            self.case_data.tag_artifact(self.evidence_id, "Relevant", self.artifact_type, artifact_id)
        for slot in self._slots:
            slot()
        return True


tagged: List[List[int]] = []


def test_tagging_with_untagged_active_drops_the_file(qtbot, make_tab, fl_case, monkeypatch):
    case, data = fl_case
    tab = make_tab(case, data)
    monkeypatch.setattr(tab_module, "TagArtifactsDialog", _FakeTagDialog)
    tagged.clear()
    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    before = tab.model._total_rows
    file_id = _shown_ids(tab)[0]
    tab.model.toggle_selection(0)

    tab._tag_selected()
    assert tagged == [[file_id]]
    assert file_id not in _shown_ids(tab)
    assert tab.model._total_rows == before - 1
    assert tab.summary_label.text().startswith(f"{before - 1:,} files")
    _wait_lists(qtbot, tab)
    assert tab.tags_combo.currentData() is tab_module._UNTAGGED_TAG
    assert tab.model.filters.untagged and file_id not in _shown_ids(tab)


def test_csv_export_follows_the_filters(make_tab, fl_case, monkeypatch, tmp_path):
    case, data = fl_case
    tab = make_tab(case, data)
    _select(tab.tags_combo, tab_module._UNTAGGED_TAG)
    tab.exclude_edit.setText("Windows")
    tab.search_btn.click()
    out = tmp_path / "export.csv"
    monkeypatch.setattr(tab_module.QFileDialog, "getSaveFileName", staticmethod(lambda *a, **k: (str(out), "")))
    monkeypatch.setattr(tab_module.QMessageBox, "information", staticmethod(lambda *a, **k: None))

    tab.export_csv()

    with open(out, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    expected = data.list_file_list_rows(case.evidence_id, FileListFilterSpec(untagged=True, exclude="Windows"))
    assert [row["File Path"] for row in rows] == [row["file_path"] for row in expected]
    assert rows and all(row["Tags"] == "" and "windows" not in row["File Path"].lower() for row in rows)


def test_refresh_worker_after_matching_uses_the_applied_filters(qtbot, make_tab, fl_case):
    case, data = fl_case
    tab = make_tab(case, data)
    spec = FileListFilterSpec(untagged=True, exclude="WinSxS", match_list="list1")
    worker = tab_module.ModelRefreshWorker(tab.model.data_access, case.evidence_id, spec, page_size=5)
    with qtbot.waitSignal(worker.finished, timeout=TIMEOUT_MS) as blocker:
        worker.start()
    worker.wait(TIMEOUT_MS)
    rows, total = blocker.args
    expected = data.list_file_list_rows(case.evidence_id, spec)
    assert [row["id"] for row in rows] == [row["id"] for row in expected[:5]]
    assert total == len(expected) == data.count_file_list(case.evidence_id, spec)
