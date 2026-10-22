"""URL tab: rows never wait for the filter dropdown lists (issue #63).

Covers the first page loading in parallel with ``FilterLoadWorker``, the
dropdowns keeping their selection while the lists (re)load, list reloads after
data changes, the unannounced ``data_version`` safety net, and the isolation
of cached lists between evidences and cases.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List

import pytest

import app.features.urls.tab as tab_module
from app.data import (
    STATS_TAGS,
    STATS_URL_MATCHES,
    STATS_URLS,
    CaseDataAccess,
    discard_case_stats_cache,
)
from app.features.urls.tab import FilterLoadWorker, UrlsTab
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000
ROWS = 250
SAFETY_NET_CATEGORIES = (STATS_URLS, STATS_TAGS, STATS_URL_MATCHES)


def _evidence_db_path(data: CaseDataAccess, evidence_id: int, label: str) -> Path:
    return data._db_manager.evidence_db_path(evidence_id, label)


@pytest.fixture
def url_case(tmp_path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=ROWS, evidence_count=2, tag_every=5, match_every=4, domain_count=5
    )
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    for evidence_id in case.evidence_ids:
        assert data.build_url_search_index(evidence_id).is_ready
    yield case, data
    data.close()
    discard_case_stats_cache(case.case_db_path)


@pytest.fixture
def make_tab(qtbot):
    created: List[UrlsTab] = []

    def _make(case, data, evidence_id, page_size=100):
        tab = UrlsTab()
        qtbot.addWidget(tab)
        created.append(tab)
        tab.set_page_size(page_size)
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        return tab

    yield _make
    for tab in created:
        tab.shutdown()


@pytest.fixture
def filter_gate(monkeypatch):
    """Blocks FilterLoadWorker runs on an event while ``gate.blocking``."""

    class Gate:
        def __init__(self) -> None:
            self.blocking = False
            self.release_event = threading.Event()
            self.entered = threading.Event()
            self.runs = 0

        def block(self) -> None:
            self.blocking = True
            self.release_event.clear()
            self.entered.clear()

        def release(self) -> None:
            self.blocking = False
            self.release_event.set()

    gate = Gate()
    original = FilterLoadWorker._load_top_domains

    def gated(self, case_data):
        gate.runs += 1
        if gate.blocking:
            gate.entered.set()
            assert gate.release_event.wait(10), "filter gate never opened"
        return original(self, case_data)

    monkeypatch.setattr(FilterLoadWorker, "_load_top_domains", gated)
    yield gate
    gate.release()


@pytest.fixture
def page_requests(monkeypatch):
    """Page indexes requested from the URL page loader (worker thread)."""
    original = tab_module._load_url_page
    calls: List[int] = []

    def counting(case_data, request):
        calls.append(request.payload.page)
        return original(case_data, request)

    monkeypatch.setattr(tab_module, "_load_url_page", counting)
    return calls


@pytest.fixture
def populate_calls(monkeypatch):
    calls: List[int] = []
    original = UrlsTab._populate_filters

    def counting(self):
        calls.append(self.evidence_id)
        return original(self)

    monkeypatch.setattr(UrlsTab, "_populate_filters", counting)
    return calls


def _rows_loaded(tab) -> bool:
    return not tab._data_loading and tab.model.rowCount() > 0


def _settled(tab) -> bool:
    """No page load running and the exact total known (may be 0 rows)."""
    return not tab._data_loading and tab.model.total_count() is not None


def _filters_ready(tab) -> bool:
    return not tab._filters_loading and tab.domain_combo.isEnabled()


def _ready(tab) -> bool:
    return _rows_loaded(tab) and _filters_ready(tab)


def _combo_values(combo) -> List[Any]:
    return [combo.itemData(i) for i in range(combo.count())]


def _filter_combos(tab):
    return (tab.domain_combo, tab.source_combo, tab.tag_combo)


# ---------------------------------------------------------------------------
# First page vs. filter lists
# ---------------------------------------------------------------------------


def test_first_page_loads_before_filter_lists(qtbot, make_tab, url_case, filter_gate, page_requests):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    filter_gate.block()
    tab.show()

    qtbot.waitUntil(filter_gate.entered.is_set, timeout=TIMEOUT_MS)  # the filter worker is running, blocked
    qtbot.waitUntil(lambda: _rows_loaded(tab) and tab.model.rowCount() == 100, timeout=TIMEOUT_MS)
    assert tab._filters_loading
    assert page_requests == [0]
    for combo in _filter_combos(tab):
        assert not combo.isEnabled()
        assert combo.toolTip() == "Loading…"
    # Search, paging and the text filters stay usable.
    assert tab.search_button.isEnabled() and tab.search_edit.isEnabled()
    assert tab.domain_contains_edit.isEnabled()
    qtbot.waitUntil(lambda: tab.next_button.isEnabled(), timeout=TIMEOUT_MS)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _rows_loaded(tab) and tab.model.current_page() == 1, timeout=TIMEOUT_MS)
    assert tab._filters_loading

    filter_gate.release()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)
    assert tab.domain_combo.findData("d1.example.com") > 0
    assert tab.source_combo.findData("chromium_history") > 0
    assert tab.tag_combo.findData("Relevant") > 0
    assert tab.match_combo.findData("list0") >= 3
    qtbot.wait(100)
    assert page_requests == [0, 1]  # the arriving lists did not reload rows
    assert tab.model.current_page() == 1


def test_search_while_lists_load_applies_filters(qtbot, make_tab, url_case, filter_gate):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    filter_gate.block()
    tab.show()
    qtbot.waitUntil(filter_gate.entered.is_set, timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: _rows_loaded(tab), timeout=TIMEOUT_MS)

    tab.domain_contains_edit.setText("d2.example")
    tab.search_button.click()
    qtbot.waitUntil(lambda: _rows_loaded(tab) and tab.model.total_count() == 50, timeout=TIMEOUT_MS)
    assert tab._filters_loading
    assert {tab.model.get_row(r)["domain"] for r in range(tab.model.rowCount())} == {"d2.example.com"}
    filter_gate.release()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)
    assert tab.model.filter_spec().domain_contains == "d2.example"


def test_selections_survive_list_reload(qtbot, make_tab, url_case, filter_gate, page_requests):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)

    tab.domain_combo.setCurrentIndex(tab.domain_combo.findData("d1.example.com"))
    tab.source_combo.setCurrentIndex(tab.source_combo.findData("firefox_history"))
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
    qtbot.waitUntil(lambda: _settled(tab) and tab.model.filter_spec().match_filter == "matched", timeout=TIMEOUT_MS)
    spec = tab.model.filter_spec()
    total = tab.model.total_count()

    filter_gate.block()
    tab.refresh()
    qtbot.waitUntil(filter_gate.entered.is_set, timeout=TIMEOUT_MS)
    # Loading: each dropdown shows only its current selection, disabled.
    assert tab.domain_combo.currentData() == "d1.example.com"
    assert tab.source_combo.currentData() == "firefox_history"
    assert tab.tag_combo.currentData() == "Relevant"
    assert tab.match_combo.currentData() == "matched"
    assert _combo_values(tab.domain_combo) == ["*", "d1.example.com"]
    assert _combo_values(tab.source_combo) == ["", "firefox_history"]
    assert _combo_values(tab.tag_combo) == ["*", "Relevant"]
    assert not any(combo.isEnabled() for combo in _filter_combos(tab))
    qtbot.waitUntil(lambda: _settled(tab) and tab.model.total_count() == total, timeout=TIMEOUT_MS)
    assert tab.model.filter_spec() == spec

    requests_before = list(page_requests)
    filter_gate.release()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)
    assert tab.domain_combo.currentData() == "d1.example.com"
    assert tab.source_combo.currentData() == "firefox_history"
    assert tab.tag_combo.currentData() == "Relevant"
    assert tab.match_combo.currentData() == "matched"
    assert tab.domain_combo.count() > 2 and tab.tag_combo.count() > 2
    assert tab.model.filter_spec() == spec
    qtbot.wait(100)
    assert page_requests == requests_before  # no reload when the lists arrive


def test_selected_value_missing_from_new_lists_is_inserted(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
    tab.domain_combo.setCurrentIndex(tab.domain_combo.findData("d4.example.com"))
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Review"))
    qtbot.waitUntil(lambda: _settled(tab) and tab.model.filter_spec().tag_name == "Review", timeout=TIMEOUT_MS)
    spec = tab.model.filter_spec()

    # The next lists no longer contain the selected domain and tag.
    def without_selection(self, case_data):
        return {"items": [{"domain": "d0.example.com", "count": 50}], "total_count": 5, "truncated": True}

    original_list_tags = CaseDataAccess.list_tags
    monkeypatch.setattr(FilterLoadWorker, "_load_top_domains", without_selection)
    monkeypatch.setattr(
        CaseDataAccess,
        "list_tags",
        lambda self, eid, *a, **k: [t for t in original_list_tags(self, eid, *a, **k) if t["name"] != "Review"],
    )
    tab._populate_filters()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)

    assert tab.domain_combo.currentData() == "d4.example.com"
    assert tab.domain_combo.currentIndex() == 1  # inserted after "All domains"
    assert tab.domain_combo.itemData(tab.domain_combo.count() - 1) == "__more__"
    assert tab.tag_combo.currentData() == "Review"
    assert tab.model.filter_spec() == spec


def test_selected_domain_matches_differently_cased_entry(qtbot, make_tab, url_case, monkeypatch, page_requests):
    """The Domain filter ignores ASCII case: a respelled entry is selected, not duplicated."""
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
    tab.domain_combo.setCurrentIndex(tab.domain_combo.findData("d4.example.com"))
    qtbot.waitUntil(lambda: _settled(tab) and tab.model.filter_spec().domain_exact == "d4.example.com",
                    timeout=TIMEOUT_MS)
    spec = tab.model.filter_spec()
    requests_before = list(page_requests)

    # After an ingestion the representative spelling (MIN(domain)) changed.
    def respelled(self, case_data):
        return {
            "items": [
                {"domain": "d0.example.com", "count": 50},
                {"domain": "D4.Example.COM", "count": 51},
            ],
            "total_count": 2,
            "truncated": False,
        }

    monkeypatch.setattr(FilterLoadWorker, "_load_top_domains", respelled)
    tab._populate_filters()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)

    assert _combo_values(tab.domain_combo) == ["*", "d0.example.com", "D4.Example.COM"]  # no duplicate
    assert tab.domain_combo.currentData() == "D4.Example.COM"
    assert tab.filters.domain == "d4.example.com"  # applied filter unchanged
    assert not tab._combo_filters_differ()
    qtbot.wait(100)
    assert page_requests == requests_before  # same (case-insensitive) filter: no reload
    assert tab.model.filter_spec() == spec

    # Other dropdowns keep exact matching: a respelled source is inserted.
    tab.source_combo.setCurrentIndex(tab.source_combo.findData("firefox_history"))
    qtbot.waitUntil(lambda: _settled(tab) and "firefox_history" in (tab.model.filter_spec().discovered_by or ()),
                    timeout=TIMEOUT_MS)
    monkeypatch.setattr(CaseDataAccess, "list_url_sources", lambda self, *a, **k: ["Firefox_History"])
    tab._populate_filters()
    qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)
    assert _combo_values(tab.source_combo) == ["", "firefox_history", "Firefox_History"]
    assert tab.source_combo.currentData() == "firefox_history"


def test_filter_load_error_keeps_selections_and_rows(qtbot, make_tab, url_case, monkeypatch, page_requests):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
    tab.source_combo.setCurrentIndex(tab.source_combo.findData("chromium_cache"))
    qtbot.waitUntil(lambda: _rows_loaded(tab) and "chromium_cache" in (tab.model.filter_spec().discovered_by or ()),
                    timeout=TIMEOUT_MS)

    def broken(self, *args, **kwargs):
        raise RuntimeError("sources unavailable")

    monkeypatch.setattr(CaseDataAccess, "list_url_sources", broken)
    requests_before = len(page_requests)
    tab.refresh()
    qtbot.waitUntil(lambda: not tab._filters_loading, timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: _rows_loaded(tab), timeout=TIMEOUT_MS)
    assert all(combo.isEnabled() for combo in _filter_combos(tab))
    assert tab.source_combo.currentData() == "chromium_cache"
    assert "chromium_cache" in (tab.model.filter_spec().discovered_by or ())
    assert len(page_requests) == requests_before + 1  # refresh() reloaded rows once
    assert tab.model.rowCount() > 0


def test_first_load_with_filter_error_still_shows_rows(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case

    def broken(self, *args, **kwargs):
        raise RuntimeError("tags unavailable")

    monkeypatch.setattr(CaseDataAccess, "list_tags", broken)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _rows_loaded(tab) and not tab._filters_loading, timeout=TIMEOUT_MS)
    assert tab.model.rowCount() == 100
    assert tab.domain_combo.isEnabled() and tab.domain_combo.currentData() == "*"


# ---------------------------------------------------------------------------
# Reloading the lists after data changes
# ---------------------------------------------------------------------------


def test_refresh_and_tagging_reload_the_lists(qtbot, make_tab, url_case, populate_calls):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
    assert populate_calls == [evidence_id]

    url_id = tab.model.get_row(0)["id"]
    data.tag_artifact(evidence_id, "Brand New", "url", url_id)  # tag writes bump the tag list
    tab._on_tags_changed()
    assert len(populate_calls) == 2
    qtbot.waitUntil(lambda: _filters_ready(tab) and tab.tag_combo.findData("Brand New") > 0, timeout=TIMEOUT_MS)

    tab.refresh()
    assert len(populate_calls) == 3
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)


def test_refresh_while_deferred_does_not_load_lists(qtbot, make_tab, url_case, populate_calls):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.refresh()
    assert populate_calls == []


def test_list_matching_invalidates_match_lists(qtbot, make_tab, url_case, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
    assert data.list_url_match_lists(evidence_id) == ["list0", "list1", "list2"]

    # A matching run wrote a new list (through another connection).
    db_path = _evidence_db_path(data, evidence_id, case.labels[evidence_id])
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO url_matches(evidence_id, url_id, list_name, match_type, matched_pattern, created_at_utc) "
                "VALUES (?, ?, 'fresh_list', 'exact', 'x', '2025-01-01T00:00:00Z')",
                (evidence_id, tab.model.get_row(0)["id"]),
            )
    finally:
        conn.close()
    assert "fresh_list" not in data.list_url_match_lists(evidence_id)  # still cached

    monkeypatch.setattr(QMessageBox, "information", staticmethod(lambda *a, **k: None))
    tab._match_finished({"fresh_list": 1}, tab._match_worker_generation)
    qtbot.waitUntil(lambda: _ready(tab) and tab.match_combo.findData("fresh_list") >= 0, timeout=TIMEOUT_MS)
    assert "fresh_list" in data.list_url_match_lists(evidence_id)


# ---------------------------------------------------------------------------
# Unannounced data_version change: safety net
# ---------------------------------------------------------------------------


@pytest.fixture
def invalidations(monkeypatch):
    calls: List[tuple] = []
    original = CaseDataAccess.invalidate_stats

    def recording(self, evidence_id=None, categories=None):
        calls.append((evidence_id, tuple(categories) if isinstance(categories, (tuple, list)) else categories))
        return original(self, evidence_id, categories)

    monkeypatch.setattr(CaseDataAccess, "invalidate_stats", recording)
    return calls


def test_unannounced_change_invalidates_stats_and_reloads_lists(
    qtbot, make_tab, url_case, invalidations, populate_calls, monkeypatch
):
    # No search index worker run (the index is ready anyway), so the change
    # is revealed directly by the next page, not after the worker settles.
    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab) and tab.model.total_count() == ROWS, timeout=TIMEOUT_MS)
    assert invalidations == []
    assert populate_calls == [evidence_id]

    # A tag renamed elsewhere, bypassing the data layer (no invalidation).
    db_path = _evidence_db_path(data, evidence_id, case.labels[evidence_id])
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute(
                "UPDATE tags SET name = 'Renamed', name_normalized = 'renamed' WHERE evidence_id = ? AND name = 'Review'",
                (evidence_id,),
            )
    finally:
        conn.close()
    assert "Review" in {t["name"] for t in data.list_tags(evidence_id)}  # stale cache

    tab.next_button.click()  # the page reveals the data_version change
    qtbot.waitUntil(lambda: (evidence_id, SAFETY_NET_CATEGORIES) in invalidations, timeout=TIMEOUT_MS)
    assert len(populate_calls) == 2
    qtbot.waitUntil(lambda: _filters_ready(tab) and tab.tag_combo.findData("Renamed") > 0, timeout=TIMEOUT_MS)
    assert tab.tag_combo.findData("Review") < 0
    assert {t["name"] for t in data.list_tags(evidence_id)} == {"Relevant", "Renamed"}


def test_tag_renamed_elsewhere_is_in_dropdown_when_returning_to_tab(
    qtbot, make_tab, url_case, invalidations
):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _ready(tab) and tab.model.total_count() == ROWS, timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=TIMEOUT_MS)
    tab.hide()
    conn = sqlite3.connect(_evidence_db_path(data, evidence_id, case.labels[evidence_id]))
    try:
        with conn:  # e.g. renamed in the Tags tab through another connection
            conn.execute(
                "UPDATE tags SET name = 'Renamed', name_normalized = 'renamed' WHERE evidence_id = ? AND name = 'Review'",
                (evidence_id,),
            )
    finally:
        conn.close()
    tab.show()  # re-checks the shown page (#67)
    qtbot.waitUntil(lambda: _filters_ready(tab) and tab.tag_combo.findData("Renamed") > 0, timeout=3000)


def test_own_search_index_build_triggers_safety_net_at_most_once(qtbot, tmp_path, invalidations, populate_calls):
    from core.database.helpers.url_search_index import url_search_index_supported

    if not url_search_index_supported():
        pytest.skip("SQLite lacks FTS5 with the trigram tokenizer")
    case = build_url_fixture_case(tmp_path / "noindex", rows_per_evidence=ROWS, domain_count=5)
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    evidence_id = case.evidence_ids[0]
    tab = UrlsTab()
    qtbot.addWidget(tab)
    try:
        tab.set_page_size(100)
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        tab.show()  # starts the background search index build (commits)
        qtbot.waitUntil(lambda: _ready(tab), timeout=TIMEOUT_MS)
        qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=TIMEOUT_MS)
        qtbot.waitUntil(lambda: tab._index_worker is None, timeout=TIMEOUT_MS)
        # The tab cannot tell its own index writes from writes made elsewhere
        # meanwhile, so the settled change is reported at most once...
        tab.next_button.click()
        qtbot.waitUntil(lambda: _rows_loaded(tab), timeout=TIMEOUT_MS)
        qtbot.waitUntil(lambda: _filters_ready(tab), timeout=TIMEOUT_MS)
        settled_invalidations = invalidations.count((evidence_id, SAFETY_NET_CATEGORIES))
        settled_populates = len(populate_calls)
        assert settled_invalidations <= 1
        assert settled_populates <= 2
        # ...and paging afterwards never triggers the safety net again.
        tab.next_button.click()
        qtbot.waitUntil(lambda: _rows_loaded(tab), timeout=TIMEOUT_MS)
        tab.prev_button.click()
        qtbot.waitUntil(lambda: _rows_loaded(tab), timeout=TIMEOUT_MS)
        qtbot.wait(100)
        assert invalidations.count((evidence_id, SAFETY_NET_CATEGORIES)) == settled_invalidations
        assert len(populate_calls) == settled_populates
    finally:
        tab.shutdown()
        qtbot.waitUntil(lambda: tab._index_worker is None or tab._index_worker.isFinished(), timeout=TIMEOUT_MS)
        data.close()
        discard_case_stats_cache(case.case_db_path)


# ---------------------------------------------------------------------------
# Isolation of cached lists
# ---------------------------------------------------------------------------


def test_cached_lists_are_isolated_between_evidences(qtbot, make_tab, url_case):
    case, data = url_case
    ev1, ev2 = case.evidence_ids
    db_path = _evidence_db_path(data, ev2, case.labels[ev2])
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute("UPDATE urls SET discovered_by = 'ev2_only' WHERE evidence_id = ? AND id % 2 = 0", (ev2,))
            conn.execute("UPDATE urls SET domain = 'only-two.example' WHERE evidence_id = ? AND id % 7 = 0", (ev2,))
    finally:
        conn.close()

    first = make_tab(case, data, ev1)
    second = make_tab(case, data, ev2)
    first.show()
    qtbot.waitUntil(lambda: _ready(first), timeout=TIMEOUT_MS)
    second.show()
    qtbot.waitUntil(lambda: _ready(second), timeout=TIMEOUT_MS)

    assert first.source_combo.findData("ev2_only") < 0
    assert first.domain_combo.findData("only-two.example") < 0
    assert second.source_combo.findData("ev2_only") > 0
    assert second.domain_combo.findData("only-two.example") > 0

    # Reloads served from the shared cache stay per evidence.
    first._populate_filters()
    second._populate_filters()
    qtbot.waitUntil(lambda: _filters_ready(first) and _filters_ready(second), timeout=TIMEOUT_MS)
    assert first.source_combo.findData("ev2_only") < 0
    assert second.source_combo.findData("ev2_only") > 0


def test_cached_lists_are_isolated_between_cases(qtbot, make_tab, tmp_path):
    cases = [
        build_url_fixture_case(tmp_path / name, rows_per_evidence=40, domain_count=count)
        for name, count in (("three", 3), ("seven", 7))
    ]
    datas = [CaseDataAccess(c.case_folder, c.case_db_path) for c in cases]
    try:
        tabs = [make_tab(c, d, c.evidence_ids[0]) for c, d in zip(cases, datas)]
        for tab in tabs:
            tab.show()
        qtbot.waitUntil(lambda: all(_ready(t) for t in tabs), timeout=TIMEOUT_MS)
        assert tabs[0].domain_combo.count() == 1 + 3
        assert tabs[1].domain_combo.count() == 1 + 7
        # Same evidence id in both cases, different lists.
        assert cases[0].evidence_ids == cases[1].evidence_ids
        for tab in tabs:
            tab._populate_filters()
        qtbot.waitUntil(lambda: all(_filters_ready(t) for t in tabs), timeout=TIMEOUT_MS)
        assert tabs[0].domain_combo.count() == 1 + 3
        assert tabs[1].domain_combo.count() == 1 + 7
    finally:
        for data in datas:
            data.close()
        for case in cases:
            discard_case_stats_cache(case.case_db_path)
