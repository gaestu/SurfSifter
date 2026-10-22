"""GUI tests for the incrementally loaded grouped URL tree (issue #64)."""
from __future__ import annotations

import threading
import time
from typing import Dict, List

import pytest
from PySide6.QtCore import QItemSelectionModel, Qt
from PySide6.QtTest import QAbstractItemModelTester

from app.data import CaseDataAccess, UrlFilterSpec
from app.features.urls.models import UrlsGroupedModel
from app.features.urls.models.urls_grouped import FAILED, LOADED, LOADING, NOT_LOADED
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

ROWS = 300
DOMAINS = 12
NO_DOMAIN = 20  # rows without a domain (NULL or '')


@pytest.fixture
def tree_case(tmp_path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=ROWS, evidence_count=2,
        timestamp_every=3, tag_every=5, match_every=4, domain_count=DOMAINS,
    )
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    for evidence_id in case.evidence_ids:
        with data._use_evidence_conn(evidence_id):
            with data._connect() as conn:
                ids = [r[0] for r in conn.execute("SELECT id FROM urls ORDER BY id LIMIT ?", (NO_DOMAIN,))]
                conn.executemany(
                    "UPDATE urls SET domain = ? WHERE id = ?",
                    [(None if i % 2 else "", url_id) for i, url_id in enumerate(ids)],
                )
                conn.execute("UPDATE urls SET domain = upper(domain) WHERE id % 9 = 0")
        assert data.build_url_search_index(evidence_id).is_ready
    yield case, data
    data.close()


@pytest.fixture
def small_pages(monkeypatch):
    monkeypatch.setattr(UrlsGroupedModel, "GROUP_PAGE_SIZE", 5)
    monkeypatch.setattr(UrlsGroupedModel, "CHILD_PAGE_SIZE", 10)


@pytest.fixture
def make_tab(qtbot):
    from app.features.urls import UrlsTab

    created = []

    def _make(case, data, evidence_id, *, grouped=True):
        tab = UrlsTab()
        qtbot.addWidget(tab)
        created.append(tab)
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        if grouped:
            tab.group_checkbox.setChecked(True)  # before the first load
        QAbstractItemModelTester(tab.group_model, QAbstractItemModelTester.FailureReportingMode.Fatal, tab)
        return tab

    yield _make
    for tab in created:
        tab.shutdown()


class _Calls:
    def __init__(self) -> None:
        self.groups: List = []
        self.children: List = []
        self.pages: List = []
        self.started: Dict = {}


def _instrument(monkeypatch, *, child_delay: float = 0.0, fail_children_once=()) -> _Calls:
    """Record (and optionally slow down / fail) the tab's background handlers."""
    import app.features.urls.tab as tab_module

    calls = _Calls()
    failing = set(fail_children_once)
    originals = {
        name: getattr(tab_module, name)
        for name in ("_load_url_domain_groups", "_load_url_domain_children", "_load_url_page")
    }

    def groups(case_data, request):
        calls.groups.append(request.payload)
        return originals["_load_url_domain_groups"](case_data, request)

    def children(case_data, request):
        query = request.payload
        calls.children.append(query)
        calls.started[query.domain] = calls.started.get(query.domain, 0) + 1
        deadline = time.perf_counter() + child_delay
        while time.perf_counter() < deadline:
            if case_data._thread_local.cancel_check():
                from app.data import QueryCancelledError

                raise QueryCancelledError("interrupted")
            time.sleep(0.005)
        if query.domain in failing:
            failing.discard(query.domain)
            raise RuntimeError("disk on fire")
        return originals["_load_url_domain_children"](case_data, request)

    def pages(case_data, request):
        calls.pages.append(request.payload)
        return originals["_load_url_page"](case_data, request)

    monkeypatch.setattr(tab_module, "_load_url_domain_groups", groups)
    monkeypatch.setattr(tab_module, "_load_url_domain_children", children)
    monkeypatch.setattr(tab_module, "_load_url_page", pages)
    return calls


def _expected_groups(data, evidence_id, spec=UrlFilterSpec()):
    from app.data import UrlDomainGroupQuery

    page = data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, spec, page_size=10_000))
    return [(g.domain, g.count) for g in page.groups]


def _shown_groups(model):
    return [(model.group_domain(r), model.group_url_count(r)) for r in range(model.group_count())]


def _child_ids(model, row):
    parent = model.index(row, 0)
    return [model.url_row(model.index(r, 0, parent))["id"] for r in range(model.loaded_url_count(row))]


def _status(model, parent=None):
    """Text of the trailing status row under ``parent`` (None if there is none)."""
    from PySide6.QtCore import QModelIndex

    parent = parent or QModelIndex()
    count = model.rowCount(parent)
    if not count:
        return None
    index = model.index(count - 1, 0, parent)
    return model.data(index) if model.is_status_index(index) else None


def _activate_status(tab, parent=None):
    from PySide6.QtCore import QModelIndex

    parent = parent or QModelIndex()
    model = tab.group_model
    index = model.index(model.rowCount(parent) - 1, 0, parent)
    assert model.is_status_index(index)
    tab.tree_view.activated.emit(index)


def _wait_groups(qtbot, tab, count):
    qtbot.waitUntil(lambda: tab.group_model.state() == LOADED and tab.group_model.group_count() == count, timeout=10000)


def test_initial_display_builds_one_group_page_and_no_children(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    _wait_groups(qtbot, tab, 5)

    model = tab.group_model
    assert len(calls.groups) == 1 and calls.children == []
    assert calls.pages == []  # the hidden flat view is not loaded
    assert _shown_groups(model) == _expected_groups(data, case.evidence_ids[0])[:5]
    assert all(model.loaded_url_count(r) == 0 and model.hasChildren(model.index(r, 0)) for r in range(5))
    assert _status(model).startswith("Load 5 more domains")
    assert tab.page_label.text().startswith("5 domains shown, more available")
    assert not tab.prev_button.isEnabled() and not tab.next_button.isEnabled()
    assert not tab.page_size_combo.isEnabled()


def test_load_more_domains_reaches_every_group_with_no_domain_last(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    expected = _expected_groups(data, case.evidence_ids[0])
    assert len(expected) == DOMAINS + 1 and expected[-1] == (None, NO_DOMAIN)
    _wait_groups(qtbot, tab, 5)
    _activate_status(tab)
    _wait_groups(qtbot, tab, 10)
    _activate_status(tab)
    _wait_groups(qtbot, tab, DOMAINS + 1)

    model = tab.group_model
    assert _shown_groups(model) == expected
    assert _status(model) is None and not model.has_more_groups()
    assert len(calls.groups) == 3 and calls.children == []
    assert model.data(model.index(DOMAINS, 0)) == f"(no domain) ({NO_DOMAIN})"
    assert sum(count for _, count in expected) == ROWS
    qtbot.waitUntil(lambda: tab.page_label.text() == f"{DOMAINS + 1} domains shown · {ROWS} URLs", timeout=10000)


def test_expanding_loads_children_in_stable_pages(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, 5)
    model = tab.group_model
    domain, count = _shown_groups(model)[0]
    flat = [r["id"] for r in data.iter_url_rows(evidence_id, UrlFilterSpec(domain_exact=domain))]
    assert len(flat) == count > 20

    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: model.loaded_url_count(0) == 10 and model.group_state(0) == LOADED, timeout=10000)
    parent = model.index(0, 0)
    assert _status(model, parent) == f"Load 10 more URLs… (10 of {count} shown)"
    while model.group_has_more(0):
        loaded = model.loaded_url_count(0)
        _activate_status(tab, parent)
        qtbot.waitUntil(lambda: model.loaded_url_count(0) > loaded and model.group_state(0) == LOADED, timeout=10000)
    assert _child_ids(model, 0) == flat
    assert _status(model, parent) is None
    assert [q.domain for q in calls.children] == [domain] * len(calls.children)
    assert len(calls.children) == -(-count // 10)


def test_no_domain_group_children_merge_null_and_empty(qtbot, make_tab, tree_case, monkeypatch):
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, DOMAINS + 1)
    model = tab.group_model
    tab.tree_view.expand(model.index(DOMAINS, 0))
    qtbot.waitUntil(lambda: model.group_state(DOMAINS) == LOADED, timeout=10000)
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            missing = {r[0] for r in conn.execute("SELECT id FROM urls WHERE domain IS NULL OR domain = ''")}
    flat = [r["id"] for r in data.iter_url_rows(evidence_id, UrlFilterSpec()) if r["id"] in missing]
    assert _child_ids(model, DOMAINS) == flat and len(flat) == NO_DOMAIN


def test_sibling_expansions_queue_and_collapse_drops_queued_request(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    calls = _instrument(monkeypatch, child_delay=0.3)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    _wait_groups(qtbot, tab, 5)
    model, view = tab.group_model, tab.tree_view
    domains = [model.group_domain(r) for r in range(3)]

    for row in range(3):
        view.expand(model.index(row, 0))
    assert [model.group_state(r) for r in range(3)] == [LOADING] * 3
    view.collapse(model.index(1, 0))  # still queued: dropped
    assert model.group_state(1) == NOT_LOADED and model.rowCount(model.index(1, 0)) == 0
    view.collapse(model.index(0, 0))  # running: still applied
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED and model.group_state(2) == LOADED, timeout=10000)
    assert [q.domain for q in calls.children] == [domains[0], domains[2]]
    assert model.loaded_url_count(0) == 10 and model.loaded_url_count(2) == 10

    # Rapid toggling of one group never issues duplicate requests.
    for _ in range(5):
        view.expand(model.index(1, 0))
        view.collapse(model.index(1, 0))
    view.expand(model.index(1, 0))
    qtbot.waitUntil(lambda: model.group_state(1) == LOADED, timeout=10000)
    qtbot.wait(400)
    assert calls.started[domains[1]] <= 2
    assert _child_ids(model, 1) == [
        r["id"] for r in data.iter_urls(case.evidence_ids[0], filters=UrlFilterSpec(domain_exact=domains[1]), limit=10)
    ]


@pytest.mark.parametrize("change", ["filter", "evidence", "refresh"])
def test_changes_reject_in_flight_children(qtbot, make_tab, tree_case, small_pages, monkeypatch, change):
    case, data = tree_case
    ev1, ev2 = case.evidence_ids
    calls = _instrument(monkeypatch, child_delay=0.4)
    tab = make_tab(case, data, ev1)
    tab.show()
    _wait_groups(qtbot, tab, 5)
    model = tab.group_model
    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: len(calls.children) == 1, timeout=10000)

    if change == "filter":
        tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
        spec = UrlFilterSpec(match_filter="matched")
        evidence_id = ev1
    elif change == "evidence":
        tab.set_evidence(ev2, evidence_label=case.labels[ev2])
        spec, evidence_id = UrlFilterSpec(), ev2
    else:
        tab.refresh()
        spec, evidence_id = UrlFilterSpec(), ev1
    qtbot.wait(700)  # the obsolete child page would have arrived by now
    expected = _expected_groups(data, evidence_id, spec)[:5]
    _wait_groups(qtbot, tab, len(expected))
    assert _shown_groups(model) == expected
    assert model.group_state(0) in (NOT_LOADED, LOADING, LOADED)
    if change == "refresh":
        # Same evidence and filters: the expanded group is re-expanded and reloaded.
        qtbot.waitUntil(lambda: model.group_state(0) == LOADED, timeout=10000)
        assert tab.tree_view.isExpanded(model.index(0, 0)) and model.loaded_url_count(0) == 10
    elif model.group_state(0) == LOADED:
        assert all(r["id"] for r in (model.url_row(model.index(i, 0, model.index(0, 0))) for i in range(model.loaded_url_count(0))))
    for row in range(model.group_count()):
        for child in _child_ids(model, row):
            assert child in {r["id"] for r in data.iter_url_rows(evidence_id, spec)}
    assert calls.pages == []


def test_grouped_filtering_skips_hidden_flat_and_toggling_reuses_views(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0], grouped=False)
    tab.show()
    qtbot.waitUntil(lambda: len(calls.pages) == 1 and not tab._data_loading, timeout=10000)

    tab.group_checkbox.setChecked(True)
    _wait_groups(qtbot, tab, 5)
    tab.tree_view.expand(tab.group_model.index(1, 0))
    qtbot.waitUntil(lambda: tab.group_model.group_state(1) == LOADED, timeout=10000)
    tab.source_combo.setCurrentIndex(max(1, tab.source_combo.findData("firefox_history")))
    qtbot.waitUntil(lambda: len(calls.groups) == 2 and tab.group_model.state() == LOADED, timeout=10000)
    assert len(calls.pages) == 1  # hidden flat view not loaded
    groups, children = len(calls.groups), len(calls.children)

    # Back and forth: the flat view reloads once (stale); the tree is reused.
    tab.group_checkbox.setChecked(False)
    qtbot.waitUntil(lambda: len(calls.pages) == 2 and not tab._data_loading, timeout=10000)
    tab.group_checkbox.setChecked(True)
    tab.group_checkbox.setChecked(False)
    tab.group_checkbox.setChecked(True)
    qtbot.wait(200)
    assert len(calls.pages) == 2 and len(calls.groups) == groups and len(calls.children) == children
    assert tab.page_label.text().startswith(f"{tab.group_model.group_count()} domains shown")


def test_tags_are_shown_and_selected_url_rows_feed_tagging(qtbot, make_tab, tree_case, monkeypatch):
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, DOMAINS + 1)
    model = tab.group_model
    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED, timeout=10000)

    parent = model.index(0, 0)
    ids = _child_ids(model, 0)
    expected = data.get_tag_strings_for_artifacts(evidence_id, "url", ids)
    shown = {model.data(model.index(r, 7, parent), Qt.UserRole): model.data(model.index(r, 7, parent)) for r in range(len(ids))}
    assert {i: t for i, t in shown.items() if t} == expected and expected

    selection = tab.tree_view.selectionModel()
    flags = QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows
    selection.select(model.index(0, 0), flags)  # group row
    selection.select(model.index(2, 0, parent), flags)
    assert [row["id"] for row in tab.get_selected_urls()] == [ids[2]]


def test_tag_dialog_reload_keeps_expanded_groups(qtbot, make_tab, tree_case, monkeypatch):
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, DOMAINS + 1)
    model = tab.group_model
    tab.tree_view.expand(model.index(3, 0))
    qtbot.waitUntil(lambda: model.group_state(3) == LOADED, timeout=10000)
    url_id = _child_ids(model, 3)[0]
    data.tag_artifact(evidence_id, "Fresh", "url", url_id)

    tab._on_tags_changed()  # TagArtifactsDialog.tags_changed
    qtbot.waitUntil(lambda: len(calls.groups) == 2 and model.group_count() and model.group_state(3) == LOADED, timeout=10000)
    assert tab.tree_view.isExpanded(model.index(3, 0))
    assert "Fresh" in model.data(model.index(0, 7, model.index(3, 0)))
    assert calls.pages == []


def test_collapsed_children_are_bounded(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    monkeypatch.setattr(UrlsGroupedModel, "RETAINED_COLLAPSED_ROWS", 25)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    _wait_groups(qtbot, tab, 5)
    model, view = tab.group_model, tab.tree_view
    for row in range(4):
        view.expand(model.index(row, 0))
        qtbot.waitUntil(lambda r=row: model.group_state(r) == LOADED, timeout=10000)
    for row in range(4):
        view.collapse(model.index(row, 0))
    # 4 x 10 rows collapsed > 25: the least recently collapsed are dropped.
    assert model.retained_collapsed_rows() <= 25
    assert [model.group_state(r) for r in range(4)] == [NOT_LOADED, NOT_LOADED, LOADED, LOADED]
    assert [model.group_url_count(r) > 0 for r in range(4)] == [True] * 4

    view.expand(model.index(0, 0))  # reloads its first page
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED and model.loaded_url_count(0) == 10, timeout=10000)


def test_child_error_shows_retry_row(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    first_domain = _expected_groups(data, evidence_id)[0][0]
    calls = _instrument(monkeypatch, fail_children_once=[first_domain])
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, 5)
    model = tab.group_model
    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: model.group_state(0) == FAILED, timeout=10000)
    parent = model.index(0, 0)
    assert "disk on fire" in _status(model, parent) and "retry" in _status(model, parent)

    _activate_status(tab, parent)
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED, timeout=10000)
    assert model.loaded_url_count(0) == 10 and len(calls.children) == 2


def test_tree_loading_runs_no_gui_thread_queries(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    from app.data._base import BaseDataAccess

    case, data = tree_case
    tab = make_tab(case, data, case.evidence_ids[0])
    counter = {"count": 0}
    main = threading.main_thread()
    original = BaseDataAccess._connect

    def counting(self):
        if threading.current_thread() is main:
            counter["count"] += 1
        return original(self)

    monkeypatch.setattr(BaseDataAccess, "_connect", counting)
    tab.show()
    _wait_groups(qtbot, tab, 5)
    _activate_status(tab)
    _wait_groups(qtbot, tab, 10)
    tab.tree_view.expand(tab.group_model.index(0, 0))
    qtbot.waitUntil(lambda: tab.group_model.group_state(0) == LOADED, timeout=10000)
    _activate_status(tab, tab.group_model.index(0, 0))
    qtbot.waitUntil(lambda: tab.group_model.loaded_url_count(0) == 20, timeout=10000)
    assert counter["count"] == 0


def test_model_api_without_threads(qtbot, tree_case):
    """Synchronous reload (backward compatibility) and outdated-page rejection."""
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    model = UrlsGroupedModel(data)
    QAbstractItemModelTester(model, QAbstractItemModelTester.FailureReportingMode.Fatal)
    model.set_evidence(evidence_id)
    assert _shown_groups(model) == _expected_groups(data, evidence_id)

    assert model.begin_child_load(0)
    query = model.build_child_query(0)
    page = data.load_url_domain_children(query)
    model.set_filters(match_filter="matched", reload=False)
    assert not model.apply_child_page(page)  # built for other filters
    model.set_filters(match_filter="all", reload=False)
    assert model.apply_child_page(page)
    assert not model.apply_child_page(page)  # already applied: cursor moved on
    assert model.loaded_url_count(0) == min(model.group_url_count(0), model.CHILD_PAGE_SIZE)

    flags = [model.flags(model.index(0, c)) for c in range(8)]
    assert not any(f & Qt.ItemIsEditable for f in flags)
    child_flags = [model.flags(model.index(0, c, model.index(0, 0))) for c in range(8)]
    assert [bool(f & Qt.ItemIsEditable) for f in child_flags] == [False] * 7 + [True]

    model.set_evidence(case.evidence_ids[1], reload=False)
    assert model.rowCount() == 0


def test_total_is_counted_after_data_changes_in_grouped_view(qtbot, make_tab, tree_case, small_pages, monkeypatch):
    """The tree's total does not depend on the hidden flat view's data_version baseline."""
    case, data = tree_case
    ev1, ev2 = case.evidence_ids
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, ev1)
    tab.show()

    def total_shown(total):
        return tab.group_model.state() == LOADED and tab.page_label.text() == f"5 domains shown, more available · {total} URLs"

    qtbot.waitUntil(lambda: total_shown(ROWS), timeout=10000)
    tab.refresh()
    qtbot.waitUntil(lambda: total_shown(ROWS), timeout=10000)
    tab.set_evidence(ev2, evidence_label=case.labels[ev2])
    qtbot.waitUntil(lambda: total_shown(ROWS), timeout=10000)

    # Inline tag edit under a Tag filter: the total is recounted.
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    tagged = data.count_urls(ev2, filters=UrlFilterSpec(tag_name="relevant"))
    model = tab.group_model
    qtbot.waitUntil(
        lambda: model.state() == LOADED and tab.page_label.text().endswith(f"· {tagged} URLs"), timeout=10000
    )
    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED, timeout=10000)
    index = model.index(0, 7, model.index(0, 0))
    remaining = ", ".join(t for t in model.data(index).split(", ") if t.lower() != "relevant")
    assert model.setData(index, remaining)
    qtbot.waitUntil(lambda: tab.page_label.text().endswith(f"· {tagged - 1} URLs"), timeout=10000)
    assert calls.pages == []


def test_unnotified_write_seen_by_flat_view_reloads_hidden_tree(qtbot, make_tab, tree_case, monkeypatch):
    """E.g. a tag merged in the Tags tab, noticed by a flat page: the tree is not reused."""
    case, data = tree_case
    evidence_id = case.evidence_ids[0]
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    _wait_groups(qtbot, tab, DOMAINS + 1)
    model = tab.group_model
    tab.tree_view.expand(model.index(0, 0))
    qtbot.waitUntil(lambda: model.group_state(0) == LOADED, timeout=10000)
    url_id = _child_ids(model, 0)[0]

    tab.group_checkbox.setChecked(False)
    qtbot.waitUntil(lambda: len(calls.pages) == 1 and not tab._data_loading, timeout=10000)
    data.tag_artifact(evidence_id, "Elsewhere", "url", url_id)  # another connection, no notification
    tab._recheck_flat_page()
    qtbot.waitUntil(lambda: len(calls.pages) == 2 and not tab._data_loading, timeout=10000)
    assert tab._grouped_stale

    tab.group_checkbox.setChecked(True)
    qtbot.waitUntil(lambda: len(calls.groups) == 2 and model.group_state(0) == LOADED, timeout=10000)
    assert "Elsewhere" in model.data(model.index(0, 7, model.index(0, 0)))


def test_case_switch_drops_tree_and_expansions(qtbot, make_tab, tree_case, tmp_path, monkeypatch):
    case, data = tree_case
    calls = _instrument(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    _wait_groups(qtbot, tab, DOMAINS + 1)
    tab.tree_view.expand(tab.group_model.index(0, 0))
    qtbot.waitUntil(lambda: tab.group_model.group_state(0) == LOADED, timeout=10000)

    other = build_url_fixture_case(tmp_path / "other", rows_per_evidence=40, domain_count=3)
    other_data = CaseDataAccess(other.case_folder, other.case_db_path)
    try:
        assert other.evidence_ids[0] == case.evidence_ids[0]  # same id, other case
        tab.set_case_data(other_data)
        assert tab.group_model.group_count() == 0
        _wait_groups(qtbot, tab, 3)
        qtbot.wait(200)
        assert not any(tab.tree_view.isExpanded(tab.group_model.index(r, 0)) for r in range(3))
        assert len(calls.children) == 1  # only the expansion in the first case
    finally:
        tab.shutdown()
        other_data.close()
