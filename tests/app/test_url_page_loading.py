"""Data-layer tests for background URL page loading (issue #60)."""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from app.data import CaseDataAccess, QueryCancelledError, UrlPageQuery
from tests.fixtures.url_fixtures import build_url_fixture_case


@pytest.fixture
def url_case(tmp_path: Path):
    case = build_url_fixture_case(
        tmp_path / "case",
        rows_per_evidence=300,
        evidence_count=2,
        tag_every=3,
        match_every=4,
        domain_count=7,
    )
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


def test_load_url_page_returns_complete_tag_and_match_maps(url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    page = data.load_url_page(UrlPageQuery(evidence_id=evidence_id, page=0, page_size=100))

    assert page.total_count == 300
    assert len(page.rows) == 100
    row_ids = {row["id"] for row in page.rows}
    # Every returned row has an entry, including untagged / unmatched rows.
    assert set(page.tags) == row_ids
    assert set(page.matches) == row_ids
    assert any(value == "" for value in page.tags.values())
    assert any(value != "" for value in page.tags.values())
    assert any(value == "" for value in page.matches.values())
    assert any(value != "" for value in page.matches.values())


def test_load_url_page_respects_page_and_filters(url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    first = data.load_url_page(UrlPageQuery(evidence_id=evidence_id, page=0, page_size=100))
    second = data.load_url_page(UrlPageQuery(evidence_id=evidence_id, page=1, page_size=100))
    assert second.query.page == 1
    assert not ({r["id"] for r in first.rows} & {r["id"] for r in second.rows})

    matched = data.load_url_page(
        UrlPageQuery(evidence_id=evidence_id, page=0, page_size=500, match_filter="matched")
    )
    assert matched.total_count == 75
    assert all(matched.matches[r["id"]] for r in matched.rows)


def test_load_url_page_does_not_leak_between_evidences(url_case):
    case, data = url_case
    ev1, ev2 = case.evidence_ids
    # Evidence DBs are separate files, so row ids overlap; untag evidence 2 and
    # check that its page never carries evidence 1's tags for the same ids.
    with data._use_evidence_conn(ev2):
        with data._connect() as conn:
            conn.execute("DELETE FROM tag_associations WHERE evidence_id = ?", (ev2,))
    page1 = data.load_url_page(UrlPageQuery(evidence_id=ev1, page=0, page_size=500))
    page2 = data.load_url_page(UrlPageQuery(evidence_id=ev2, page=0, page_size=500))
    assert page1.query.evidence_id == ev1 and page2.query.evidence_id == ev2
    assert any(page1.tags.values())
    assert set(page1.tags) & set(page2.tags)
    assert not any(page2.tags.values())


def test_model_displays_untagged_rows_without_individual_queries(url_case, monkeypatch):
    """100 untagged rows after batch loading -> zero per-row tag/match lookups."""
    from app.features.urls.models import UrlsTableModel

    case, data = url_case
    evidence_id = case.evidence_ids[1]
    # Evidence 2 data with no tags/matches at all for the page we display.
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute("DELETE FROM tag_associations WHERE evidence_id = ?", (evidence_id,))
            conn.execute("DELETE FROM url_matches WHERE evidence_id = ?", (evidence_id,))

    calls = {"tags": 0, "matches": 0}

    def fail_tags(*_args, **_kwargs):
        calls["tags"] += 1
        return ""

    def fail_matches(*_args, **_kwargs):
        calls["matches"] += 1
        return ""

    monkeypatch.setattr(CaseDataAccess, "get_artifact_tags_str", fail_tags)
    monkeypatch.setattr(CaseDataAccess, "get_url_matches", fail_matches)

    page = data.load_url_page(UrlPageQuery(evidence_id=evidence_id, page=0, page_size=100))
    model = UrlsTableModel(data)
    model.set_evidence(evidence_id, reload=False)
    model.set_loaded_page(page)

    assert model.rowCount() == 100
    for row in range(model.rowCount()):
        assert model.data(model.index(row, 8)) == ""
        assert model.data(model.index(row, 9)) == "—"
    assert calls == {"tags": 0, "matches": 0}


def test_cancellable_reads_interrupts_long_running_query(url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    cancel = threading.Event()
    timer = threading.Timer(0.2, cancel.set)
    slow_sql = (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000000) "
        "SELECT COUNT(*) FROM c"
    )
    start = time.perf_counter()
    timer.start()
    with pytest.raises(QueryCancelledError):
        with data.cancellable_reads(cancel.is_set):
            with data._use_evidence_conn(evidence_id):
                with data._connect() as conn:
                    conn.execute(slow_sql).fetchone()
    elapsed = time.perf_counter() - start
    timer.cancel()
    assert elapsed < 5.0

    # Handler is reset: the reused connection runs queries normally afterwards.
    cancel.set()
    page = data.load_url_page(UrlPageQuery(evidence_id=evidence_id, page=0, page_size=10))
    assert len(page.rows) == 10


def test_cancellable_reads_passes_through_non_cancel_errors(url_case):
    import sqlite3

    case, data = url_case
    with pytest.raises(sqlite3.OperationalError):
        with data.cancellable_reads(lambda: False):
            with data._use_evidence_conn(case.evidence_ids[0]):
                with data._connect() as conn:
                    conn.execute("SELECT * FROM table_that_does_not_exist")
