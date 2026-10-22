"""Request counts of the shared case statistics cache (issue #63).

Counts the SQL statements the cached readers actually execute (via
``sqlite3.Connection.set_trace_callback``) across several ``CaseDataAccess``
instances of one case: each aggregate runs once, and exactly once again after
the relevant invalidation.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from app.data import STATS_TAGS, STATS_URLS, CaseDataAccess, discard_case_stats_cache
from tests.fixtures.url_fixtures import build_url_fixture_case

URLS_COUNT_SQL = "SELECT COUNT(*) FROM urls WHERE evidence_id"
TAGS_COUNT_SQL = "SELECT COUNT(*) FROM tags WHERE evidence_id"
SOURCES_SQL = "SELECT DISTINCT discovered_by"


@pytest.fixture
def traced_case(tmp_path: Path, monkeypatch):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=60, evidence_count=2, tag_every=3, domain_count=4
    )
    discard_case_stats_cache(case.case_db_path)
    statements: List[str] = []
    original = CaseDataAccess._connect

    def tracing_connect(self):
        conn = original(self)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(CaseDataAccess, "_connect", tracing_connect)
    datas = [CaseDataAccess(case.case_folder, case.case_db_path) for _ in range(3)]
    yield case, datas, statements
    for data in datas:
        data.close()
    discard_case_stats_cache(case.case_db_path)


def _count(statements: List[str], needle: str) -> int:
    return sum(1 for sql in statements if needle in sql)


def test_overview_sql_runs_once_across_instances(traced_case):
    case, (first, second, third), statements = traced_case
    evidence_id = case.evidence_ids[0]

    expected = first.get_overview_stats(evidence_id)
    assert expected.counts.urls == 60
    assert _count(statements, URLS_COUNT_SQL) == 1
    assert _count(statements, TAGS_COUNT_SQL) == 1

    statements.clear()
    for data in (second, third, first):
        assert data.get_overview_stats(evidence_id) == expected
        assert data.peek_overview_stats(evidence_id) == expected
    assert statements == []


def test_overview_reloads_exactly_once_per_relevant_invalidation(traced_case):
    case, (first, second, third), statements = traced_case
    evidence_id = case.evidence_ids[0]
    first.get_overview_stats(evidence_id)

    # URL change: counts are read again once, tag statistics stay cached.
    statements.clear()
    third.invalidate_stats(evidence_id, STATS_URLS)
    for data in (first, second, third):
        data.get_overview_stats(evidence_id)
    assert _count(statements, URLS_COUNT_SQL) == 1
    assert _count(statements, TAGS_COUNT_SQL) == 0

    # Tag write through another instance: only the tag statistics are read again.
    second.tag_artifact(evidence_id, "Fresh", "url", 1)
    statements.clear()
    stats = [data.get_overview_stats(evidence_id) for data in (first, second, third)]
    assert _count(statements, URLS_COUNT_SQL) == 0
    assert _count(statements, TAGS_COUNT_SQL) == 1
    assert stats[0] == stats[1] == stats[2]
    assert stats[0].tag_count == 3  # Relevant, Review, Fresh

    # Changes of another evidence do not touch this one.
    statements.clear()
    first.invalidate_stats(case.evidence_ids[1])
    second.get_overview_stats(evidence_id)
    assert statements == []


def test_no_caching_while_writes_are_open_and_end_bumps(traced_case):
    case, (first, second, _third), statements = traced_case
    ev1, ev2 = case.evidence_ids
    first.get_overview_stats(ev1)
    first.get_overview_stats(ev2)

    scope = second.stats_write_scope(ev1)
    statements.clear()
    first.get_overview_stats(ev1)
    second.get_overview_stats(ev1)
    assert _count(statements, URLS_COUNT_SQL) == 2  # read every time, never cached
    assert first.peek_overview_stats(ev1) is None
    # The other evidence is still served from the cache.
    statements.clear()
    assert first.get_overview_stats(ev2).counts.urls == 60
    assert statements == []

    scope.close()
    statements.clear()
    first.get_overview_stats(ev1)
    second.get_overview_stats(ev1)
    assert _count(statements, URLS_COUNT_SQL) == 1
    assert _count(statements, TAGS_COUNT_SQL) == 1


def test_end_writes_invalidates_values_cached_before_begin(traced_case):
    case, (first, second, _third), statements = traced_case
    evidence_id = case.evidence_ids[0]
    first.list_url_sources(evidence_id)
    first.list_tags(evidence_id)
    with second.stats_write_scope(evidence_id):
        pass
    statements.clear()
    first.list_url_sources(evidence_id)
    assert _count(statements, SOURCES_SQL) == 1
    assert first.peek_overview_stats(evidence_id) is None


@pytest.mark.parametrize("fail", [False, True], ids=["success", "helper_raises"])
def test_deduplication_invalidates_shared_stats_in_every_outcome(traced_case, monkeypatch, fail):
    """Regression: a failing dedup has already committed process_log rows (and
    possibly partial changes), so it must invalidate like a successful one."""
    import core.database.helpers as helpers

    case, (first, second, _third), statements = traced_case
    evidence_id = case.evidence_ids[0]
    sources = first.list_url_sources(evidence_id)
    first.get_overview_stats(evidence_id)

    if fail:
        def boom(*_args, **_kwargs):
            raise RuntimeError("dedup failed")

        monkeypatch.setattr(helpers, "deduplicate_urls", boom)
        with pytest.raises(RuntimeError, match="dedup failed"):
            second.deduplicate_urls(evidence_id, sources)
    else:
        second.deduplicate_urls(evidence_id, sources)

    statements.clear()
    assert first.list_url_sources(evidence_id) == sources
    assert _count(statements, SOURCES_SQL) == 1
    stats = first.get_overview_stats(evidence_id)
    assert _count(statements, URLS_COUNT_SQL) == 1
    # The dedup run is logged in process_log: the Overview's last run reflects it.
    assert stats.counts.last_run_utc is not None


def test_tag_write_does_not_reload_url_lists(traced_case):
    case, (first, second, _third), statements = traced_case
    evidence_id = case.evidence_ids[0]
    first.list_url_sources(evidence_id)
    first.list_tags(evidence_id)
    second.tag_artifact(evidence_id, "Another", "url", 2)
    statements.clear()
    first.list_url_sources(evidence_id)
    assert statements == []
    assert "Another" in {t["name"] for t in first.list_tags(evidence_id)}
    second.invalidate_stats(evidence_id, STATS_TAGS)
    statements.clear()
    first.list_url_sources(evidence_id)
    assert statements == []
