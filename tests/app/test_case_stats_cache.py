"""Tests for the shared case statistics cache (issue #63)."""
from __future__ import annotations

import threading
from pathlib import Path

import pytest

from app.data import (
    ALL_STATS_CATEGORIES,
    OVERVIEW_STATS_CATEGORIES,
    STATS_IMAGES,
    STATS_TAGS,
    STATS_URL_MATCHES,
    STATS_URLS,
    CaseDataAccess,
    CaseStatsCache,
    OverviewStats,
    QueryCancelledError,
    StatsWriteScope,
    case_stats_cache,
    discard_case_stats_cache,
)
from tests.fixtures.url_fixtures import build_url_fixture_case


# ---------------------------------------------------------------------------
# CaseStatsCache semantics
# ---------------------------------------------------------------------------


def test_put_get_roundtrip_returns_copies():
    cache = CaseStatsCache()
    token = cache.token(1, {STATS_URLS})
    value = {"items": [{"domain": "a.com", "count": 1}]}
    assert cache.put(1, "k", {STATS_URLS}, token, value)

    value["items"].append("mutated after put")
    hit, got = cache.get(1, "k")
    assert hit and got == {"items": [{"domain": "a.com", "count": 1}]}
    got["items"].clear()
    hit, again = cache.get(1, "k")
    assert hit and again == {"items": [{"domain": "a.com", "count": 1}]}


def test_miss_for_unknown_key():
    assert CaseStatsCache().get(1, "nothing") == (False, None)


def test_put_rejected_when_invalidated_after_token():
    cache = CaseStatsCache()
    token = cache.token(1, {STATS_URLS, STATS_TAGS})
    cache.invalidate(1, {STATS_TAGS})
    assert not cache.is_current(1, {STATS_URLS, STATS_TAGS}, token)
    assert not cache.put(1, "k", {STATS_URLS, STATS_TAGS}, token, 1)
    assert cache.get(1, "k") == (False, None)


def test_invalidate_only_affects_matching_categories_and_evidence():
    cache = CaseStatsCache()
    for evidence_id in (1, 2):
        for key, cats in (("u", {STATS_URLS}), ("t", {STATS_TAGS})):
            assert cache.put(evidence_id, key, cats, cache.token(evidence_id, cats), key)

    cache.invalidate(1, STATS_TAGS)
    assert cache.get(1, "u") == (True, "u")
    assert cache.get(1, "t") == (False, None)
    assert cache.get(2, "t") == (True, "t")

    cache.invalidate(2)  # all categories of evidence 2
    assert cache.get(2, "u") == (False, None)
    assert cache.get(1, "u") == (True, "u")


def test_invalidate_all_evidences_includes_unseen_ones():
    cache = CaseStatsCache()
    old_token = cache.token(99, {STATS_URLS})  # evidence never stored anything
    assert cache.put(1, "u", {STATS_URLS}, cache.token(1, {STATS_URLS}), 1)
    cache.invalidate(None, {STATS_URLS})
    assert cache.get(1, "u") == (False, None)
    assert not cache.is_current(99, {STATS_URLS}, old_token)
    assert not cache.put(99, "u", {STATS_URLS}, old_token, 1)


def test_revisions_never_return_to_old_tokens():
    cache = CaseStatsCache()
    token = cache.token(1, ALL_STATS_CATEGORIES)
    cache.drop_evidence(1)
    cache.clear()
    assert not cache.is_current(1, ALL_STATS_CATEGORIES, token)


def test_token_is_canonical_for_category_order():
    cache = CaseStatsCache()
    assert cache.token(1, [STATS_URLS, STATS_TAGS]) == cache.token(1, (STATS_TAGS, STATS_URLS))
    assert cache.token(1, STATS_URLS) == cache.token(1, {STATS_URLS})


def test_unknown_category_rejected():
    with pytest.raises(ValueError):
        CaseStatsCache().token(1, {"bogus"})


def test_tokens_of_different_instances_never_match():
    first, second = CaseStatsCache(), CaseStatsCache()
    assert first.token(1, {STATS_URLS}) != second.token(1, {STATS_URLS})
    assert not second.put(1, "k", {STATS_URLS}, first.token(1, {STATS_URLS}), 1)


def test_open_writes_prevent_caching_and_end_bumps():
    cache = CaseStatsCache()
    before = cache.token(1, {STATS_URLS})
    assert cache.put(1, "old", {STATS_URLS}, before, "old")

    cache.begin_writes(1)
    assert cache.writes_open(1)
    # Tokens taken before the writes started are no longer current.
    assert not cache.is_current(1, {STATS_URLS}, before)
    assert cache.get(1, "old") == (False, None)

    during = cache.token(1, {STATS_URLS})
    assert cache.is_current(1, {STATS_URLS}, during)  # may be displayed ...
    assert not cache.put(1, "k", {STATS_URLS}, during, 1)  # ... but not cached
    # Other evidences are unaffected.
    assert cache.put(2, "k", {STATS_URLS}, cache.token(2, {STATS_URLS}), 2)

    cache.begin_writes(1)  # nested
    cache.end_writes(1)
    assert cache.writes_open(1)
    assert not cache.is_current(1, {STATS_URLS}, during)  # every end bumps
    cache.end_writes(1)
    assert not cache.writes_open(1)

    after = cache.token(1, {STATS_URLS})
    assert cache.put(1, "k", {STATS_URLS}, after, 1)
    assert cache.get(1, "k") == (True, 1)


def test_end_writes_without_begin_is_harmless_and_bumps():
    cache = CaseStatsCache()
    token = cache.token(1, {STATS_IMAGES})
    cache.end_writes(1)
    assert not cache.writes_open(1)
    assert not cache.is_current(1, {STATS_IMAGES}, token)
    token = cache.token(1, {STATS_IMAGES})
    assert cache.put(1, "k", {STATS_IMAGES}, token, 1)


def test_drop_evidence_resets_open_writes_and_entries():
    cache = CaseStatsCache()
    assert cache.put(1, "k", {STATS_URLS}, cache.token(1, {STATS_URLS}), 1)
    cache.begin_writes(1)
    cache.drop_evidence(1)
    assert not cache.writes_open(1)
    assert cache.get(1, "k") == (False, None)
    assert cache.put(1, "k", {STATS_URLS}, cache.token(1, {STATS_URLS}), 2)


def test_clear_keeps_open_writes():
    cache = CaseStatsCache()
    cache.begin_writes(1)
    cache.clear()
    assert cache.writes_open(1)
    assert not cache.put(1, "k", {STATS_URLS}, cache.token(1, {STATS_URLS}), 1)


def test_concurrent_puts_and_invalidations_are_safe():
    cache = CaseStatsCache()
    errors = []

    def writer():
        try:
            for _ in range(500):
                cache.invalidate(1, {STATS_URLS})
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def reader():
        try:
            for i in range(500):
                token = cache.token(1, {STATS_URLS})
                cache.put(1, i % 5, {STATS_URLS}, token, [i])
                hit, value = cache.get(1, i % 5)
                if hit:
                    assert isinstance(value, list)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


# ---------------------------------------------------------------------------
# StatsWriteScope
# ---------------------------------------------------------------------------


class _CountingCache(CaseStatsCache):
    def __init__(self) -> None:
        super().__init__()
        self.begins = []
        self.ends = []

    def begin_writes(self, evidence_id: int) -> None:
        self.begins.append(evidence_id)
        super().begin_writes(evidence_id)

    def end_writes(self, evidence_id: int) -> None:
        self.ends.append(evidence_id)
        super().end_writes(evidence_id)


def test_write_scope_close_is_idempotent_and_bumps():
    cache = _CountingCache()
    token = cache.token(1, {STATS_URLS})
    assert cache.put(1, "k", {STATS_URLS}, token, 1)

    scope = cache.write_scope(1)
    assert isinstance(scope, StatsWriteScope)
    assert scope.evidence_id == 1 and not scope.closed
    assert cache.begins == [1] and cache.writes_open(1)
    assert cache.get(1, "k") == (False, None)  # writes open: earlier value gone

    scope.close()
    scope.close("signal", "args")  # extra signal arguments are ignored
    assert scope.closed
    assert cache.ends == [1]
    assert not cache.writes_open(1)
    assert not cache.is_current(1, {STATS_URLS}, token)


def test_write_scope_close_from_other_threads_ends_exactly_once():
    cache = _CountingCache()
    scope = cache.write_scope(7)
    start = threading.Barrier(8)

    def close():
        start.wait()
        scope.close()

    threads = [threading.Thread(target=close) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert cache.ends == [7]
    assert scope.closed and not cache.writes_open(7)


def test_write_scope_context_manager_closes_on_exit_and_on_error():
    cache = _CountingCache()
    with cache.write_scope(1) as scope:
        assert cache.writes_open(1) and not scope.closed
    assert scope.closed and not cache.writes_open(1)

    with pytest.raises(RuntimeError):
        with cache.write_scope(2) as failing:
            raise RuntimeError("write failed")
    assert failing.closed and not cache.writes_open(2)
    assert cache.ends == [1, 2]


def test_write_scopes_nest():
    cache = CaseStatsCache()
    outer = cache.write_scope(1)
    inner = cache.write_scope(1)
    other = cache.write_scope(2)
    inner.close()
    inner.close()  # a double close must not end the outer scope
    assert cache.writes_open(1)
    assert not cache.put(1, "k", {STATS_URLS}, cache.token(1, {STATS_URLS}), 1)
    outer.close()
    assert not cache.writes_open(1)
    assert cache.writes_open(2)  # scopes of other evidences are independent
    assert cache.put(1, "k", {STATS_URLS}, cache.token(1, {STATS_URLS}), 1)
    other.close()
    assert not cache.writes_open(2)


def test_write_scope_on_retired_cache_is_harmless(tmp_path: Path):
    db = tmp_path / "case.sqlite"
    old = case_stats_cache(db)
    scope = old.write_scope(1)
    discard_case_stats_cache(db)
    new = case_stats_cache(db)

    scope.close()  # ends on the cache it began on (now retired)
    assert scope.closed
    assert not old.writes_open(1)
    assert not new.writes_open(1)  # never opened on the new cache
    token = new.token(1, {STATS_URLS})
    assert new.put(1, "k", {STATS_URLS}, token, 1)
    assert new.is_current(1, {STATS_URLS}, token)  # the old scope did not bump the new cache
    assert not old.put(1, "k", {STATS_URLS}, old.token(1, {STATS_URLS}), 1)
    discard_case_stats_cache(db)


def test_data_access_write_scope_uses_the_scope_cache_after_discard(tmp_path: Path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=5, evidence_count=1)
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        evidence_id = case.evidence_ids[0]
        first_cache = data.stats_cache
        scope = data.stats_write_scope(evidence_id)
        assert first_cache.writes_open(evidence_id)
        discard_case_stats_cache(case.case_db_path)
        assert data.stats_cache is not first_cache
        assert not data.stats_cache.writes_open(evidence_id)
        scope.close()
        assert not first_cache.writes_open(evidence_id)
        assert not data.stats_cache.writes_open(evidence_id)
    finally:
        data.close()
        discard_case_stats_cache(case.case_db_path)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_shares_per_resolved_path_and_isolates_cases(tmp_path: Path):
    db_a = tmp_path / "a" / "case.sqlite"
    db_b = tmp_path / "b" / "case.sqlite"
    first = case_stats_cache(db_a)
    assert case_stats_cache(tmp_path / "a" / ".." / "a" / "case.sqlite") is first
    assert case_stats_cache(db_b) is not first


def test_discard_retires_old_instance(tmp_path: Path):
    db = tmp_path / "case.sqlite"
    old = case_stats_cache(db)
    token = old.token(1, {STATS_URLS})
    assert old.put(1, "k", {STATS_URLS}, token, 1)

    discard_case_stats_cache(db)
    new = case_stats_cache(db)
    assert new is not old
    assert old.get(1, "k") == (False, None)
    assert not old.put(1, "k", {STATS_URLS}, old.token(1, {STATS_URLS}), 1)
    assert not new.is_current(1, {STATS_URLS}, token)
    discard_case_stats_cache(tmp_path / "never_registered.sqlite")  # no error


# ---------------------------------------------------------------------------
# Data layer on the shared cache
# ---------------------------------------------------------------------------


@pytest.fixture
def two_case_data(tmp_path: Path, monkeypatch):
    case = build_url_fixture_case(
        tmp_path / "case",
        rows_per_evidence=120,
        evidence_count=2,
        tag_every=3,
        match_every=4,
        domain_count=7,
    )
    discard_case_stats_cache(case.case_db_path)  # start cold
    connects = {"count": 0}
    original = CaseDataAccess._connect

    def counting_connect(self):
        connects["count"] += 1
        return original(self)

    monkeypatch.setattr(CaseDataAccess, "_connect", counting_connect)
    first = CaseDataAccess(case.case_folder, case.case_db_path)
    second = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, first, second, connects
    first.close()
    second.close()
    discard_case_stats_cache(case.case_db_path)


READERS = [
    ("get_evidence_counts", ()),
    ("get_tag_statistics", ()),
    ("get_top_domains", ()),
    ("list_url_domains", ()),
    ("list_url_domains", (3,)),
    ("list_url_sources", ()),
    ("list_url_match_lists", ()),
    ("list_tags", ()),
    ("get_overview_stats", ()),
]


@pytest.mark.parametrize("name,args", READERS)
def test_readers_hit_shared_cache_across_instances(two_case_data, name, args):
    case, first, second, connects = two_case_data
    evidence_id = case.evidence_ids[0]
    expected = getattr(first, name)(evidence_id, *args)
    assert connects["count"] > 0

    connects["count"] = 0
    assert getattr(second, name)(evidence_id, *args) == expected
    assert connects["count"] == 0
    # use_cache=False reads again with the same result.
    assert getattr(second, name)(evidence_id, *args, use_cache=False) == expected
    assert connects["count"] > 0


def test_cached_results_match_fresh_reads(two_case_data):
    case, first, _second, _connects = two_case_data
    evidence_id = case.evidence_ids[0]
    counts = first.get_evidence_counts(evidence_id)
    assert counts.urls == 120
    assert first.get_evidence_counts(evidence_id, use_cache=False) == counts
    assert first.get_top_domains(evidence_id, limit=3)["truncated"] is True
    assert first.get_top_domains(evidence_id)["total_count"] == 7
    assert len(first.list_url_domains(evidence_id, 3)) == 3
    assert len(first.list_url_domains(evidence_id)) == 7


def test_evidences_are_isolated(two_case_data):
    case, first, second, connects = two_case_data
    ev1, ev2 = case.evidence_ids
    first.list_url_sources(ev1)
    connects["count"] = 0
    second.list_url_sources(ev2)
    assert connects["count"] == 1

    first.invalidate_filter_cache(ev2)
    connects["count"] = 0
    second.list_url_sources(ev1)
    assert connects["count"] == 0


def test_cases_are_isolated(tmp_path: Path):
    cases = [
        build_url_fixture_case(tmp_path / name, rows_per_evidence=rows, domain_count=3)
        for name, rows in (("one", 10), ("two", 20))
    ]
    datas = [CaseDataAccess(c.case_folder, c.case_db_path) for c in cases]
    try:
        assert datas[0].stats_cache is not datas[1].stats_cache
        assert datas[0].get_evidence_counts(cases[0].evidence_ids[0]).urls == 10
        assert datas[1].get_evidence_counts(cases[1].evidence_ids[0]).urls == 20
    finally:
        for data in datas:
            data.close()
        for case in cases:
            discard_case_stats_cache(case.case_db_path)


def test_invalidate_filter_cache_clears_url_domain_entries(two_case_data):
    case, first, second, connects = two_case_data
    evidence_id = case.evidence_ids[0]
    first.list_url_domains(evidence_id, 3)
    first.list_url_domains(evidence_id)
    second.invalidate_filter_cache(evidence_id)
    connects["count"] = 0
    first.list_url_domains(evidence_id, 3)
    first.list_url_domains(evidence_id)
    assert connects["count"] == 2

    second.invalidate_filter_cache()  # all evidences
    connects["count"] = 0
    first.list_url_domains(evidence_id)
    assert connects["count"] == 1


def test_tag_write_invalidates_tag_readers_only(two_case_data):
    case, first, second, connects = two_case_data
    evidence_id = case.evidence_ids[0]
    tags_before = first.list_tags(evidence_id)
    stats_before = first.get_tag_statistics(evidence_id)
    sources = first.list_url_sources(evidence_id)

    second.tag_artifact(evidence_id, "NewTag", "url", 1)

    tags_after = first.list_tags(evidence_id)
    assert {t["name"] for t in tags_after} == {t["name"] for t in tags_before} | {"NewTag"}
    stats_after = first.get_tag_statistics(evidence_id)
    assert stats_after["tag_count"] == stats_before["tag_count"] + 1
    assert stats_after["tagged_artifact_count"] == stats_before["tagged_artifact_count"] + 1
    connects["count"] = 0
    assert first.list_url_sources(evidence_id) == sources
    assert connects["count"] == 0


def test_stale_compute_is_not_stored(two_case_data, monkeypatch):
    """A value read while the data changed is returned but not cached."""
    case, first, second, connects = two_case_data
    evidence_id = case.evidence_ids[0]
    original = CaseDataAccess._read_evidence_counts

    def read_then_invalidate(self, eid):
        result = original(self, eid)
        second.invalidate_stats(eid, {STATS_IMAGES})
        return result

    monkeypatch.setattr(CaseDataAccess, "_read_evidence_counts", read_then_invalidate)
    counts = first.get_evidence_counts(evidence_id)
    monkeypatch.setattr(CaseDataAccess, "_read_evidence_counts", original)
    assert counts.urls == 120
    assert first.stats_cache.get(evidence_id, ("evidence_counts",)) == (False, None)


def test_cancelled_read_is_not_cached(two_case_data, monkeypatch):
    case, first, _second, _connects = two_case_data
    evidence_id = case.evidence_ids[0]
    monkeypatch.setattr("app.data._base.CANCEL_CHECK_INTERVAL_OPS", 1)
    with pytest.raises(QueryCancelledError):
        with first.cancellable_reads(lambda: True):
            first.list_url_match_lists(evidence_id)
    assert first.stats_cache.get(evidence_id, ("url_match_lists",)) == (False, None)
    assert first.list_url_match_lists(evidence_id) == ["list0", "list1", "list2"]


def test_failed_compute_stores_nothing(two_case_data):
    case, first, _second, _connects = two_case_data
    evidence_id = case.evidence_ids[0]

    def boom():
        raise QueryCancelledError("interrupted")

    with pytest.raises(QueryCancelledError):
        first._cached_stat(evidence_id, ("x",), {STATS_URLS}, boom)
    assert first.stats_cache.get(evidence_id, ("x",)) == (False, None)


def test_missing_evidence_db_is_not_cached(two_case_data):
    case, first, _second, _connects = two_case_data
    data = first
    assert data.get_top_domains(999) == {"items": [], "total_count": 0, "truncated": False}
    assert data.list_url_sources(999) == []
    assert data.stats_cache.entry_count() == 0


def test_overview_stats_peek_and_token(two_case_data):
    case, first, second, connects = two_case_data
    evidence_id = case.evidence_ids[0]
    assert first.peek_overview_stats(evidence_id) is None

    stats, token = second.load_overview_stats(evidence_id)
    assert isinstance(stats, OverviewStats)
    assert stats.counts.urls == 120 and stats.tag_count == 2
    assert first.is_overview_stats_current(evidence_id, token)

    connects["count"] = 0
    assert first.peek_overview_stats(evidence_id) == stats
    assert connects["count"] == 0

    first.invalidate_stats(evidence_id, {STATS_URL_MATCHES})  # not an overview category
    assert first.is_overview_stats_current(evidence_id, token)
    scope = first.stats_write_scope(evidence_id)
    assert not first.is_overview_stats_current(evidence_id, token)
    assert first.peek_overview_stats(evidence_id) is None
    scope.close()
    assert first.peek_overview_stats(evidence_id) is None
    assert first.overview_stats_token(evidence_id) != token
    assert STATS_TAGS in OVERVIEW_STATS_CATEGORIES


def test_drop_evidence_stats(two_case_data):
    case, first, second, _connects = two_case_data
    evidence_id = case.evidence_ids[0]
    first.get_overview_stats(evidence_id)
    second.drop_evidence_stats(evidence_id)
    assert first.peek_overview_stats(evidence_id) is None
