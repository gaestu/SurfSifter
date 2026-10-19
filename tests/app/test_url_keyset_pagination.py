"""Keyset pagination, shared filter predicates, and domain modes (issue #61)."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from app.data import CaseDataAccess, UrlCursor, UrlFilterSpec, UrlPageQuery
from tests.fixtures.url_fixtures import build_url_fixture_case

Row = Tuple[Optional[str], Optional[str]]  # (domain, first_seen_utc)


@pytest.fixture
def empty_case(tmp_path: Path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=0, evidence_count=2)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


def _insert(data: CaseDataAccess, evidence_id: int, rows: Sequence[Row], *, source: str = "src") -> List[int]:
    ids = []
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            for index, (domain, first_seen) in enumerate(rows):
                ids.append(
                    conn.execute(
                        "INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (evidence_id, f"https://{domain or 'none'}/e{evidence_id}/{index}", domain, source, first_seen),
                    ).lastrowid
                )
    return ids


def _expected_order(data: CaseDataAccess, evidence_id: int) -> List[int]:
    """Sort contract computed in Python: timestamp DESC (NULLs last), id ASC."""
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            rows = [tuple(r) for r in conn.execute("SELECT id, first_seen_utc FROM urls WHERE evidence_id = ?", (evidence_id,))]
    by_id = sorted(rows)
    timestamped = sorted((r for r in by_id if r[1] is not None), key=lambda r: r[1], reverse=True)
    return [r[0] for r in timestamped] + [r[0] for r in by_id if r[1] is None]


def _walk(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, page_size: int):
    """Walk all pages forward; return (pages of ids, start cursor per page)."""
    pages: List[List[int]] = []
    starts: List[Optional[UrlCursor]] = []
    query = UrlPageQuery(evidence_id, spec, page=0, page_size=page_size)
    while True:
        page = data.load_url_page(query)
        assert len(page.rows) <= page_size
        pages.append([row["id"] for row in page.rows])
        starts.append(query.after)
        if not page.has_more:
            return pages, starts
        assert len(page.rows) == page_size
        query = UrlPageQuery(evidence_id, spec, page=query.page + 1, page_size=page_size, after=page.next_cursor)


DATASETS: Dict[str, List[Row]] = {
    "all_null": [("a.com", None)] * 23,
    "mixed": [("a.com", None if i % 3 else f"2024-01-{1 + i % 9:02d}T00:00:00Z") for i in range(31)],
    "repeated": [("a.com", "2024-05-05T05:05:05Z" if i % 4 else None) for i in range(26)],
    "empty_string_and_null": [("a.com", ["", None, "2024-01-01", "2023-12-31"][i % 4]) for i in range(17)],
}


@pytest.mark.parametrize("dataset", sorted(DATASETS))
@pytest.mark.parametrize("page_size", [1, 4, 7, 100])
def test_forward_and_backward_walk_returns_every_row_once_in_stable_order(empty_case, dataset, page_size):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    _insert(data, evidence_id, DATASETS[dataset])
    spec = UrlFilterSpec()

    pages, starts = _walk(data, evidence_id, spec, page_size)
    walked = [url_id for page in pages for url_id in page]
    assert walked == _expected_order(data, evidence_id)
    assert len(set(walked)) == len(walked) == data.count_urls(evidence_id, filters=spec)
    assert walked == [r["id"] for r in data.iter_urls(evidence_id, filters=spec, limit=1000)]

    # Backward: re-loading each page from its stored start cursor is identical.
    for index in reversed(range(len(pages))):
        page = data.load_url_page(UrlPageQuery(evidence_id, spec, page=index, page_size=page_size, after=starts[index]))
        assert [row["id"] for row in page.rows] == pages[index]


def test_ids_with_gaps(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    ids = _insert(data, evidence_id, DATASETS["mixed"])
    removed = ids[::3]
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.executemany("DELETE FROM urls WHERE id = ?", [(i,) for i in removed])

    pages, _ = _walk(data, evidence_id, UrlFilterSpec(), 4)
    walked = [url_id for page in pages for url_id in page]
    assert walked == _expected_order(data, evidence_id)
    assert not set(walked) & set(removed)


def test_cursor_from_deleted_row_still_positions_correctly(empty_case):
    """Data changes between requests: a cursor stays a valid sort position."""
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    _insert(data, evidence_id, DATASETS["mixed"])
    first = data.load_url_page(UrlPageQuery(evidence_id, page_size=10))
    cursor = first.next_cursor
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute("DELETE FROM urls WHERE id = ?", (cursor.id,))
    second = data.load_url_page(UrlPageQuery(evidence_id, page=1, page_size=10, after=cursor))
    expected = _expected_order(data, evidence_id)
    assert [r["id"] for r in second.rows] == expected[9:19]


def test_empty_result(empty_case):
    case, data = empty_case
    page = data.load_url_page(UrlPageQuery(case.evidence_ids[0], page_size=10))
    assert page.rows == [] and not page.has_more and page.next_cursor is None
    assert data.count_urls(case.evidence_ids[0], filters=UrlFilterSpec()) == 0


def test_exact_page_size_has_no_next_page(empty_case):
    case, data = empty_case
    _insert(data, case.evidence_ids[0], DATASETS["mixed"][:10])
    page = data.load_url_page(UrlPageQuery(case.evidence_ids[0], page_size=10))
    assert len(page.rows) == 10 and not page.has_more


# ---------------------------------------------------------------------------
# Filters: row and count queries agree, keyset walk matches full listing
# ---------------------------------------------------------------------------


@pytest.fixture
def fixture_case(tmp_path: Path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=400, evidence_count=2,
        timestamp_every=3, tag_every=5, match_every=4, domain_count=7,
    )
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


FILTERS = {
    "none": UrlFilterSpec(),
    "domain_exact": UrlFilterSpec(domain_exact="D3.Example.COM"),
    "domain_contains": UrlFilterSpec(domain_contains="5.EXAMPLE"),
    "source": UrlFilterSpec(discovered_by=("firefox_history",)),
    "tag": UrlFilterSpec(tag_like="Relevant"),
    "matched": UrlFilterSpec(match_filter="matched"),
    "unmatched": UrlFilterSpec(match_filter="unmatched"),
    "list": UrlFilterSpec(match_filter="list1"),
    "url_text": UrlFilterSpec(url_like="%q=1%"),
    "tag_name": UrlFilterSpec(tag_name="relevant"),
    "url_contains": UrlFilterSpec(url_contains="Q=1"),
    "combined": UrlFilterSpec(domain_exact="d2.example.com", match_filter="unmatched", discovered_by=("chromium_cache", "bulk_extractor")),
}


@pytest.mark.parametrize("name", sorted(FILTERS))
def test_filtered_walk_matches_count_and_full_listing(fixture_case, name):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    spec = FILTERS[name]
    pages, _ = _walk(data, evidence_id, spec, 9)
    walked = [url_id for page in pages for url_id in page]
    full = [r["id"] for r in data.iter_urls(evidence_id, filters=spec, limit=10_000)]
    assert walked == full
    assert len(walked) == data.count_urls(evidence_id, filters=spec)
    assert 0 < len(walked) <= 400
    assert list(data.iter_url_rows(evidence_id, spec, batch_size=11)) == data.iter_urls(evidence_id, filters=spec, limit=10_000)


def test_pages_are_isolated_per_evidence(fixture_case):
    case, data = fixture_case
    for evidence_id in case.evidence_ids:
        rows = list(data.iter_url_rows(evidence_id, UrlFilterSpec(), batch_size=50))
        assert len(rows) == 400
        assert all("/path/" in r["url"] for r in rows)
    ev1, ev2 = case.evidence_ids
    _insert(data, ev2, [("only-ev2.com", None)])
    assert data.count_urls(ev1, filters=UrlFilterSpec(domain_exact="only-ev2.com")) == 0
    assert data.count_urls(ev2, filters=UrlFilterSpec(domain_exact="only-ev2.com")) == 1


def test_legacy_like_kwargs_still_supported(fixture_case):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    legacy = data.iter_urls(evidence_id, domain_like="%3.example%", limit=1000)
    assert legacy and all("3.example" in r["domain"] for r in legacy)
    assert data.count_urls(evidence_id, domain_like="%3.example%") == len(legacy)


def test_filter_spec_normalizes_equivalent_filters():
    assert UrlFilterSpec(discovered_by=[], match_filter="all", domain_exact="") == UrlFilterSpec()
    assert hash(UrlFilterSpec(discovered_by=["a"])) == hash(UrlFilterSpec(discovered_by=("a",)))


# ---------------------------------------------------------------------------
# Domain modes
# ---------------------------------------------------------------------------


@pytest.fixture
def domain_case(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    domains = [
        "example.com", "Example.COM", "sub.example.com", "notexample.com",
        "a%b.com", "axb.com", "a_b.com", "a\\b.com", "STRASSE.de", None, "",
    ]
    _insert(data, evidence_id, [(d, None) for d in domains])
    return data, evidence_id


def _domains(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec) -> List[Any]:
    rows = data.iter_urls(evidence_id, filters=spec, limit=100)
    assert len(rows) == data.count_urls(evidence_id, filters=spec)
    return sorted(r["domain"] for r in rows)


@pytest.mark.parametrize(
    "spec, expected",
    [
        # Exact: whole value, ASCII case-insensitive, no subdomains.
        (UrlFilterSpec(domain_exact="example.com"), ["Example.COM", "example.com"]),
        (UrlFilterSpec(domain_exact="EXAMPLE.com"), ["Example.COM", "example.com"]),
        (UrlFilterSpec(domain_exact="strasse.DE"), ["STRASSE.de"]),
        (UrlFilterSpec(domain_exact="a%b.com"), ["a%b.com"]),
        (UrlFilterSpec(domain_exact="a_b.com"), ["a_b.com"]),
        (UrlFilterSpec(domain_exact="missing.com"), []),
        # Contains: substring, ASCII case-insensitive, wildcards are literal.
        (UrlFilterSpec(domain_contains="EXAMPLE.com"), ["Example.COM", "example.com", "notexample.com", "sub.example.com"]),
        (UrlFilterSpec(domain_contains="a%b"), ["a%b.com"]),
        (UrlFilterSpec(domain_contains="%"), ["a%b.com"]),
        (UrlFilterSpec(domain_contains="a_b"), ["a_b.com"]),
        (UrlFilterSpec(domain_contains="_"), ["a_b.com"]),
        (UrlFilterSpec(domain_contains="a\\b"), ["a\\b.com"]),
        (UrlFilterSpec(domain_contains="\\"), ["a\\b.com"]),
        # Exact and contains together must both match.
        (UrlFilterSpec(domain_exact="sub.example.com", domain_contains="sub"), ["sub.example.com"]),
        (UrlFilterSpec(domain_exact="example.com", domain_contains="sub"), []),
    ],
)
def test_domain_mode_semantics(domain_case, spec, expected):
    data, evidence_id = domain_case
    assert _domains(data, evidence_id, spec) == expected


def test_no_domain_filter_includes_null_and_empty_domains(domain_case):
    data, evidence_id = domain_case
    rows = data.iter_urls(evidence_id, filters=UrlFilterSpec(), limit=100)
    assert len(rows) == 11
    assert None in {r["domain"] for r in rows} and "" in {r["domain"] for r in rows}


# ---------------------------------------------------------------------------
# Query plans and work per page
# ---------------------------------------------------------------------------


def _plan(data: CaseDataAccess, evidence_id: int, sql: str, params: Sequence[Any]) -> str:
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            return " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params))


@pytest.mark.parametrize(
    "sql, params, index",
    [
        # Default browsing, timestamped phase with cursor.
        (
            "SELECT u.id FROM urls u WHERE u.evidence_id = ? AND u.first_seen_utc IS NOT NULL "
            "AND u.first_seen_utc <= ? AND (u.first_seen_utc < ? OR u.id > ?) "
            "ORDER BY u.first_seen_utc DESC, u.id ASC LIMIT 501",
            (1, "2024-06-01", "2024-06-01", 10),
            "idx_urls_evidence_first_seen",
        ),
        # Default browsing, NULL bucket with cursor.
        (
            "SELECT u.id FROM urls u WHERE u.evidence_id = ? AND u.first_seen_utc IS NULL AND u.id > ? "
            "ORDER BY u.id ASC LIMIT 501",
            (1, 10),
            "idx_urls_evidence_first_seen",
        ),
        # Exact domain, both phases and count.
        (
            "SELECT u.id FROM urls u WHERE u.evidence_id = ? AND u.domain = ? COLLATE NOCASE "
            "AND u.first_seen_utc IS NOT NULL AND u.first_seen_utc <= ? AND (u.first_seen_utc < ? OR u.id > ?) "
            "ORDER BY u.first_seen_utc DESC, u.id ASC LIMIT 501",
            (1, "d1.example.com", "2024-06-01", "2024-06-01", 10),
            "idx_urls_evidence_domain_nocase_first_seen",
        ),
        (
            "SELECT u.id FROM urls u WHERE u.evidence_id = ? AND u.domain = ? COLLATE NOCASE "
            "AND u.first_seen_utc IS NULL AND u.id > ? ORDER BY u.id ASC LIMIT 501",
            (1, "d1.example.com", 10),
            "idx_urls_evidence_domain_nocase_first_seen",
        ),
        (
            "SELECT COUNT(*) FROM urls u WHERE u.evidence_id = ? AND u.domain = ? COLLATE NOCASE",
            (1, "d1.example.com"),
            "idx_urls_evidence_domain_nocase_first_seen",
        ),
    ],
)
def test_indexed_browsing_paths_seek_without_sorting(fixture_case, sql, params, index):
    case, data = fixture_case
    plan = _plan(data, case.evidence_ids[0], sql, params)
    assert f"INDEX {index}" in plan
    assert "TEMP B-TREE" not in plan


def _vm_work(data: CaseDataAccess, fn) -> int:
    """SQLite work in units of CANCEL_CHECK_INTERVAL_OPS VM instructions."""
    ticks = [0]

    def tick() -> bool:
        ticks[0] += 1
        return False

    with data.cancellable_reads(tick):
        fn()
    return ticks[0]


@pytest.mark.parametrize("spec", [UrlFilterSpec(), UrlFilterSpec(domain_exact="d1.example.com")], ids=["default", "domain"])
def test_deep_cursor_page_costs_no_more_than_first_page(tmp_path, spec):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=30_000, timestamp_every=10, domain_count=3)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        evidence_id = case.evidence_ids[0]
        total = data.count_urls(evidence_id, filters=spec)
        depth = int(total * 0.9)
        # Cursors inside the timestamped range and deep inside the NULL bucket.
        before_deep = data.iter_urls(evidence_id, filters=spec, limit=1, offset=depth - 1)[0]
        before_ts = data.iter_urls(evidence_id, filters=spec, limit=1, offset=total // 20)[0]
        assert before_deep["first_seen_utc"] is None and before_ts["first_seen_utc"] is not None

        first = _vm_work(data, lambda: data.fetch_url_rows_after(evidence_id, spec, None, 501))
        deep = _vm_work(data, lambda: data.fetch_url_rows_after(evidence_id, spec, UrlCursor.from_row(before_deep), 501))
        mid = _vm_work(data, lambda: data.fetch_url_rows_after(evidence_id, spec, UrlCursor.from_row(before_ts), 501))
        offset = _vm_work(data, lambda: data.iter_urls(evidence_id, filters=spec, limit=501, offset=depth))

        assert deep <= first + 2 and mid <= first + 2
        assert offset > 5 * max(deep, 1)
    finally:
        data.close()


def test_page_data_version_reveals_writes_from_other_connections(fixture_case):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    query = UrlPageQuery(evidence_id, page_size=10)
    first = data.load_url_page(query)
    assert first.data_version is not None
    assert data.load_url_page(query).data_version == first.data_version

    other = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        other.tag_artifact(evidence_id, "Elsewhere", "url", first.rows[0]["id"])
    finally:
        other.close()
    assert data.load_url_page(query).data_version != first.data_version


# ---------------------------------------------------------------------------
# Exact tag filter and literal URL search (issue #66)
# ---------------------------------------------------------------------------


def _ids(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec) -> List[int]:
    rows = data.iter_urls(evidence_id, filters=spec, limit=100)
    assert len(rows) == data.count_urls(evidence_id, filters=spec)
    return sorted(r["id"] for r in rows)


TAG_NAMES = ["a_b", "axb", "50%", "50x5", "a\\b", "Mixed Case"]


@pytest.fixture
def tag_case(empty_case):
    """One URL per tag name (plus one untagged URL) in each evidence."""
    case, data = empty_case
    by_evidence = {}
    for evidence_id in case.evidence_ids:
        ids = _insert(data, evidence_id, [("t.com", None)] * (len(TAG_NAMES) + 1))
        for url_id, name in zip(ids, TAG_NAMES):
            data.tag_artifact(evidence_id, name, "url", url_id)
        by_evidence[evidence_id] = dict(zip(TAG_NAMES, ids))
    return case, data, by_evidence


@pytest.mark.parametrize(
    "tag, expected",
    [
        ("a_b", ["a_b"]),
        ("axb", ["axb"]),
        ("50%", ["50%"]),
        ("a\\b", ["a\\b"]),
        ("Mixed Case", ["Mixed Case"]),
        ("mixed case", ["Mixed Case"]),
        ("MIXED CASE", ["Mixed Case"]),
        ("%", []),
        ("_", []),
        ("\\", []),
        ("missing", []),
    ],
)
def test_tag_filter_matches_exact_name(tag_case, tag, expected):
    case, data, by_evidence = tag_case
    for evidence_id in case.evidence_ids:
        names = by_evidence[evidence_id]
        assert _ids(data, evidence_id, UrlFilterSpec(tag_name=tag)) == sorted(names[n] for n in expected)


def test_tag_filter_is_isolated_per_evidence(tag_case):
    case, data, by_evidence = tag_case
    ev1, ev2 = case.evidence_ids
    only_ev2 = _insert(data, ev2, [("t.com", None)])[0]
    data.tag_artifact(ev2, "Only Ev2", "url", only_ev2)
    assert _ids(data, ev1, UrlFilterSpec(tag_name="only ev2")) == []
    assert _ids(data, ev2, UrlFilterSpec(tag_name="only ev2")) == [only_ev2]
    assert _ids(data, ev1, UrlFilterSpec(tag_name="a_b")) == [by_evidence[ev1]["a_b"]]


def test_legacy_tag_like_keeps_wildcards(tag_case):
    case, data, by_evidence = tag_case
    evidence_id = case.evidence_ids[0]
    names = by_evidence[evidence_id]
    expected = sorted([names["a_b"], names["axb"], names["a\\b"]])
    assert _ids(data, evidence_id, UrlFilterSpec(tag_like="a_b")) == expected
    legacy = data.iter_urls(evidence_id, tag_like="a_b", limit=100)
    assert sorted(r["id"] for r in legacy) == expected
    assert data.count_urls(evidence_id, tag_like="a_b") == 3


SEARCH_URLS = {
    "utm": ("x.com", "https://x.com/?utm_source=a"),
    "utmx": ("x.com", "https://x.com/?utmXsource=a"),
    "pct": ("x.com", "https://x.com/50%off"),
    "pctx": ("x.com", "https://x.com/50xoff"),
    "backslash": ("x.com", "https://x.com/a\\b"),
    "upper": ("X.COM", "https://X.COM/UPPER"),
    "plain": ("y.org", "https://y.org/plain"),
}


@pytest.fixture
def search_case(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    ids = {}
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            for key, (domain, url) in SEARCH_URLS.items():
                source = "other" if key == "upper" else "src"
                ids[key] = conn.execute(
                    "INSERT INTO urls(evidence_id, url, domain, discovered_by) VALUES (?, ?, ?, ?)",
                    (evidence_id, url, domain, source),
                ).lastrowid
    return data, evidence_id, ids


@pytest.mark.parametrize(
    "spec, expected",
    [
        (UrlFilterSpec(url_contains="utm_source"), ["utm"]),
        (UrlFilterSpec(url_contains="_"), ["utm"]),
        (UrlFilterSpec(url_contains="50%off"), ["pct"]),
        (UrlFilterSpec(url_contains="%"), ["pct"]),
        (UrlFilterSpec(url_contains="a\\b"), ["backslash"]),
        (UrlFilterSpec(url_contains="\\"), ["backslash"]),
        # ASCII case-insensitive.
        (UrlFilterSpec(url_contains="upper"), ["upper"]),
        (UrlFilterSpec(url_contains="Y.ORG/PLAIN"), ["plain"]),
        (UrlFilterSpec(url_contains="x.com"), ["backslash", "pct", "pctx", "upper", "utm", "utmx"]),
        # Combined with the other filters (all must match).
        (UrlFilterSpec(url_contains="x.com", discovered_by=("src",)), ["backslash", "pct", "pctx", "utm", "utmx"]),
        (UrlFilterSpec(url_contains="x.com", domain_exact="x.com"), ["backslash", "pct", "pctx", "upper", "utm", "utmx"]),
        (UrlFilterSpec(url_contains="50", domain_contains="X.C", match_filter="unmatched"), ["pct", "pctx"]),
        (UrlFilterSpec(url_contains="x.com", domain_exact="y.org"), []),
        (UrlFilterSpec(url_contains="missing"), []),
    ],
)
def test_url_search_is_literal_substring(search_case, spec, expected):
    data, evidence_id, ids = search_case
    assert _ids(data, evidence_id, spec) == sorted(ids[k] for k in expected)


def test_url_search_combines_with_tag_filter(search_case):
    data, evidence_id, ids = search_case
    data.tag_artifact(evidence_id, "Hit", "url", ids["utm"])
    data.tag_artifact(evidence_id, "Hit", "url", ids["plain"])
    spec = UrlFilterSpec(url_contains="_", tag_name="HIT")
    assert _ids(data, evidence_id, spec) == [ids["utm"]]


def test_legacy_url_like_keeps_wildcards(search_case):
    data, evidence_id, ids = search_case
    expected = sorted([ids["utm"], ids["utmx"]])
    assert _ids(data, evidence_id, UrlFilterSpec(url_like="%utm_source%")) == expected
    legacy = data.iter_urls(evidence_id, url_like="%utm_source%", limit=100)
    assert sorted(r["id"] for r in legacy) == expected
    assert data.count_urls(evidence_id, url_like="%utm_source%") == 2


def test_ui_filters_use_literal_url_search_and_exact_tag():
    spec = UrlFilterSpec().with_ui_filters(text="a_b%", tag="Some_Tag")
    assert spec.url_contains == "a_b%" and spec.tag_name == "Some_Tag"
    assert spec.url_like == "%" and spec.tag_like == "%"
    assert spec.with_ui_filters(text="", tag="*") == UrlFilterSpec()
    assert UrlFilterSpec(url_contains="", tag_name="") == UrlFilterSpec()


# ---------------------------------------------------------------------------
# Inline tag editing: update_url_tags (issue #66)
# ---------------------------------------------------------------------------


def _url_tag_names(data: CaseDataAccess, evidence_id: int, url_id: int) -> List[str]:
    return sorted(t["name"] for t in data.get_artifact_tags(evidence_id, "url", url_id))


@pytest.mark.parametrize(
    "initial, edited, expected",
    [
        # Changing only the letter case keeps the tag (no silent removal).
        (["Suspicious"], "suspicious", ["Suspicious"]),
        (["Suspicious", "Other"], "SUSPICIOUS, other, New", ["New", "Other", "Suspicious"]),
        # Removing a name untags it; the rest is unchanged.
        (["A", "B"], "a", ["A"]),
        (["A"], "", []),
        # A tag name containing a comma survives an edit of the cell text.
        (["Case 12, Suspect A"], "Case 12, Suspect A, reviewed", ["Case 12, Suspect A", "reviewed"]),
        (["Case 12, Suspect A", "B"], "b, case 12,suspect a", ["B", "Case 12, Suspect A"]),
        # ... and is removed (not split into new tags) when deleted.
        (["Case 12, Suspect A", "B"], "B", ["B"]),
    ],
)
def test_update_url_tags_preserves_tags(empty_case, initial, edited, expected):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    url_id = _insert(data, evidence_id, [("t.com", None)])[0]
    for name in initial:
        data.tag_artifact(evidence_id, name, "url", url_id)
    data.update_url_tags(evidence_id, url_id, edited)
    assert _url_tag_names(data, evidence_id, url_id) == expected
