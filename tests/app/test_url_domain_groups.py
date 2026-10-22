"""Grouped URL view data layer: paged domain groups and their URL children (issue #64)."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pytest

from app.data import (
    CaseDataAccess,
    UrlCursor,
    UrlDomainChildQuery,
    UrlDomainGroup,
    UrlDomainGroupQuery,
    UrlFilterSpec,
)
from tests.fixtures.url_fixtures import build_url_fixture_case

Row = Tuple[Optional[str], Optional[str]]  # (domain, first_seen_utc)


@pytest.fixture
def empty_case(tmp_path: Path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=0, evidence_count=2)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


@pytest.fixture
def fixture_case(tmp_path: Path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=600, evidence_count=2,
        timestamp_every=3, tag_every=5, match_every=4, domain_count=23,
    )
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    evidence_id = case.evidence_ids[0]
    # Mixed-case spellings and URLs without a domain (NULL and '').
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute("UPDATE urls SET domain = upper(domain) WHERE id % 7 = 0")
            conn.execute("UPDATE urls SET domain = NULL WHERE id % 11 = 0")
            conn.execute("UPDATE urls SET domain = '' WHERE id % 13 = 0")
    yield case, data
    data.close()


def _insert(data: CaseDataAccess, evidence_id: int, rows: Sequence[Row]) -> List[int]:
    ids = []
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            for index, (domain, first_seen) in enumerate(rows):
                ids.append(
                    conn.execute(
                        "INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc) "
                        "VALUES (?, ?, ?, 'src', ?)",
                        (evidence_id, f"https://{domain or 'none'}/{index}", domain, first_seen),
                    ).lastrowid
                )
    return ids


def _all_groups(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, page_size: int) -> List[UrlDomainGroup]:
    groups: List[UrlDomainGroup] = []
    after = None
    while True:
        page = data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, spec, after=after, page_size=page_size))
        assert len([g for g in page.groups if g.domain is not None]) <= page_size
        groups.extend(page.groups)
        if not page.has_more:
            return groups
        assert page.groups and page.groups[-1].domain is not None
        after = page.next_after


def _all_children(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, domain: Optional[str], page_size: int) -> List[int]:
    ids: List[int] = []
    after: Optional[UrlCursor] = None
    while True:
        page = data.load_url_domain_children(
            UrlDomainChildQuery(evidence_id, domain, spec, after=after, page_size=page_size)
        )
        assert len(page.rows) <= page_size
        ids.extend(row["id"] for row in page.rows)
        if not page.has_more:
            return ids
        assert len(page.rows) == page_size
        after = page.next_cursor


def _assert_tree_matches_flat(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, group_page: int, child_page: int):
    """Every matching URL is in exactly one group, in flat order; counts are exact."""
    groups = _all_groups(data, evidence_id, spec, group_page)
    flat = [row["id"] for row in data.iter_url_rows(evidence_id, spec)]
    position = {url_id: index for index, url_id in enumerate(flat)}
    seen: List[int] = []
    for group in groups:
        children = _all_children(data, evidence_id, spec, group.domain, child_page)
        assert len(children) == group.count > 0
        assert children == sorted(children, key=position.__getitem__)  # flat order
        seen.extend(children)
    assert sorted(seen) == sorted(flat)
    assert sum(g.count for g in groups) == len(flat) == data.count_urls(evidence_id, filters=spec)
    return groups


def test_groups_are_case_insensitive_alphabetical_with_no_domain_last(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    _insert(data, evidence_id, [
        ("b.com", None), ("A.com", None), ("a.COM", None), ("a.com", None),
        (None, None), ("", "2024-01-01"), ("_x.com", None), ("Z.com", None), ("c.com", None),
    ])
    groups = _all_groups(data, evidence_id, UrlFilterSpec(), 2)
    # NOCASE compares ASCII letters lower-cased: '_' (0x5F) < 'a' (0x61),
    # although '_' > 'A' (0x41) in binary order.
    assert groups == [
        UrlDomainGroup("_x.com", 1),
        UrlDomainGroup("A.com", 3),  # smallest spelling (MIN) labels the group
        UrlDomainGroup("b.com", 1),
        UrlDomainGroup("c.com", 1),
        UrlDomainGroup("Z.com", 1),
        UrlDomainGroup(None, 2),  # NULL and '' together, last
    ]


def test_group_label_and_children_are_those_of_the_exact_domain_filter(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    _insert(data, evidence_id, [("Example.com", None), ("EXAMPLE.com", "2024-01-01"), ("example.com", None)])
    (group,) = _all_groups(data, evidence_id, UrlFilterSpec(), 10)
    children = _all_children(data, evidence_id, UrlFilterSpec(), group.domain, 10)
    flat = [r["id"] for r in data.iter_url_rows(evidence_id, UrlFilterSpec(domain_exact="example.COM"))]
    assert children == flat and group.count == 3


@pytest.mark.parametrize("group_page", [1, 3, 500])
def test_group_pages_cover_many_domains_once(empty_case, group_page):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    domains = [f"site{index:03d}.example" for index in range(57)]
    _insert(data, evidence_id, [(domain, None) for domain in domains for _ in range(2)] + [(None, None)])
    groups = _all_groups(data, evidence_id, UrlFilterSpec(), group_page)
    assert [g.domain for g in groups] == sorted(domains) + [None]
    assert all(g.count == 2 for g in groups[:-1]) and groups[-1].count == 1


def test_no_domain_group_alone_and_absent(empty_case):
    case, data = empty_case
    ev1, ev2 = case.evidence_ids
    _insert(data, ev1, [(None, None), ("", None)])
    assert _all_groups(data, ev1, UrlFilterSpec(), 5) == [UrlDomainGroup(None, 2)]
    assert _all_groups(data, ev2, UrlFilterSpec(), 5) == []
    _insert(data, ev2, [("only.com", None)])
    # The page ending the named groups ends the list: no empty no-domain group.
    assert _all_groups(data, ev2, UrlFilterSpec(), 1) == [UrlDomainGroup("only.com", 1)]


TIMESTAMPS = [None, "2024-05-05T05:05:05Z", "", "2024-05-05T05:05:05Z", None, "2023-01-01", "2025-12-31", "2024-05-05T05:05:05Z"]


@pytest.mark.parametrize("child_page", [1, 2, 3, 7, 100])
@pytest.mark.parametrize("domain", ["big.example", None], ids=["named", "no_domain"])
def test_children_follow_flat_order_with_timestamp_ties_and_nulls(empty_case, child_page, domain):
    """One large group; ties, NULL and '' timestamps; the no-domain group mixes NULL and ''."""
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    rows: List[Row] = []
    for index in range(61):
        group_domain = domain if domain is not None else (None if index % 2 else "")
        rows.append((group_domain, TIMESTAMPS[index % len(TIMESTAMPS)]))
        rows.append(("other.example", TIMESTAMPS[index % 3]))
    _insert(data, evidence_id, rows)

    spec = UrlFilterSpec()
    groups = _assert_tree_matches_flat(data, evidence_id, spec, 10, child_page)
    expected = [g for g in groups if g.domain == domain or (domain is None and g.domain is None)]
    assert len(expected) == 1 and expected[0].count == 61


def test_cursor_from_deleted_row_still_positions_children(empty_case):
    case, data = empty_case
    evidence_id = case.evidence_ids[0]
    _insert(data, evidence_id, [("a.com", TIMESTAMPS[i % len(TIMESTAMPS)]) for i in range(30)])
    spec = UrlFilterSpec()
    full = _all_children(data, evidence_id, spec, "a.com", 100)
    first = data.load_url_domain_children(UrlDomainChildQuery(evidence_id, "a.com", spec, page_size=10))
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute("DELETE FROM urls WHERE id = ?", (first.rows[-1]["id"],))
    second = data.load_url_domain_children(
        UrlDomainChildQuery(evidence_id, "a.com", spec, after=first.next_cursor, page_size=10)
    )
    assert [r["id"] for r in second.rows] == full[10:20]


FILTERS: Dict[str, UrlFilterSpec] = {
    "none": UrlFilterSpec(),
    "domain_exact": UrlFilterSpec(domain_exact="D3.Example.COM"),
    "domain_contains": UrlFilterSpec(domain_contains="1.EXAMPLE"),
    "source": UrlFilterSpec(discovered_by=("firefox_history",)),
    "sources": UrlFilterSpec(discovered_by=("chromium_cache", "bulk_extractor")),
    "tag": UrlFilterSpec(tag_name="relevant"),
    "matched": UrlFilterSpec(match_filter="matched"),
    "unmatched": UrlFilterSpec(match_filter="unmatched"),
    "list": UrlFilterSpec(match_filter="list1"),
    "url_contains": UrlFilterSpec(url_contains="Q=1"),
    "url_contains_short": UrlFilterSpec(url_contains="/9"),
    "combined": UrlFilterSpec(match_filter="unmatched", discovered_by=("chromium_cache",), url_contains="path"),
}


@pytest.mark.parametrize("search_index", [False, True], ids=["scan", "search_index"])
@pytest.mark.parametrize("name", sorted(FILTERS))
def test_filtered_tree_matches_flat_listing(fixture_case, name, search_index):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    if search_index:
        assert data.build_url_search_index(evidence_id).is_ready
    groups = _assert_tree_matches_flat(data, evidence_id, FILTERS[name], 4, 13)
    assert groups
    keys = [g.domain.lower() for g in groups if g.domain is not None]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)


def test_children_carry_complete_tag_and_match_maps(fixture_case):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    spec = UrlFilterSpec()
    group = _all_groups(data, evidence_id, spec, 1)[0]
    page = data.load_url_domain_children(UrlDomainChildQuery(evidence_id, group.domain, spec, page_size=50))
    ids = {row["id"] for row in page.rows}
    assert set(page.tags) == ids and set(page.matches) == ids
    tags = data.get_tag_strings_for_artifacts(evidence_id, "url", sorted(ids))
    assert {i: page.tags[i] for i in tags} == tags and any(page.tags.values())
    assert any(page.matches.values())


def test_groups_and_children_are_isolated_per_evidence(fixture_case):
    case, data = fixture_case
    ev1, ev2 = case.evidence_ids
    _insert(data, ev2, [("only-ev2.com", None)])
    assert "only-ev2.com" not in {g.domain for g in _all_groups(data, ev1, UrlFilterSpec(), 50)}
    assert _all_children(data, ev1, UrlFilterSpec(), "only-ev2.com", 10) == []
    assert UrlDomainGroup("only-ev2.com", 1) in _all_groups(data, ev2, UrlFilterSpec(), 50)


# Query plans and bounded cost ------------------------------------------------


def _traced_plans(data: CaseDataAccess, evidence_id: int, fn) -> List[str]:
    """EXPLAIN QUERY PLAN of every URL statement ``fn`` runs."""
    statements: List[str] = []
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.set_trace_callback(lambda sql: statements.append(sql) if "urls u" in sql else None)
            try:
                fn()
            finally:
                conn.set_trace_callback(None)
            return [
                " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql))
                for sql in statements
            ]


@pytest.mark.parametrize("domain", ["d3.example.com", None], ids=["named", "no_domain"])
def test_group_and_child_pages_seek_the_domain_index_without_sorting(fixture_case, domain):
    case, data = fixture_case
    evidence_id = case.evidence_ids[0]
    spec = UrlFilterSpec()
    group_plans = _traced_plans(
        data, evidence_id,
        lambda: data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, spec, after="d1", page_size=5)),
    )
    assert "INDEX idx_urls_evidence_domain_nocase_first_seen" in group_plans[0]
    assert "TEMP B-TREE" not in group_plans[0]

    child_plans = _traced_plans(
        data, evidence_id,
        lambda: data.load_url_domain_children(
            UrlDomainChildQuery(evidence_id, domain, spec, after=UrlCursor("2024-01-01", 5), page_size=5)
        ),
    )
    assert child_plans
    for plan in child_plans:
        assert "INDEX idx_urls_evidence_domain_nocase_first_seen" in plan
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


def test_group_page_cost_does_not_grow_with_domain_count_or_depth(tmp_path):
    """Initial display touches only the rows of the returned groups."""
    small = build_url_fixture_case(tmp_path / "small", rows_per_evidence=2_000, domain_count=1_000)
    large = build_url_fixture_case(tmp_path / "large", rows_per_evidence=40_000, domain_count=20_000)
    for case in (small, large):
        case.data = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        def first(case):
            return case.data.load_url_domain_groups(UrlDomainGroupQuery(case.evidence_ids[0], page_size=50))

        small_first = _vm_work(small.data, lambda: first(small))
        large_first = _vm_work(large.data, lambda: first(large))
        deep_after = "d19000.example.com"
        large_deep = _vm_work(
            large.data,
            lambda: large.data.load_url_domain_groups(
                UrlDomainGroupQuery(large.evidence_ids[0], after=deep_after, page_size=50)
            ),
        )
        # Same number of rows per group (2): the first page is not 20x dearer.
        assert large_first <= small_first + 2
        assert large_deep <= small_first + 2
    finally:
        for case in (small, large):
            case.data.close()


def test_child_page_cost_does_not_grow_with_group_size(tmp_path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=60_000, timestamp_every=10, domain_count=2)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        evidence_id = case.evidence_ids[0]
        spec = UrlFilterSpec()
        domain = "d1.example.com"

        def page(after):
            return data.load_url_domain_children(UrlDomainChildQuery(evidence_id, domain, spec, after=after, page_size=500))

        first = _vm_work(data, lambda: page(None))
        deep_row = data.iter_urls(evidence_id, filters=UrlFilterSpec(domain_exact=domain), limit=1, offset=25_000)[0]
        deep = _vm_work(data, lambda: page(UrlCursor.from_row(deep_row)))
        assert deep <= first + 2
    finally:
        data.close()
