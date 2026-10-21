"""URL search results are identical with and without the search index (issue #62).

The oracle is plain Python over the inserted rows: a URL matches a search term
when ``ascii_lower(term) in ascii_lower(url)`` (``LIKE`` folds ASCII letters
only), plus the other filter semantics; order is ``first_seen_utc DESC``
(NULLs last), ``id ASC``. Every query is checked on the indexed path and on the
scan fallback, with the index not built, partially built (then changed), and
fully built.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Set

import pytest

import app.data._urls as urls_module
from app.data import CaseDataAccess, QueryCancelledError, UrlFilterSpec, UrlPageQuery, UrlSearchIndexStatus
from core.database.helpers.url_search_index import (
    build_url_search_index_step,
    url_search_index_supported,
)
from tests.fixtures.url_fixtures import build_url_fixture_case

SOURCES = ("chromium_history", "firefox_history", "bulk_extractor")

LONG_URL = "https://long.example/" + "seg/" * 600 + "needle-in-long?" + "a" * 300

TRICKY_URLS = [
    "https://p.example/a?b=c&d=e#f;g:h/@i!j$k'l(m)n*o+p,q",
    'https://q.example/say?"hello"&x="a""b"',
    'https://q.example/"',
    "https://b.example/c:\\Windows\\path\\file.txt",
    "file:///C:\\\\double\\\\slash",
    "https://e.example/a%20b%2Fc%25d",
    "https://e.example/100%",
    "https://e.example/under_score_",
    "https://e.example/a_b%c",
    "https://e.example/a%2fb",
    "https://e.example/%%__\\\\",
    "HTTPS://MiXeD.Example.COM/UPPER/lower/CamelCase",
    "https://mixed.example.com/upper/LOWER",
    "https://de.example/straße/STRASSE",
    "https://de.example/Ärger",
    "https://de.example/ärger",
    "https://de.example/ÄRGER/ärGER",
    "https://tr.example/İstanbul",
    "https://tr.example/ıstanbul",
    "https://tr.example/ISTANBUL/istanbul",
    "https://k.example/\u212aelvin",
    "https://k.example/kelvin/KELVIN",
    "https://emoji.example/😀/🎉party",
    "https://cjk.example/中文/日本語/한국어",
    "https://grk.example/ΣΑΣ/σας/ς",
    "https://fts.example/AND/OR/NOT/NEAR(a b)/x*y^z:w-v+(u)",
    'https://fts.example/"NEAR"/col:val/and/or',
    "https://ws.example/a b\tc\nd",
    "  leading-space.example",
    "trailing-space.example  ",
    LONG_URL,
    "https://long.example/" + "aaaa" * 200,
    "x",
    "",
]

# Terms: 1-2 chars and wildcard-only terms (always scanned), 3+ char terms of
# every corpus category, ASCII case variants, non-ASCII case variants that
# must not match, FTS syntax, and terms without hits.
TERMS = [
    "a", "A", "/", "%", "_", "\\", '"', "ä", "Ä", "😀", " ",
    "ab", "%%", "__", "\\\\", '""', "ex", "Ä/", "ς",
    "example", "EXAMPLE", "ExAmPlE", "site3.", "SITE3.example", "/path/1", "q=1", "#frag4",
    "&d=e#", "@i!j$", "'l(m)", "*o+p,q", ";g:h",
    '"hello"', '""b', 'say?"', '/"',
    "c:\\", "\\Windows\\", "\\\\double", "%%__\\\\",
    "%20", "%2f", "%2F", "%25", "100%", "_b%", "a_b", "under_score", "%2fb",
    "mixed.example", "MIXED.EXAMPLE.COM/upper", "camelcase", "UPPER/LOWER",
    "straße", "STRASSE", "STRAẞE", "ärger", "Ärger", "ÄRGER", "äRGER", "ärg", "/Är",
    "İstanbul", "istanbul", "ISTANBUL", "ıstanbul", "Istanbul",
    "kelvin", "\u212aelvin", "KELVIN", "Kelvin",
    "😀/🎉", "🎉party", "中文/", "日本語", "σας", "ΣΑΣ", "Σας",
    "AND", "and/or", "NEAR(", "NEAR(a b)", "x*y^z", "col:val", "-v+", "(u)", "OR/NOT",
    " b\tc", "c\nd", "  leading", "example  ",
    "needle-in-long", "seg/seg/seg/", "aaaaaaaaaaaaa", "a" * 301,
    "updated", "tail\\x", "zzz-no-hit", "example.com/path/9999999",
]

COMBO_TERMS = ["example", "EXAMPLE", "/path/1", "ärger", "%2", "a_b", "ab", "x", "zzz-no-hit"]
COMBOS = {
    "domain_exact": dict(domain_exact="site3.example.com"),
    "domain_exact_case": dict(domain_exact="SITE3.Example.COM"),
    "domain_contains": dict(domain_contains="E3.EXA"),
    "one_source": dict(discovered_by=("firefox_history",)),
    "two_sources": dict(discovered_by=("chromium_history", "bulk_extractor")),
    "tag": dict(tag_name="relevant"),
    "tag_other": dict(tag_name="Review"),
    "matched": dict(match_filter="matched"),
    "unmatched": dict(match_filter="unmatched"),
    "list": dict(match_filter="list2"),
    "everything": dict(domain_contains="example", discovered_by=SOURCES[:2], tag_name="Relevant", match_filter="unmatched"),
}

pytestmark = pytest.mark.skipif(
    not url_search_index_supported(), reason="SQLite lacks FTS5 with the trigram tokenizer"
)


# ---------------------------------------------------------------------------
# Python model + oracle
# ---------------------------------------------------------------------------


@dataclass
class Row:
    id: int
    evidence_id: int
    url: str
    domain: Optional[str]
    discovered_by: str
    first_seen_utc: Optional[str]
    db: int  # evidence database the row lives in
    tags: Set[str] = field(default_factory=set)
    lists: Set[str] = field(default_factory=set)


def ascii_lower(text: str) -> str:
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in text)


def oracle(rows: Sequence[Row], evidence_id: int, spec: UrlFilterSpec) -> List[int]:
    def keep(row: Row) -> bool:
        if row.evidence_id != evidence_id or row.db != evidence_id:
            return False
        if spec.url_contains is not None and ascii_lower(spec.url_contains) not in ascii_lower(row.url):
            return False
        if spec.domain_exact is not None and (
            row.domain is None or ascii_lower(row.domain) != ascii_lower(spec.domain_exact)
        ):
            return False
        if spec.domain_contains is not None and (
            row.domain is None or ascii_lower(spec.domain_contains) not in ascii_lower(row.domain)
        ):
            return False
        if spec.tag_name is not None and spec.tag_name.lower() not in {t.lower() for t in row.tags}:
            return False
        if spec.discovered_by and row.discovered_by not in spec.discovered_by:
            return False
        if spec.match_filter == "matched" and not row.lists:
            return False
        if spec.match_filter == "unmatched" and row.lists:
            return False
        if spec.match_filter not in (None, "matched", "unmatched") and spec.match_filter not in row.lists:
            return False
        return True

    kept = sorted((r for r in rows if keep(r)), key=lambda r: r.id)
    stamped = sorted((r for r in kept if r.first_seen_utc is not None), key=lambda r: r.first_seen_utc, reverse=True)
    return [r.id for r in stamped] + [r.id for r in kept if r.first_seen_utc is None]


# ---------------------------------------------------------------------------
# Case construction
# ---------------------------------------------------------------------------


@dataclass
class SearchCase:
    data: CaseDataAccess
    evidence_ids: List[int]
    rows: List[Row]
    mode: str


def _insert(data: CaseDataAccess, evidence_id: int, rows: List[Row], specs: Sequence[dict]) -> List[Row]:
    added = []
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            for spec in specs:
                row_id = conn.execute(
                    "INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by, first_seen_utc, "
                    "last_seen_utc, source_path, notes) VALUES (?, ?, ?, 'https', ?, ?, ?, ?, ?)",
                    (
                        spec["evidence_id"], spec["url"], spec["domain"], spec["discovered_by"],
                        spec["first_seen_utc"], spec["first_seen_utc"], spec.get("source_path"), spec.get("notes"),
                    ),
                ).lastrowid
                added.append(Row(row_id, spec["evidence_id"], spec["url"], spec["domain"],
                                 spec["discovered_by"], spec["first_seen_utc"], evidence_id))
    rows.extend(added)
    return added


def _row_specs(evidence_id: int, urls: Sequence[str], offset: int = 0) -> List[dict]:
    specs = []
    for n, url in enumerate(urls):
        i = n + offset
        specs.append(
            dict(
                evidence_id=evidence_id,
                url=url,
                domain=None if i % 11 == 0 else (f"Site{i % 5}.Example.com" if i % 4 == 0 else f"site{i % 5}.example.com"),
                discovered_by=SOURCES[i % len(SOURCES)],
                # NULL every third row; repeated timestamps exercise the id tie-breaker.
                first_seen_utc=None if i % 3 == 0 else ("" if i % 29 == 1 else f"2024-{1 + i % 12:02d}-{1 + i % 4:02d}T00:00:00Z"),
                source_path=None if i % 2 else f"/p/History{i % 3}",
                notes=None if i % 5 else "note",
            )
        )
    return specs


def _generated_urls(count: int, offset: int = 0) -> List[str]:
    return [
        f"https://site{i % 5}.example.com/path/{i}?q={i % 13}&x=Y#frag{i % 7}" for i in range(offset, offset + count)
    ]


def _decorate(data: CaseDataAccess, evidence_id: int, rows: Sequence[Row]) -> None:
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            for n, row in enumerate(rows):
                for list_name, every in (("list1", 5), ("list2", 7)):
                    if n % every == 0:
                        conn.execute(
                            "INSERT INTO url_matches(evidence_id, url_id, list_name, match_type, matched_pattern) "
                            "VALUES (?, ?, ?, 'domain', 'example')",
                            (row.evidence_id, row.id, list_name),
                        )
                        row.lists.add(list_name)
    for n, row in enumerate(rows):
        for tag, every in (("Relevant", 4), ("Review", 9)):
            if n % every == 1:
                data.tag_artifact(evidence_id, tag, "url", row.id)
                row.tags.add(tag)


def _build_steps(data: CaseDataAccess, evidence_id: int, steps: Optional[int], batch_size: int) -> None:
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            done = 0
            while (steps is None or done < steps) and build_url_search_index_step(conn, batch_size):
                done += 1


def _execute(data: CaseDataAccess, evidence_id: int, sql: str, params=()) -> None:
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute(sql, params)


def _make_case(root: Path, mode: str) -> SearchCase:
    case = build_url_fixture_case(root, rows_per_evidence=0, evidence_count=2)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    ev1, ev2 = case.evidence_ids
    rows: List[Row] = []

    corpus_a = _generated_urls(200) + TRICKY_URLS + _generated_urls(40, offset=500)
    added = _insert(data, ev1, rows, _row_specs(ev1, corpus_a))
    _decorate(data, ev1, added)
    # Rows of another evidence in this database must never be returned.
    _insert(data, ev1, rows, _row_specs(ev2, ["https://site3.example.com/path/1-foreign", "https://de.example/ärger"]))
    added2 = _insert(data, ev2, rows, _row_specs(ev2, _generated_urls(12) + TRICKY_URLS[:16], offset=3))
    _decorate(data, ev2, added2)

    if mode != "unbuilt":
        _build_steps(data, ev1, steps=3, batch_size=40)
        _build_steps(data, ev2, steps=1, batch_size=10)

    # Changes in the indexed range and in the tail (after a partial build).
    for victim in (added[5], added[50], added[240], added2[2], added2[20]):
        _execute(data, victim.evidence_id, "DELETE FROM urls WHERE id = ?", (victim.id,))
        rows.remove(victim)
    for row, url in (
        (added[10], 'https://UPDATED.example/Ärger"x"%41'),
        (added[100], "https://updated.example/AND/NEAR"),
        (added[250], "https://updated.example/tail\\x"),
        (added2[3], "https://updated.example/ev2"),
    ):
        _execute(data, row.evidence_id, "UPDATE urls SET url = ? WHERE id = ?", (url, row.id))
        row.url = url
    _execute(data, ev1, "UPDATE urls SET notes = 'changed', domain = NULL WHERE id = ?", (added[11].id,))
    added[11].domain = None
    tail = _insert(data, ev1, rows, _row_specs(ev1, _generated_urls(25, offset=900) + TRICKY_URLS[::3], offset=7))
    _decorate(data, ev1, tail)
    _insert(data, ev2, rows, _row_specs(ev2, ["https://tail.example/ev2/ärger"]))

    if mode == "built":
        _build_steps(data, ev1, steps=None, batch_size=64)
        _build_steps(data, ev2, steps=None, batch_size=64)

    for evidence_id in (ev1, ev2):
        status = data.get_url_search_index_status(evidence_id)
        if mode == "built":
            assert status.is_ready
        else:
            assert status.state == UrlSearchIndexStatus.BUILDING
            assert (status.indexed_through_id == 0) == (mode == "unbuilt")
    return SearchCase(data, [ev1, ev2], rows, mode)


@pytest.fixture(scope="module", params=["unbuilt", "partial", "built"])
def search_case(request, tmp_path_factory) -> Iterator[SearchCase]:
    case = _make_case(tmp_path_factory.mktemp(f"search_{request.param}") / "case", request.param)
    yield case
    case.data.close()


# ---------------------------------------------------------------------------
# Query paths and checks
# ---------------------------------------------------------------------------


@contextmanager
def query_path(path: str) -> Iterator[List]:
    """'index': spy on candidate plans; 'scan': force the fallback."""
    plans: List = []
    original = urls_module.plan_url_search_candidates

    def spy(conn, text, **kwargs):
        plan = original(conn, text, **kwargs)
        plans.append((text, plan))
        return plan

    with pytest.MonkeyPatch.context() as mp:
        if path == "index":
            mp.setattr(urls_module, "plan_url_search_candidates", spy)
        else:
            mp.setattr(urls_module, "plan_url_search_candidates", lambda conn, text, **kwargs: None)
        yield plans


def walk(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, page_size: int) -> List[int]:
    ids: List[int] = []
    query = UrlPageQuery(evidence_id, spec, page=0, page_size=page_size)
    while True:
        page = data.load_url_page(query)
        page_ids = [row["id"] for row in page.rows]
        assert set(page.tags) == set(page.matches) == set(page_ids)
        ids.extend(page_ids)
        if not page.has_more:
            return ids
        assert len(page_ids) == page_size
        query = UrlPageQuery(evidence_id, spec, page=query.page + 1, page_size=page_size, after=page.next_cursor)


def check(case: SearchCase, evidence_id: int, spec: UrlFilterSpec, path: str, *, deep: bool = True) -> List[int]:
    expected = oracle(case.rows, evidence_id, spec)
    data = case.data
    with query_path(path) as plans:
        assert data.count_urls(evidence_id, filters=spec) == len(expected)
        page_sizes = [500, 7] + ([1] if len(expected) <= 40 else [])
        for page_size in page_sizes if deep else [500]:
            assert walk(data, evidence_id, spec, page_size) == expected, page_size
        assert [r["id"] for r in data.iter_urls(evidence_id, filters=spec, limit=100_000)] == expected
        assert [r["id"] for r in data.iter_url_rows(evidence_id, spec, batch_size=17)] == expected
    if path == "index" and spec.url_contains is not None:
        assert plans, "URL search did not consult the index"
        indexable = len(spec.url_contains) >= 3 and "\x00" not in spec.url_contains
        # Small evidence: the index (with its unindexed tail) always serves 3+ char terms.
        assert all((plan is not None) == indexable for _text, plan in plans), plans
    return expected


@pytest.mark.parametrize("path", ["index", "scan"])
@pytest.mark.parametrize("term", TERMS)
def test_url_contains_matches_oracle(search_case, term, path):
    check(search_case, search_case.evidence_ids[0], UrlFilterSpec(url_contains=term), path)


@pytest.mark.parametrize("path", ["index", "scan"])
@pytest.mark.parametrize("term", ["example", "ärger", "ÄRGER", '"', "updated", "a_b", "ab", "%2"])
def test_second_evidence_is_scoped(search_case, term, path):
    expected = check(search_case, search_case.evidence_ids[1], UrlFilterSpec(url_contains=term), path)
    ev2 = search_case.evidence_ids[1]
    foreign = {r.id for r in search_case.rows if r.evidence_id == ev2 and r.db != ev2}
    assert foreign and not foreign & set(expected) or not expected


@pytest.mark.parametrize("path", ["index", "scan"])
@pytest.mark.parametrize("combo", sorted(COMBOS))
@pytest.mark.parametrize("term", COMBO_TERMS)
def test_url_contains_with_other_filters_matches_oracle(search_case, term, combo, path):
    spec = UrlFilterSpec(url_contains=term, **COMBOS[combo])
    check(search_case, search_case.evidence_ids[0], spec, path, deep=False)


@pytest.mark.parametrize("path", ["index", "scan"])
def test_filters_without_text_are_unchanged(search_case, path):
    for combo in sorted(COMBOS):
        check(search_case, search_case.evidence_ids[0], UrlFilterSpec(**COMBOS[combo]), path, deep=False)
    check(search_case, search_case.evidence_ids[0], UrlFilterSpec(), path)


def test_index_and_scan_return_identical_rows(search_case):
    ev1 = search_case.evidence_ids[0]
    for term in ("example", "ärger", "%25", '"', "updated"):
        spec = UrlFilterSpec(url_contains=term)
        with query_path("index"):
            indexed = search_case.data.iter_urls(ev1, filters=spec, limit=100_000)
        with query_path("scan"):
            scanned = search_case.data.iter_urls(ev1, filters=spec, limit=100_000)
        assert indexed == scanned


def _urls_for(case: SearchCase, term: str) -> Set[str]:
    with query_path("index"):
        return {r["url"] for r in case.data.iter_urls(case.evidence_ids[0], filters=UrlFilterSpec(url_contains=term), limit=10_000)}


def test_contract_examples(search_case):
    """Spot checks of the literal, ASCII-only case-insensitive contract."""
    case = search_case
    # Ä and ä differ; the ASCII letters around them fold.
    assert _urls_for(case, "de.example/ärger") == {"https://de.example/ärger"}
    assert _urls_for(case, "DE.EXAMPLE/Ärger") == {"https://de.example/Ärger", "https://de.example/ÄRGER/ärGER"}
    assert _urls_for(case, "/ärGer") == {"https://de.example/ärger", "https://de.example/ÄRGER/ärGER"}
    assert _urls_for(case, "ÄRGER/") == {"https://de.example/ÄRGER/ärGER"}
    assert _urls_for(case, "straẞe") == set()  # U+1E9E is not folded by LIKE
    assert "https://k.example/kelvin/KELVIN" in _urls_for(case, "kelvin")
    assert "https://k.example/\u212aelvin" not in _urls_for(case, "kelvin")
    assert _urls_for(case, "\u212aelvin") == {"https://k.example/\u212aelvin"}
    assert _urls_for(case, "100%") == {"https://e.example/100%"}
    assert _urls_for(case, "a_b%") == {"https://e.example/a_b%c"}
    assert _urls_for(case, "%2f") == {"https://e.example/a%20b%2Fc%25d", "https://e.example/a%2fb"}
    assert _urls_for(case, '"a""b"') == {'https://q.example/say?"hello"&x="a""b"'}
    assert _urls_for(case, "NEAR(a b)") == {"https://fts.example/AND/OR/NOT/NEAR(a b)/x*y^z:w-v+(u)"}
    assert _urls_for(case, "needle-in-long") == {LONG_URL}


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


def test_cancelled_indexed_search_raises(search_case):
    data = search_case.data
    ev1 = search_case.evidence_ids[0]
    spec = UrlFilterSpec(url_contains="example")
    with query_path("index") as plans:
        with pytest.raises(QueryCancelledError):
            with data.cancellable_reads(lambda: True):
                data.count_urls(ev1, filters=spec)
        with pytest.raises(QueryCancelledError):
            with data.cancellable_reads(lambda: True):
                data.load_url_page(UrlPageQuery(ev1, spec, page_size=50))
        # Connections work normally afterwards.
        assert data.count_urls(ev1, filters=spec) == len(oracle(search_case.rows, ev1, spec))
    assert plans  # the indexed path was taken


# ---------------------------------------------------------------------------
# Data-layer build
# ---------------------------------------------------------------------------


@pytest.fixture
def small_case(tmp_path: Path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=0, evidence_count=2)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    ev1 = case.evidence_ids[0]
    rows: List[Row] = []
    _insert(data, ev1, rows, _row_specs(ev1, _generated_urls(25)))
    yield data, ev1, rows
    data.close()


def _build_logs(data: CaseDataAccess, evidence_id: int) -> List[dict]:
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT evidence_id, task, command, exit_code, stdout, stderr, finished_at_utc "
                    "FROM process_log WHERE task = 'url_search_index_build' ORDER BY id"
                )
            ]


def test_build_reports_progress_and_logs(small_case):
    data, ev1, rows = small_case
    assert data.get_url_search_index_status(ev1).state == UrlSearchIndexStatus.BUILDING
    seen: List[UrlSearchIndexStatus] = []
    status = data.build_url_search_index(ev1, progress_callback=seen.append, batch_size=10)

    assert status.is_ready and status.indexed_through_id == rows[-1].id
    assert [s.state for s in seen] == ["building", "building", "ready"]
    assert [s.indexed_through_id for s in seen] == [rows[9].id, rows[19].id, rows[-1].id]
    assert [s.progress for s in seen] == sorted(s.progress for s in seen)
    (log,) = _build_logs(data, ev1)
    assert log["evidence_id"] == ev1 and log["exit_code"] == 0 and log["finished_at_utc"]
    assert "id > 0" in log["command"]
    assert json.loads(log["stdout"]) == {
        "result": "ready", "rows_indexed": 25, "indexed_through_id": rows[-1].id, "max_url_id": rows[-1].id,
    }

    # Already ready: nothing to do, nothing logged.
    assert data.build_url_search_index(ev1, progress_callback=seen.append).is_ready
    assert len(_build_logs(data, ev1)) == 1 and len(seen) == 3


def test_build_cancel_and_resume(small_case):
    data, ev1, rows = small_case
    seen: List[UrlSearchIndexStatus] = []
    status = data.build_url_search_index(
        ev1, is_cancelled=lambda: bool(seen), progress_callback=seen.append, batch_size=8
    )
    assert status.state == UrlSearchIndexStatus.BUILDING and status.indexed_through_id == rows[7].id
    assert data.get_url_search_index_status(ev1) == status
    (log,) = _build_logs(data, ev1)
    assert json.loads(log["stdout"]) == {
        "result": "cancelled", "rows_indexed": 8, "indexed_through_id": rows[7].id, "max_url_id": rows[-1].id,
    }

    # Cancelled before starting: no work, no log.
    assert data.build_url_search_index(ev1, is_cancelled=lambda: True) == status
    assert len(_build_logs(data, ev1)) == 1

    status = data.build_url_search_index(ev1, batch_size=8)
    assert status.is_ready
    logs = _build_logs(data, ev1)
    assert len(logs) == 2 and f"id > {rows[7].id}" in logs[1]["command"]
    assert json.loads(logs[1]["stdout"])["rows_indexed"] == 17
    check(SearchCase(data, [ev1], rows, "built"), ev1, UrlFilterSpec(url_contains="path/1"), "index")


def test_build_failure_before_any_batch_leaves_no_log_and_raises(small_case, monkeypatch):
    """A session that indexed nothing (e.g. locked on its first batch) leaves no process_log row."""
    data, ev1, rows = small_case

    def boom(conn, batch_size):
        raise RuntimeError("simulated build failure")

    monkeypatch.setattr(urls_module, "build_url_search_index_step", boom)
    with pytest.raises(RuntimeError, match="simulated build failure"):
        data.build_url_search_index(ev1)
    assert _build_logs(data, ev1) == []
    assert data.get_url_search_index_status(ev1).indexed_through_id == 0
    # Search still works (everything is scanned as tail).
    monkeypatch.undo()
    check(SearchCase(data, [ev1], rows, "unbuilt"), ev1, UrlFilterSpec(url_contains="path/2"), "index")


def test_build_failure_after_a_batch_is_logged_and_raised(small_case, monkeypatch):
    data, ev1, rows = small_case
    original = urls_module.build_url_search_index_step
    calls = []

    def fail_second(conn, batch_size):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("simulated build failure")
        return original(conn, 5)

    monkeypatch.setattr(urls_module, "build_url_search_index_step", fail_second)
    with pytest.raises(RuntimeError, match="simulated build failure"):
        data.build_url_search_index(ev1)
    (log,) = _build_logs(data, ev1)
    assert log["exit_code"] == 1 and log["stdout"] is None
    assert "simulated build failure" in log["stderr"]
    assert data.get_url_search_index_status(ev1).indexed_through_id > 0  # first batch kept
    monkeypatch.undo()
    assert data.build_url_search_index(ev1).is_ready
    check(SearchCase(data, [ev1], rows, "built"), ev1, UrlFilterSpec(url_contains="path/2"), "index")


def test_status_for_missing_evidence_database_is_nothing_to_index(small_case):
    """No evidence DB yet (nothing extracted): ready, not reported as a failure."""
    data, _ev1, _rows = small_case
    assert data.get_url_search_index_status(9999).state == UrlSearchIndexStatus.READY
    assert data.build_url_search_index(9999).state == UrlSearchIndexStatus.READY


def test_index_builds_do_not_count_as_last_extraction(small_case):
    """The Overview's "Last Extraction" ignores url_search_index_build sessions."""
    data, ev1, _rows = small_case
    before = data.get_evidence_counts(ev1).last_run_utc
    assert data.build_url_search_index(ev1).is_ready
    assert _build_logs(data, ev1)
    assert data.get_evidence_counts(ev1).last_run_utc == before


def test_failed_finalize_keeps_original_build_error(small_case, monkeypatch):
    import app.data._urls as urls_module
    from core.database import helpers

    data, ev1, _rows = small_case

    original = urls_module.build_url_search_index_step
    calls = []

    def failing_step(conn, batch_size):
        calls.append(1)
        if len(calls) == 2:
            raise sqlite3.OperationalError("database is locked")
        return original(conn, 5)

    def failing_finalize(*args, **kwargs):
        raise sqlite3.OperationalError("still locked")

    monkeypatch.setattr(urls_module, "build_url_search_index_step", failing_step)
    monkeypatch.setattr(helpers, "finalize_process_log", failing_finalize)
    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        data.build_url_search_index(ev1)


def test_new_evidence_database_has_index_objects(small_case):
    """DatabaseManager creates the index objects when opening the evidence DB."""
    data, ev1, _rows = small_case
    with data._use_evidence_conn(ev1):
        with data._connect() as conn:
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    assert {"urls_search_index", "url_search_index_state", "urls_search_index_ad", "urls_search_index_au"} <= names
