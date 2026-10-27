"""File List data layer: shared filters, Exclude and Untagged (issue #73).

Results are checked against an independent Python oracle, and the filters
whose results must not change are pinned against a copy of the SQL the File
List model used before the move to the data layer.
"""
from __future__ import annotations

import itertools
import sqlite3
import string
from dataclasses import replace
from typing import Any, Dict, List, Optional, Set

import pytest

from app.data import FILE_LIST_ANY_MATCH, CaseDataAccess, FileListFilterSpec, discard_case_stats_cache
from app.data._file_list import build_file_list_filter_predicates
from core.database import DatabaseManager
from tests.fixtures.file_list_fixtures import build_file_list_fixture_case

OTHER_EVIDENCE = 999
_FOLD = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)

# Edge rows: (file_path, file_name, extension, size_bytes, deleted)
EDGE_ROWS = [
    ("C:\\Temp\\100%_done.txt", "100%_done.txt", "txt", 10, 0),
    ("C:\\Temp\\100xxdone.txt", "100xxdone.txt", "txt", 10, 0),
    ("C:\\Temp\\a_b.txt", "a_b.txt", "txt", 10, 0),
    ("C:\\Temp\\axb.txt", "axb.txt", "txt", 10, 0),
    ("C:\\Temp\\README.TXT", "README.TXT", "TXT", 10, 0),
    ("C:\\Temp\\Ärger.txt", "Ärger.txt", "txt", 10, 0),
    ("C:\\Temp\\ärger2.txt", "ärger2.txt", "txt", 10, 0),
    # name-only and path-only hits for "needle"
    ("C:\\Other\\renamed.bin", "needle.bin", "bin", 10, 0),
    ("C:\\needle\\plain.bin", "plain.bin", "bin", 10, 0),
    ("C:\\Temp\\backslash.bin", "back\\slash.bin", "bin", 10, 0),
    # same path on two partitions (order tie broken by id)
    ("C:\\Dup\\same.txt", "same.txt", "txt", 10, 0),
    ("C:\\Dup\\same.txt", "same.txt", "txt", 10, 1),
    ("C:\\Temp\\nulldeleted.txt", "nulldeleted.txt", "txt", 10, None),
]


def _fold(value: str) -> str:
    return value.translate(_FOLD)


@pytest.fixture
def fl_case(tmp_path):
    case = build_file_list_fixture_case(tmp_path / "case", rows=60, tag_every=7, match_every=5)
    discard_case_stats_cache(case.case_db_path)
    manager = DatabaseManager(case.case_folder, case_db_path=case.case_db_path)
    conn = manager.get_evidence_conn(case.evidence_id, case.label)
    ev = case.evidence_id
    with conn:
        ids = {}
        for index, (path, name, ext, size, deleted) in enumerate(EDGE_ROWS):
            ids[index] = conn.execute(
                "INSERT INTO file_list(evidence_id, file_path, file_name, extension, size_bytes, deleted, "
                "import_timestamp, partition_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ev, path, name, ext, size, deleted, "2025-01-01T00:00:00Z", 1 + (index == 11)),
            ).lastrowid

        def tag(name: str, evidence_id: int = ev) -> int:
            row = conn.execute(
                "SELECT id FROM tags WHERE evidence_id = ? AND name_normalized = ?", (evidence_id, name.lower())
            ).fetchone()
            if row:
                return row[0]
            return conn.execute(
                "INSERT INTO tags(evidence_id, name, name_normalized) VALUES (?, ?, ?)",
                (evidence_id, name, name.lower()),
            ).lastrowid

        def associate(tag_id: int, file_id: int, evidence_id: int = ev, artifact_type: str = "file_list") -> None:
            conn.execute(
                "INSERT INTO tag_associations(tag_id, evidence_id, artifact_type, artifact_id) VALUES (?, ?, ?, ?)",
                (tag_id, evidence_id, artifact_type, file_id),
            )

        associate(tag("any"), ids[0])
        associate(tag("Untagged"), ids[1])
        associate(tag("Relevant"), ids[2])
        # Tagged only in another evidence: not tagged here.
        associate(tag("Relevant", OTHER_EVIDENCE), ids[3], OTHER_EVIDENCE)
        # Association of this evidence to another evidence's tag.
        associate(tag("Foreign", OTHER_EVIDENCE), ids[4])
        # A URL with the same id is tagged, not the file.
        associate(tag("Relevant"), ids[5], artifact_type="url")
        gone = tag("Gone")
        associate(gone, ids[6])
        conn.execute(
            "INSERT INTO file_list_matches(evidence_id, file_list_id, reference_list_name, match_type, "
            "matched_value, matched_at) VALUES (?, ?, 'list1', 'path', 'x', '2025-01-01T00:00:00Z')",
            (ev, ids[7]),
        )
    # Association to a deleted tag (PRAGMA foreign_keys is a no-op inside a
    # transaction, so it is set outside one).
    conn.execute("PRAGMA foreign_keys = OFF")
    with conn:
        conn.execute("DELETE FROM tags WHERE id = ?", (gone,))
    conn.execute("PRAGMA foreign_keys = ON")
    assert conn.execute("SELECT COUNT(*) FROM tag_associations WHERE tag_id = ?", (gone,)).fetchone()[0] == 1
    manager.close_all()
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data, ids
    data.close()
    discard_case_stats_cache(case.case_db_path)


def _raw(data: CaseDataAccess, evidence_id: int) -> sqlite3.Connection:
    return data._db_manager.get_evidence_conn(evidence_id, data.get_evidence_label(evidence_id))


class Oracle:
    """Python reimplementation of the File List filters."""

    def __init__(self, conn: sqlite3.Connection, evidence_id: int) -> None:
        self.rows = [
            dict(zip(("id", "file_path", "file_name", "extension", "size_bytes", "deleted"), row))
            for row in conn.execute(
                "SELECT id, file_path, file_name, extension, size_bytes, deleted FROM file_list WHERE evidence_id = ?",
                (evidence_id,),
            )
        ]
        assoc = conn.execute(
            "SELECT ta.artifact_id, ta.evidence_id, t.evidence_id, t.name FROM tag_associations ta "
            "LEFT JOIN tags t ON t.id = ta.tag_id WHERE ta.artifact_type = 'file_list'"
        ).fetchall()
        # Tags column: association and tag both of this evidence.
        self.tagged: Set[int] = {a for a, ta_ev, t_ev, _ in assoc if ta_ev == evidence_id and t_ev == evidence_id}
        self.tag_names: Dict[int, Set[str]] = {}
        for artifact_id, _, t_ev, name in assoc:
            if t_ev is not None:
                self.tag_names.setdefault(artifact_id, set()).add(name)
        self.matches: Dict[int, Set[str]] = {}
        for file_id, name in conn.execute("SELECT file_list_id, reference_list_name FROM file_list_matches"):
            self.matches.setdefault(file_id, set()).add(name)

    def select(self, spec: FileListFilterSpec) -> List[int]:
        def contains(row: Dict[str, Any], term: str) -> bool:
            return _fold(term) in _fold(row["file_name"]) or _fold(term) in _fold(row["file_path"])

        def keep(row: Dict[str, Any]) -> bool:
            if spec.extension is not None and row["extension"] != spec.extension:
                return False
            size = row["size_bytes"]
            if spec.size_min is not None and (size is None or size < spec.size_min):
                return False
            if spec.size_max is not None and (size is None or size > spec.size_max):
                return False
            if spec.deleted == "show_only" and row["deleted"] != 1:
                return False
            if spec.deleted == "hide" and row["deleted"] != 0:
                return False
            lists = self.matches.get(row["id"], set())
            if spec.match_list == FILE_LIST_ANY_MATCH and not lists:
                return False
            if spec.match_list not in (None, FILE_LIST_ANY_MATCH) and spec.match_list not in lists:
                return False
            if spec.tag_name is not None and spec.tag_name not in self.tag_names.get(row["id"], set()):
                return False
            if spec.any_tag and row["id"] not in self.tagged:
                return False
            if spec.untagged and row["id"] in self.tagged:
                return False
            if spec.search is not None and not contains(row, spec.search):
                return False
            if spec.exclude is not None and contains(row, spec.exclude):
                return False
            return True

        selected = [row for row in self.rows if keep(row)]
        selected.sort(key=lambda row: (row["file_path"], row["id"]))
        return [row["id"] for row in selected]


def _legacy_ids(conn: sqlite3.Connection, evidence_id: int, filters: Dict[str, Any]) -> List[int]:
    """The File List model's filter SQL before issue #73 (verbatim logic)."""
    filter_clauses = []
    params: List[Any] = [evidence_id]
    if filters.get("extension"):
        filter_clauses.append("fl.extension = ?")
        params.append(filters["extension"])
    if filters.get("size_min") is not None:
        filter_clauses.append("fl.size_bytes >= ?")
        params.append(filters["size_min"])
    if filters.get("size_max") is not None:
        filter_clauses.append("fl.size_bytes <= ?")
        params.append(filters["size_max"])
    if filters.get("deleted") == "show_only":
        filter_clauses.append("fl.deleted = 1")
    elif filters.get("deleted") == "hide":
        filter_clauses.append("fl.deleted = 0")
    if filters.get("matches"):
        if filters["matches"] == "any":
            filter_clauses.append("EXISTS (SELECT 1 FROM file_list_matches WHERE file_list_id = fl.id)")
        else:
            filter_clauses.append(
                "EXISTS (SELECT 1 FROM file_list_matches WHERE file_list_id = fl.id AND reference_list_name = ?)"
            )
            params.append(filters["matches"])
    if filters.get("tags"):
        if filters["tags"] == "any":
            filter_clauses.append(
                "EXISTS (SELECT 1 FROM tag_associations ta WHERE ta.artifact_type = 'file_list' AND ta.artifact_id = fl.id)"
            )
        else:
            filter_clauses.append(
                "EXISTS (SELECT 1 FROM tag_associations ta JOIN tags t ON ta.tag_id = t.id "
                "WHERE ta.artifact_type = 'file_list' AND ta.artifact_id = fl.id AND t.name = ?)"
            )
            params.append(filters["tags"])
    if filters.get("search"):
        filter_clauses.append("(fl.file_name LIKE ? OR fl.file_path LIKE ?)")
        params.extend([f"%{filters['search']}%"] * 2)
    clause = ("AND " + " AND ".join(filter_clauses)) if filter_clauses else ""
    sql = f"SELECT fl.id FROM file_list fl WHERE fl.evidence_id = ? {clause} ORDER BY fl.file_path, fl.id"
    return [row[0] for row in conn.execute(sql, params)]


def _rows(data: CaseDataAccess, evidence_id: int, spec: FileListFilterSpec) -> List[int]:
    return [row["id"] for row in data.list_file_list_rows(evidence_id, spec, limit=100_000)]


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------


def test_spec_normalizes_empty_values():
    spec = FileListFilterSpec(extension="", match_list="", tag_name="", search="", exclude="", deleted="bogus")
    assert spec == FileListFilterSpec()
    assert FileListFilterSpec(untagged=1).untagged is True
    assert not FileListFilterSpec().tag_filtered
    assert FileListFilterSpec(untagged=True).tag_filtered and FileListFilterSpec(any_tag=True).tag_filtered


def test_predicates_escape_search_and_exclude():
    where, params = build_file_list_filter_predicates(1, FileListFilterSpec(search="a%_\\", exclude="b_"))
    assert params == [1, "%a\\%\\_\\\\%", "%a\\%\\_\\\\%", "%b\\_%", "%b\\_%"]
    assert where[-1].startswith("NOT (")


# ---------------------------------------------------------------------------
# Unchanged filters (pinned against the pre-#73 SQL)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "legacy, spec",
    [
        ({}, FileListFilterSpec()),
        ({"extension": "dll"}, FileListFilterSpec(extension="dll")),
        ({"extension": "TXT"}, FileListFilterSpec(extension="TXT")),
        ({"size_min": 1_048_576}, FileListFilterSpec(size_min=1_048_576)),
        ({"size_max": 1_000_000}, FileListFilterSpec(size_max=1_000_000)),
        ({"deleted": "show_only"}, FileListFilterSpec(deleted="show_only")),
        ({"deleted": "hide"}, FileListFilterSpec(deleted="hide")),
        ({"matches": "any"}, FileListFilterSpec(match_list=FILE_LIST_ANY_MATCH)),
        ({"matches": "list1"}, FileListFilterSpec(match_list="list1")),
        ({"tags": "Relevant"}, FileListFilterSpec(tag_name="Relevant")),
        ({"tags": "Untagged"}, FileListFilterSpec(tag_name="Untagged")),
        ({"tags": "Foreign"}, FileListFilterSpec(tag_name="Foreign")),
        # Search without %, _ or \ matches as before
        ({"search": "WinSxS"}, FileListFilterSpec(search="WinSxS")),
        ({"search": "readme"}, FileListFilterSpec(search="readme")),
        ({"extension": "txt", "deleted": "hide", "search": "temp"},
         FileListFilterSpec(extension="txt", deleted="hide", search="temp")),
    ],
)
def test_unchanged_filters_match_legacy_sql(fl_case, legacy, spec):
    case, data, _ = fl_case
    conn = _raw(data, case.evidence_id)
    expected = _legacy_ids(conn, case.evidence_id, legacy)
    assert _rows(data, case.evidence_id, spec) == expected
    assert data.count_file_list(case.evidence_id, spec) == len(expected)


def test_tag_named_any_is_a_normal_tag(fl_case):
    case, data, ids = fl_case
    assert _rows(data, case.evidence_id, FileListFilterSpec(tag_name="any")) == [ids[0]]
    assert _rows(data, case.evidence_id, FileListFilterSpec(tag_name="Untagged")) == [ids[1]]


def test_any_tag_only_drops_associations_without_a_tag_of_this_evidence(fl_case):
    case, data, ids = fl_case
    conn = _raw(data, case.evidence_id)
    legacy = set(_legacy_ids(conn, case.evidence_id, {"tags": "any"}))
    now = set(_rows(data, case.evidence_id, FileListFilterSpec(any_tag=True)))
    # Other-evidence association, foreign tag, deleted tag
    assert legacy - now == {ids[3], ids[4], ids[6]}
    assert now <= legacy


def test_literal_search_differs_from_legacy_wildcards(fl_case):
    case, data, ids = fl_case
    conn = _raw(data, case.evidence_id)
    assert ids[3] in _legacy_ids(conn, case.evidence_id, {"search": "a_b"})  # "_" was a wildcard
    assert _rows(data, case.evidence_id, FileListFilterSpec(search="a_b")) == [ids[2]]
    assert _rows(data, case.evidence_id, FileListFilterSpec(search="100%_")) == [ids[0]]


# ---------------------------------------------------------------------------
# Search / Exclude
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "term", ["%", "_", "\\", "100%_", "a_b", "README", "readme", "ärger", "Ärger", "needle", "back\\slash", "WINSXS", ":\\"]
)
def test_search_and_exclude_against_oracle(fl_case, term):
    case, data, _ = fl_case
    oracle = Oracle(_raw(data, case.evidence_id), case.evidence_id)
    total = data.count_file_list(case.evidence_id)
    search = FileListFilterSpec(search=term)
    exclude = FileListFilterSpec(exclude=term)
    assert _rows(data, case.evidence_id, search) == oracle.select(search)
    assert _rows(data, case.evidence_id, exclude) == oracle.select(exclude)
    # Complement: Search X + Exclude X = all, and both together = nothing
    assert data.count_file_list(case.evidence_id, search) + data.count_file_list(case.evidence_id, exclude) == total
    assert data.count_file_list(case.evidence_id, FileListFilterSpec(search=term, exclude=term)) == 0


def test_exclude_hits_name_only_and_path_only(fl_case):
    case, data, ids = fl_case
    shown = set(_rows(data, case.evidence_id, FileListFilterSpec(exclude="NEEDLE")))
    assert ids[7] not in shown and ids[8] not in shown
    assert ids[9] in shown


def test_exclude_is_ascii_case_insensitive_only(fl_case):
    case, data, ids = fl_case
    shown = set(_rows(data, case.evidence_id, FileListFilterSpec(exclude="ärger")))
    assert ids[5] in shown  # "Ärger": non-ASCII case is not folded
    assert ids[6] not in shown


def test_search_plus_exclude_combine(fl_case):
    case, data, _ = fl_case
    spec = FileListFilterSpec(search=".dll", exclude="WinSxS")
    rows = data.list_file_list_rows(case.evidence_id, spec)
    assert rows and all(".dll" in r["file_name"].lower() and "winsxs" not in r["file_path"].lower() for r in rows)
    assert [r["id"] for r in rows] == Oracle(_raw(data, case.evidence_id), case.evidence_id).select(spec)


# ---------------------------------------------------------------------------
# Untagged / Any Tag
# ---------------------------------------------------------------------------


def test_untagged_is_exactly_the_empty_tags_column(fl_case):
    case, data, ids = fl_case
    untagged = set(_rows(data, case.evidence_id, FileListFilterSpec(untagged=True)))
    every = _rows(data, case.evidence_id, FileListFilterSpec())
    for file_id in every:
        assert (data.get_artifact_tags_str(case.evidence_id, "file_list", file_id) == "") == (file_id in untagged)
    for index in (3, 4, 5, 6):
        assert ids[index] in untagged
    for index in (0, 1, 2):
        assert ids[index] not in untagged


def test_any_tag_and_untagged_are_complements(fl_case):
    case, data, _ = fl_case
    any_tag = _rows(data, case.evidence_id, FileListFilterSpec(any_tag=True))
    untagged = _rows(data, case.evidence_id, FileListFilterSpec(untagged=True))
    every = _rows(data, case.evidence_id, FileListFilterSpec())
    assert not set(any_tag) & set(untagged)
    assert sorted(any_tag + untagged) == sorted(every)
    assert data.count_file_list(case.evidence_id, FileListFilterSpec(any_tag=True, untagged=True)) == 0


def test_tag_name_keeps_matching_without_evidence_check(fl_case):
    """The exact-tag filter is unchanged (issue #73): it also matches a file
    tagged only with another evidence's tag of that name, whose Tags cell is
    empty."""
    case, data, ids = fl_case
    relevant = set(_rows(data, case.evidence_id, FileListFilterSpec(tag_name="Relevant")))
    assert ids[2] in relevant and ids[3] in relevant
    assert _rows(data, case.evidence_id, FileListFilterSpec(tag_name="Relevant", untagged=True)) == [ids[3]]


# ---------------------------------------------------------------------------
# Combinations, paging, missing DB
# ---------------------------------------------------------------------------

TAG_MODES = [
    {},
    {"any_tag": True},
    {"untagged": True},
    {"tag_name": "Relevant"},
    {"tag_name": "any"},
    {"tag_name": "Untagged"},
]


def test_every_filter_combination_matches_oracle(fl_case):
    case, data, _ = fl_case
    oracle = Oracle(_raw(data, case.evidence_id), case.evidence_id)
    checked = 0
    for ext, size_min, deleted, match, tags, search, exclude in itertools.product(
        [None, "txt"],
        [None, 1_048_576],
        ["all", "hide", "show_only"],
        [None, FILE_LIST_ANY_MATCH, "list1"],
        TAG_MODES,
        [None, "windows", "%"],
        [None, "winsxs", "_"],
    ):
        spec = FileListFilterSpec(
            extension=ext, size_min=size_min, deleted=deleted, match_list=match, search=search, exclude=exclude, **tags
        )
        expected = oracle.select(spec)
        assert _rows(data, case.evidence_id, spec) == expected, spec
        assert data.count_file_list(case.evidence_id, spec) == len(expected), spec
        checked += bool(expected)
    assert checked > 100  # the fixture exercises non-empty results


def test_pages_concatenate_to_the_full_ordered_list(fl_case):
    case, data, ids = fl_case
    spec = FileListFilterSpec(exclude="System32")
    full = _rows(data, case.evidence_id, spec)
    pages: List[int] = []
    offset = 0
    while True:
        page = data.list_file_list_rows(case.evidence_id, spec, offset=offset, limit=7)
        if not page:
            break
        pages.extend(row["id"] for row in page)
        offset += len(page)
    assert pages == full
    # Duplicate paths are ordered by id
    assert full.index(ids[10]) + 1 == full.index(ids[11])


def test_missing_evidence_db_returns_empty_without_creating_it(fl_case):
    case, data, _ = fl_case
    conn = data._connect_case()
    with conn:
        missing = conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (1, 'Missing', '/x', 'now')"
        ).lastrowid
    conn.close()
    assert data.list_file_list_rows(missing, FileListFilterSpec(untagged=True)) == []
    assert data.count_file_list(missing) == 0
    assert data.get_file_list_matches_str(missing, 1) == ""
    assert data.list_file_list_filter_values(missing) == {"extensions": [], "matches": [], "tags": []}
    assert not data._evidence_db_exists(missing)


def test_matches_column_and_filter_values(fl_case):
    case, data, ids = fl_case
    assert data.get_file_list_matches_str(case.evidence_id, ids[7]) == "list1"
    assert data.get_file_list_matches_str(case.evidence_id, ids[8]) == ""
    # Empty filter cache: extensions and tags fall back to DISTINCT queries
    values = data.list_file_list_filter_values(case.evidence_id)
    assert {"dll", "txt", "TXT", "bin"} <= set(values["extensions"])
    assert {"Relevant", "any", "Untagged", "Foreign"} <= set(values["tags"])
    assert "Gone" not in values["tags"]
    # Match lists come from the cache filled by matching
    conn = _raw(data, case.evidence_id)
    with conn:
        conn.executemany(
            "INSERT INTO file_list_filter_cache(evidence_id, filter_type, filter_value, count, last_updated) "
            "VALUES (?, ?, ?, 1, '2025-01-01T00:00:00Z')",
            [(case.evidence_id, "match", "list1"), (case.evidence_id, "match", "list0"), (case.evidence_id, "extension", "dll")],
        )
    values = data.list_file_list_filter_values(case.evidence_id)
    assert values["matches"] == ["list0", "list1"]
    assert values["extensions"] == ["dll"]
