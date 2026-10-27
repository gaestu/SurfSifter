"""URL substring search index helper (issue #62).

Raw evidence connections (migrations + ``ensure_url_search_index``). The
index must hold exactly the URL rows with ``id <= W`` (watermark) with their
current URL; ``_assert_index_exact`` checks that at any build state against a
freshly built reference index, and FTS5 ``integrity-check`` checks a fully
built index against the ``urls`` content table.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

import pytest

import core.database.helpers.url_search_index as usi
from core.database import EVIDENCE_MIGRATIONS_DIR, migrate
from core.database.helpers import deduplicate_urls, delete_urls_by_run, insert_urls
from core.database.helpers.batch import purge_evidence_data
from core.database.helpers.url_search_index import (
    URL_SEARCH_INDEX_TABLE,
    URL_SEARCH_STATE_TABLE,
    UrlSearchIndexStatus,
    build_url_search_index_step,
    ensure_url_search_index,
    get_url_search_index_status,
    plan_url_search_candidates,
    reset_url_search_index,
    url_search_index_supported,
    url_search_match_query,
)

pytestmark = pytest.mark.skipif(
    not url_search_index_supported(), reason="SQLite lacks FTS5 with the trigram tokenizer"
)

TRIGGERS = ("urls_search_index_ad", "urls_search_index_au")
INDEX_OBJECTS = {URL_SEARCH_STATE_TABLE, URL_SEARCH_INDEX_TABLE, *TRIGGERS}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _migrated_template(tmp_path_factory) -> Path:
    """Evidence database with all migrations (copied per test: migrating is slow)."""
    path = tmp_path_factory.mktemp("url_search_index") / "template.sqlite"
    conn = sqlite3.connect(path)
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    conn.close()
    return path


_TEMPLATE: Optional[Path] = None


@pytest.fixture(autouse=True)
def _template(_migrated_template):
    global _TEMPLATE
    _TEMPLATE = _migrated_template


def _connect_copy(tmp_path: Path, name: str, **kwargs) -> sqlite3.Connection:
    target = tmp_path / name
    shutil.copy(_TEMPLATE, target)
    conn = sqlite3.connect(target, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


def _new_db(tmp_path: Path, name: str = "evidence.sqlite", *, ensure: bool = True) -> sqlite3.Connection:
    conn = _connect_copy(tmp_path, name)
    if ensure:
        assert ensure_url_search_index(conn) is True
    return conn


def _migrate_before(conn: sqlite3.Connection, tmp_path: Path, version: int) -> None:
    previous = tmp_path / f"migrations_before_{version}"
    previous.mkdir()
    for path in sorted(EVIDENCE_MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name.split("_", 1)[0]) < version:
            shutil.copy(path, previous / path.name)
    migrate(conn, migrations_dir=previous)


def _add(conn: sqlite3.Connection, urls: Iterable[str], *, evidence_id: int = 1, **columns) -> List[int]:
    ids = []
    for url in urls:
        row = {"discovered_by": "src", **columns, "evidence_id": evidence_id, "url": url}
        placeholders = ",".join("?" for _ in row)
        ids.append(
            conn.execute(f"INSERT INTO urls({','.join(row)}) VALUES ({placeholders})", list(row.values())).lastrowid
        )
    conn.commit()
    return ids


def _corpus(count: int, prefix: str = "https://ex.com") -> List[str]:
    return [f"{prefix}/p{i}?q={i % 7}&Tag=V{i % 3}" for i in range(count)]


def _watermark(conn: sqlite3.Connection) -> int:
    return conn.execute(f"SELECT indexed_through_id FROM {URL_SEARCH_STATE_TABLE} WHERE id = 1").fetchone()[0]


def _objects(conn: sqlite3.Connection) -> set:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master")} & INDEX_OBJECTS


def _integrity_ok(conn: sqlite3.Connection) -> bool:
    """FTS5 integrity-check incl. content table (fails while a tail is unindexed)."""
    try:
        conn.execute(
            f"INSERT INTO {URL_SEARCH_INDEX_TABLE}({URL_SEARCH_INDEX_TABLE}, rank) VALUES ('integrity-check', 1)"
        )
    except sqlite3.DatabaseError:
        conn.rollback()
        return False
    conn.commit()
    return True


def _index_instances(conn: sqlite3.Connection, schema: str, table: str) -> set:
    vocab = f"vocab_{table}"
    conn.execute(f"CREATE VIRTUAL TABLE temp.{vocab} USING fts5vocab({schema}, {table}, instance)")
    try:
        return set(conn.execute(f"SELECT term, doc FROM temp.{vocab}"))
    finally:
        conn.execute(f"DROP TABLE temp.{vocab}")


def _indexed_docs(conn: sqlite3.Connection) -> set:
    return {doc for _term, doc in _index_instances(conn, "main", URL_SEARCH_INDEX_TABLE)}


def _assert_index_exact(conn: sqlite3.Connection) -> None:
    """The index holds exactly the current URLs with id <= W (any build state)."""
    watermark = _watermark(conn)
    conn.execute("CREATE VIRTUAL TABLE temp.ref_index USING fts5(url, tokenize='trigram', detail=none)")
    try:
        conn.execute(
            "INSERT INTO temp.ref_index(rowid, url) SELECT id, url FROM main.urls WHERE id <= ?", (watermark,)
        )
        conn.commit()
        expected = _index_instances(conn, "temp", "ref_index")
    finally:
        conn.execute("DROP TABLE temp.ref_index")
    actual = _index_instances(conn, "main", URL_SEARCH_INDEX_TABLE)
    assert actual == expected
    assert all(doc <= watermark for _term, doc in actual)


def _build_all(conn: sqlite3.Connection, batch_size: int = 1000) -> int:
    total = 0
    while True:
        indexed = build_url_search_index_step(conn, batch_size)
        if not indexed:
            return total
        total += indexed


def _candidates(conn: sqlite3.Connection, text: str, **kwargs) -> Optional[set]:
    plan = plan_url_search_candidates(conn, text, **kwargs)
    if plan is None:
        return None
    sql, params = plan
    return {row[0] for row in conn.execute(sql, params)}


def _ascii_lower(text: str) -> str:
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in text)


def _like_ids(conn: sqlite3.Connection, text: str) -> set:
    """Rows containing ``text`` under the search contract (ASCII case-insensitive)."""
    needle = _ascii_lower(text)
    return {row_id for row_id, url in conn.execute("SELECT id, url FROM urls") if needle in _ascii_lower(url)}


class _HookedConnection(sqlite3.Connection):
    """Connection that runs ``hook(sql)`` before executing matching statements."""

    hook = None

    def execute(self, sql, *args):  # type: ignore[override]
        if self.hook is not None:
            self.hook(self, sql)
        return super().execute(sql, *args)


def _hooked_db(tmp_path: Path) -> _HookedConnection:
    conn = _connect_copy(tmp_path, "hooked.sqlite", factory=_HookedConnection)
    assert ensure_url_search_index(conn)
    return conn


# ---------------------------------------------------------------------------
# Support detection, ensure, status
# ---------------------------------------------------------------------------


def test_runtime_supports_trigram_index():
    assert url_search_index_supported() is True
    assert url_search_index_supported() is True  # cached


def test_fresh_migration_creates_state_row_but_no_index_objects(tmp_path):
    conn = _new_db(tmp_path, ensure=False)
    assert conn.execute(f"SELECT id, indexed_through_id FROM {URL_SEARCH_STATE_TABLE}").fetchall() == [(1, 0)]
    assert _objects(conn) == {URL_SEARCH_STATE_TABLE}
    assert get_url_search_index_status(conn) == UrlSearchIndexStatus(UrlSearchIndexStatus.UNAVAILABLE)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(f"INSERT INTO {URL_SEARCH_STATE_TABLE}(id, indexed_through_id) VALUES (2, 0)")
    conn.close()


def test_ensure_creates_objects_and_is_idempotent(tmp_path):
    conn = _new_db(tmp_path, ensure=False)
    assert ensure_url_search_index(conn) is True
    assert _objects(conn) == INDEX_OBJECTS
    types = dict(conn.execute("SELECT name, type FROM sqlite_master WHERE name IN (?, ?, ?)", (URL_SEARCH_INDEX_TABLE, *TRIGGERS)))
    assert types == {URL_SEARCH_INDEX_TABLE: "table", TRIGGERS[0]: "trigger", TRIGGERS[1]: "trigger"}
    assert not conn.in_transaction

    ids = _add(conn, _corpus(10))
    _build_all(conn)
    schema_before = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()

    # Re-opening a ready database keeps the built index (no reset).
    for _ in range(3):
        assert ensure_url_search_index(conn) is True
    assert conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall() == schema_before
    assert _watermark(conn) == ids[-1]
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    conn.close()


def test_ensure_returns_false_before_migration_0006(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.sqlite")
    _migrate_before(conn, tmp_path, 6)
    assert ensure_url_search_index(conn) is False
    assert _objects(conn) == set()
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.UNAVAILABLE
    assert plan_url_search_candidates(conn, "example") is None
    assert build_url_search_index_step(conn) == 0
    assert reset_url_search_index(conn) is False
    conn.close()


def test_ensure_returns_false_without_urls_table(tmp_path):
    conn = sqlite3.connect(tmp_path / "minimal.sqlite")
    assert ensure_url_search_index(conn) is False
    conn.execute(f"CREATE TABLE {URL_SEARCH_STATE_TABLE} (id INTEGER PRIMARY KEY, indexed_through_id INTEGER)")
    assert ensure_url_search_index(conn) is False
    assert _objects(conn) == {URL_SEARCH_STATE_TABLE}
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.UNAVAILABLE
    conn.close()


def test_ensure_never_raises(tmp_path):
    conn = _new_db(tmp_path, ensure=False)
    conn.execute("BEGIN")  # _write_transaction refuses an open transaction
    assert ensure_url_search_index(conn) is False
    conn.rollback()
    conn.close()
    assert ensure_url_search_index(conn) is False  # closed connection


@pytest.mark.parametrize("dropped", [URL_SEARCH_INDEX_TABLE, *TRIGGERS])
def test_ensure_repairs_missing_object_and_resets_coverage(tmp_path, dropped):
    conn = _new_db(tmp_path)
    _add(conn, _corpus(20))
    _build_all(conn)
    kind = "TABLE" if dropped == URL_SEARCH_INDEX_TABLE else "TRIGGER"
    conn.execute(f"DROP {kind} {dropped}")
    conn.commit()
    if kind == "TRIGGER":
        # Changes while a trigger was missing are not mirrored. (Without the
        # FTS table the remaining triggers make URL writes fail until repair.)
        conn.execute("DELETE FROM urls WHERE id IN (2, 3)")
        conn.execute("UPDATE urls SET url = 'https://changed.example/x' WHERE id = 5")
        conn.commit()

    assert ensure_url_search_index(conn) is True
    assert _objects(conn) == INDEX_OBJECTS
    assert _watermark(conn) == 0
    assert _indexed_docs(conn) == set()
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.BUILDING
    _build_all(conn)
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    conn.close()


def test_status_transitions_and_progress(tmp_path):
    conn = _new_db(tmp_path, ensure=False)
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.UNAVAILABLE
    assert ensure_url_search_index(conn)

    empty = get_url_search_index_status(conn)
    assert empty == UrlSearchIndexStatus(UrlSearchIndexStatus.READY, 0, 0, 0)
    assert empty.is_ready and empty.progress == 1.0

    ids = _add(conn, _corpus(10))
    status = get_url_search_index_status(conn)
    assert status == UrlSearchIndexStatus(UrlSearchIndexStatus.BUILDING, 0, ids[0], ids[-1])
    assert not status.is_ready and status.progress == 0.0

    assert build_url_search_index_step(conn, 4) == 4
    status = get_url_search_index_status(conn)
    assert status.state == UrlSearchIndexStatus.BUILDING
    assert status.indexed_through_id == ids[3]
    assert status.progress == pytest.approx(0.4)

    assert build_url_search_index_step(conn, 100) == 6
    status = get_url_search_index_status(conn)
    assert status == UrlSearchIndexStatus(UrlSearchIndexStatus.READY, ids[-1], ids[0], ids[-1])
    assert status.progress == 1.0

    # New URLs land above W: building again; deleting the tail makes it ready.
    new_ids = _add(conn, _corpus(2, "https://new.example"))
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.BUILDING
    conn.execute(f"DELETE FROM urls WHERE id IN ({','.join('?' * len(new_ids))})", new_ids)
    conn.commit()
    assert get_url_search_index_status(conn).is_ready
    conn.close()


def test_status_progress_bounds():
    S = UrlSearchIndexStatus
    assert S(S.UNSUPPORTED).progress == 0.0 and not S(S.UNSUPPORTED).is_ready
    assert S(S.UNAVAILABLE).progress == 0.0
    assert S(S.BUILDING, 0, 101, 200).progress == 0.0
    assert S(S.BUILDING, 150, 101, 200).progress == pytest.approx(0.5)
    assert S(S.BUILDING, 5, 101, 200).progress == 0.0  # W below the id range (rows deleted)
    assert S(S.BUILDING, 0, 0, 0).progress == 1.0  # empty span


def test_upgrade_from_0005_with_existing_urls(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.sqlite")
    _migrate_before(conn, tmp_path, 6)
    ids = _add(conn, _corpus(30))
    conn.execute("DELETE FROM urls WHERE id = ?", (ids[4],))
    conn.commit()

    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
    assert versions == list(range(1, len(versions) + 1)) and 6 in versions
    assert conn.execute(f"SELECT id, indexed_through_id FROM {URL_SEARCH_STATE_TABLE}").fetchall() == [(1, 0)]

    assert ensure_url_search_index(conn) is True
    status = get_url_search_index_status(conn)
    assert status.state == UrlSearchIndexStatus.BUILDING and status.indexed_through_id == 0
    # Not built yet: every URL is a candidate, search is still complete.
    assert _candidates(conn, "p12?") == {row[0] for row in conn.execute("SELECT id FROM urls")}

    assert _build_all(conn, 7) == 29
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    assert _candidates(conn, "p12?") == _like_ids(conn, "p12?") == {ids[12]}

    # Re-running migrations and the 0006 script is a no-op.
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    (script,) = EVIDENCE_MIGRATIONS_DIR.glob("0006_*.sql")
    conn.executescript(script.read_text())
    assert _watermark(conn) == ids[-1]
    assert _integrity_ok(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Build steps
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch_size", [1, 3, 1000])
def test_build_steps_advance_watermark_by_batch(tmp_path, batch_size):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(17))
    gone = {ids[2], ids[3], ids[10]}  # id gaps
    conn.executemany("DELETE FROM urls WHERE id = ?", [(i,) for i in gone])
    conn.commit()
    remaining = [i for i in ids if i not in gone]

    steps = []
    while True:
        indexed = build_url_search_index_step(conn, batch_size)
        assert not conn.in_transaction
        if not indexed:
            break
        steps.append(indexed)
        expected_w = remaining[min(sum(steps), len(remaining)) - 1]
        assert _watermark(conn) == expected_w
        _assert_index_exact(conn)

    assert sum(steps) == len(remaining)
    assert all(step == batch_size for step in steps[:-1]) and steps[-1] <= batch_size
    assert build_url_search_index_step(conn, batch_size) == 0
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    conn.close()


def test_build_step_validates_arguments_and_transaction_state(tmp_path):
    conn = _new_db(tmp_path)
    _add(conn, _corpus(3))
    with pytest.raises(ValueError):
        build_url_search_index_step(conn, 0)
    conn.execute("INSERT INTO urls(evidence_id, url, discovered_by) VALUES (1, 'x', 's')")
    assert conn.in_transaction
    with pytest.raises(RuntimeError):
        build_url_search_index_step(conn, 10)
    conn.rollback()
    assert _watermark(conn) == 0
    assert build_url_search_index_step(conn, 10) == 3
    conn.close()


def test_build_step_failure_rolls_back_batch_and_resumes(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(12))
    assert build_url_search_index_step(conn, 4) == 4
    docs_before = _indexed_docs(conn)

    # Fail after the batch rows were inserted into the index (watermark update).
    conn.execute(
        f"CREATE TEMP TRIGGER fail_watermark BEFORE UPDATE ON main.{URL_SEARCH_STATE_TABLE} "
        "BEGIN SELECT RAISE(ABORT, 'simulated crash'); END"
    )
    with pytest.raises(sqlite3.DatabaseError, match="simulated crash"):
        build_url_search_index_step(conn, 4)
    assert not conn.in_transaction
    assert _watermark(conn) == ids[3]
    assert _indexed_docs(conn) == docs_before
    _assert_index_exact(conn)
    conn.execute("DROP TRIGGER temp.fail_watermark")

    assert _build_all(conn, 4) == 8
    assert _integrity_ok(conn)
    conn.close()


def test_build_step_interrupted_mid_insert_rolls_back(tmp_path):
    conn = _hooked_db(tmp_path)
    ids = _add(conn, _corpus(40))
    assert build_url_search_index_step(conn, 10) == 10
    docs_before = _indexed_docs(conn)

    calls = {"n": 0}

    def interrupt_index_insert(c, sql):
        if sql.startswith(f"INSERT INTO {URL_SEARCH_INDEX_TABLE}(rowid"):
            def handler():
                calls["n"] += 1
                return 1 if calls["n"] > 5 else 0  # partway into the INSERT
            c.set_progress_handler(handler, 50)

    conn.hook = interrupt_index_insert
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        build_url_search_index_step(conn, 20)
    conn.hook = None
    conn.set_progress_handler(None, 0)
    assert calls["n"] > 5
    assert not conn.in_transaction
    assert _watermark(conn) == ids[9]
    assert _indexed_docs(conn) == docs_before

    assert _build_all(conn, 20) == 30
    assert _integrity_ok(conn)
    conn.close()


def test_build_from_zero_clears_stale_entries(tmp_path):
    conn = _new_db(tmp_path)
    _add(conn, _corpus(10))
    _build_all(conn)
    # Simulate a coverage reset that did not clear the index (see _ensure).
    conn.execute(f"UPDATE {URL_SEARCH_STATE_TABLE} SET indexed_through_id = 0")
    conn.commit()
    for name in TRIGGERS:
        conn.execute(f"DROP TRIGGER {name}")
    conn.execute("DELETE FROM urls WHERE id IN (1, 2)")
    conn.execute("UPDATE urls SET url = 'https://other.example' WHERE id = 3")
    conn.commit()
    ensure_url_search_index(conn)

    assert build_url_search_index_step(conn, 3) == 3
    _assert_index_exact(conn)
    _build_all(conn)
    assert _integrity_ok(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Triggers and writers
# ---------------------------------------------------------------------------


def _partially_built(tmp_path: Path, count: int = 30, built: int = 15):
    conn = _new_db(tmp_path)
    conn.row_factory = sqlite3.Row
    ids = _add(conn, _corpus(count))
    assert build_url_search_index_step(conn, built) == built
    assert _watermark(conn) == ids[built - 1]
    return conn, ids


def test_triggers_keep_indexed_range_exact(tmp_path):
    conn, ids = _partially_built(tmp_path)
    w = _watermark(conn)

    def change(sql, params=()):
        conn.execute(sql, params)
        conn.commit()
        _assert_index_exact(conn)

    change("DELETE FROM urls WHERE id = ?", (ids[2],))  # indexed
    change("DELETE FROM urls WHERE id = ?", (ids[20],))  # tail
    change("UPDATE urls SET url = 'https://renamed.example/ZZZ' WHERE id = ?", (ids[4],))  # indexed
    change("UPDATE urls SET url = 'https://renamed.example/YYY' WHERE id = ?", (ids[21],))  # tail
    change("UPDATE urls SET notes = 'n', domain = 'd', first_seen_utc = '2024' WHERE id = ?", (ids[5],))
    change("UPDATE urls SET url = url WHERE id = ?", (ids[6],))  # same value
    change("UPDATE urls SET id = ? WHERE id = ?", (ids[-1] + 100, ids[7]))  # indexed -> tail
    change("UPDATE urls SET id = ? WHERE id = ?", (ids[2], ids[22]))  # tail -> indexed gap
    change("UPDATE urls SET url = url || '/more' WHERE id <= ?", (w,))  # bulk
    change("DELETE FROM urls WHERE id % 3 = 0")  # bulk mixed

    def indexed_candidates(text):
        return {c for c in _candidates(conn, text) if c <= w}

    assert indexed_candidates("renamed.example/zzz") == {ids[4]}
    assert ids[4] not in indexed_candidates("ex.com/p4?q")
    assert ids[7] not in _candidates(conn, "ex.com/p7?q")  # moved row is a tail candidate under its new id
    assert ids[-1] + 100 in _candidates(conn, "ex.com/p7?q")

    _build_all(conn, 4)
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    conn.close()


def test_rolled_back_writer_transaction_leaves_index_unchanged(tmp_path):
    conn, ids = _partially_built(tmp_path)
    before = _index_instances(conn, "main", URL_SEARCH_INDEX_TABLE)

    conn.execute("BEGIN")
    conn.execute("INSERT INTO urls(evidence_id, url, discovered_by) VALUES (1, 'https://tmp.example', 's')")
    conn.execute("DELETE FROM urls WHERE id IN (?, ?)", (ids[1], ids[25]))
    conn.execute("UPDATE urls SET url = 'https://rolled.example' WHERE id IN (?, ?)", (ids[3], ids[26]))
    assert _candidates(conn, "rolled.example") == {ids[3], *[i for i in ids[15:] if i != ids[25]], ids[-1] + 1}
    conn.rollback()

    assert _index_instances(conn, "main", URL_SEARCH_INDEX_TABLE) == before
    _assert_index_exact(conn)
    _build_all(conn)
    assert _integrity_ok(conn)
    conn.close()


def test_deduplicate_urls_keeps_index_exact(tmp_path):
    conn = _new_db(tmp_path)
    conn.row_factory = sqlite3.Row
    urls = [f"https://dup.example/{i % 6}" for i in range(30)]
    _add(conn, urls[:20], discovered_by="history")
    _build_all(conn)
    _add(conn, urls[20:], discovered_by="history")  # duplicates in the tail too
    result = deduplicate_urls(conn, 1, ["history"], unique_by_first_seen=False, unique_by_last_seen=False)
    assert result["total_after"] == 6
    _assert_index_exact(conn)
    _build_all(conn)
    assert _integrity_ok(conn)
    conn.close()


def test_delete_urls_by_run_keeps_index_exact(tmp_path):
    conn = _new_db(tmp_path)
    records = [{"url": u, "discovered_by": "s"} for u in _corpus(20)]
    insert_urls(conn, 1, records[:10], run_id="run-a")
    insert_urls(conn, 1, records[10:], run_id="run-b")
    conn.commit()
    build_url_search_index_step(conn, 15)
    delete_urls_by_run(conn, 1, "run-b")  # indexed + tail rows
    _assert_index_exact(conn)
    assert conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 10
    delete_urls_by_run(conn, 1, "run-a")
    _assert_index_exact(conn)
    assert conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 0
    assert get_url_search_index_status(conn).is_ready
    assert _integrity_ok(conn)
    conn.close()


def test_delete_urls_by_run_count_excludes_index_maintenance(tmp_path):
    conn = _new_db(tmp_path)
    insert_urls(conn, 1, [{"url": u, "discovered_by": "s"} for u in _corpus(10)], run_id="run-a")
    _build_all(conn)
    assert delete_urls_by_run(conn, 1, "run-a") == 10
    conn.close()


def test_purge_evidence_data_keeps_index_exact_and_new_ids_land_in_tail(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(12))
    _add(conn, _corpus(3, "https://other-evidence.example"), evidence_id=2)
    _build_all(conn)
    assert purge_evidence_data(conn, 1) >= 12
    _assert_index_exact(conn)
    assert _integrity_ok(conn)

    new_ids = _add(conn, _corpus(4))
    assert min(new_ids) > ids[-1]  # AUTOINCREMENT: new rows are always in the tail
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.BUILDING
    _build_all(conn)
    assert _integrity_ok(conn)
    conn.close()


def test_reset_clears_index_and_rebuild_is_exact(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(15))
    _build_all(conn)
    assert reset_url_search_index(conn) is True
    assert not conn.in_transaction
    assert _watermark(conn) == 0
    assert _indexed_docs(conn) == set()
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.BUILDING
    assert _candidates(conn, "p3?q") == set(ids)  # all tail
    _build_all(conn, 4)
    assert _integrity_ok(conn)
    assert _candidates(conn, "p3?q") == _like_ids(conn, "p3?q")
    conn.close()


# ---------------------------------------------------------------------------
# Unsupported runtime
# ---------------------------------------------------------------------------


def test_unsupported_runtime_drops_triggers_then_supported_runtime_rebuilds(tmp_path, monkeypatch):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(20))
    _build_all(conn)

    monkeypatch.setattr(usi, "url_search_index_supported", lambda: False)
    assert ensure_url_search_index(conn) is False
    assert _objects(conn) == {URL_SEARCH_STATE_TABLE, URL_SEARCH_INDEX_TABLE}
    assert _watermark(conn) == 0
    assert get_url_search_index_status(conn) == UrlSearchIndexStatus(UrlSearchIndexStatus.UNSUPPORTED)
    assert plan_url_search_candidates(conn, "example") is None
    assert build_url_search_index_step(conn) == 0
    assert reset_url_search_index(conn) is False
    assert ensure_url_search_index(conn) is False  # repeated open: still nothing to do

    # URL writes keep working and are not mirrored (no triggers).
    conn.execute("DELETE FROM urls WHERE id IN (?, ?)", (ids[0], ids[5]))
    conn.execute("UPDATE urls SET url = 'https://while-unsupported.example' WHERE id = ?", (ids[6],))
    new_ids = _add(conn, _corpus(3, "https://added.example"))
    assert not conn.in_transaction

    monkeypatch.setattr(usi, "url_search_index_supported", url_search_index_supported)
    assert ensure_url_search_index(conn) is True
    assert _objects(conn) == INDEX_OBJECTS
    assert _watermark(conn) == 0
    status = get_url_search_index_status(conn)
    assert status.state == UrlSearchIndexStatus.BUILDING and status.max_url_id == new_ids[-1]
    _build_all(conn, 5)
    assert _integrity_ok(conn)
    assert _candidates(conn, "while-unsupported") == {ids[6]}
    conn.close()


def test_unsupported_runtime_creates_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(usi, "url_search_index_supported", lambda: False)
    conn = _new_db(tmp_path, ensure=False)
    assert ensure_url_search_index(conn) is False
    assert _objects(conn) == {URL_SEARCH_STATE_TABLE}
    _add(conn, _corpus(3))
    assert get_url_search_index_status(conn).state == UrlSearchIndexStatus.UNSUPPORTED
    conn.close()


# ---------------------------------------------------------------------------
# Match query
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text", ["", "a", "ab", "ä€", "\x00ab", "abc\x00", "abcdef\x00ghi"])
def test_match_query_none_for_short_or_nul_terms(text):
    assert url_search_match_query(text) is None


def _phrases(query: str) -> List[str]:
    assert _all_quoted(query), query
    return [p[1:-1].replace('""', '"') for p in query.split(" AND ")]


def test_match_query_trigrams():
    assert url_search_match_query("abc") == '"abc"'
    assert url_search_match_query("aaaaaa") == '"aaa"'  # duplicates dropped
    assert url_search_match_query('a"b') == '"a""b"'
    assert url_search_match_query('"""') == '""""""""'
    for text in ("abcd", "abcdef", "abcdefg", "https://example.com/a?b=c", "x" * 3 + "yz" * 40, LONG_TERM):
        phrases = _phrases(url_search_match_query(text))
        # Distinct trigrams of the term (so every URL containing it matches),
        # including both ends; capped for long terms.
        assert all(len(p) == 3 and p in text for p in phrases)
        assert len(set(phrases)) == len(phrases)
        assert phrases[0] == text[:3] and phrases[-1] == text[-3:]
        assert len(phrases) <= max(usi._MAX_QUERY_TRIGRAMS, 1)


LONG_TERM = "https://long.example/" + "".join(chr(0x4E00 + i) for i in range(200))


FTS_SYNTAX_TERMS = [
    "AND", "OR x", "NOT", "NEAR", "NEAR(a b, 2)", "a AND b", "x OR y", "NOT z",
    "***", "^ab", "col:val", "url:abc", "-abc", "a-b-c", "(abc)", "((", "))", "+abc",
    '"', '"""', 'a"b', '"AND"', "{url}:x", "a*b^c:d-e+f(g)", "\\\\", "%_%", "'abc'", "a\tb\nc",
]


def _all_quoted(query: str) -> bool:
    """True if the query is only quoted phrases joined by ' AND '."""
    i = 0
    phrases = 0
    while i < len(query):
        if query[i] != '"':
            return False
        i += 1
        while True:
            if i >= len(query):
                return False
            if query[i] == '"':
                if query[i + 1:i + 2] == '"':
                    i += 2
                    continue
                i += 1
                break
            i += 1
        phrases += 1
        if i < len(query):
            if query[i:i + 5] != " AND ":
                return False
            i += 5
    return phrases > 0


@pytest.mark.parametrize("text", FTS_SYNTAX_TERMS)
def test_match_query_quotes_fts_syntax_and_runs(tmp_path, text):
    conn = _new_db(tmp_path)
    urls = [f"https://fts.example/{text}/tail", "https://fts.example/unrelated", f"pre{text}"]
    ids = _add(conn, urls)
    _build_all(conn)
    query = url_search_match_query(text)
    if len(text) < 3:
        assert query is None
        return
    assert _all_quoted(query), query
    hits = {row[0] for row in conn.execute(
        f"SELECT rowid FROM {URL_SEARCH_INDEX_TABLE} WHERE {URL_SEARCH_INDEX_TABLE} MATCH ?", (query,)
    )}
    assert {ids[0], ids[2]} <= hits
    assert _candidates(conn, text) >= _like_ids(conn, text)
    conn.close()


# ---------------------------------------------------------------------------
# Candidate planning
# ---------------------------------------------------------------------------


def test_plan_returns_none_for_short_terms_and_bad_limits(tmp_path):
    conn = _new_db(tmp_path)
    _add(conn, _corpus(5))
    _build_all(conn)
    for text in ("", "p", "p1", "\x00\x00\x00"):
        assert plan_url_search_candidates(conn, text) is None
    assert plan_url_search_candidates(conn, "p1?", max_candidates=0) is None
    assert plan_url_search_candidates(conn, "p1?", max_candidates=1) is not None
    conn.close()


def test_plan_falls_back_when_tail_exceeds_limit(tmp_path):
    conn = _new_db(tmp_path)
    _add(conn, _corpus(10))
    assert plan_url_search_candidates(conn, "zzz-rare", max_candidates=9) is None
    assert _candidates(conn, "zzz-rare", max_candidates=10) == {r[0] for r in conn.execute("SELECT id FROM urls")}
    build_url_search_index_step(conn, 5)  # tail 5
    assert plan_url_search_candidates(conn, "zzz-rare", max_candidates=4) is None
    assert _candidates(conn, "zzz-rare", max_candidates=5) == {r[0] for r in conn.execute("SELECT id FROM urls WHERE id > 5")}
    conn.close()


def test_plan_falls_back_when_index_hits_exceed_limit(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(20))
    _build_all(conn)
    assert plan_url_search_candidates(conn, "ex.com", max_candidates=19) is None
    assert _candidates(conn, "ex.com", max_candidates=20) == set(ids)

    # Index hits + tail are counted together.
    rare_ids = _add(conn, ["https://rare.example/a", "https://rare.example/b"])
    _build_all(conn)
    tail = _add(conn, _corpus(3, "https://tail.example"))
    assert plan_url_search_candidates(conn, "rare.example", max_candidates=4) is None
    assert _candidates(conn, "rare.example", max_candidates=5) == set(rare_ids) | set(tail)
    conn.close()


def test_plan_candidates_are_rare_term_matches_on_built_index(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, _corpus(50) + ["https://needle.example/X", "HTTPS://NEEDLE.EXAMPLE/y"])
    _build_all(conn)
    plan = plan_url_search_candidates(conn, "needle.ex")
    assert plan is not None
    sql, params = plan
    assert params == [url_search_match_query("needle.ex")]
    assert {row[0] for row in conn.execute(sql, params)} == {ids[-2], ids[-1]}
    assert _candidates(conn, "no-such-url") == set()
    conn.close()


def test_plan_candidates_are_superset_under_unicode_case_folding(tmp_path):
    conn = _new_db(tmp_path)
    ids = _add(conn, ["https://x/ärger", "https://x/Ärger", "https://x/Kelvin", "https://x/kelvin"])
    _build_all(conn)
    # LIKE is case-sensitive for non-ASCII letters; the index folds them.
    assert _like_ids(conn, "/ärg") == {ids[0]}
    assert _candidates(conn, "/ärg") >= {ids[0]}
    assert _like_ids(conn, "/kelvin") == {ids[3]}
    assert _candidates(conn, "/kelvin") >= {ids[3]}
    assert _candidates(conn, "/Kelvin") >= {ids[2]}
    conn.close()


def test_cancelled_probe_raises_interrupted(tmp_path):
    conn = _hooked_db(tmp_path)
    _add(conn, _corpus(200))
    _build_all(conn)

    def cancel_probe(c, sql):
        if sql.startswith(f"SELECT COUNT(*) FROM (SELECT 1 FROM {URL_SEARCH_INDEX_TABLE}"):
            c.set_progress_handler(lambda: 1, 1)

    conn.hook = cancel_probe
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        plan_url_search_candidates(conn, "ex.com/p1")
    conn.hook = None
    conn.set_progress_handler(None, 0)
    assert plan_url_search_candidates(conn, "ex.com/p1") is not None
    conn.close()


def test_failed_probe_falls_back(tmp_path):
    conn = _hooked_db(tmp_path)
    _add(conn, _corpus(5))
    _build_all(conn)

    def fail_probe(c, sql):
        if sql.startswith(f"SELECT COUNT(*) FROM (SELECT 1 FROM {URL_SEARCH_INDEX_TABLE}"):
            raise sqlite3.DatabaseError("simulated corruption")

    conn.hook = fail_probe
    assert plan_url_search_candidates(conn, "ex.com/p1") is None
    conn.close()


def test_progress_handler_on_whole_plan_raises_not_falls_back(tmp_path):
    """A cancelled connection never silently degrades to 'no index'."""
    conn = _new_db(tmp_path)
    _add(conn, _corpus(50))
    _build_all(conn)
    conn.set_progress_handler(lambda: 1, 1)
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        plan_url_search_candidates(conn, "ex.com/p1")
    conn.set_progress_handler(None, 0)
    conn.close()
