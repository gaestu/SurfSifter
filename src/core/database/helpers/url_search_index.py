"""
URL substring search index (issue #62).

An evidence-local SQLite FTS5 trigram index over ``urls.url`` that narrows the
candidate rows of a literal URL substring search. It never decides a result:
callers always apply their literal ``LIKE`` predicate to the candidates, and
fall back to scanning whenever the index cannot be used.

Coverage (watermark)
    ``url_search_index_state.indexed_through_id`` (W) records that the index
    holds exactly the URL rows with ``id <= W``, with their current ``url``.
    Candidates are the index matches with ``rowid <= W`` plus every row with
    ``id > W`` (the unindexed tail, e.g. URLs ingested after the last build).

    - ``urls.id`` is AUTOINCREMENT and URLs are never inserted with an explicit
      id, so new rows always land in the tail. There is deliberately no insert
      trigger: even one that never fires doubles ingest time.
    - Delete and url-update triggers keep the indexed range exact, in the same
      transaction as the change (so rollback undoes both).
    - W only advances in ``build_url_search_index_step`` (one transaction per
      batch, so an interrupted build loses at most one batch) and returns to 0
      only together with clearing the index.

Superset guarantee
    The index stores no URL text (``content='urls'``) and no positions
    (``detail=none``). A term becomes an AND of quoted trigrams taken from it,
    which matches every URL containing the term. The trigram tokenizer folds
    Unicode case, a superset of LIKE's ASCII-only folding, so the index can
    return extra candidates but never miss a match. Terms shorter than three
    characters have no trigram and are always scanned.

Runtime support
    FTS5 with the trigram tokenizer needs SQLite >= 3.34. The index objects are
    created by ``ensure_url_search_index`` (not by the SQL migration, which
    must not fail) only on runtimes that support them. On a runtime without
    support the triggers are dropped so URL writes keep working, and W is reset
    to 0 so that a supported runtime clears and rebuilds the index.
"""
from __future__ import annotations

import functools
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Tuple

from core.logging import get_logger

__all__ = [
    "URL_SEARCH_INDEX_TABLE",
    "URL_SEARCH_STATE_TABLE",
    "URL_SEARCH_MIN_CHARS",
    "URL_SEARCH_MAX_CANDIDATES",
    "URL_SEARCH_BUILD_BATCH_SIZE",
    "URL_SEARCH_INDEX_BUILD_TASK",
    "UrlSearchIndexStatus",
    "url_search_index_supported",
    "ensure_url_search_index",
    "get_url_search_index_status",
    "build_url_search_index_step",
    "reset_url_search_index",
    "url_search_match_query",
    "plan_url_search_candidates",
]

LOGGER = get_logger("core.database.helpers.url_search_index")

URL_SEARCH_INDEX_TABLE = "urls_search_index"
URL_SEARCH_STATE_TABLE = "url_search_index_state"  # evidence migration 0006

# Shortest term the index can serve (one trigram).
URL_SEARCH_MIN_CHARS = 3
# Use the index only while candidates (index matches + unindexed tail) stay
# below this: beyond it, sorting/looking up candidates costs more than the
# ordered scan, which stops after one page for common terms (measured, #62).
URL_SEARCH_MAX_CANDIDATES = 25_000
# Rows per build transaction (~0.2 s each at 1M rows; keeps write locks short).
URL_SEARCH_BUILD_BATCH_SIZE = 5_000
# process_log task of build sessions.
URL_SEARCH_INDEX_BUILD_TASK = "url_search_index_build"
# Most trigrams ANDed per search term (long pasted URLs).
_MAX_QUERY_TRIGRAMS = 16

_WATERMARK = f"(SELECT indexed_through_id FROM {URL_SEARCH_STATE_TABLE} WHERE id = 1)"

_CREATE_INDEX_SQL = (
    f"CREATE VIRTUAL TABLE IF NOT EXISTS {URL_SEARCH_INDEX_TABLE} USING fts5("
    "url, content='urls', content_rowid='id', tokenize='trigram', detail=none)"
)

# External-content 'delete' needs the indexed values; the WHEN guards limit
# maintenance to the indexed range (tail rows are not in the index).
_TRIGGER_SQL = {
    "urls_search_index_ad": f"""
        CREATE TRIGGER IF NOT EXISTS urls_search_index_ad AFTER DELETE ON urls
        WHEN old.id <= {_WATERMARK}
        BEGIN
            INSERT INTO {URL_SEARCH_INDEX_TABLE}({URL_SEARCH_INDEX_TABLE}, rowid, url)
            VALUES ('delete', old.id, old.url);
        END
    """,
    "urls_search_index_au": f"""
        CREATE TRIGGER IF NOT EXISTS urls_search_index_au AFTER UPDATE OF id, url ON urls
        BEGIN
            INSERT INTO {URL_SEARCH_INDEX_TABLE}({URL_SEARCH_INDEX_TABLE}, rowid, url)
            SELECT 'delete', old.id, old.url WHERE old.id <= {_WATERMARK};
            INSERT INTO {URL_SEARCH_INDEX_TABLE}(rowid, url)
            SELECT new.id, new.url WHERE new.id <= {_WATERMARK};
        END
    """,
}

_INDEX_OBJECTS = (URL_SEARCH_STATE_TABLE, URL_SEARCH_INDEX_TABLE, *_TRIGGER_SQL)


@dataclass(frozen=True)
class UrlSearchIndexStatus:
    """Coverage of the URL search index for one evidence database.

    Attributes:
        state: ``"ready"`` (every URL indexed), ``"building"`` (URLs above
            ``indexed_through_id`` are not indexed yet), ``"unavailable"``
            (index objects missing, e.g. creation failed) or
            ``"unsupported"`` (SQLite lacks FTS5 with the trigram tokenizer).
            Search results are complete in every state.
        indexed_through_id: Watermark W (URLs with ``id <= W`` are indexed).
        min_url_id: Smallest URL id (0 without URLs).
        max_url_id: Largest URL id (0 without URLs).
    """

    state: str
    indexed_through_id: int = 0
    min_url_id: int = 0
    max_url_id: int = 0

    READY = "ready"
    BUILDING = "building"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"

    @property
    def is_ready(self) -> bool:
        return self.state == self.READY

    @property
    def progress(self) -> float:
        """Approximate indexed share of the id range (0.0-1.0)."""
        if self.state == self.READY:
            return 1.0
        if self.state != self.BUILDING:
            return 0.0
        low = self.min_url_id - 1
        span = self.max_url_id - low
        if span <= 0:
            return 1.0
        return min(1.0, max(0.0, (self.indexed_through_id - low) / span))


@functools.lru_cache(maxsize=None)
def url_search_index_supported() -> bool:
    """True if this process's SQLite has FTS5 with a working trigram tokenizer."""
    try:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE VIRTUAL TABLE t USING fts5(x, tokenize='trigram', detail=none)")
            conn.execute("INSERT INTO t(x) VALUES ('https://Example.com')")
            hits = conn.execute("SELECT COUNT(*) FROM t WHERE t MATCH ?", ('"EXA" AND "com"',)).fetchone()[0]
            return hits == 1
        finally:
            conn.close()
    except sqlite3.Error as exc:
        LOGGER.info("URL search index unsupported by SQLite %s: %s", sqlite3.sqlite_version, exc)
        return False


@contextmanager
def _write_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Run the block in one ``BEGIN IMMEDIATE`` transaction (DDL included)."""
    if conn.in_transaction:
        raise RuntimeError("URL search index changes need a connection without an open transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def _present_objects(conn: sqlite3.Connection) -> set:
    placeholders = ",".join("?" for _ in _INDEX_OBJECTS)
    return {
        row[0]
        for row in conn.execute(
            f"SELECT name FROM sqlite_master WHERE name IN ({placeholders})", _INDEX_OBJECTS
        )
    }


def _has_urls_table(conn: sqlite3.Connection) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'urls'").fetchone() is not None


def _index_usable(conn: sqlite3.Connection) -> bool:
    return url_search_index_supported() and len(_present_objects(conn)) == len(_INDEX_OBJECTS)


def _watermark(conn: sqlite3.Connection) -> int:
    row = conn.execute(_WATERMARK[1:-1]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _clear_index(conn: sqlite3.Connection) -> None:
    """Empty the index and set W = 0 (caller holds the write transaction)."""
    conn.execute(f"INSERT INTO {URL_SEARCH_INDEX_TABLE}({URL_SEARCH_INDEX_TABLE}) VALUES ('delete-all')")
    conn.execute(f"INSERT OR IGNORE INTO {URL_SEARCH_STATE_TABLE} (id, indexed_through_id) VALUES (1, 0)")
    conn.execute(f"UPDATE {URL_SEARCH_STATE_TABLE} SET indexed_through_id = 0 WHERE id = 1")


def ensure_url_search_index(conn: sqlite3.Connection) -> bool:
    """Create or repair the index objects on an evidence database.

    Called when an evidence connection is opened, after the migrations. Cheap
    when everything is in place (one ``sqlite_master`` lookup). Never raises:
    failures are logged and search keeps scanning.

    Returns:
        True if the index objects are in place and usable.
    """
    try:
        return _ensure(conn)
    except Exception as exc:  # never block opening a case
        LOGGER.warning("URL search index setup failed; URL search will scan: %s", exc)
        return False


def _ensure(conn: sqlite3.Connection) -> bool:
    present = _present_objects(conn)
    if URL_SEARCH_STATE_TABLE not in present or not _has_urls_table(conn):
        return False  # pre-0006 or minimal schema
    triggers = [name for name in _TRIGGER_SQL if name in present]

    if not url_search_index_supported():
        if triggers:
            # The triggers would fail without the FTS5 module and block URL
            # writes. Without them deletes/updates are no longer mirrored, so
            # coverage drops to 0; a supported runtime clears and rebuilds.
            with _write_transaction(conn):
                for name in triggers:
                    conn.execute(f"DROP TRIGGER IF EXISTS {name}")
                conn.execute(f"UPDATE {URL_SEARCH_STATE_TABLE} SET indexed_through_id = 0 WHERE id = 1")
            LOGGER.warning(
                "SQLite %s lacks FTS5 trigram support: URL search index triggers removed, "
                "URL search will scan", sqlite3.sqlite_version,
            )
        return False

    if len(present) == len(_INDEX_OBJECTS):
        return True

    with _write_transaction(conn):
        if len(_present_objects(conn)) == len(_INDEX_OBJECTS):
            return True  # another connection completed it meanwhile
        conn.execute(_CREATE_INDEX_SQL)
        for sql in _TRIGGER_SQL.values():
            conn.execute(sql)
        # Changes made while objects were missing were not mirrored.
        _clear_index(conn)
    LOGGER.info("URL search index created (not built yet)")
    return True


def get_url_search_index_status(conn: sqlite3.Connection) -> UrlSearchIndexStatus:
    """Return the index coverage for the evidence database behind ``conn``."""
    if not url_search_index_supported():
        return UrlSearchIndexStatus(UrlSearchIndexStatus.UNSUPPORTED)
    if not _has_urls_table(conn) or len(_present_objects(conn)) != len(_INDEX_OBJECTS):
        return UrlSearchIndexStatus(UrlSearchIndexStatus.UNAVAILABLE)
    watermark = _watermark(conn)
    # Two queries: SQLite's MIN/MAX rowid shortcut applies to single aggregates.
    min_id = conn.execute("SELECT MIN(id) FROM urls").fetchone()[0] or 0
    max_id = conn.execute("SELECT MAX(id) FROM urls").fetchone()[0] or 0
    state = UrlSearchIndexStatus.READY if max_id <= watermark else UrlSearchIndexStatus.BUILDING
    return UrlSearchIndexStatus(state, watermark, int(min_id), int(max_id))


def build_url_search_index_step(
    conn: sqlite3.Connection,
    batch_size: int = URL_SEARCH_BUILD_BATCH_SIZE,
) -> int:
    """Index the next ``batch_size`` unindexed URLs in one write transaction.

    Safe to interrupt and to run from several connections: each batch reads
    W inside its own ``BEGIN IMMEDIATE`` transaction and commits the indexed
    rows together with the new W.

    Returns:
        Number of URL rows indexed (0 when there is nothing to do).
    """
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if not _index_usable(conn):
        return 0
    with _write_transaction(conn):
        watermark = _watermark(conn)
        if watermark == 0:
            # Drop entries left from before a coverage reset (see _ensure).
            _clear_index(conn)
        upper = conn.execute(
            "SELECT MAX(id) FROM (SELECT id FROM urls WHERE id > ? ORDER BY id LIMIT ?)",
            (watermark, batch_size),
        ).fetchone()[0]
        if upper is None:
            return 0
        indexed = conn.execute(
            f"INSERT INTO {URL_SEARCH_INDEX_TABLE}(rowid, url) SELECT id, url FROM urls WHERE id > ? AND id <= ?",
            (watermark, upper),
        ).rowcount
        conn.execute(f"UPDATE {URL_SEARCH_STATE_TABLE} SET indexed_through_id = ? WHERE id = 1", (upper,))
    return max(indexed, 0)


def reset_url_search_index(conn: sqlite3.Connection) -> bool:
    """Clear the index so it is rebuilt from scratch; False if it is not usable."""
    if not _index_usable(conn):
        return False
    with _write_transaction(conn):
        _clear_index(conn)
    return True


def url_search_match_query(text: str) -> Optional[str]:
    """FTS5 query matching every URL that contains ``text``, or ``None``.

    The query is an AND of quoted trigrams of ``text``, so user text is never
    read as FTS5 syntax. It matches a superset of the URLs containing
    ``text``. All distinct trigrams are used up to ``_MAX_QUERY_TRIGRAMS``
    (evenly spaced beyond that): FTS5 skips through the other trigrams' lists
    at the pace of the rarest one, so including it matters more than the
    number of trigrams (measured, #62). ``None`` for terms the index cannot
    serve (shorter than ``URL_SEARCH_MIN_CHARS``, or containing NUL).
    """
    if len(text) < URL_SEARCH_MIN_CHARS or "\x00" in text:
        return None
    trigrams = list(dict.fromkeys(text[i:i + 3] for i in range(len(text) - 2)))
    if len(trigrams) > _MAX_QUERY_TRIGRAMS:
        last = len(trigrams) - 1
        trigrams = list(dict.fromkeys(
            trigrams[round(i * last / (_MAX_QUERY_TRIGRAMS - 1))] for i in range(_MAX_QUERY_TRIGRAMS)
        ))
    return " AND ".join('"' + trigram.replace('"', '""') + '"' for trigram in trigrams)


def plan_url_search_candidates(
    conn: sqlite3.Connection,
    text: str,
    *,
    max_candidates: int = URL_SEARCH_MAX_CANDIDATES,
) -> Optional[Tuple[str, List[Any]]]:
    """Plan candidate URL ids for a literal substring search of ``text``.

    Returns ``(sql, params)`` of a subquery with one column ``id``: index
    matches within the indexed range plus every unindexed URL. W is read
    inside that subquery, so it is consistent with the index it reads. The
    candidates are a superset of the matches; the caller must still apply
    its literal ``LIKE`` predicate.

    Returns ``None`` (caller scans ``urls``) if the term cannot use the index,
    the index is unusable, or the candidates (bounded probe) exceed
    ``max_candidates``. The choice affects speed only, never results.

    Raises:
        sqlite3.OperationalError: If the probe was interrupted (cancellation).
    """
    match = url_search_match_query(text)
    if match is None or max_candidates < 1 or not _index_usable(conn):
        return None
    watermark = _watermark(conn)
    max_id = conn.execute("SELECT MAX(id) FROM urls").fetchone()[0] or 0
    tail = max(0, max_id - watermark)  # upper bound: ids may have gaps
    if tail > max_candidates:
        return None
    try:
        hits = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {URL_SEARCH_INDEX_TABLE} "
            f"WHERE {URL_SEARCH_INDEX_TABLE} MATCH ? AND rowid <= ? LIMIT ?)",
            (match, watermark, max_candidates - tail + 1),
        ).fetchone()[0]
    except sqlite3.DatabaseError as exc:
        if "interrupted" in str(exc).lower():
            raise
        LOGGER.warning("URL search index probe failed; URL search will scan: %s", exc)
        return None
    if hits + tail > max_candidates:
        return None
    # FTS5 returns each rowid once and the two parts cover disjoint id ranges,
    # so the candidates contain no duplicates.
    sql = (
        f"SELECT rowid AS id FROM {URL_SEARCH_INDEX_TABLE} "
        f"WHERE {URL_SEARCH_INDEX_TABLE} MATCH ? AND rowid <= {_WATERMARK} "
        f"UNION ALL SELECT id FROM urls WHERE id > {_WATERMARK}"
    )
    return sql, [match]
