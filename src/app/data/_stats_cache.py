"""Shared, revision-checked statistics cache for one open case (issue #63).

Aggregate reads (overview counts, filter dropdown lists, top domains, tag
lists) are expensive on large evidences and were repeated by every
``CaseDataAccess`` instance, including the short-lived instances owned by
background workers. ``CaseStatsCache`` holds their results once per case and
is shared by every ``CaseDataAccess`` of that case (see ``case_stats_cache``).

Correctness rests on revisions, not on time:

- Every (evidence, category) pair has a revision counter that only grows.
  Writers announce changes with ``invalidate`` (or ``begin_writes`` /
  ``end_writes`` around long writes), which bumps the counters.
- A reader takes ``token()`` *before* it queries the database and passes it
  to ``put()``. ``put()`` refuses the value if any of its categories was
  bumped in the meantime, so an in-flight result computed from older data can
  never overwrite or outlive a newer revision.
- ``get()`` is a hit only while the token stored with the entry is still the
  current token of its categories.
- While writes are open for an evidence, nothing is stored for it. A value
  computed during writes may be displayed (``is_current`` stays true until the
  next bump) but is never cached. If an ``end_writes`` is ever missed, the only
  cost is lost caching, never stale data.

Write scopes: ``write_scope(evidence_id)`` returns a ``StatsWriteScope``
handle that opens a write scope and closes it exactly once (``close()`` is
idempotent and thread-safe, so it can run when a worker thread ends and also
from GUI cleanup paths). Prefer it over raw ``begin_writes`` /
``end_writes``.

Writers outside ``CaseDataAccess`` (rule): ``CaseDataAccess`` methods that
write invalidate the categories they touch themselves. Any other code path that
writes rows a category depends on (extractor/ingestion workers, report and
export code, matching or download runs writing their own connections) must
either hold a write scope for the evidence while it writes, or invalidate the
affected categories once it is done, in every outcome (success, failure,
cancel). Otherwise cached values stay stale until the next bump. Notably
``process_log`` rows are written by many such paths; ``STATS_PROCESS_LOG`` is
invalidated today by:

- ``MainWindow._on_download_tab_finished`` (download runs),
- ``ImagesTab._invalidate_hash_check_stats`` (hash check, with ``STATS_IMAGES``),
- ``UrlsTab._invalidate_match_stats`` (URL list matching, with
  ``STATS_URL_MATCHES``),
- ``ReportsTab._on_process_log_written`` (report preview/PDF/tag export),
- ``FileListTab._log_export`` (file list export),
- and implicitly by every write scope (``end_writes`` bumps all categories):
  ``ExtractionTab`` extraction/ingestion/Run All/purge, ``MainWindow``
  executor runs and case-wide extract & ingest.

Pure Python, thread-safe, deterministic (no wall clock, no randomness).
Values are deep-copied on the way in and out, so callers can never mutate a
shared entry.
"""
from __future__ import annotations

import copy
import itertools
import threading
from pathlib import Path
from typing import Any, Dict, FrozenSet, Hashable, Iterable, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# Categories: what kind of evidence data a cached value depends on.
# -----------------------------------------------------------------------------

STATS_URLS = "urls"  # urls table (counts, domains, sources)
STATS_IMAGES = "images"  # images table (and hash matches on images)
STATS_OS_INDICATORS = "os_indicators"  # os_indicators table
STATS_TAGS = "tags"  # tags / tag_associations
STATS_URL_MATCHES = "url_matches"  # url_matches (URL list matching)
# process_log (last extraction run). Many writers append process_log rows
# outside CaseDataAccess: each must invalidate this category (or hold a write
# scope) when it is done -- see "Writers outside CaseDataAccess" above.
STATS_PROCESS_LOG = "process_log"

ALL_STATS_CATEGORIES: FrozenSet[str] = frozenset(
    {
        STATS_URLS,
        STATS_IMAGES,
        STATS_OS_INDICATORS,
        STATS_TAGS,
        STATS_URL_MATCHES,
        STATS_PROCESS_LOG,
    }
)

# Category sets of the cached readers (CaseDataAccess mixins), defined once.
EVIDENCE_COUNTS_CATEGORIES: FrozenSet[str] = frozenset(
    {STATS_URLS, STATS_IMAGES, STATS_OS_INDICATORS, STATS_PROCESS_LOG}
)
TAG_STATISTICS_CATEGORIES: FrozenSet[str] = frozenset({STATS_TAGS})
OVERVIEW_STATS_CATEGORIES: FrozenSet[str] = (
    EVIDENCE_COUNTS_CATEGORIES | TAG_STATISTICS_CATEGORIES
)

Categories = Union[str, Iterable[str]]

# Deterministic per-process serial number for cache instances. It is part of
# every token, so a token issued by one cache instance is never current in
# another one (e.g. after a case was discarded and reopened).
_instance_serials = itertools.count(1)


def _normalize_categories(categories: Optional[Categories]) -> FrozenSet[str]:
    """Return ``categories`` as a validated frozenset (None = all categories)."""
    if categories is None:
        return ALL_STATS_CATEGORIES
    if isinstance(categories, str):
        result = frozenset((categories,))
    else:
        result = frozenset(categories)
    unknown = result - ALL_STATS_CATEGORIES
    if unknown:
        raise ValueError(f"Unknown stats categories: {sorted(unknown)}")
    if not result:
        raise ValueError("At least one stats category is required")
    return result


class CaseStatsCache:
    """Thread-safe, evidence-aware, revision-checked cache for one case.

    Entries are addressed by ``(evidence_id, key)``; ``key`` is any hashable
    chosen by the reader (e.g. ``("url_domains", 200)``).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._serial = next(_instance_serials)
        # Revisions that apply to every evidence (invalidate(None, ...)).
        self._global_revisions: Dict[str, int] = {c: 0 for c in ALL_STATS_CATEGORIES}
        # Per (evidence_id, category) revisions; missing means 0. Never reset.
        self._revisions: Dict[Tuple[int, str], int] = {}
        # evidence_id -> number of currently open write scopes.
        self._open_writes: Dict[int, int] = {}
        # (evidence_id, key) -> (categories, token, value)
        self._entries: Dict[Tuple[int, Hashable], Tuple[FrozenSet[str], Hashable, Any]] = {}
        # Set once the cache was discarded from the registry: never store again.
        self._retired = False

    # ------------------------------------------------------------------
    # Tokens
    # ------------------------------------------------------------------

    def _token_locked(self, evidence_id: int, categories: FrozenSet[str]) -> Hashable:
        revisions = tuple(
            (
                category,
                self._global_revisions[category],
                self._revisions.get((evidence_id, category), 0),
            )
            for category in sorted(categories)
        )
        writes_open = self._open_writes.get(evidence_id, 0) > 0
        return (self._serial, evidence_id, writes_open, revisions)

    def token(self, evidence_id: int, categories: Categories) -> Hashable:
        """Current revision token of ``categories`` for ``evidence_id``.

        Take it *before* reading the database and pass it to ``put`` /
        ``is_current`` afterwards.
        """
        cats = _normalize_categories(categories)
        with self._lock:
            return self._token_locked(evidence_id, cats)

    def is_current(self, evidence_id: int, categories: Categories, token: Hashable) -> bool:
        """True if none of ``categories`` changed since ``token`` was taken.

        A token taken while writes were open stays current until the next
        bump; such a value may be shown but is never cached.
        """
        cats = _normalize_categories(categories)
        with self._lock:
            return token == self._token_locked(evidence_id, cats)

    def writes_open(self, evidence_id: int) -> bool:
        """True while at least one ``begin_writes`` for the evidence is open."""
        with self._lock:
            return self._open_writes.get(evidence_id, 0) > 0

    # ------------------------------------------------------------------
    # Entries
    # ------------------------------------------------------------------

    def get(self, evidence_id: int, key: Hashable) -> Tuple[bool, Any]:
        """Return ``(True, copy_of_value)`` on a current hit, else ``(False, None)``."""
        with self._lock:
            entry = self._entries.get((evidence_id, key))
            if entry is None:
                return False, None
            categories, token, value = entry
            if token != self._token_locked(evidence_id, categories):
                del self._entries[(evidence_id, key)]
                return False, None
        # Stored values are private copies that are never mutated, so they
        # can be copied outside the lock.
        return True, copy.deepcopy(value)

    def put(
        self,
        evidence_id: int,
        key: Hashable,
        categories: Categories,
        token: Hashable,
        value: Any,
    ) -> bool:
        """Store a copy of ``value`` if ``token`` is still current.

        Refused (returns False) when any category changed since the token was
        taken, while writes are open for the evidence, or after the cache was
        discarded.
        """
        cats = _normalize_categories(categories)
        stored = copy.deepcopy(value)
        with self._lock:
            if self._retired or self._open_writes.get(evidence_id, 0) > 0:
                return False
            if token != self._token_locked(evidence_id, cats):
                return False
            self._entries[(evidence_id, key)] = (cats, token, stored)
            return True

    def entry_count(self) -> int:
        """Number of stored entries (stale ones included until next lookup)."""
        with self._lock:
            return len(self._entries)

    # ------------------------------------------------------------------
    # Invalidation
    # ------------------------------------------------------------------

    def _bump_locked(self, evidence_id: Optional[int], categories: FrozenSet[str]) -> None:
        if evidence_id is None:
            for category in categories:
                self._global_revisions[category] += 1
        else:
            for category in categories:
                slot = (evidence_id, category)
                self._revisions[slot] = self._revisions.get(slot, 0) + 1
        stale = [
            entry_key
            for entry_key, (entry_categories, _token, _value) in self._entries.items()
            if (evidence_id is None or entry_key[0] == evidence_id)
            and not entry_categories.isdisjoint(categories)
        ]
        for entry_key in stale:
            del self._entries[entry_key]

    def invalidate(
        self,
        evidence_id: Optional[int] = None,
        categories: Optional[Categories] = None,
    ) -> None:
        """Bump revisions and drop affected entries.

        ``evidence_id=None`` applies to every evidence, including ones this
        cache has not seen yet; ``categories=None`` means all categories.
        """
        cats = _normalize_categories(categories)
        with self._lock:
            self._bump_locked(evidence_id, cats)

    def begin_writes(self, evidence_id: int) -> None:
        """Mark the start of a (possibly long) write to the evidence.

        Scopes nest. While any is open nothing is cached for the evidence, and
        tokens taken before the first ``begin_writes`` are no longer current.
        """
        with self._lock:
            self._open_writes[evidence_id] = self._open_writes.get(evidence_id, 0) + 1

    def end_writes(self, evidence_id: int) -> None:
        """Close one write scope and bump all categories of the evidence.

        An ``end_writes`` without a matching ``begin_writes`` is harmless and
        still bumps.
        """
        with self._lock:
            depth = self._open_writes.get(evidence_id, 0) - 1
            if depth > 0:
                self._open_writes[evidence_id] = depth
            else:
                self._open_writes.pop(evidence_id, None)
            self._bump_locked(evidence_id, ALL_STATS_CATEGORIES)

    def write_scope(self, evidence_id: int) -> "StatsWriteScope":
        """Open a write scope for the evidence and return its handle.

        Calls ``begin_writes`` now; ``StatsWriteScope.close`` calls
        ``end_writes`` exactly once, on this same cache instance.
        """
        return StatsWriteScope(self, evidence_id)

    def drop_evidence(self, evidence_id: int) -> None:
        """Forget an evidence (e.g. removed from the case).

        Bumps all its categories, removes its entries and resets its open
        write scopes.
        """
        with self._lock:
            self._open_writes.pop(evidence_id, None)
            self._bump_locked(evidence_id, ALL_STATS_CATEGORIES)

    def clear(self) -> None:
        """Invalidate every evidence and drop all entries.

        Open write scopes are kept, so a running writer still prevents caching.
        """
        with self._lock:
            self._bump_locked(None, ALL_STATS_CATEGORIES)
            self._entries.clear()

    def _retire(self) -> None:
        """Clear and refuse all future stores (called when discarded)."""
        with self._lock:
            self._retired = True
            self._bump_locked(None, ALL_STATS_CATEGORIES)
            self._entries.clear()


class StatsWriteScope:
    """Handle of one open write scope (see ``CaseStatsCache.write_scope``).

    ``close()`` ends the scope exactly once: it is idempotent and thread-safe,
    so it may be connected to a worker thread's end *and* called from the GUI
    thread's terminal paths. It always ends on the cache the scope began on;
    if that cache was discarded meanwhile, ending it there is harmless (the
    retired cache stores nothing). Usable as a context manager.

    Keep the handle referenced until it is closed. To close it from a Qt
    signal, connect a lambda that captures it (``lambda: scope.close()``):
    PySide does not keep the instance of a connected bound method alive, and
    calling into a collected instance crashes.
    """

    def __init__(self, cache: CaseStatsCache, evidence_id: int) -> None:
        self._cache = cache
        self._evidence_id = int(evidence_id)
        self._lock = threading.Lock()
        self._closed = False
        cache.begin_writes(self._evidence_id)

    @property
    def evidence_id(self) -> int:
        return self._evidence_id

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def close(self, *_args: Any) -> None:
        """End the scope (bumps all categories); later calls do nothing.

        Extra positional arguments (e.g. signal values) are ignored.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._cache.end_writes(self._evidence_id)

    def __enter__(self) -> "StatsWriteScope":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"<StatsWriteScope evidence={self._evidence_id} {state}>"


# -----------------------------------------------------------------------------
# Registry: one cache per open case, keyed by the resolved case DB path.
# -----------------------------------------------------------------------------

_registry_lock = threading.Lock()
_registry: Dict[Path, CaseStatsCache] = {}


def _registry_key(case_db_path: Union[str, Path]) -> Path:
    return Path(case_db_path).resolve()


def case_stats_cache(case_db_path: Union[str, Path]) -> CaseStatsCache:
    """Return the shared cache for the case DB at ``case_db_path``.

    Every ``CaseDataAccess`` of the same case (GUI instance and worker-owned
    instances alike) gets the same object; different cases are isolated by
    path. The cache is created on first use.
    """
    return _cache_for_key(_registry_key(case_db_path))


def _cache_for_key(key: Path) -> CaseStatsCache:
    """Registry lookup for an already resolved case DB path."""
    with _registry_lock:
        cache = _registry.get(key)
        if cache is None:
            cache = CaseStatsCache()
            _registry[key] = cache
        return cache


def discard_case_stats_cache(case_db_path: Union[str, Path]) -> None:
    """Drop the shared cache of a case so its next use starts cold.

    ``MainWindow`` calls this when a case is opened (and closed), so a reopened
    case never serves values cached before it was closed. The discarded
    instance is cleared and refuses further stores, so code still holding it
    (e.g. a worker that has not stopped yet) cannot serve or store stale data,
    and its tokens are never current in the new instance.
    """
    key = _registry_key(case_db_path)
    with _registry_lock:
        cache = _registry.pop(key, None)
    if cache is not None:
        cache._retire()
