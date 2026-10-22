"""URL query operations for UI layer.

This module provides URL-specific queries for the UI:
- Keyset-paginated URL listing with a stable sort contract (issue #61)
- Shared filter predicates for row and count queries
- Tag-aware queries via tag_associations
- Match filtering (matched/unmatched/specific list)
- Domain and source listing with caching
- Indexed URL substring search with scan fallback (issue #62)

Extracted from case_data.py for modular repository pattern.
"""
from __future__ import annotations

import logging
import sqlite3
import string
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from core.database.helpers.sql_utils import LIKE_ESCAPE_CHAR, escape_like
from core.database.helpers.url_search_index import (
    URL_SEARCH_BUILD_BATCH_SIZE,
    URL_SEARCH_INDEX_BUILD_TASK,
    UrlSearchIndexStatus,
    build_url_search_index_step,
    get_url_search_index_status,
    plan_url_search_candidates,
)

from ._base import BaseDataAccess
from ._stats_cache import STATS_URL_MATCHES, STATS_URLS


# Shared statistics cache categories of the cached URL filter readers (#63).
URL_FILTER_LIST_CATEGORIES = frozenset({STATS_URLS})
URL_MATCH_LIST_CATEGORIES = frozenset({STATS_URL_MATCHES})

# Stable sort contract for flat URL listing (issue #61): newest first, URLs
# without a timestamp last, ties broken by ascending id. The implicit rowid of
# idx_urls_evidence_first_seen (evidence_id, first_seen_utc DESC) is ascending,
# so this order is served by that index without a sort step.
URL_ORDER_BY = "u.first_seen_utc DESC, u.id ASC"

_ASCII_FOLD_TABLE = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def ascii_fold(value: str) -> str:
    """Lower-case ASCII letters only, like SQLite's ``COLLATE NOCASE``.

    The Python form of the domain comparison used by the exact domain filter
    and the grouped view's domain groups.
    """
    return value.translate(_ASCII_FOLD_TABLE)


_URL_ROW_COLUMNS = (
    "u.id, u.url, u.domain, u.scheme, u.discovered_by, u.first_seen_utc, "
    "u.last_seen_utc, u.source_path, u.notes, u.occurrence_count"
)

@dataclass(frozen=True)
class UrlFilterSpec:
    """Filters for flat URL listing and counting (issue #61).

    Row and count queries are built from the same spec by
    ``build_url_filter_predicates`` so they always agree.

    Attributes:
        domain_exact: Exact domain, ASCII case-insensitive (``COLLATE NOCASE``).
        domain_contains: Literal substring of the domain, ASCII
            case-insensitive; ``%``, ``_`` and ``\\`` are not wildcards.
        domain_like: Legacy raw SQL LIKE pattern on the domain (``None`` = off).
        url_contains: Literal substring of the URL, ASCII case-insensitive;
            ``%``, ``_`` and ``\\`` are not wildcards (issue #66).
        url_like: Legacy raw SQL LIKE pattern on the URL (``"%"`` = off).
        tag_name: Exact tag name, case-insensitive like tag creation
            (compared with ``tags.name_normalized``; issue #66).
        tag_like: Legacy raw SQL LIKE pattern on tag names (``"%"`` = off).
        discovered_by: Allowed sources (``None`` = all).
        match_filter: ``"matched"``, ``"unmatched"``, a list name, or ``None``.
    """

    domain_exact: Optional[str] = None
    domain_contains: Optional[str] = None
    domain_like: Optional[str] = None
    url_contains: Optional[str] = None
    url_like: str = "%"
    tag_name: Optional[str] = None
    tag_like: str = "%"
    discovered_by: Optional[Tuple[str, ...]] = None
    match_filter: Optional[str] = None

    def __post_init__(self) -> None:
        # Normalize so equal filters hash equally (used as count-cache key).
        if self.discovered_by is not None:
            object.__setattr__(self, "discovered_by", tuple(self.discovered_by) or None)
        for name in ("domain_exact", "domain_contains", "domain_like", "url_contains", "tag_name"):
            if getattr(self, name) == "":
                object.__setattr__(self, name, None)
        if self.domain_like == "%":
            object.__setattr__(self, "domain_like", None)
        if self.match_filter == "all":
            object.__setattr__(self, "match_filter", None)

    def with_ui_filters(
        self,
        *,
        domain: Optional[str] = None,
        domain_contains: Optional[str] = None,
        text: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> "UrlFilterSpec":
        """Return a copy updated from URLs-tab filter controls.

        Shared by the flat and grouped views so both select the same URLs.
        ``None`` leaves a filter unchanged; ``""`` (and ``"*"`` / ``"all"``
        where the controls use them) clears it.

        Args:
            domain: Exact domain from the dropdown
            domain_contains: Literal domain substring
            text: Literal URL substring
            sources: Allowed sources
            match_filter: "all", "matched", "unmatched", or list name
            tag: Exact tag name
        """
        changes: Dict[str, Any] = {}
        if domain is not None:
            changes["domain_exact"] = domain if domain and domain != "*" else None
        if domain_contains is not None:
            changes["domain_contains"] = domain_contains or None
        if text is not None:
            changes["url_contains"] = text or None
        if sources is not None:
            changes["discovered_by"] = tuple(sources) if sources else None
        if match_filter is not None:
            changes["match_filter"] = match_filter or None
        if tag is not None:
            changes["tag_name"] = tag if tag and tag != "*" else None
        return replace(self, **changes)


def build_url_filter_predicates(evidence_id: int, spec: UrlFilterSpec) -> Tuple[List[str], List[Any]]:
    """Build parameterized WHERE terms (alias ``u``) for ``spec``.

    Shared by row and count queries so both always select the same URLs.
    """
    params: List[Any] = [evidence_id]
    where = ["u.evidence_id = ?"]

    if spec.domain_exact is not None:
        # Served by idx_urls_evidence_domain_nocase_first_seen (migration 0004).
        where.append("u.domain = ? COLLATE NOCASE")
        params.append(spec.domain_exact)
    if spec.domain_contains is not None:
        where.append(f"u.domain LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
        params.append(f"%{escape_like(spec.domain_contains)}%")
    # Skip no-op wildcard filters for performance (Issue #47)
    # LIKE '%' forces full-table evaluation - omit when not needed
    if spec.domain_like is not None:
        where.append("COALESCE(u.domain, '') LIKE ?")
        params.append(spec.domain_like)
    if spec.url_contains is not None:
        where.append(f"u.url LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
        params.append(f"%{escape_like(spec.url_contains)}%")
    if spec.url_like and spec.url_like != "%":
        where.append("u.url LIKE ?")
        params.append(spec.url_like)

    if spec.tag_name is not None:
        # Same normalization as tag creation (core.database.helpers.tags).
        where.append("""
            EXISTS (
                SELECT 1 FROM tag_associations ta
                JOIN tags t ON ta.tag_id = t.id
                WHERE ta.artifact_type = 'url'
                AND ta.artifact_id = u.id
                AND ta.evidence_id = u.evidence_id
                AND t.evidence_id = u.evidence_id
                AND t.name_normalized = ?
            )
        """)
        params.append(spec.tag_name.lower())
    if spec.tag_like and spec.tag_like != "%":
        where.append("""
            EXISTS (
                SELECT 1 FROM tag_associations ta
                JOIN tags t ON ta.tag_id = t.id
                WHERE ta.artifact_type = 'url'
                AND ta.artifact_id = u.id
                AND t.name LIKE ?
            )
        """)
        params.append(spec.tag_like)

    if spec.discovered_by:
        placeholders = ",".join("?" for _ in spec.discovered_by)
        where.append(f"u.discovered_by IN ({placeholders})")
        params.extend(spec.discovered_by)

    # Match filter - use EXISTS for performance (no JOIN needed)
    if spec.match_filter:
        if spec.match_filter == "matched":
            where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
        elif spec.match_filter == "unmatched":
            where.append("NOT EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
        else:
            # Specific list name
            where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id AND m.list_name = ?)")
            params.append(spec.match_filter)

    return where, params


def build_url_from_clause(conn: sqlite3.Connection, spec: UrlFilterSpec) -> Tuple[str, List[Any]]:
    """Return the ``FROM`` clause (alias ``u``) and its params for ``spec``.

    Issue #62: for a literal URL substring (``url_contains``) the URL search
    index may narrow the rows to a candidate set; otherwise (and whenever the
    index cannot serve the term) ``urls`` is scanned. The WHERE predicates of
    ``build_url_filter_predicates`` (including the literal ``LIKE``) are applied
    either way, so the choice never changes results or their order. The
    ``CROSS JOIN`` keeps SQLite from scanning ``urls`` first.
    """
    if spec.url_contains is not None:
        plan = plan_url_search_candidates(conn, spec.url_contains)
        if plan is not None:
            candidates_sql, params = plan
            return f"FROM ({candidates_sql}) c CROSS JOIN urls u ON u.id = c.id", list(params)
    return "FROM urls u", []


@dataclass(frozen=True)
class UrlCursor:
    """Keyset position in the URL sort order: the last row of a page."""

    first_seen_utc: Optional[str]
    id: int

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "UrlCursor":
        return cls(row.get("first_seen_utc"), int(row["id"]))


@dataclass(frozen=True)
class UrlPageQuery:
    """Filters and page position for one flat URL page (background-load request).

    ``after`` is the cursor of the last row of the previous page (``None`` for
    the first page). ``page`` is the page index: the query ignores it (the
    position comes from ``after``), but the table model uses it for display
    and for its stack of page-start cursors. ``data_revision`` is the
    caller's data-revision tag, echoed back unchanged (not used by the query).
    """

    evidence_id: int
    filters: UrlFilterSpec = field(default_factory=UrlFilterSpec)
    page: int = 0
    page_size: int = 500
    after: Optional[UrlCursor] = None
    data_revision: int = 0


@dataclass
class UrlPage:
    """Prepared flat URL page: rows plus tag/match strings for every row id.

    ``tags`` and ``matches`` contain an entry for every returned row id
    (empty string when the URL has no tags / matches), so consumers never
    need per-row lookups. The total count is loaded separately (issue #61).

    ``data_version`` is SQLite's ``PRAGMA data_version`` read on the loading
    connection before the rows: it changes when any other connection
    commits to the evidence database, so a consumer that keeps loading
    through the same connection can detect changes it was not notified of.
    """

    query: UrlPageQuery
    rows: List[Dict[str, Any]] = field(default_factory=list)
    has_more: bool = False
    data_version: Optional[int] = None
    tags: Dict[int, str] = field(default_factory=dict)
    matches: Dict[int, str] = field(default_factory=dict)

    @property
    def next_cursor(self) -> Optional[UrlCursor]:
        """Cursor for the following page (``None`` if there is none)."""
        if not self.has_more or not self.rows:
            return None
        return UrlCursor.from_row(self.rows[-1])


# Grouped URL view (issue #64) ----------------------------------------------
#
# Groups are domains compared ASCII case-insensitively (``COLLATE NOCASE``),
# like the exact domain filter, in ascending NOCASE order; URLs without a
# domain (NULL or '') form one group (``domain is None``) after all others.
# Children of a group are its URLs in ``URL_ORDER_BY`` order.


@dataclass(frozen=True)
class UrlDomainGroup:
    """One domain group: its domain and the exact number of matching URLs.

    ``domain`` is the smallest spelling (``MIN``) among the group's matching
    URLs, or ``None`` for the group of URLs without a domain.
    """

    domain: Optional[str]
    count: int


@dataclass(frozen=True)
class UrlDomainGroupQuery:
    """Filters and keyset position for one page of domain groups.

    ``after`` is the domain of the last group of the previous page (``None``
    for the first page).
    """

    evidence_id: int
    filters: UrlFilterSpec = field(default_factory=UrlFilterSpec)
    after: Optional[str] = None
    page_size: int = 500


@dataclass
class UrlDomainGroupPage:
    """Prepared page of domain groups.

    The group without a domain is appended to the page on which the named
    groups end, so that page may hold ``page_size + 1`` groups.
    """

    query: UrlDomainGroupQuery
    groups: List[UrlDomainGroup] = field(default_factory=list)
    has_more: bool = False

    @property
    def next_after(self) -> Optional[str]:
        """Keyset position of the following page (``None`` if there is none)."""
        if not self.has_more or not self.groups:
            return None
        return self.groups[-1].domain


@dataclass(frozen=True)
class UrlDomainChildQuery:
    """Filters, group and keyset position for one page of a group's URLs.

    ``domain`` is the group's domain (``None``: URLs without a domain);
    ``after`` is the cursor of the last URL already loaded.
    """

    evidence_id: int
    domain: Optional[str]
    filters: UrlFilterSpec = field(default_factory=UrlFilterSpec)
    after: Optional[UrlCursor] = None
    page_size: int = 500


@dataclass
class UrlDomainChildPage:
    """Prepared page of a group's URLs with tag/match strings for every row id."""

    query: UrlDomainChildQuery
    rows: List[Dict[str, Any]] = field(default_factory=list)
    has_more: bool = False
    tags: Dict[int, str] = field(default_factory=dict)
    matches: Dict[int, str] = field(default_factory=dict)

    @property
    def next_cursor(self) -> Optional[UrlCursor]:
        """Cursor for the following page (``None`` if there is none)."""
        if not self.has_more or not self.rows:
            return None
        return UrlCursor.from_row(self.rows[-1])


# The no-domain group as two predicates: each is an index seek on
# idx_urls_evidence_domain_nocase_first_seen in URL_ORDER_BY order, while
# their OR would need a sort step on every page.
_NO_DOMAIN_PREDICATES = ("u.domain IS NULL", "u.domain = '' COLLATE NOCASE")


def _url_sort_key_sorted(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return ``rows`` in ``URL_ORDER_BY`` order (timestamp DESC, NULLs last, id ASC)."""
    rows = sorted(rows, key=lambda row: row["id"])
    # Stable: equal timestamps (and all NULLs) keep ascending id order.
    rows.sort(
        key=lambda row: (row["first_seen_utc"] is not None, row["first_seen_utc"] or ""),
        reverse=True,
    )
    return rows


class UrlQueryMixin(BaseDataAccess):
    """Mixin providing URL query operations for UI views.

    Features:
    - Keyset-paginated URL listing with shared filter predicates
      (exact/contains domain, literal URL substring, exact tag)
    - Tag-aware queries via tag_associations
    - Match filtering (matched/unmatched/specific list)
    - In-memory caching for filter dropdowns
    - Domain statistics for filter dropdowns

    All methods operate on the evidence database.

    Extracted from CaseDataAccess for modular architecture.
    Added get_top_domains() for domain statistics.
    """

    # -------------------------------------------------------------------------
    # URL Queries (Evidence DB)
    # -------------------------------------------------------------------------

    @staticmethod
    def _legacy_url_filter_spec(
        filters: Optional[UrlFilterSpec],
        domain_like: str,
        url_like: str,
        tag_like: str,
        discovered_by: Optional[Iterable[str]],
        match_filter: Optional[str],
    ) -> UrlFilterSpec:
        """Return ``filters`` or build a spec from the legacy LIKE kwargs."""
        if filters is not None:
            return filters
        return UrlFilterSpec(
            domain_like=domain_like if domain_like and domain_like != "%" else None,
            url_like=url_like or "%",
            tag_like=tag_like or "%",
            discovered_by=tuple(discovered_by) if discovered_by else None,
            match_filter=match_filter,
        )

    def count_urls(
        self,
        evidence_id: int,
        *,
        domain_like: str = "%",
        url_like: str = "%",
        tag_like: str = "%",
        discovered_by: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,
        filters: Optional[UrlFilterSpec] = None,
    ) -> int:
        """
        Exact count of URLs matching filters (without GROUP_CONCAT).

        Issue #61: runs separately from page loading so first rows do not wait
        for it; use inside ``cancellable_reads`` to make it interruptible.

        Args:
            evidence_id: Evidence ID
            domain_like: Legacy domain filter pattern (SQL LIKE)
            url_like: URL filter pattern (SQL LIKE)
            tag_like: Tag filter pattern (SQL LIKE)
            discovered_by: Optional list of source filters
            match_filter: Match filter ("matched", "unmatched", or specific list name)
            filters: Filter spec; overrides the legacy keyword filters when given

        Returns:
            Total count of matching URLs
        """
        logger = logging.getLogger(__name__)
        start_time = time.time()

        spec = self._legacy_url_filter_spec(filters, domain_like, url_like, tag_like, discovered_by, match_filter)
        where, params = build_url_filter_predicates(evidence_id, spec)

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                from_sql, from_params = build_url_from_clause(conn, spec)
                # Fast COUNT query (no JOIN, no GROUP_CONCAT)
                sql = f"""
                    SELECT COUNT(*)
                    {from_sql}
                    WHERE {' AND '.join(where)}
                """
                count = conn.execute(sql, from_params + params).fetchone()[0]
                elapsed = time.time() - start_time
                logger.info(
                    "count_urls: evidence_id=%s, filters=%s, search_index=%s, count=%d, elapsed=%.3fs",
                    evidence_id,
                    spec,
                    bool(from_params),
                    count,
                    elapsed,
                )
                return count

    def iter_urls(
        self,
        evidence_id: int,
        *,
        domain_like: str = "%",
        url_like: str = "%",
        tag_like: str = "%",
        discovered_by: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,  # "all", "matched", "unmatched", or specific list name
        limit: int = 100,
        offset: int = 0,
        filters: Optional[UrlFilterSpec] = None,
    ) -> List[Dict[str, Any]]:
        """
        Paginated URL listing with filtering (OFFSET-based).

        Prefer ``fetch_url_rows_after`` for page navigation: OFFSET work grows
        with the page depth (issue #61).

        Args:
            evidence_id: Evidence ID
            domain_like: Legacy domain filter pattern (SQL LIKE)
            url_like: URL filter pattern (SQL LIKE)
            tag_like: Tag filter pattern (SQL LIKE)
            discovered_by: Optional list of source filters
            match_filter: "all", "matched", "unmatched", or specific list name
            limit: Page size
            offset: Page offset
            filters: Filter spec; overrides the legacy keyword filters when given

        Returns:
            List of URL dictionaries in ``URL_ORDER_BY`` order
        """
        logger = logging.getLogger(__name__)
        start_time = time.time()

        spec = self._legacy_url_filter_spec(filters, domain_like, url_like, tag_like, discovered_by, match_filter)
        where, params = build_url_filter_predicates(evidence_id, spec)

        params.extend([limit, offset])
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                from_sql, from_params = build_url_from_clause(conn, spec)
                # Tags/matches are batch-loaded separately (no GROUP_CONCAT JOIN).
                # The id tie-breaker makes OFFSET batches deterministic (issue #61).
                sql = f"""
                    SELECT {_URL_ROW_COLUMNS}
                    {from_sql}
                    WHERE {' AND '.join(where)}
                    ORDER BY {URL_ORDER_BY}
                    LIMIT ? OFFSET ?
                """
                cursor = conn.execute(sql, from_params + params)
                results = [dict(row) for row in cursor.fetchall()]
                elapsed = time.time() - start_time
                logger.info(
                    "iter_urls: evidence_id=%s, returned %d rows, elapsed=%.3fs",
                    evidence_id,
                    len(results),
                    elapsed,
                )
                return results

    def fetch_url_rows_after(
        self,
        evidence_id: int,
        filters: UrlFilterSpec,
        after: Optional[UrlCursor],
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Return up to ``limit`` URL rows following ``after`` in ``URL_ORDER_BY`` order.

        Issue #61 keyset pagination. Runs in two phases so NULL timestamps
        (common, see #47) are not OR-ed into the timestamp range
        (``first_seen_utc < ? OR first_seen_utc IS NULL``); each phase is an
        index seek:

        1. Timestamped rows after the cursor (skipped once the cursor is
           inside the NULL bucket).
        2. Rows without timestamp, by ascending id, if phase 1 ran short.

        Args:
            evidence_id: Evidence ID
            filters: Filter spec
            after: Cursor of the last row already returned (``None`` = start)
            limit: Maximum number of rows to return

        Returns:
            List of URL dictionaries
        """
        return self._fetch_url_rows_after(evidence_id, filters, after, limit)

    def _fetch_url_rows_after(
        self,
        evidence_id: int,
        filters: UrlFilterSpec,
        after: Optional[UrlCursor],
        limit: int,
        extra_where: Tuple[str, ...] = (),
    ) -> List[Dict[str, Any]]:
        """``fetch_url_rows_after`` with additional parameterless WHERE terms."""
        logger = logging.getLogger(__name__)
        start_time = time.time()
        if limit <= 0:
            return []

        base_where, base_params = build_url_filter_predicates(evidence_id, filters)
        base_where.extend(extra_where)
        rows: List[Dict[str, Any]] = []

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                from_sql, from_params = build_url_from_clause(conn, filters)
                if after is None or after.first_seen_utc is not None:
                    where = base_where + ["u.first_seen_utc IS NOT NULL"]
                    params = from_params + base_params
                    if after is not None:
                        where.append("u.first_seen_utc <= ? AND (u.first_seen_utc < ? OR u.id > ?)")
                        params.extend([after.first_seen_utc, after.first_seen_utc, after.id])
                    sql = f"""
                        SELECT {_URL_ROW_COLUMNS}
                        {from_sql}
                        WHERE {' AND '.join(where)}
                        ORDER BY {URL_ORDER_BY}
                        LIMIT ?
                    """
                    params.append(limit)
                    rows.extend(dict(row) for row in conn.execute(sql, params).fetchall())

                remaining = limit - len(rows)
                if remaining > 0:
                    where = base_where + ["u.first_seen_utc IS NULL"]
                    params = from_params + base_params
                    if after is not None and after.first_seen_utc is None:
                        where.append("u.id > ?")
                        params.append(after.id)
                    sql = f"""
                        SELECT {_URL_ROW_COLUMNS}
                        {from_sql}
                        WHERE {' AND '.join(where)}
                        ORDER BY u.id ASC
                        LIMIT ?
                    """
                    params.append(remaining)
                    rows.extend(dict(row) for row in conn.execute(sql, params).fetchall())

        logger.info(
            "fetch_url_rows_after: evidence_id=%s, after=%s, returned %d rows, elapsed=%.3fs",
            evidence_id,
            after,
            len(rows),
            time.time() - start_time,
        )
        return rows

    def iter_url_rows(
        self,
        evidence_id: int,
        filters: UrlFilterSpec,
        *,
        batch_size: int = 10000,
    ) -> Iterator[Dict[str, Any]]:
        """Yield every URL row matching ``filters`` in ``URL_ORDER_BY`` order.

        Uses keyset batches, so the cost per batch does not grow with depth.
        """
        after: Optional[UrlCursor] = None
        while True:
            batch = self.fetch_url_rows_after(evidence_id, filters, after, batch_size)
            yield from batch
            if len(batch) < batch_size:
                return
            after = UrlCursor.from_row(batch[-1])

    def load_url_page(self, query: UrlPageQuery) -> UrlPage:
        """
        Load one flat URL page with batch tag/match maps.

        Issue #60: single entry point for background URL loading. The tag and
        match maps are complete for the returned rows (empty values included).
        Issue #61: keyset page (``page_size + 1`` rows fetched to detect a next
        page); the exact total is loaded separately via ``count_urls``.

        Args:
            query: Filters and page position

        Returns:
            UrlPage with rows, has_more flag, data version, and per-row tag/match strings
        """
        with self._use_evidence_conn(query.evidence_id):
            with self._connect() as conn:
                data_version = conn.execute("PRAGMA data_version").fetchone()[0]
        rows = self.fetch_url_rows_after(
            query.evidence_id, query.filters, query.after, query.page_size + 1
        )
        has_more = len(rows) > query.page_size
        rows = rows[: query.page_size]
        tags, matches = self._load_url_row_strings(query.evidence_id, rows)
        return UrlPage(
            query=query, rows=rows, has_more=has_more, data_version=data_version, tags=tags, matches=matches
        )

    def _load_url_row_strings(
        self, evidence_id: int, rows: List[Dict[str, Any]]
    ) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Batch-load tag and match strings with an entry for every row id ("" if none)."""
        url_ids = [row["id"] for row in rows if row.get("id")]
        tags: Dict[int, str] = {url_id: "" for url_id in url_ids}
        matches: Dict[int, str] = {url_id: "" for url_id in url_ids}
        if url_ids:
            tags.update(self.get_tag_strings_for_artifacts(evidence_id, "url", url_ids))
            matches.update(self.get_url_matches_batch(evidence_id, url_ids))
        return tags, matches

    # -------------------------------------------------------------------------
    # Grouped URL view (issue #64)
    # -------------------------------------------------------------------------

    def load_url_domain_groups(self, query: UrlDomainGroupQuery) -> UrlDomainGroupPage:
        """
        Load one page of domain groups with exact URL counts.

        Groups cover all URLs matching ``query.filters`` (the flat view's
        filters). Keyset page over ``GROUP BY domain COLLATE NOCASE`` in that
        order: served by idx_urls_evidence_domain_nocase_first_seen without a
        sort step and ended by the LIMIT, so the cost is bounded by the rows of
        the returned groups (plus non-matching rows skipped on the way). The
        group of URLs without a domain follows the last named group.

        Args:
            query: Filters and keyset position

        Returns:
            UrlDomainGroupPage with up to ``page_size`` named groups
        """
        logger = logging.getLogger(__name__)
        start_time = time.time()
        page_size = max(1, query.page_size)
        where, params = build_url_filter_predicates(query.evidence_id, query.filters)

        with self._use_evidence_conn(query.evidence_id):
            with self._connect() as conn:
                from_sql, from_params = build_url_from_clause(conn, query.filters)
                # Every named domain is greater than '' (NULL compares to
                # nothing): one index range excludes the no-domain group.
                sql = f"""
                    SELECT MIN(u.domain) AS domain, COUNT(*) AS cnt
                    {from_sql}
                    WHERE {' AND '.join(where)} AND u.domain > ? COLLATE NOCASE
                    GROUP BY u.domain COLLATE NOCASE
                    ORDER BY u.domain COLLATE NOCASE
                    LIMIT ?
                """
                rows = conn.execute(
                    sql, from_params + params + [query.after or "", page_size + 1]
                ).fetchall()
                has_more = len(rows) > page_size
                groups = [UrlDomainGroup(row[0], int(row[1])) for row in rows[:page_size]]
                if not has_more:
                    count_sql = f"""
                        SELECT COUNT(*)
                        {from_sql}
                        WHERE {' AND '.join(where)}
                          AND ({' OR '.join(_NO_DOMAIN_PREDICATES)})
                    """
                    missing = conn.execute(count_sql, from_params + params).fetchone()[0]
                    if missing:
                        groups.append(UrlDomainGroup(None, int(missing)))

        logger.info(
            "load_url_domain_groups: evidence_id=%s, after=%r, returned %d groups, has_more=%s, elapsed=%.3fs",
            query.evidence_id,
            query.after,
            len(groups),
            has_more,
            time.time() - start_time,
        )
        return UrlDomainGroupPage(query=query, groups=groups, has_more=has_more)

    def load_url_domain_children(self, query: UrlDomainChildQuery) -> UrlDomainChildPage:
        """
        Load one keyset page of a domain group's URLs with batch tag/match maps.

        A named group's URLs are the flat listing with its domain as exact
        domain filter (same ``COLLATE NOCASE`` comparison as the grouping);
        the user's own exact domain filter can only select that group anyway.
        URLs without a domain are read as two index-served streams (NULL,
        '') merged by the sort key.

        Args:
            query: Filters, group and keyset position

        Returns:
            UrlDomainChildPage in ``URL_ORDER_BY`` order
        """
        page_size = max(1, query.page_size)
        limit = page_size + 1
        if query.domain is not None:
            spec = replace(query.filters, domain_exact=query.domain)
            rows = self._fetch_url_rows_after(query.evidence_id, spec, query.after, limit)
        else:
            merged: List[Dict[str, Any]] = []
            for predicate in _NO_DOMAIN_PREDICATES:
                merged.extend(
                    self._fetch_url_rows_after(
                        query.evidence_id, query.filters, query.after, limit, extra_where=(predicate,)
                    )
                )
            rows = _url_sort_key_sorted(merged)[:limit]
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        tags, matches = self._load_url_row_strings(query.evidence_id, rows)
        return UrlDomainChildPage(query=query, rows=rows, has_more=has_more, tags=tags, matches=matches)

    # -------------------------------------------------------------------------
    # URL Search Index (issue #62)
    # -------------------------------------------------------------------------

    def get_url_search_index_status(self, evidence_id: int) -> UrlSearchIndexStatus:
        """Return the URL search index coverage for an evidence."""
        if not self._evidence_db_exists(evidence_id):
            # No database yet (nothing extracted): nothing to index.
            return UrlSearchIndexStatus(UrlSearchIndexStatus.READY)
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return get_url_search_index_status(conn)

    def build_url_search_index(
        self,
        evidence_id: int,
        *,
        is_cancelled: Callable[[], bool] = lambda: False,
        progress_callback: Optional[Callable[[UrlSearchIndexStatus], None]] = None,
        batch_size: int = URL_SEARCH_BUILD_BATCH_SIZE,
    ) -> UrlSearchIndexStatus:
        """
        Index unindexed URLs in batches until the index is ready or cancelled.

        Each batch is its own write transaction, so cancelling (checked between
        batches) or a crash loses at most one batch; the next call resumes.
        Search stays complete meanwhile (unindexed URLs are scanned). A build
        session is recorded in ``process_log`` once its first batch committed
        (sessions that index nothing leave no entry).

        Args:
            evidence_id: Evidence ID
            is_cancelled: Checked before every batch
            progress_callback: Called with the status after every batch
            batch_size: URL rows per batch

        Returns:
            Index status after the last batch
        """
        import json
        from core.database.helpers import create_process_log, finalize_process_log

        logger = logging.getLogger(__name__)
        status = self.get_url_search_index_status(evidence_id)
        if status.state != UrlSearchIndexStatus.BUILDING or is_cancelled():
            return status

        start_watermark = status.indexed_through_id
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                log_id: Optional[int] = None
                indexed = 0
                start_time = time.time()
                try:
                    while not is_cancelled():
                        rows = build_url_search_index_step(conn, batch_size)
                        if rows and log_id is None:
                            log_id = create_process_log(
                                conn,
                                evidence_id,
                                task=URL_SEARCH_INDEX_BUILD_TASK,
                                command=(
                                    f"Index URLs with id > {start_watermark} for substring search "
                                    f"(SQLite {sqlite3.sqlite_version} FTS5 trigram)"
                                ),
                            )
                        indexed += rows
                        status = get_url_search_index_status(conn)
                        if progress_callback is not None:
                            progress_callback(status)
                        if status.state != UrlSearchIndexStatus.BUILDING:
                            break
                except Exception as exc:
                    if log_id is not None:
                        try:
                            finalize_process_log(conn, log_id, exit_code=1, stdout=None, stderr=str(exc))
                        except sqlite3.Error as log_exc:  # e.g. still locked: keep the original error
                            logger.error("Could not finalize process_log %s: %s", log_id, log_exc)
                    raise
                result = "cancelled" if status.state == UrlSearchIndexStatus.BUILDING else status.state
                if log_id is not None:
                    finalize_process_log(
                        conn,
                        log_id,
                        exit_code=0,
                        stdout=json.dumps(
                            {
                                "result": result,
                                "rows_indexed": indexed,
                                "indexed_through_id": status.indexed_through_id,
                                "max_url_id": status.max_url_id,
                            },
                            sort_keys=True,
                        ),
                        stderr=None,
                    )
        logger.info(
            "build_url_search_index: evidence_id=%s, result=%s, rows_indexed=%d, elapsed=%.1fs",
            evidence_id,
            result,
            indexed,
            time.time() - start_time,
        )
        return status

    def update_url_tags(self, evidence_id: int, url_id: int, tags_str: str) -> None:
        """
        Update tags for a URL (Legacy compatibility wrapper).
        Parses comma-separated string and updates tag associations.

        Tags are compared by normalized (lowercased) name, like tag lookup
        and creation, so changing only the letter case keeps the tag. A
        current tag whose name contains a comma is kept while its full name
        still appears as consecutive entries (issue #66).

        Args:
            evidence_id: Evidence ID
            url_id: URL ID
            tags_str: Comma-separated tag names
        """
        # 1. Get current tags
        current_tags = {t['name'] for t in self.get_artifact_tags(evidence_id, 'url', url_id)}

        # 2. Parse new tags; keep current comma-containing names intact
        entries = [t.strip() for t in tags_str.split(',')]
        new_tags = set()
        for name in sorted((n for n in current_tags if ',' in n), key=len, reverse=True):
            parts = [p.strip().lower() for p in name.split(',')]
            for start in range(len(entries) - len(parts) + 1):
                if [e.lower() for e in entries[start:start + len(parts)]] == parts:
                    new_tags.add(name)
                    entries[start:start + len(parts)] = [""] * len(parts)
                    break
        new_tags.update(e for e in entries if e)

        # 3. Determine changes by normalized name
        current_by_key = {t.lower(): t for t in current_tags}
        new_by_key = {t.lower(): t for t in new_tags}
        to_add = [new_by_key[k] for k in sorted(new_by_key.keys() - current_by_key.keys())]
        to_remove = [current_by_key[k] for k in sorted(current_by_key.keys() - new_by_key.keys())]

        # 4. Apply changes
        for tag in to_add:
            self.tag_artifact(evidence_id, tag, 'url', url_id)

        for tag in to_remove:
            self.untag_artifact(evidence_id, tag, 'url', url_id)

    def get_urls_by_tag(self, evidence_id: int, tag: str) -> List[Dict[str, Any]]:
        """
        Get all URLs associated with a specific tag.

        Args:
            evidence_id: Evidence ID
            tag: Tag name to filter by

        Returns:
            List of URL dictionaries
        """
        # Updated to use unified tagging system
        # Issue #47: Use first_seen_utc directly for ORDER BY to enable index usage
        sql = """
            SELECT u.id, u.url, u.domain, u.scheme, u.discovered_by, u.first_seen_utc,
                   u.last_seen_utc, u.source_path, u.tags, u.notes
            FROM urls u
            JOIN tag_associations ta ON u.id = ta.artifact_id
            JOIN tags t ON ta.tag_id = t.id
            WHERE u.evidence_id = ?
              AND ta.artifact_type = 'url'
              AND t.name_normalized = ?
            ORDER BY u.first_seen_utc DESC
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id, tag.lower()))
                return [dict(row) for row in cursor.fetchall()]

    def get_url_matches(self, evidence_id: int, url_id: int) -> str:
        """
        Get matched list names for a single URL (on-demand loading).

        Args:
            evidence_id: Evidence ID
            url_id: URL ID

        Returns:
            Comma-separated list of matched list names, or empty string if no matches
        """
        sql = """
            SELECT GROUP_CONCAT(DISTINCT list_name)
            FROM url_matches
            WHERE evidence_id = ? AND url_id = ?
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                result = conn.execute(sql, (evidence_id, url_id)).fetchone()
                # SQLite's GROUP_CONCAT with DISTINCT uses ',' as default separator
                return result[0] if result and result[0] else ""

    def get_url_matches_batch(
        self,
        evidence_id: int,
        url_ids: Iterable[int],
    ) -> Dict[int, str]:
        """
        Get matched list names for multiple URLs in a single query.

        Issue #47: Batch loading to avoid N+1 query pattern.
        Loads all matches for a page of URLs in one query instead of
        calling get_url_matches() individually per row.

        Args:
            evidence_id: Evidence ID
            url_ids: Iterable of URL IDs to fetch matches for

        Returns:
            Mapping of url_id -> comma-separated list of matched list names.
            URLs without matches are not included in the result.
        """
        url_ids_list = list(url_ids)
        if not url_ids_list:
            return {}

        placeholders = ",".join("?" for _ in url_ids_list)
        sql = f"""
            SELECT url_id, GROUP_CONCAT(DISTINCT list_name) AS matched_lists
            FROM url_matches
            WHERE evidence_id = ? AND url_id IN ({placeholders})
            GROUP BY url_id
        """
        params = [evidence_id, *url_ids_list]

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                return {
                    row["url_id"]: row["matched_lists"] or ""
                    for row in cursor.fetchall()
                }

    # -------------------------------------------------------------------------
    # URL Filter Helpers (Cached)
    # -------------------------------------------------------------------------

    def list_url_domains(self, evidence_id: int, limit: Optional[int] = None, use_cache: bool = True) -> List[str]:
        """
        List unique domains for an evidence.

        Args:
            evidence_id: Evidence ID
            limit: Optional max number of domains to return (for performance)
            use_cache: Whether to use cached results (default True)

        Returns:
            List of domain strings, sorted alphabetically

        Served from the shared case statistics cache (issue #63).
        """
        def compute() -> List[str]:
            if limit:
                sql = """
                    SELECT domain
                    FROM urls
                    WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                    GROUP BY domain
                    ORDER BY domain
                    LIMIT ?
                """
                params: Tuple[Any, ...] = (evidence_id, limit)
            else:
                sql = """
                    SELECT DISTINCT domain
                    FROM urls
                    WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                    ORDER BY domain
                """
                params = (evidence_id,)
            with self._use_evidence_conn(evidence_id):
                with self._connect() as conn:
                    cursor = conn.execute(sql, params)
                    return [row[0] for row in cursor.fetchall()]

        return self._cached_stat(
            evidence_id,
            ("url_domains", limit or None),
            URL_FILTER_LIST_CATEGORIES,
            compute,
            use_cache=use_cache,
            # Guard: empty list (uncached) if the evidence DB doesn't exist yet
            empty=list,
        )

    def list_url_sources(self, evidence_id: int, use_cache: bool = True) -> List[str]:
        """
        List all unique discovery sources for URLs.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of source strings, sorted alphabetically

        Served from the shared case statistics cache (issue #63).
        """
        def compute() -> List[str]:
            sql = """
                SELECT DISTINCT discovered_by
                FROM urls
                WHERE evidence_id = ? AND discovered_by IS NOT NULL
                ORDER BY discovered_by
            """
            with self._use_evidence_conn(evidence_id):
                with self._connect() as conn:
                    cursor = conn.execute(sql, (evidence_id,))
                    return [row[0] for row in cursor.fetchall()]

        return self._cached_stat(
            evidence_id,
            ("url_sources",),
            URL_FILTER_LIST_CATEGORIES,
            compute,
            use_cache=use_cache,
            empty=list,
        )

    def list_url_match_lists(self, evidence_id: int, use_cache: bool = True) -> List[str]:
        """
        List all unique match list names for URLs.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of list names, sorted alphabetically

        Served from the shared case statistics cache (issue #63).
        """
        def compute() -> List[str]:
            sql = """
                SELECT DISTINCT list_name
                FROM url_matches
                WHERE evidence_id = ?
                ORDER BY list_name
            """
            with self._use_evidence_conn(evidence_id):
                with self._connect() as conn:
                    try:
                        cursor = conn.execute(sql, (evidence_id,))
                        return [row[0] for row in cursor.fetchall()]
                    except sqlite3.OperationalError as exc:
                        # Only a missing table (very old DB) means "no lists";
                        # anything else (e.g. a cancelled read) propagates
                        # so it is never cached as an empty list.
                        if "no such table" not in str(exc).lower():
                            raise
                        return []

        return self._cached_stat(
            evidence_id,
            ("url_match_lists",),
            URL_MATCH_LIST_CATEGORIES,
            compute,
            use_cache=use_cache,
            empty=list,
        )

    # -------------------------------------------------------------------------
    # URL Domain Statistics
    # -------------------------------------------------------------------------

    def get_top_domains(
        self,
        evidence_id: int,
        limit: int = 500,
        *,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Get top domains by URL frequency for filter dropdowns.

        Domains are grouped ASCII case-insensitively, like the exact domain
        filter (``UrlFilterSpec.domain_exact``), so each entry's count equals
        the number of URLs selecting it shows (issue #67). The entry's domain
        is the smallest spelling in its group (``MIN``), which is deterministic.
        Ties in count are ordered by that domain, ignoring ASCII case (unique,
        since the groups are case-insensitively distinct).

        Served from the shared case statistics cache (issue #63).

        Args:
            evidence_id: Evidence ID
            limit: Maximum number of domains to return (default 500)
            use_cache: Whether to use cached results (default True)

        Returns:
            Dict with 'items' (list of {domain, count}), 'total_count'
            (case-insensitively distinct non-empty domains), 'truncated'
        """
        def compute() -> Dict[str, Any]:
            with self._use_evidence_conn(evidence_id):
                with self._connect() as conn:
                    # Both queries are covering scans of
                    # idx_urls_evidence_domain_nocase_first_seen (migration 0004).
                    # GROUP BY in a subquery walks that index in order; COUNT(DISTINCT
                    # domain COLLATE NOCASE) would build a temporary B-tree instead.
                    total_count = conn.execute(
                        """
                        SELECT COUNT(*) FROM (
                            SELECT 1
                            FROM urls
                            WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                            GROUP BY domain COLLATE NOCASE
                        )
                        """,
                        (evidence_id,)
                    ).fetchone()[0]

                    # Get top N domains by frequency
                    cursor = conn.execute(
                        """
                        SELECT MIN(domain) AS representative, COUNT(*) AS cnt
                        FROM urls
                        WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                        GROUP BY domain COLLATE NOCASE
                        ORDER BY cnt DESC, representative COLLATE NOCASE ASC
                        LIMIT ?
                        """,
                        (evidence_id, limit)
                    )

                    domains = [
                        {"domain": row[0], "count": row[1]}
                        for row in cursor.fetchall()
                    ]

                    return {
                        "items": domains,
                        "total_count": total_count,
                        "truncated": total_count > limit,
                    }

        return self._cached_stat(
            evidence_id,
            ("top_domains", limit),
            URL_FILTER_LIST_CATEGORIES,
            compute,
            use_cache=use_cache,
            # Guard: same shape as "no domains" if the evidence DB doesn't exist yet
            empty=lambda: {"items": [], "total_count": 0, "truncated": False},
        )

    # -------------------------------------------------------------------------
    # URL Deduplication
    # -------------------------------------------------------------------------

    def analyze_url_duplicates(
        self,
        evidence_id: int,
        sources: List[str],
        *,
        unique_by_first_seen: bool = True,
        unique_by_last_seen: bool = True,
        unique_by_source: bool = False,
    ) -> Dict[str, Any]:
        """
        Analyze URL duplicates based on uniqueness constraints.

        Args:
            evidence_id: Evidence ID
            sources: List of discovered_by values to include
            unique_by_first_seen: Include first_seen_utc in uniqueness
            unique_by_last_seen: Include last_seen_utc in uniqueness
            unique_by_source: Include discovered_by in uniqueness

        Returns:
            Dict with total, unique_count, duplicates counts

        Initial implementation for URL debloating.
        """
        from core.database.helpers import analyze_url_duplicates as helper_analyze

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return helper_analyze(
                    conn,
                    evidence_id,
                    sources,
                    unique_by_first_seen=unique_by_first_seen,
                    unique_by_last_seen=unique_by_last_seen,
                    unique_by_source=unique_by_source,
                )

    def deduplicate_urls(
        self,
        evidence_id: int,
        sources: List[str],
        *,
        unique_by_first_seen: bool = True,
        unique_by_last_seen: bool = True,
        unique_by_source: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, Any]:
        """
        Deduplicate URLs based on uniqueness constraints.

        Merges duplicate rows by consolidating source_paths, aggregating
        timestamps, and merging tags.

        Args:
            evidence_id: Evidence ID
            sources: List of discovered_by values to include
            unique_by_first_seen: Include first_seen_utc in uniqueness
            unique_by_last_seen: Include last_seen_utc in uniqueness
            unique_by_source: Include discovered_by in uniqueness
            progress_callback: Optional callback(current, total) for progress

        Returns:
            Dict with total_before, total_after, duplicates_removed, unique_urls_affected

        Initial implementation for URL debloating.
        """
        import json
        from core.database.helpers import deduplicate_urls as helper_dedupe
        from core.database.helpers import create_process_log, finalize_process_log

        with self._use_evidence_conn(evidence_id):
            try:
                with self._connect() as conn:
                    # Log the operation start
                    constraints = []
                    if unique_by_first_seen:
                        constraints.append("first_seen")
                    if unique_by_last_seen:
                        constraints.append("last_seen")
                    if unique_by_source:
                        constraints.append("source")

                    log_id = create_process_log(
                        conn,
                        evidence_id,
                        task="url_deduplication",
                        command=f"Deduplicate URLs from {', '.join(sources)} (constraints: url, {', '.join(constraints)})",
                    )

                    try:
                        result = helper_dedupe(
                            conn,
                            evidence_id,
                            sources,
                            unique_by_first_seen=unique_by_first_seen,
                            unique_by_last_seen=unique_by_last_seen,
                            unique_by_source=unique_by_source,
                            progress_callback=progress_callback,
                        )

                        # Finalize log with success
                        stdout = json.dumps({
                            "sources": sources,
                            "constraints": ["url"] + constraints,
                            "total_before": result["total_before"],
                            "total_after": result["total_after"],
                            "duplicates_removed": result["duplicates_removed"],
                        })
                        finalize_process_log(conn, log_id, exit_code=0, stdout=stdout, stderr=None)

                    except Exception as e:
                        # Finalize log with error
                        finalize_process_log(conn, log_id, exit_code=1, stdout=None, stderr=str(e))
                        raise
                return result
            finally:
                # Invalidate URL-related caches for this evidence only after
                # the final commit (on leaving the with block), so a reader
                # cannot cache pre-commit data under the new revision. Also
                # on failure: the process_log rows (committed by their
                # helpers) and possibly partial changes are already written.
                self.invalidate_filter_cache(evidence_id)

