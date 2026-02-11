"""URL query operations for UI layer.

This module provides URL-specific queries for the UI:
- Paginated URL listing with LIKE filtering
- Tag-aware queries via tag_associations
- Match filtering (matched/unmatched/specific list)
- Domain and source listing with caching

Extracted from case_data.py for modular repository pattern.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional

from ._base import BaseDataAccess


class UrlQueryMixin(BaseDataAccess):
    """Mixin providing URL query operations for UI views.

    Features:
    - Paginated URL listing with LIKE filtering
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

    def count_urls(
        self,
        evidence_id: int,
        *,
        domain_like: str = "%",
        url_like: str = "%",
        tag_like: str = "%",
        discovered_by: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,
    ) -> int:
        """
        Fast count of URLs matching filters (without GROUP_CONCAT).

        Args:
            evidence_id: Evidence ID
            domain_like: Domain filter pattern (SQL LIKE)
            url_like: URL filter pattern (SQL LIKE)
            tag_like: Tag filter pattern (SQL LIKE)
            discovered_by: Optional list of source filters
            match_filter: Match filter ("matched", "unmatched", or specific list name)

        Returns:
            Total count of matching URLs
        """
        logger = logging.getLogger(__name__)
        start_time = time.time()

        params: List[Any] = [evidence_id, domain_like, url_like]
        where = ["u.evidence_id = ?", "COALESCE(u.domain, '') LIKE ?", "u.url LIKE ?"]

        if tag_like and tag_like != "%":
            where.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'url'
                    AND ta.artifact_id = u.id
                    AND t.name LIKE ?
                )
            """)
            params.append(tag_like)

        if discovered_by:
            placeholders = ",".join("?" for _ in discovered_by)
            where.append(f"u.discovered_by IN ({placeholders})")
            params.extend(discovered_by)

        # Match filter (same logic as iter_urls)
        if match_filter and match_filter != "all":
            if match_filter == "matched":
                where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            elif match_filter == "unmatched":
                where.append("NOT EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            else:
                # Specific list name
                where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id AND m.list_name = ?)")
                params.append(match_filter)

        # Fast COUNT query (no JOIN, no GROUP_CONCAT)
        sql = f"""
            SELECT COUNT(*)
            FROM urls u
            WHERE {' AND '.join(where)}
        """

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                count = conn.execute(sql, params).fetchone()[0]
                elapsed = time.time() - start_time
                logger.info(
                    "count_urls: evidence_id=%s, filters=%s, count=%d, elapsed=%.3fs",
                    evidence_id,
                    {"domain": domain_like, "url": url_like, "discovered_by": discovered_by, "match_filter": match_filter},
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
    ) -> List[Dict[str, Any]]:
        """
        Paginated URL listing with filtering.

        Args:
            evidence_id: Evidence ID
            domain_like: Domain filter pattern (SQL LIKE)
            url_like: URL filter pattern (SQL LIKE)
            tag_like: Tag filter pattern (SQL LIKE)
            discovered_by: Optional list of source filters
            match_filter: "all", "matched", "unmatched", or specific list name
            limit: Page size
            offset: Page offset

        Returns:
            List of URL dictionaries
        """
        logger = logging.getLogger(__name__)
        start_time = time.time()

        params: List[Any] = [evidence_id, domain_like, url_like]
        where = ["u.evidence_id = ?", "COALESCE(u.domain, '') LIKE ?", "u.url LIKE ?"]

        if tag_like and tag_like != "%":
            where.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'url'
                    AND ta.artifact_id = u.id
                    AND t.name LIKE ?
                )
            """)
            params.append(tag_like)

        if discovered_by:
            placeholders = ",".join("?" for _ in discovered_by)
            where.append(f"u.discovered_by IN ({placeholders})")
            params.extend(discovered_by)

        # Match filter - use EXISTS for performance (no JOIN needed)
        if match_filter and match_filter != "all":
            if match_filter == "matched":
                where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            elif match_filter == "unmatched":
                where.append("NOT EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            else:
                # Specific list name
                where.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id AND m.list_name = ?)")
                params.append(match_filter)

        # Removed GROUP_CONCAT - tags loaded on-demand via get_artifact_tags_str()
        # This removes the LEFT JOIN overhead for every query (30-50% faster)
        sql = f"""
            SELECT u.id, u.url, u.domain, u.scheme, u.discovered_by, u.first_seen_utc,
                   u.last_seen_utc, u.source_path, u.notes, u.occurrence_count
            FROM urls u
            WHERE {' AND '.join(where)}
            ORDER BY COALESCE(u.first_seen_utc, u.last_seen_utc) DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                results = [dict(row) for row in cursor.fetchall()]
                elapsed = time.time() - start_time
                logger.info(
                    "iter_urls: evidence_id=%s, returned %d rows, elapsed=%.3fs",
                    evidence_id,
                    len(results),
                    elapsed,
                )
                return results

    def update_url_tags(self, evidence_id: int, url_id: int, tags_str: str) -> None:
        """
        Update tags for a URL (Legacy compatibility wrapper).
        Parses comma-separated string and updates tag associations.

        Args:
            evidence_id: Evidence ID
            url_id: URL ID
            tags_str: Comma-separated tag names
        """
        # 1. Get current tags
        current_tags = {t['name'] for t in self.get_artifact_tags(evidence_id, 'url', url_id)}

        # 2. Parse new tags
        new_tags = {t.strip() for t in tags_str.split(',') if t.strip()}

        # 3. Determine changes
        to_add = new_tags - current_tags
        to_remove = current_tags - new_tags

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
        sql = """
            SELECT u.id, u.url, u.domain, u.scheme, u.discovered_by, u.first_seen_utc,
                   u.last_seen_utc, u.source_path, u.tags, u.notes
            FROM urls u
            JOIN tag_associations ta ON u.id = ta.artifact_id
            JOIN tags t ON ta.tag_id = t.id
            WHERE u.evidence_id = ?
              AND ta.artifact_type = 'url'
              AND t.name_normalized = ?
            ORDER BY COALESCE(u.first_seen_utc, u.last_seen_utc) DESC
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

        Added in-memory caching for performance.
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []
        cache_key = f"url_domains_{evidence_id}_{limit or 'all'}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        if limit:
            sql = """
                SELECT domain
                FROM urls
                WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                GROUP BY domain
                ORDER BY domain
                LIMIT ?
            """
            params = (evidence_id, limit)
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
                result = [row[0] for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def list_url_sources(self, evidence_id: int, use_cache: bool = True) -> List[str]:
        """
        List all unique discovery sources for URLs.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of source strings, sorted alphabetically

        Added in-memory caching for performance.
        """
        cache_key = f"url_sources_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        sql = """
            SELECT DISTINCT discovered_by
            FROM urls
            WHERE evidence_id = ? AND discovered_by IS NOT NULL
            ORDER BY discovered_by
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                result = [row[0] for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def list_url_match_lists(self, evidence_id: int, use_cache: bool = True) -> List[str]:
        """
        List all unique match list names for URLs.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of list names, sorted alphabetically

        Added in-memory caching for performance.
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []
        cache_key = f"url_match_lists_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        sql = """
            SELECT DISTINCT list_name
            FROM url_matches
            WHERE evidence_id = ?
            ORDER BY list_name
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Check if table exists first (migration might not have run if very old DB)
                # But we assume schema is up to date.
                try:
                    cursor = conn.execute(sql, (evidence_id,))
                    result = [row[0] for row in cursor.fetchall()]
                except sqlite3.OperationalError:
                    # Table might not exist yet
                    result = []

        self._set_cached(cache_key, result)
        return result

    # -------------------------------------------------------------------------
    # URL Domain Statistics
    # -------------------------------------------------------------------------

    def get_top_domains(
        self,
        evidence_id: int,
        limit: int = 500,
    ) -> Dict[str, Any]:
        """
        Get top domains by URL frequency for filter dropdowns.

        Args:
            evidence_id: Evidence ID
            limit: Maximum number of domains to return (default 500)

        Returns:
            Dict with 'items' (list of {domain, count}), 'total_count', 'truncated'
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Get total count of unique domains
                total_count = conn.execute(
                    "SELECT COUNT(DISTINCT domain) FROM urls WHERE evidence_id = ?",
                    (evidence_id,)
                ).fetchone()[0]

                # Get top N domains by frequency
                cursor = conn.execute(
                    """
                    SELECT domain, COUNT(*) as cnt
                    FROM urls
                    WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
                    GROUP BY domain
                    ORDER BY cnt DESC
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

                # Invalidate URL-related caches for this evidence
                self.invalidate_filter_cache(evidence_id)

                return result

