"""Download query operations for UI layer.

This module provides download-specific queries for the UI:
- Investigator download CRUD (wrapping core helpers)
- Downloadable URL listing with file_classifier filters
- Tag inheritance between URLs and downloads
- Statistics and domain listing

Extracted from case_data.py for modular repository pattern.
Added file_extension column backfill for optimized queries.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from core.database.helpers import downloads as downloads_helpers
from core.file_classifier import (
    get_extension,
    classify_file_type,
    DOWNLOADABLE_EXTENSIONS,
)

from ._base import BaseDataAccess

logger = logging.getLogger(__name__)


class DownloadQueryMixin(BaseDataAccess):
    """Mixin providing download query operations for UI views.

    Features:
    - CRUD operations for investigator downloads (wrapping core helpers)
    - Downloadable URL listing with file_classifier integration
    - Tag inheritance between URLs and downloads
    - Statistics and domain listing for dropdowns
    - Lazy backfill of file_extension column for fast queries

    All methods operate on the evidence database.

    Extracted from CaseDataAccess for modular architecture.
    Added file_extension backfill for 10-100x query speedup.
    """

    # -------------------------------------------------------------------------
    # URL Extension Backfill
    # -------------------------------------------------------------------------

    def backfill_url_extensions(
        self,
        evidence_id: int,
        batch_size: int = 5000,
    ) -> Tuple[int, float]:
        """
        Backfill file_extension and file_type columns for URLs.

        This is called lazily when the Download tab is opened. It only
        processes URLs where file_extension IS NULL, making it idempotent.

        Args:
            evidence_id: Evidence ID
            batch_size: Number of rows to process per batch (default 5000)

        Returns:
            Tuple of (rows_updated, elapsed_seconds)

        New function for query optimization.
        """
        if not self._evidence_db_exists(evidence_id):
            return (0, 0.0)

        start_time = time.time()
        total_updated = 0

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Check if columns exist (migration may not have run yet)
                cursor = conn.execute("PRAGMA table_info(urls)")
                columns = {row[1] for row in cursor.fetchall()}
                if 'file_extension' not in columns:
                    logger.warning("file_extension column not found - migration 0008 required")
                    return (0, 0.0)

                # Count URLs needing backfill
                count_row = conn.execute(
                    "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND file_extension IS NULL",
                    (evidence_id,)
                ).fetchone()
                pending_count = count_row[0] if count_row else 0

                if pending_count == 0:
                    logger.debug("No URLs need extension backfill for evidence %d", evidence_id)
                    return (0, time.time() - start_time)

                logger.info(
                    "Backfilling file_extension for %d URLs (evidence_id=%d)",
                    pending_count, evidence_id
                )

                # Process in batches to avoid long locks
                while True:
                    # Fetch batch of URLs without extensions
                    rows = conn.execute(
                        """
                        SELECT id, url FROM urls
                        WHERE evidence_id = ? AND file_extension IS NULL
                        LIMIT ?
                        """,
                        (evidence_id, batch_size)
                    ).fetchall()

                    if not rows:
                        break

                    # Build batch update
                    updates = []
                    for row in rows:
                        url_id, url = row[0], row[1]
                        ext = get_extension(url)  # Returns lowercase, e.g., '.jpg'
                        file_type = DOWNLOADABLE_EXTENSIONS.get(ext)  # None if not downloadable
                        updates.append((ext, file_type, url_id))

                    # Execute batch update
                    conn.executemany(
                        "UPDATE urls SET file_extension = ?, file_type = ? WHERE id = ?",
                        updates
                    )
                    conn.commit()
                    total_updated += len(rows)

                    logger.debug("Backfilled %d URLs (total: %d)", len(rows), total_updated)

        elapsed = time.time() - start_time
        logger.info(
            "Backfill complete: %d URLs updated in %.2fs (evidence_id=%d)",
            total_updated, elapsed, evidence_id
        )
        return (total_updated, elapsed)

    def get_extension_backfill_status(self, evidence_id: int) -> Dict[str, int]:
        """
        Check backfill status for an evidence.

        Returns:
            Dict with 'total', 'backfilled', 'pending' counts
        """
        if not self._evidence_db_exists(evidence_id):
            return {'total': 0, 'backfilled': 0, 'pending': 0}

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Check if column exists
                cursor = conn.execute("PRAGMA table_info(urls)")
                columns = {row[1] for row in cursor.fetchall()}
                if 'file_extension' not in columns:
                    # Column doesn't exist yet
                    total = conn.execute(
                        "SELECT COUNT(*) FROM urls WHERE evidence_id = ?",
                        (evidence_id,)
                    ).fetchone()[0]
                    return {'total': total, 'backfilled': 0, 'pending': total}

                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) as total,
                        COUNT(file_extension) as backfilled
                    FROM urls WHERE evidence_id = ?
                    """,
                    (evidence_id,)
                ).fetchone()

                total = row[0] if row else 0
                backfilled = row[1] if row else 0
                return {
                    'total': total,
                    'backfilled': backfilled,
                    'pending': total - backfilled
                }

    # -------------------------------------------------------------------------
    # Downloadable URL Queries (Evidence DB) - UI-specific
    # -------------------------------------------------------------------------

    def list_downloadable_urls(
        self,
        evidence_id: int,
        *,
        file_type: Optional[str] = None,
        domain_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        tag_filter: Optional[str] = None,
        match_filter: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List URLs that have downloadable file extensions.

        Queries the urls table filtered by downloadable extensions
        and checks downloads table for status.

        Args:
            evidence_id: Evidence ID
            file_type: Filter by type ('image', 'video', 'audio', 'document', 'archive')
            domain_filter: Filter by domain (partial match)
            search_text: Search in URL text
            tag_filter: Filter by tag name
            match_filter: Filter by match status ('matched', 'unmatched', or specific list name)
            limit: Max results per page
            offset: Pagination offset

        Returns:
            List of URL dicts with download status

        Optimized to use indexed file_type column when available.
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        # Build WHERE clause
        conditions = ["u.evidence_id = ?"]
        params: List[Any] = [evidence_id]

        # Check if file_type column is populated (backfill completed)
        use_optimized = self._can_use_optimized_query(evidence_id)

        if use_optimized:
            # Optimized path: use indexed file_type column
            if file_type and file_type != 'all':
                conditions.append("u.file_type = ?")
                params.append(file_type)
            else:
                # All downloadable types
                conditions.append("u.file_type IS NOT NULL")
        else:
            # Fallback path: use LIKE queries (slow but works without backfill)
            extensions: List[str] = []
            if file_type and file_type != 'all':
                for ext, ftype in DOWNLOADABLE_EXTENSIONS.items():
                    if ftype == file_type:
                        extensions.append(ext)
            else:
                extensions = list(DOWNLOADABLE_EXTENSIONS.keys())

            if not extensions:
                return []

            ext_conditions = " OR ".join([f"LOWER(u.url) LIKE ?" for _ in extensions])
            conditions.append(f"({ext_conditions})")
            params.extend([f"%{ext}" for ext in extensions])

        if domain_filter:
            conditions.append("u.domain LIKE ?")
            params.append(f"%{domain_filter}%")

        if search_text:
            conditions.append("u.url LIKE ?")
            params.append(f"%{search_text}%")

        # Tag filter using EXISTS subquery
        if tag_filter:
            conditions.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'url'
                    AND ta.artifact_id = u.id
                    AND t.name = ?
                )
            """)
            params.append(tag_filter)

        # Match filter using EXISTS subquery
        if match_filter:
            if match_filter == "matched":
                conditions.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            elif match_filter == "unmatched":
                conditions.append("NOT EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            else:
                # Specific list name
                conditions.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id AND m.list_name = ?)")
                params.append(match_filter)

        where_clause = " AND ".join(conditions)

        # Group by URL to deduplicate - same URL from multiple sources shown once
        # Use MIN(id) for representative ID, GROUP_CONCAT for sources
        sql = f"""
            SELECT
                MIN(u.id) as id,
                u.url,
                u.domain,
                GROUP_CONCAT(DISTINCT u.discovered_by) as discovered_by,
                COUNT(*) as source_count,
                MIN(u.first_seen_utc) as first_seen_utc,
                MAX(d.id) as download_id,
                MAX(d.status) as download_status
            FROM urls u
            LEFT JOIN downloads d ON u.url = d.url AND d.evidence_id = u.evidence_id
            WHERE {where_clause}
            GROUP BY u.url, u.domain
            ORDER BY u.domain, u.url
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]

    def _can_use_optimized_query(self, evidence_id: int) -> bool:
        """
        Check if we can use the optimized file_type query.

        Returns True if file_type column exists AND has been backfilled
        for at least some rows.
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Check if column exists
                cursor = conn.execute("PRAGMA table_info(urls)")
                columns = {row[1] for row in cursor.fetchall()}
                if 'file_type' not in columns:
                    return False

                # Check if any rows have file_type populated
                row = conn.execute(
                    "SELECT 1 FROM urls WHERE evidence_id = ? AND file_type IS NOT NULL LIMIT 1",
                    (evidence_id,)
                ).fetchone()
                return row is not None

    def count_downloadable_urls(
        self,
        evidence_id: int,
        *,
        file_type: Optional[str] = None,
        domain_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        tag_filter: Optional[str] = None,
        match_filter: Optional[str] = None,
    ) -> int:
        """Count downloadable URLs matching filters.

        Added tag_filter and match_filter parameters.
        Optimized to use indexed file_type column when available.
        """
        # Guard: return 0 if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return 0

        conditions = ["u.evidence_id = ?"]
        params: List[Any] = [evidence_id]

        # Check if file_type column is populated (backfill completed)
        use_optimized = self._can_use_optimized_query(evidence_id)

        if use_optimized:
            # Optimized path: use indexed file_type column
            if file_type and file_type != 'all':
                conditions.append("u.file_type = ?")
                params.append(file_type)
            else:
                conditions.append("u.file_type IS NOT NULL")
        else:
            # Fallback path: use LIKE queries
            extensions: List[str] = []
            if file_type and file_type != 'all':
                for ext, ftype in DOWNLOADABLE_EXTENSIONS.items():
                    if ftype == file_type:
                        extensions.append(ext)
            else:
                extensions = list(DOWNLOADABLE_EXTENSIONS.keys())

            if not extensions:
                return 0

            ext_conditions = " OR ".join([f"LOWER(u.url) LIKE ?" for _ in extensions])
            conditions.append(f"({ext_conditions})")
            params.extend([f"%{ext}" for ext in extensions])

        if domain_filter:
            conditions.append("u.domain LIKE ?")
            params.append(f"%{domain_filter}%")

        if search_text:
            conditions.append("u.url LIKE ?")
            params.append(f"%{search_text}%")

        # Tag filter using EXISTS subquery
        if tag_filter:
            conditions.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'url'
                    AND ta.artifact_id = u.id
                    AND t.name = ?
                )
            """)
            params.append(tag_filter)

        # Match filter using EXISTS subquery
        if match_filter:
            if match_filter == "matched":
                conditions.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            elif match_filter == "unmatched":
                conditions.append("NOT EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id)")
            else:
                # Specific list name
                conditions.append("EXISTS (SELECT 1 FROM url_matches m WHERE m.url_id = u.id AND m.evidence_id = u.evidence_id AND m.list_name = ?)")
                params.append(match_filter)

        where_clause = " AND ".join(conditions)

        # Count DISTINCT URLs (grouped)
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                row = conn.execute(
                    f"SELECT COUNT(DISTINCT u.url) FROM urls u WHERE {where_clause}",
                    params,
                ).fetchone()
                return row[0] if row else 0

    # -------------------------------------------------------------------------
    # Download CRUD (wrapping core helpers)
    # -------------------------------------------------------------------------

    def get_url_download_status(self, evidence_id: int, url_id: int) -> Optional[str]:
        """Check if a URL has already been downloaded."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_url_download_status(conn, evidence_id, url_id)

    def insert_download(
        self,
        evidence_id: int,
        url: str,
        domain: str,
        file_type: str,
        file_extension: str,
        *,
        url_id: Optional[int] = None,
        status: str = "pending",
        filename: Optional[str] = None,
    ) -> int:
        """
        Insert a new download record.

        Returns the download ID.
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                download_id = downloads_helpers.insert_download(
                    conn,
                    evidence_id,
                    url,
                    domain,
                    file_type,
                    file_extension,
                    url_id=url_id,
                    status=status,
                    filename=filename,
                )
                conn.commit()
                return download_id

    def update_download_status(
        self,
        evidence_id: int,
        download_id: int,
        status: str,
        *,
        dest_path: Optional[str] = None,
        filename: Optional[str] = None,
        size_bytes: Optional[int] = None,
        md5: Optional[str] = None,
        sha256: Optional[str] = None,
        content_type: Optional[str] = None,
        response_code: Optional[int] = None,
        error_message: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        attempts: Optional[int] = None,
    ) -> None:
        """Update download status and metadata."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                downloads_helpers.update_download_status(
                    conn,
                    evidence_id,
                    download_id,
                    status,
                    dest_path=dest_path,
                    filename=filename,
                    size_bytes=size_bytes,
                    md5=md5,
                    sha256=sha256,
                    content_type=content_type,
                    response_code=response_code,
                    error_message=error_message,
                    duration_seconds=duration_seconds,
                    attempts=attempts,
                )
                conn.commit()

    def update_download_image_metadata(
        self,
        evidence_id: int,
        download_id: int,
        phash: Optional[str] = None,
        exif_json: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
    ) -> None:
        """Update image-specific metadata for a download."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                downloads_helpers.update_download_image_metadata(
                    conn,
                    evidence_id,
                    download_id,
                    phash=phash,
                    exif_json=exif_json,
                    width=width,
                    height=height,
                )
                conn.commit()

    def list_downloads(
        self,
        evidence_id: int,
        *,
        file_type: Optional[str] = None,
        status_filter: Optional[str] = None,
        domain_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List downloads with optional filters.

        Args:
            evidence_id: Evidence ID
            file_type: Filter by file type ('image', 'video', etc.)
            status_filter: Filter by status ('completed', 'failed', etc.)
            domain_filter: Filter by domain (partial match)
            search_text: Search in filename or URL
            limit: Max results
            offset: Pagination offset

        Returns:
            List of download dicts
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_downloads(
                    conn,
                    evidence_id,
                    file_type=file_type,
                    status=status_filter,
                    domain=domain_filter,
                    search_text=search_text,
                    limit=limit,
                    offset=offset,
                )

    def count_downloads(
        self,
        evidence_id: int,
        *,
        file_type: Optional[str] = None,
        status_filter: Optional[str] = None,
    ) -> int:
        """Count downloads with optional filters."""
        # Guard: return 0 if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return 0

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_download_count(
                    conn,
                    evidence_id,
                    file_type=file_type,
                    status=status_filter,
                )

    def get_download(self, evidence_id: int, download_id: int) -> Optional[Dict[str, Any]]:
        """Get a single download by ID."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_download(conn, evidence_id, download_id)

    def get_download_by_path(self, evidence_id: int, dest_path: str) -> Optional[Dict[str, Any]]:
        """Get a download by destination path (for backfill dedup)."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_download_by_path(conn, evidence_id, dest_path)

    def find_url_by_filename_domain(
        self,
        evidence_id: int,
        filename: str,
        domain: str,
    ) -> Optional[int]:
        """
        Try to find a URL ID by filename and domain (best-effort linkage for backfill).

        Returns url_id if found, None otherwise.
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.find_url_by_filename_domain(
                    conn, evidence_id, filename, domain
                )

    def copy_tags_from_url(
        self,
        evidence_id: int,
        url_id: int,
        download_id: int,
    ) -> None:
        """Copy tags from a URL to a download (tag inheritance)."""
        # Get tags from source URL (uses TagQueryMixin)
        url_tags = self.get_artifact_tags(evidence_id, 'url', url_id)

        # Apply each tag to the download (uses TagQueryMixin)
        for tag in url_tags:
            self.tag_artifact(evidence_id, tag['name'], 'download', download_id, tagged_by='inherited')

    def get_download_stats(self, evidence_id: int) -> Dict[str, int]:
        """Get download statistics for the evidence."""
        # Guard: return empty dict if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return {}

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_download_stats(conn, evidence_id)

    def list_download_domains(self, evidence_id: int) -> List[str]:
        """List unique domains from downloads."""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return downloads_helpers.get_download_domains(conn, evidence_id)
