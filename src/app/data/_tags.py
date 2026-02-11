"""Tag query operations for UI layer.

This module provides tag-specific queries for the UI:
- Tag CRUD with caching
- Artifact tagging/untagging
- Tag-based artifact iterators for all artifact types

Extracted from case_data.py for modular repository pattern.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence

from core.database.helpers import tags as tag_helpers

from ._base import BaseDataAccess


class TagQueryMixin(BaseDataAccess):
    """Mixin providing tag query operations for UI views.

    Features:
    - Tag CRUD with in-memory caching
    - Artifact tagging/untagging with cache invalidation
    - Generic and artifact-specific tag iterators

    All methods operate on the evidence database.

    Extracted from CaseDataAccess for modular architecture.
    """

    # -------------------------------------------------------------------------
    # Tag CRUD (with caching)
    # -------------------------------------------------------------------------

    def list_tags(self, evidence_id: int, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        List all tags for an evidence with usage counts.

        Added in-memory caching for performance.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results

        Returns:
            List of tag dicts with keys: id, name, name_normalized,
            usage_count, created_by, created_at_utc
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        cache_key = f"tags_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                result = tag_helpers.get_all_tags(conn, evidence_id)

        self._set_cached(cache_key, result)
        return result

    def get_tag(self, evidence_id: int, name: str) -> Optional[Dict[str, Any]]:
        """
        Get a tag by name (case-insensitive).

        Args:
            evidence_id: Evidence ID
            name: Tag name

        Returns:
            Tag dict or None if not found
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.get_tag_by_name(conn, evidence_id, name)

    def create_tag(self, evidence_id: int, name: str, created_by: str = "manual") -> int:
        """
        Create a new tag. Returns tag ID.

        Args:
            evidence_id: Evidence ID
            name: Tag name
            created_by: Creator identifier

        Returns:
            Tag ID (existing or newly created)
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_id = tag_helpers.get_or_create_tag(conn, evidence_id, name, created_by)
                conn.commit()

        self._invalidate_tag_cache(evidence_id)
        return tag_id

    def rename_tag(self, evidence_id: int, tag_id: int, new_name: str) -> None:
        """
        Rename a tag.

        Args:
            evidence_id: Evidence ID
            tag_id: Tag ID
            new_name: New tag name
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_helpers.update_tag_name(conn, tag_id, evidence_id, new_name)
                conn.commit()

        self._invalidate_tag_cache(evidence_id)

    def delete_tag(self, evidence_id: int, tag_id: int) -> None:
        """
        Delete a tag and all its associations.

        Args:
            evidence_id: Evidence ID
            tag_id: Tag ID
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_helpers.delete_tag(conn, tag_id, evidence_id)
                conn.commit()

        self._invalidate_tag_cache(evidence_id)

    def _invalidate_tag_cache(self, evidence_id: int) -> None:
        """
        Invalidate tag-related caches for an evidence.

        Added thread lock for thread-safety (Bug #4 fix).
        """
        cache_key = f"tags_{evidence_id}"
        with self._filter_cache_lock:
            if cache_key in self._filter_cache:
                del self._filter_cache[cache_key]

    def merge_tags(
        self,
        evidence_id: int,
        source_tag_ids: List[int],
        target_tag_id: int,
    ) -> None:
        """
        Merge source tags into target tag.

        Args:
            evidence_id: Evidence ID
            source_tag_ids: Tag IDs to merge from
            target_tag_id: Tag ID to merge into
        """
        if not source_tag_ids:
            return

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_helpers.merge_tag_associations(
                    conn, evidence_id, source_tag_ids, target_tag_id
                )
                conn.commit()

        # Invalidate tag cache after merge (Bug #2 fix)
        self._invalidate_tag_cache(evidence_id)

    # -------------------------------------------------------------------------
    # Artifact Tagging
    # -------------------------------------------------------------------------

    def tag_artifact(
        self,
        evidence_id: int,
        tag_name: str,
        artifact_type: str,
        artifact_id: int,
        tagged_by: str = "manual",
    ) -> None:
        """
        Apply a tag to an artifact.

        Args:
            evidence_id: Evidence ID
            tag_name: Tag name (creates if not exists)
            artifact_type: Artifact type ('url', 'image', 'file_list', etc.)
            artifact_id: Artifact ID
            tagged_by: Tagger identifier
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_id = tag_helpers.get_or_create_tag(
                    conn, evidence_id, tag_name, created_by=tagged_by
                )
                tag_helpers.insert_tag_association(
                    conn, tag_id, evidence_id, artifact_type, artifact_id, tagged_by
                )
                conn.commit()

        self._invalidate_tag_cache(evidence_id)

    def untag_artifact(
        self,
        evidence_id: int,
        tag_name: str,
        artifact_type: str,
        artifact_id: int,
    ) -> None:
        """
        Remove a tag from an artifact.

        Args:
            evidence_id: Evidence ID
            tag_name: Tag name
            artifact_type: Artifact type
            artifact_id: Artifact ID
        """
        tag = self.get_tag(evidence_id, tag_name)
        if not tag:
            return

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_helpers.delete_tag_association(
                    conn, tag["id"], artifact_type, artifact_id
                )
                conn.commit()

        self._invalidate_tag_cache(evidence_id)

    # -------------------------------------------------------------------------
    # Tag Queries
    # -------------------------------------------------------------------------

    def get_artifact_tags(
        self,
        evidence_id: int,
        artifact_type: str,
        artifact_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Get all tags for a specific artifact.

        Args:
            evidence_id: Evidence ID
            artifact_type: Artifact type
            artifact_id: Artifact ID

        Returns:
            List of tag dicts
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.get_artifact_tags(
                    conn, evidence_id, artifact_type, artifact_id
                )

    def get_artifact_tags_str(
        self,
        evidence_id: int,
        artifact_type: str,
        artifact_id: int,
    ) -> str:
        """
        Get tags for a specific artifact as a comma-separated string.

        Added for Phase 2.2 performance optimization - tags loaded on-demand
        instead of JOINed in iter_urls/iter_images queries.

        Args:
            evidence_id: Evidence ID
            artifact_type: Type of artifact ('url', 'image', 'file_list',
                           'download', 'cookie', 'bookmark', 'browser_download',
                           'autofill', 'credential', 'session_tab', 'site_permission',
                           'media_playback', 'timeline')
            artifact_id: Artifact ID

        Returns:
            Comma-separated list of tag names, or empty string if no tags
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.get_artifact_tags_str(
                    conn, evidence_id, artifact_type, artifact_id
                )

    def get_tag_strings_for_artifacts(
        self,
        evidence_id: int,
        artifact_type: str,
        artifact_ids: Sequence[int],
    ) -> Dict[int, str]:
        """
        Get tags for multiple artifacts in a single query.

        Args:
            evidence_id: Evidence ID
            artifact_type: Artifact type
            artifact_ids: Sequence of artifact IDs

        Returns:
            Mapping of artifact_id -> comma-separated tag names
        """
        if not artifact_ids:
            return {}

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.get_tag_strings_for_artifacts(
                    conn, evidence_id, artifact_type, artifact_ids
                )

    def get_artifacts_by_tag(
        self,
        evidence_id: int,
        tag_name: str,
    ) -> Dict[str, List[int]]:
        """
        Get all artifact IDs associated with a tag, grouped by type.

        Args:
            evidence_id: Evidence ID
            tag_name: Tag name

        Returns:
            Dict mapping artifact_type -> list of artifact IDs
        """
        tag = self.get_tag(evidence_id, tag_name)
        if not tag:
            return {}

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.get_artifacts_by_tag_id(conn, tag["id"])

    # -------------------------------------------------------------------------
    # Generic Tag-Based Artifact Iterators
    # -------------------------------------------------------------------------

    def _iter_artifacts_by_tags(
        self,
        evidence_id: int,
        artifact_type: str,
        table_name: str,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Iterable[sqlite3.Row]:
        """
        Generic helper to query artifacts by tags with AND/OR logic.

        Args:
            evidence_id: Evidence ID
            artifact_type: Artifact type string stored in tag_associations
            table_name: Table to query
            tag_ids: Tag IDs to filter by
            tag_mode: "all" (AND) or "any" (OR)
            limit: Optional max number of results
            order_by: Optional ORDER BY clause (without the keyword)

        Returns:
            Iterable of matching rows
        """
        if not tag_ids:
            return []

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.query_artifacts_by_tags(
                    conn,
                    evidence_id,
                    artifact_type,
                    table_name,
                    tag_ids,
                    tag_mode=tag_mode,
                    limit=limit,
                    order_by=order_by,
                )

    def _iter_all_tagged_artifacts(
        self,
        evidence_id: int,
        artifact_type: str,
        table_name: str,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Iterable[sqlite3.Row]:
        """
        Generic helper to query all tagged artifacts of a given type.

        Args:
            evidence_id: Evidence ID
            artifact_type: Artifact type
            table_name: Table to query
            limit: Optional max results
            order_by: Optional ORDER BY clause (without keyword)

        Returns:
            Iterable of matching rows
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return tag_helpers.query_all_tagged_artifacts(
                    conn,
                    evidence_id,
                    artifact_type,
                    table_name,
                    limit=limit,
                    order_by=order_by,
                )

    # -------------------------------------------------------------------------
    # URL Tag Iterators
    # -------------------------------------------------------------------------

    def iter_urls_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query URLs by multiple tags with AND/OR logic via tag_associations table."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "url",
            "urls",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="COALESCE(a.first_seen_utc, a.last_seen_utc) DESC",
        )

    def iter_all_tagged_urls(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all URLs that have at least one tag."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "url",
            "urls",
            limit=limit,
            order_by="COALESCE(a.first_seen_utc, a.last_seen_utc) DESC",
        )

    # -------------------------------------------------------------------------
    # Image Tag Iterators (Custom SQL for v_image_sources JOIN)
    # -------------------------------------------------------------------------

    def iter_images_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query images by multiple tags with AND/OR logic via tag_associations table."""
        if not tag_ids:
            return []

        placeholders = ",".join("?" for _ in tag_ids)
        params: List[Any] = ["image", evidence_id, *tag_ids]
        having_clause = (
            f"HAVING COUNT(DISTINCT ta.tag_id) = {len(tag_ids)}" if tag_mode == "all" else ""
        )

        sql = f"""
            SELECT a.*,
                   a.first_discovered_by AS discovered_by,
                   COALESCE(vis.has_browser_source, 0) AS has_browser_source,
                   vis.browser_sources
            FROM images a
            LEFT JOIN v_image_sources vis ON vis.evidence_id = a.evidence_id AND vis.image_id = a.id
            JOIN tag_associations ta
              ON ta.artifact_type = ?
             AND ta.artifact_id = a.id
            WHERE a.evidence_id = ?
              AND ta.tag_id IN ({placeholders})
            GROUP BY a.id
            {having_clause}
            ORDER BY COALESCE(a.ts_utc, '') DESC, a.filename ASC
        """

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return conn.execute(sql, params).fetchall()

    def iter_all_tagged_images(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """
        Query all images that have at least one tag.

        Joins v_image_sources to include has_browser_source for report highlighting.
        """
        params: List[Any] = ["image", evidence_id]
        sql = """
            SELECT DISTINCT a.*, COALESCE(vis.has_browser_source, 0) AS has_browser_source,
                   vis.browser_sources
            FROM images a
            LEFT JOIN v_image_sources vis ON vis.evidence_id = a.evidence_id AND vis.image_id = a.id
            JOIN tag_associations ta
              ON ta.artifact_type = ?
             AND ta.artifact_id = a.id
            WHERE a.evidence_id = ?
            ORDER BY a.id DESC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return conn.execute(sql, params).fetchall()

    # -------------------------------------------------------------------------
    # File List Tag Iterators
    # -------------------------------------------------------------------------

    def iter_file_list_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query file list entries by multiple tags with AND/OR logic."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "file_list",
            "file_list",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="a.file_path ASC",
        )

    def iter_all_tagged_files(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all file_list entries that have at least one tag."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "file_list",
            "file_list",
            limit=limit,
            order_by="a.file_path ASC",
        )

    # -------------------------------------------------------------------------
    # Cookie Tag Iterators
    # -------------------------------------------------------------------------

    def iter_cookies_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query cookies by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "cookie",
            "cookies",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="a.domain ASC, a.name ASC",
        )

    def iter_all_tagged_cookies(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged cookies."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "cookie",
            "cookies",
            limit=limit,
            order_by="a.domain ASC, a.name ASC",
        )

    # -------------------------------------------------------------------------
    # Bookmark Tag Iterators
    # -------------------------------------------------------------------------

    def iter_bookmarks_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query bookmarks by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "bookmark",
            "bookmarks",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="a.folder_path ASC, a.title ASC",
        )

    def iter_all_tagged_bookmarks(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged bookmarks."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "bookmark",
            "bookmarks",
            limit=limit,
            order_by="a.folder_path ASC, a.title ASC",
        )

    # -------------------------------------------------------------------------
    # Browser Downloads Tag Iterators
    # -------------------------------------------------------------------------

    def iter_browser_downloads_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query browser downloads by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "browser_download",
            "browser_downloads",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="COALESCE(a.start_time_utc, '') DESC",
        )

    def iter_all_tagged_browser_downloads(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged browser downloads."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "browser_download",
            "browser_downloads",
            limit=limit,
            order_by="COALESCE(a.start_time_utc, '') DESC",
        )

    # -------------------------------------------------------------------------
    # Autofill Tag Iterators
    # -------------------------------------------------------------------------

    def iter_autofill_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query autofill entries by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "autofill",
            "autofill",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="COALESCE(a.date_last_used_utc, a.date_created_utc, '') DESC",
        )

    def iter_all_tagged_autofill(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged autofill entries."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "autofill",
            "autofill",
            limit=limit,
            order_by="COALESCE(a.date_last_used_utc, a.date_created_utc, '') DESC",
        )

    # -------------------------------------------------------------------------
    # Credentials Tag Iterators
    # -------------------------------------------------------------------------

    def iter_credentials_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query credentials by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "credential",
            "credentials",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="COALESCE(a.date_last_used_utc, '') DESC",
        )

    def iter_all_tagged_credentials(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged credentials."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "credential",
            "credentials",
            limit=limit,
            order_by="COALESCE(a.date_last_used_utc, '') DESC",
        )

    # -------------------------------------------------------------------------
    # Session Tabs Tag Iterators
    # -------------------------------------------------------------------------

    def iter_session_tabs_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query session tabs by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "session_tab",
            "session_tabs",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="a.window_id ASC, a.tab_index ASC",
        )

    def iter_all_tagged_session_tabs(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged session tabs."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "session_tab",
            "session_tabs",
            limit=limit,
            order_by="a.window_id ASC, a.tab_index ASC",
        )

    # -------------------------------------------------------------------------
    # Site Permissions Tag Iterators
    # -------------------------------------------------------------------------

    def iter_site_permissions_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query site permissions by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "site_permission",
            "site_permissions",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="a.origin ASC, a.permission_type ASC",
        )

    def iter_all_tagged_site_permissions(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged site permissions."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "site_permission",
            "site_permissions",
            limit=limit,
            order_by="a.origin ASC, a.permission_type ASC",
        )

    # -------------------------------------------------------------------------
    # Media Playback Tag Iterators
    # -------------------------------------------------------------------------

    def iter_media_playback_by_tags(
        self,
        evidence_id: int,
        tag_ids: List[int],
        tag_mode: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query media playback history by tags."""
        return self._iter_artifacts_by_tags(
            evidence_id,
            "media_playback",
            "media_playback",
            tag_ids,
            tag_mode=tag_mode,
            limit=limit,
            order_by="COALESCE(a.last_played_utc, '') DESC",
        )

    def iter_all_tagged_media_playback(
        self,
        evidence_id: int,
        limit: Optional[int] = None,
    ) -> Iterable[sqlite3.Row]:
        """Query all tagged media playback entries."""
        return self._iter_all_tagged_artifacts(
            evidence_id,
            "media_playback",
            "media_playback",
            limit=limit,
            order_by="COALESCE(a.last_played_utc, '') DESC",
        )
