"""Image query operations for UI layer.

This module provides image-specific queries for the UI:
- Paginated image listing with filtering (tag, source, extension, hash, size)
- Image source and extension dropdown data with caching
- Single image lookup and batch lookup by IDs
- Path resolution for image files across extractor outputs
- Similar image search via perceptual hash (pHash)
- Hash match listing for evidence/images

Extracted from case_data.py for modular repository pattern.
Added find_similar_images and list_hash_matches from case_data.py.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.database import slugify_label
from core.phash import hamming_distance

from ._base import BaseDataAccess


class ImageQueryMixin(BaseDataAccess):
    """Mixin providing image query operations for UI views.

    Features:
    - Paginated image listing with 7+ filters
    - Image source/extension/hash dropdown data with caching
    - Single and batch image lookup
    - Path resolution with extractor mapping
    - Similar image search via perceptual hash (two-phase: SQL prefix + Python Hamming)
    - Hash match listing for evidence/images

    All methods operate on the evidence database.

    Database tables: images, image_discoveries, hash_matches, v_image_sources (view)

    Extracted from CaseDataAccess for modular architecture.
    Added find_similar_images and list_hash_matches from case_data.py.
    """

    # -------------------------------------------------------------------------
    # Image Queries (Evidence DB)
    # -------------------------------------------------------------------------

    def get_images_by_tag(self, evidence_id: int, tag: str) -> List[Dict[str, Any]]:
        """
        Get all images associated with a specific tag.

        Args:
            evidence_id: Evidence ID
            tag: Tag name to filter by

        Returns:
            List of image dictionaries
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        # Updated to use unified tagging system
        # Alias first_discovered_by AS discovered_by for UI/report compat
        sql = """
            SELECT i.id, i.rel_path, i.filename, i.md5, i.sha256, i.phash,
                   i.first_discovered_by AS discovered_by,
                   i.ts_utc, i.notes
            FROM images i
            JOIN tag_associations ta ON i.id = ta.artifact_id
            JOIN tags t ON ta.tag_id = t.id
            WHERE i.evidence_id = ?
              AND ta.artifact_type = 'image'
              AND t.name_normalized = ?
            ORDER BY COALESCE(i.ts_utc, '') DESC
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id, tag.lower()))
                return [dict(row) for row in cursor.fetchall()]

    def list_images_by_ids(self, evidence_id: int, image_ids: Sequence[int]) -> List[Dict[str, Any]]:
        """
        Get images by a list of IDs, preserving order.

        Alias first_discovered_by AS discovered_by, join v_image_sources for browser badge.

        Args:
            evidence_id: Evidence ID
            image_ids: Sequence of image IDs to retrieve

        Returns:
            List of image dictionaries in the same order as image_ids
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        ids = [int(image_id) for image_id in image_ids if image_id is not None]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        sql = f"""
            SELECT i.id, i.evidence_id, i.rel_path, i.filename, i.md5, i.sha256, i.phash,
                   i.first_discovered_by AS discovered_by, i.ts_utc, i.notes,
                   i.exif_json,
                   COALESCE(vis.has_browser_source, 0) AS has_browser_source,
                   vis.browser_sources,
                   GROUP_CONCAT(t.name, ', ') as tags
            FROM images i
            LEFT JOIN v_image_sources vis ON vis.evidence_id = i.evidence_id AND vis.image_id = i.id
            LEFT JOIN tag_associations ta ON ta.artifact_type = 'image' AND ta.artifact_id = i.id
            LEFT JOIN tags t ON ta.tag_id = t.id
            WHERE i.id IN ({placeholders})
            GROUP BY i.id
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, ids)
                rows = [dict(row) for row in cursor.fetchall()]
        rows_by_id = {row["id"]: row for row in rows}
        ordered: List[Dict[str, Any]] = []
        for image_id in ids:
            row = rows_by_id.get(image_id)
            if row:
                ordered.append(row)
        return ordered

    def iter_images(
        self,
        evidence_id: int,
        *,
        tag_like: str = "%",
        discovered_by: Optional[Iterable[str]] = None,
        extension: Optional[str] = None,
        hash_match: Optional[str] = None,
        min_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Iterate images with optional filtering.

        Updated to use unified tagging system.
        Added size filtering (min_size_bytes, max_size_bytes).
        Added extension filtering and hash_match filtering.

        Args:
            evidence_id: Evidence ID
            tag_like: Tag name filter (SQL LIKE pattern)
            discovered_by: List of source filters
            extension: File extension filter (e.g., 'jpg', 'gif', 'bmp')
            hash_match: Hash list name filter (only show images matching this list)
            min_size_bytes: Minimum file size filter (Phase 3)
            max_size_bytes: Maximum file size filter (Phase 3)
            limit: Page size
            offset: Page offset
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        params: List[Any] = [evidence_id]
        where = ["i.evidence_id = ?"]

        # Filter by tags using new tag_associations table
        if tag_like and tag_like != "%":
            where.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'image'
                    AND ta.artifact_id = i.id
                    AND t.name LIKE ?
                )
            """)
            params.append(tag_like)

        # Filter by discovered_by via image_discoveries table
        if discovered_by:
            placeholders = ",".join("?" for _ in discovered_by)
            where.append(f"""
                EXISTS (
                    SELECT 1 FROM image_discoveries d
                    WHERE d.evidence_id = i.evidence_id
                    AND d.image_id = i.id
                    AND d.discovered_by IN ({placeholders})
                )
            """)
            params.extend(discovered_by)

        # Extension filter
        if extension:
            where.append("LOWER(i.filename) LIKE ?")
            params.append(f"%.{extension.lower()}")

        # Hash match filter
        # Handle __any__ to match images with any hash list match
        if hash_match == "__any__":
            where.append("""
                EXISTS (
                    SELECT 1 FROM hash_matches hm
                    WHERE hm.image_id = i.id
                    AND hm.evidence_id = i.evidence_id
                )
            """)
        elif hash_match:
            where.append("""
                EXISTS (
                    SELECT 1 FROM hash_matches hm
                    WHERE hm.image_id = i.id
                    AND hm.evidence_id = i.evidence_id
                    AND hm.list_name = ?
                )
            """)
            params.append(hash_match)

        # Phase 3: Size filtering
        # Only filter if size_bytes is not NULL (backward compatible with older data)
        if min_size_bytes is not None:
            where.append("(i.size_bytes IS NULL OR i.size_bytes >= ?)")
            params.append(min_size_bytes)
        if max_size_bytes is not None:
            where.append("(i.size_bytes IS NULL OR i.size_bytes <= ?)")
            params.append(max_size_bytes)

        # Removed GROUP_CONCAT - tags loaded on-demand via get_artifact_tags_str()
        # Alias first_discovered_by AS discovered_by, join v_image_sources for browser badge
        # Added sources, source_count, fs_path for multi-source provenance display
        sql = f"""
            SELECT i.id, i.rel_path, i.filename, i.md5, i.sha256, i.phash,
                   i.first_discovered_by AS discovered_by, i.ts_utc, i.notes, i.exif_json, i.size_bytes,
                   COALESCE(vis.has_browser_source, 0) AS has_browser_source,
                   vis.browser_sources,
                   vis.sources,
                   vis.source_count,
                   vis.fs_path
            FROM images i
            LEFT JOIN v_image_sources vis ON vis.evidence_id = i.evidence_id AND vis.image_id = i.id
            WHERE {' AND '.join(where)}
            ORDER BY COALESCE(i.ts_utc, '') DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]

    def update_image_size(self, evidence_id: int, image_id: int, size_bytes: int) -> None:
        """
        Update size_bytes for an image (used for backfill).

        Args:
            evidence_id: Evidence ID
            image_id: Image ID
            size_bytes: File size in bytes
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                conn.execute(
                    "UPDATE images SET size_bytes = ? WHERE id = ?",
                    (size_bytes, image_id)
                )
                conn.commit()

    def list_image_sources(self, evidence_id: int, use_cache: bool = True) -> List[str]:
        """
        List unique discovered_by values for images.

        Added in-memory caching for performance.
        Query image_discoveries table instead of images.
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        cache_key = f"image_sources_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        sql = """
            SELECT DISTINCT discovered_by
            FROM image_discoveries
            WHERE evidence_id = ?
            ORDER BY discovered_by
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                result = [row[0] for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def list_image_sources_counts(self, evidence_id: int, use_cache: bool = True) -> List[Tuple[str, int]]:
        """
        List image sources with counts for provenance filtering.

        Added in-memory caching for performance.
        Query image_discoveries table with COUNT(DISTINCT image_id).
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        cache_key = f"image_sources_counts_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        sql = """
            SELECT discovered_by, COUNT(DISTINCT image_id) as count
            FROM image_discoveries
            WHERE evidence_id = ?
            GROUP BY discovered_by
            ORDER BY discovered_by
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                result = [(row[0], row[1]) for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def list_image_extensions_counts(self, evidence_id: int, use_cache: bool = True) -> List[Tuple[str, int]]:
        """
        List image file extensions with counts for filtering.

        Extracts extension from filename (e.g., 'image.jpg' -> 'jpg').

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of (extension, count) tuples sorted by count descending

        Added in-memory caching for performance.
        Fixed to extract extension after LAST dot, not first (Bug #3 fix).
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        cache_key = f"image_extensions_counts_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        # Fixed to extract extension after LAST dot, not first (Bug #3 fix)
        # For 'foo.bar.jpg': REPLACE(filename, '.', '') = 'foobarjpg' (3 chars shorter)
        # LENGTH(filename) - LENGTH(REPLACE(...)) = 3 (number of dots)
        # RTRIM(filename, REPLACE(filename, '.', '')) trims from right until last dot
        # REPLACE(filename, RTRIM_result, '') = '.jpg' (the extension with dot)
        # SUBSTR(..., 2) = 'jpg' (extension without dot)
        sql = """
            SELECT
                LOWER(
                    CASE
                        WHEN INSTR(filename, '.') > 0
                        THEN REPLACE(
                            filename,
                            RTRIM(filename, REPLACE(filename, '.', '')),
                            ''
                        )
                        ELSE ''
                    END
                ) as ext,
                COUNT(*) as count
            FROM images
            WHERE evidence_id = ?
            GROUP BY ext
            HAVING ext != '' AND ext != '.'
            ORDER BY count DESC, ext ASC
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                result = [(row[0], row[1]) for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def list_hash_match_lists(self, evidence_id: int, use_cache: bool = True) -> List[Tuple[str, int]]:
        """
        List hash match lists with counts for filtering.

        Added for hash match filtering in Images tab.
        Added in-memory caching for performance.

        Args:
            evidence_id: Evidence ID
            use_cache: Whether to use cached results (default True)

        Returns:
            List of (list_name, count) tuples sorted by count descending
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        cache_key = f"hash_match_lists_{evidence_id}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        sql = """
            SELECT hm.list_name, COUNT(DISTINCT hm.image_id) as count
            FROM hash_matches hm
            WHERE hm.evidence_id = ?
            GROUP BY hm.list_name
            ORDER BY count DESC, hm.list_name ASC
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                result = [(row[0], row[1]) for row in cursor.fetchall()]

        self._set_cached(cache_key, result)
        return result

    def get_image(self, evidence_id: int, image_id: int) -> Optional[Dict[str, Any]]:
        """
        Get image by ID.

        Updated to use unified tagging system.
        Use v_image_sources view for first_discovered_by.
        """
        # Guard: return None if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return None

        sql = """
            SELECT i.id, i.evidence_id, i.rel_path, i.filename, i.md5, i.sha256,
                   i.phash, v.first_discovered_by as discovered_by, i.ts_utc, i.notes,
                   GROUP_CONCAT(t.name, ', ') as tags
            FROM images i
            LEFT JOIN v_image_sources v ON v.image_id = i.id
            LEFT JOIN tag_associations ta ON ta.artifact_type = 'image' AND ta.artifact_id = i.id
            LEFT JOIN tags t ON ta.tag_id = t.id
            WHERE i.id = ?
            GROUP BY i.id
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                row = conn.execute(sql, (image_id,)).fetchone()
                return dict(row) if row else None

    def resolve_image_path(
        self,
        rel_path: str,
        evidence_id: Optional[int] = None,
        discovered_by: Optional[str] = None,
    ) -> Path:
        """
        Resolve an image's relative path to its full filesystem path.

        Args:
            rel_path: Relative path stored in database (e.g., 'jpeg/000/12345.jpg')
            evidence_id: Evidence ID (optional, for evidence-local resolution)
            discovered_by: Source extractor (optional, determines base directory)

        Returns:
            Full path to the image file
        """
        # If no evidence context, fall back to simple resolution (legacy behavior)
        if evidence_id is None or discovered_by is None:
            return (self.case_folder / rel_path).resolve()

        # Get evidence label and convert to folder slug
        label = self._get_evidence_label(evidence_id)
        if not label:
            return (self.case_folder / rel_path).resolve()

        # Convert label to filesystem-safe slug (matching folder name)
        evidence_slug = slugify_label(label, evidence_id)
        evidence_dir = self.case_folder / "evidences" / evidence_slug

        # Map discovered_by to the extractor's output subdirectory
        # Note: rel_path already includes 'carved/' prefix from ingestion
        # (e.g., 'carved/gif/00001234.gif'), so base_subdir should point
        # to the extractor directory, not including 'carved/'
        # Added cache extractors (cache_simple, cache_firefox, browser_storage, safari)
        source_map = {
            # Carving extractors
            "bulk_extractor": "bulk_extractor",
            "bulk_extractor:images": "bulk_extractor",
            "bulk_extractor_images": "bulk_extractor",
            "foremost_carver": "foremost_carver",
            "scalpel": "scalpel",
            "image_carving": "",  # Legacy: rel_path is full path
            # Filesystem extractor
            "filesystem_images": "filesystem_images/extracted",
            # Cache/browser extractors
            # rel_path format: <run_id>/carved_images/<filename> or similar
            "cache_simple": "cache_simple",
            "cache_blockfile": "cache_simple",  # Blockfile uses same output dir
            "cache_firefox": "cache_firefox",
            "browser_storage_indexeddb": "browser_storage",
            "safari": "safari",
            # Favicon extractors
            "firefox_favicons": "firefox_favicons",
            "chromium_favicons": "chromium_favicons",
        }

        base_subdir = source_map.get(discovered_by, "")
        if base_subdir:
            return (evidence_dir / base_subdir / rel_path).resolve()

        # Fallback: try direct path under evidence directory
        return (evidence_dir / rel_path).resolve()

    # -------------------------------------------------------------------------
    # Image Similarity & Hash Matching
    # -------------------------------------------------------------------------

    def find_similar_images(
        self,
        evidence_id: int,
        target_phash: Optional[str],
        threshold: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find images with perceptual hash similar to target.

        Uses two-phase search for performance:
        1. SQL prefix filter: Reduces candidates using phash_prefix index
        2. Python Hamming: Exact distance on reduced set

        Args:
            evidence_id: Evidence ID
            target_phash: Target perceptual hash (hex string)
            threshold: Hamming distance threshold (0-64, typically ≤10)

        Returns:
            List of image records sorted by distance (closest first)
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        # Compute target prefix for SQL filtering
        try:
            target_prefix = int(target_phash[:4], 16)
        except ValueError:
            # Invalid phash format, fall back to full scan
            target_prefix = None

        results = []
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                if target_prefix is not None:
                    # Phase 1: SQL prefix filter
                    # Allow prefix difference based on threshold
                    # Each hex char = 4 bits, so threshold/4 gives prefix tolerance
                    prefix_range = max(1, threshold // 4) * 256  # 256 = 0x100
                    prefix_min = max(0, target_prefix - prefix_range)
                    prefix_max = min(65535, target_prefix + prefix_range)

                    cursor = conn.execute(
                        """SELECT * FROM images
                           WHERE evidence_id = ?
                           AND phash IS NOT NULL
                           AND phash_prefix IS NOT NULL
                           AND phash_prefix BETWEEN ? AND ?""",
                        (evidence_id, prefix_min, prefix_max)
                    )
                else:
                    # Fallback: Full scan (for images without prefix or invalid target)
                    cursor = conn.execute(
                        "SELECT * FROM images WHERE evidence_id = ? AND phash IS NOT NULL",
                        (evidence_id,)
                    )

                # Phase 2: Exact Hamming distance on reduced set
                for row in cursor:
                    phash = row["phash"]
                    if not phash:
                        continue

                    dist = hamming_distance(target_phash, phash)
                    if dist <= threshold:
                        record = dict(row)
                        record["hamming_distance"] = dist
                        results.append(record)

        # Sort by distance
        results.sort(key=lambda x: x["hamming_distance"])
        return results

    def list_hash_matches(
        self,
        evidence_id: int,
        image_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """
        List all hash matches for an evidence.

        Args:
            evidence_id: Evidence ID
            image_ids: Optional list of image IDs to filter by

        Returns:
            List of hash match dictionaries ordered by matched_at_utc DESC
        """
        # Guard: return empty list if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return []

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                query = "SELECT * FROM hash_matches WHERE evidence_id = ?"
                params: List[Any] = [evidence_id]

                if image_ids:
                    placeholders = ",".join("?" * len(image_ids))
                    query += f" AND image_id IN ({placeholders})"
                    params.extend(image_ids)

                query += " ORDER BY matched_at_utc DESC"

                rows = conn.execute(query, params).fetchall()
                return [dict(row) for row in rows]
