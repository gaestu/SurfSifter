"""Appendix Image List Module.

Displays a grid of images with tag and hash match filters.
Uses multi-select filters with OR/AND mode like the file list appendix.

Performance notes:
- Thumbnails are generated in parallel via ThreadPoolExecutor
- Thumbnails are cached to disk under ``{case_folder}/report_thumbs/``
- Thumbnails are referenced via file:// URIs to keep HTML small
- SQL batch queries are chunked to stay within SQLite variable limits
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir
from core.image_codecs import ensure_pillow_heif_registered
from core.database.manager import slugify_label

logger = logging.getLogger(__name__)

# Try to import PIL for thumbnail generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Maximum number of SQL parameters per query (conservative, SQLite default is 999)
_SQL_CHUNK_SIZE = 500

# Number of parallel threads for thumbnail generation
_THUMB_WORKERS = 8

# Human-readable display names for extractor identifiers
EXTRACTOR_DISPLAY_NAMES: Dict[str, str] = {
    "bulk_extractor": "Bulk Extractor",
    "bulk_extractor:images": "Bulk Extractor",
    "bulk_extractor_images": "Bulk Extractor",
    "foremost_carver": "Foremost Carver",
    "scalpel": "Scalpel",
    "image_carving": "Image Carving",
    "filesystem_images": "Filesystem",
    "cache_simple": "Chromium Cache",
    "cache_blockfile": "Chromium Cache (Blockfile)",
    "cache_firefox": "Firefox Cache",
    "browser_storage_indexeddb": "Browser Storage (IndexedDB)",
    "safari": "Safari",
    "safari_cache": "Safari Cache",
    "firefox_favicons": "Firefox Favicons",
    "chromium_favicons": "Chromium Favicons",
}


def _humanize_extractor(name: str) -> str:
    """Return a human-readable display name for an extractor identifier."""
    if name in EXTRACTOR_DISPLAY_NAMES:
        return EXTRACTOR_DISPLAY_NAMES[name]
    # Fallback: title-case with underscores replaced
    return name.replace("_", " ").title()


class AppendixImageListModule(BaseAppendixModule):
    """Appendix module for listing images with tag and hash match filters."""

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_image_list",
            name="Image List",
            description="Lists images in a grid with tag and hash match filters",
            category="Appendix",
            icon="🖼️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.TAG_SELECT,
                help_text="Filter by one or more tags",
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Hash Matches",
                filter_type=FilterType.MULTI_SELECT,
                help_text="Filter by one or more hash match lists",
                required=False,
            ),
            FilterField(
                key="filter_mode",
                label="Filter Mode",
                filter_type=FilterType.DROPDOWN,
                default="or",
                options=[
                    ("or", "OR - Any tag or any match"),
                    ("and", "AND - Must have tag AND match"),
                ],
                help_text="OR: Images with any selected tag or match. AND: Images must have a selected tag AND a selected match.",
                required=False,
            ),
            FilterField(
                key="include_filepath",
                label="Include File Path",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show the file path under each image",
                required=False,
            ),
            FilterField(
                key="include_url",
                label="Include URLs",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Show associated URLs (from cache/browser sources) for each image",
                required=False,
            ),
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="date_desc",
                options=[
                    ("date_desc", "Date (Newest First)"),
                    ("date_asc", "Date (Oldest First)"),
                    ("filename_asc", "Filename (A-Z)"),
                    ("filename_desc", "Filename (Z-A)"),
                ],
                help_text="Sort order for the images",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'image'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "match_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT db_name
                    FROM hash_matches
                    ORDER BY db_name
                    """
                )
                for (list_name,) in cursor.fetchall():
                    if list_name:
                        options.append((list_name, list_name))
            except Exception:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        # Extract config values
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        tag_filter = config.get("tag_filter") or []
        match_filter = config.get("match_filter") or []
        filter_mode = config.get("filter_mode", "or")
        include_filepath = bool(config.get("include_filepath", False))
        include_url = bool(config.get("include_url", False))
        sort_by = config.get("sort_by", "date_desc")

        # Get context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        # Optional progress / cancellation callbacks (injected by ReportBuildTask)
        progress_cb: Optional[Callable[[int, str], None]] = config.get("_progress_callback")
        cancelled_fn: Optional[Callable[[], bool]] = config.get("_cancelled_fn")

        query, params = self._build_query(
            evidence_id, tag_filter, match_filter, filter_mode, sort_by
        )

        # ── Step 1: Load image rows from database ──────────────────────
        images: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                image_id = row["id"]
                if image_id in seen_ids:
                    continue
                seen_ids.add(image_id)
                images.append(dict(row))
        except Exception as exc:
            return f'<div class="module-error">Error loading images: {exc}</div>'

        if progress_cb:
            progress_cb(5, f"Loaded {len(images)} images from database")

        # ── Step 2: Generate thumbnails in parallel with disk caching ──
        thumb_cache_dir = self._get_thumb_cache_dir(case_folder)
        # Register HEIF support once before the parallel batch
        if HAS_PIL:
            ensure_pillow_heif_registered()

        thumbnail_map = self._generate_thumbnails_batch(
            images,
            case_folder,
            evidence_id,
            evidence_label,
            thumb_cache_dir,
            progress_cb=progress_cb,
            cancelled_fn=cancelled_fn,
        )

        if cancelled_fn and cancelled_fn():
            return '<div class="module-error">Report generation was cancelled.</div>'

        # ── Step 3: Build display data for each image ──────────────────
        processed: List[Dict[str, Any]] = []
        for row in images:
            image_data = self._process_image(
                row, date_format, translations, thumbnail_map.get(row["id"], "")
            )
            processed.append(image_data)

        if progress_cb:
            progress_cb(70, "Fetching metadata")

        # ── Step 4: Enrich with sources, dates, URLs (chunked queries) ─
        image_ids = [img["id"] for img in processed]
        if image_ids:
            image_sources = self._get_image_sources(db_conn, image_ids)
            fs_dates = self._get_filesystem_dates(db_conn, image_ids)
            for img in processed:
                raw_sources = image_sources.get(img["id"], [])
                img["found_by"] = [_humanize_extractor(s) for s in raw_sources]

                # Use filesystem creation date when available for fs images
                if img["id"] in fs_dates:
                    img["timestamp"] = format_datetime(
                        fs_dates[img["id"]], date_format,
                        include_time=True, include_seconds=True,
                    )
                    img["timestamp_label"] = translations.get(
                        "creation_date", "Creation Date"
                    )
                elif "filesystem_images" in raw_sources and not img.get("timestamp"):
                    img["timestamp_label"] = translations.get(
                        "creation_date", "Creation Date"
                    )
                else:
                    img["timestamp_label"] = translations.get(
                        "source_date", "Source Date"
                    )

        # Fetch URLs for images if requested
        if include_url and processed:
            image_urls = self._get_image_urls(db_conn, image_ids)
            for img in processed:
                img["urls"] = image_urls.get(img["id"], [])

        if progress_cb:
            progress_cb(80, "Rendering template")

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            images=processed,
            total_count=len(processed),
            include_filepath=include_filepath,
            include_url=include_url,
            t=translations,
            locale=locale,
        )

    # ── Chunked batch query helpers ───────────────────────────────────

    @staticmethod
    def _chunked(lst: List[Any], size: int = _SQL_CHUNK_SIZE):
        """Yield successive chunks of *lst* with at most *size* items."""
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    def _get_image_urls(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, List[str]]:
        """Fetch unique URLs associated with images from image_discoveries.

        Queries are chunked to stay within SQLite variable limits.
        """
        if not image_ids:
            return {}

        result: Dict[int, List[str]] = {}
        try:
            for chunk in self._chunked(image_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor = db_conn.execute(
                    f"""
                    SELECT image_id, cache_url
                    FROM image_discoveries
                    WHERE image_id IN ({placeholders})
                      AND cache_url IS NOT NULL
                      AND cache_url != ''
                    ORDER BY image_id, discovered_at
                    """,
                    chunk,
                )
                for image_id, cache_url in cursor.fetchall():
                    if image_id not in result:
                        result[image_id] = []
                    if cache_url not in result[image_id]:
                        result[image_id].append(cache_url)
        except Exception:
            pass
        return result

    def _get_filesystem_dates(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, str]:
        """Fetch filesystem creation dates for images from image_discoveries.

        Returns the best available filesystem timestamp (fs_crtime preferred,
        then fs_mtime as fallback) for images discovered by filesystem_images.
        Queries are chunked to stay within SQLite variable limits.
        """
        if not image_ids:
            return {}

        result: Dict[int, str] = {}
        try:
            for chunk in self._chunked(image_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor = db_conn.execute(
                    f"""
                    SELECT image_id,
                           COALESCE(fs_crtime, fs_mtime) AS best_date
                    FROM image_discoveries
                    WHERE image_id IN ({placeholders})
                      AND discovered_by = 'filesystem_images'
                      AND (fs_crtime IS NOT NULL OR fs_mtime IS NOT NULL)
                    """,
                    chunk,
                )
                for image_id, best_date in cursor.fetchall():
                    if best_date and image_id not in result:
                        result[image_id] = best_date
        except Exception:
            pass
        return result

    def _get_image_sources(
        self, db_conn: sqlite3.Connection, image_ids: List[int]
    ) -> Dict[int, List[str]]:
        """Fetch distinct extractor sources for images using v_image_sources.

        Queries are chunked to stay within SQLite variable limits.
        """
        if not image_ids:
            return {}

        result: Dict[int, List[str]] = {}
        try:
            for chunk in self._chunked(image_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor = db_conn.execute(
                    f"""
                    SELECT image_id, sources
                    FROM v_image_sources
                    WHERE image_id IN ({placeholders})
                    """,
                    chunk,
                )
                for image_id, sources in cursor.fetchall():
                    if sources:
                        result[image_id] = [
                            s.strip() for s in sources.split(",") if s.strip()
                        ]
                    else:
                        result[image_id] = []
        except Exception:
            pass
        return result

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        match_filter: List[str],
        filter_mode: str,
        sort_by: str,
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["i.evidence_id = ?"]

        tag_condition = None
        match_condition = None

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = i.id
                      AND ta.artifact_type = 'image'
                      AND ta.evidence_id = i.evidence_id
                      AND t.name IN ({placeholders})
                )
                """

        if match_filter:
            placeholders = ", ".join(["?"] * len(match_filter))
            match_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM hash_matches hm
                    WHERE hm.image_id = i.id
                      AND hm.db_name IN ({placeholders})
                )
                """

        # Apply filter logic based on mode
        if filter_mode == "and" and tag_filter and match_filter:
            # AND mode: must have a selected tag AND a selected match
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(f"({match_condition})")
            params.extend(match_filter)
        elif tag_condition or match_condition:
            # OR mode (default): any selected tag OR any selected match
            or_parts = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if match_condition:
                or_parts.append(match_condition)
                params.extend(match_filter)
            if or_parts:
                conditions.append(f"({' OR '.join(or_parts)})")

        where_clause = " AND ".join(conditions)

        # Sort order
        order_map = {
            "date_desc": "i.ts_utc DESC NULLS LAST, i.filename ASC",
            "date_asc": "i.ts_utc ASC NULLS FIRST, i.filename ASC",
            "filename_asc": "i.filename ASC",
            "filename_desc": "i.filename DESC",
        }
        order = order_map.get(sort_by, "i.ts_utc DESC NULLS LAST")

        query = f"""
            SELECT DISTINCT
                i.id,
                i.rel_path,
                i.filename,
                i.md5,
                i.sha256,
                i.ts_utc,
                i.exif_json,
                i.size_bytes,
                i.first_discovered_by
            FROM images i
            WHERE {where_clause}
            ORDER BY {order}
        """

        return query, params

    def _process_image(
        self,
        row: Dict[str, Any],
        date_format: str,
        t: Dict[str, str],
        thumbnail_ref: str,
    ) -> Dict[str, Any]:
        """Process an image row into display data.

        Args:
            row: Image database row as dict.
            date_format: Date format preference ("eu" / "us").
            t: Translation dict.
            thumbnail_ref: Pre-computed thumbnail reference (file URI, data URI,
                or empty string).
        """
        # Parse EXIF if available
        exif_data = {}
        if row.get("exif_json"):
            try:
                exif_data = json.loads(row["exif_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        # Format EXIF for display (key fields only)
        exif_display = []
        exif_keys = [
            ("DateTimeOriginal", t.get("exif_taken", "Taken")),
            ("Make", t.get("exif_camera", "Camera")),
            ("Model", t.get("exif_model", "Model")),
            ("GPSLatitude", t.get("exif_gps_lat", "GPS Lat")),
            ("GPSLongitude", t.get("exif_gps_lon", "GPS Lon")),
        ]
        for exif_key, label in exif_keys:
            if exif_key in exif_data:
                exif_display.append(f"{label}: {exif_data[exif_key]}")

        return {
            "id": row["id"],
            "rel_path": row.get("rel_path", ""),
            "md5": row.get("md5", ""),
            "sha256": row.get("sha256", ""),
            "timestamp": format_datetime(
                row.get("ts_utc", ""), date_format, include_time=True, include_seconds=True
            ),
            "timestamp_label": t.get("source_date", "Source Date"),  # default; overridden in render()
            "size_bytes": row.get("size_bytes"),
            "exif_display": exif_display,
            "thumbnail_src": thumbnail_ref,
            "filename": row.get("filename", ""),  # kept for alt text
        }

    # ── Thumbnail generation (parallel + cached) ──────────────────────

    @staticmethod
    def _get_thumb_cache_dir(case_folder: Optional[Path]) -> Optional[Path]:
        """Return (and create) the thumbnail cache directory under the case folder."""
        if not case_folder:
            return None
        cache_dir = Path(case_folder) / "report_thumbs"
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return None
        return cache_dir

    @staticmethod
    def _thumb_cache_key(image_id: int, md5: Optional[str], rel_path: str) -> str:
        """Deterministic cache key for a thumbnail.

        Uses the image's MD5 hash when available, otherwise a hash of the
        relative path combined with the DB id.
        """
        if md5:
            return md5
        raw = f"{image_id}:{rel_path}"
        return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()

    def _generate_single_thumbnail(
        self,
        image_path: Optional[Path],
        cache_path: Optional[Path],
    ) -> str:
        """Generate a thumbnail for one image, using cache when available.

        Returns a ``file://`` URI if *cache_path* is provided and writable,
        otherwise an inline ``data:image/jpeg;base64,...`` string.
        *image_path* may be ``None`` when the caller already verified a
        cache hit.
        """
        # Check disk cache first
        if cache_path and cache_path.exists() and cache_path.stat().st_size > 100:
            return cache_path.as_uri()

        if not image_path:
            return ""

        try:
            with PILImage.open(image_path) as img:
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                img.thumbnail(self.THUMB_SIZE, PILImage.Resampling.LANCZOS)

                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=85)
                thumb_bytes = buffer.getvalue()

            # Try to write to disk cache
            if cache_path:
                try:
                    cache_path.write_bytes(thumb_bytes)
                    return cache_path.as_uri()
                except OSError:
                    pass

            # Fallback: inline base64
            b64 = base64.b64encode(thumb_bytes).decode("utf-8")
            return f"data:image/jpeg;base64,{b64}"
        except Exception:
            return ""

    def _generate_thumbnails_batch(
        self,
        image_rows: List[Dict[str, Any]],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        thumb_cache_dir: Optional[Path],
        progress_cb: Optional[Callable[[int, str], None]] = None,
        cancelled_fn: Optional[Callable[[], bool]] = None,
    ) -> Dict[int, str]:
        """Generate thumbnails for all images in parallel with disk caching.

        Args:
            image_rows: List of image database row dicts.
            case_folder: Case workspace root.
            evidence_id: Evidence ID.
            evidence_label: Evidence label for slug construction.
            thumb_cache_dir: Directory for cached thumbnails (may be None).
            progress_cb: Optional ``(percent, message)`` callback.
            cancelled_fn: Optional callable returning True when cancelled.

        Returns:
            Dict mapping image ID to a thumbnail reference string
            (``file://`` URI, inline data URI, or empty string).
        """
        result: Dict[int, str] = {}
        if not HAS_PIL or not image_rows:
            return result

        # Build work items: (image_id, source_path_or_None, cache_path_or_None)
        # We check the cache FIRST — if a cached thumbnail exists we don't need
        # the source image at all, so we can skip the (potentially expensive)
        # path resolution.
        work_items: List[tuple] = []
        for row in image_rows:
            rel_path = row.get("rel_path")
            if not rel_path:
                continue
            image_id = row["id"]

            cache_path: Optional[Path] = None
            if thumb_cache_dir:
                key = self._thumb_cache_key(
                    image_id, row.get("md5"), rel_path
                )
                cache_path = thumb_cache_dir / f"{key}.jpg"

                # Cache hit — no source resolution needed
                if cache_path.exists() and cache_path.stat().st_size > 100:
                    work_items.append((image_id, None, cache_path))
                    continue

            # Need to generate: resolve source path
            source_path = self._resolve_image_path(
                rel_path,
                row.get("first_discovered_by"),
                case_folder,
                evidence_id,
                evidence_label,
            )
            if not source_path or not source_path.exists():
                continue

            work_items.append((image_id, source_path, cache_path))

        total = len(work_items)
        if total == 0:
            return result

        # Fast path: serve entirely from cache if all work items are cache hits
        uncached = [item for item in work_items if item[1] is not None]
        all_cached = len(uncached) == 0

        if all_cached:
            # Everything is cached — no PIL work needed
            logger.debug("All %d thumbnails served from cache", total)
            for image_id, _src, cache_path in work_items:
                result[image_id] = cache_path.as_uri()  # type: ignore[union-attr]
            if progress_cb:
                progress_cb(60, f"All {total} thumbnails loaded from cache")
            return result

        # Parallel generation
        done_count = 0
        workers = min(_THUMB_WORKERS, total)

        def _task(item: tuple) -> tuple:
            image_id, source_path, cache_path = item
            ref = self._generate_single_thumbnail(source_path, cache_path)
            return (image_id, ref)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_task, item): item for item in work_items}
            for future in as_completed(futures):
                if cancelled_fn and cancelled_fn():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    image_id, ref = future.result()
                    result[image_id] = ref
                except Exception:
                    pass
                done_count += 1
                if progress_cb and done_count % 50 == 0:
                    pct = 10 + int(50 * done_count / total)
                    progress_cb(pct, f"Thumbnails: {done_count}/{total}")

        if progress_cb:
            progress_cb(60, f"Generated {len(result)} thumbnails")

        return result

    def _resolve_image_path(
        self,
        rel_path: str,
        discovered_by: Optional[str],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> Optional[Path]:
        """Resolve an image's relative path to its full filesystem path."""
        if not case_folder:
            # Try simple paths as fallback
            simple_path = Path(rel_path)
            if simple_path.exists():
                return simple_path
            return None

        # Build evidence slug using canonical slugify function
        if evidence_label:
            evidence_slug = slugify_label(evidence_label, evidence_id)
        else:
            evidence_slug = f"evidence_{evidence_id}"

        evidence_dir = case_folder / "evidences" / evidence_slug

        # Map discovered_by to the extractor's output subdirectory
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
            "cache_simple": "cache_simple",
            "cache_blockfile": "cache_simple",
            "cache_firefox": "cache_firefox",
            "browser_storage_indexeddb": "browser_storage",
            "safari": "safari",
            # Favicon extractors
            "firefox_favicons": "firefox_favicons",
            "chromium_favicons": "chromium_favicons",
        }

        base_subdir = source_map.get(discovered_by or "", "")

        # Try extractor-specific path first
        if base_subdir:
            full_path = (evidence_dir / base_subdir / rel_path).resolve()
            if full_path.exists():
                return full_path

        # Fallback: try direct path under evidence directory
        fallback_path = (evidence_dir / rel_path).resolve()
        if fallback_path.exists():
            return fallback_path

        # Last resort: try from case folder directly
        last_resort = (case_folder / rel_path).resolve()
        if last_resort.exists():
            return last_resort

        return None
