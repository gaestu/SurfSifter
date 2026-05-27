"""Appendix Download List Module.

Displays all downloaded images in a grid for the appendix.
Uses parallel thumbnail generation with disk caching for performance.

Performance notes:
- Thumbnails are generated in parallel via ThreadPoolExecutor
- Thumbnails are cached to disk under ``{case_folder}/report_thumbs/downloads/``
- Cached thumbnails are read back through no-follow checks and embedded inline
"""

from __future__ import annotations

import base64
import hashlib
import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...dates import format_datetime
from ...paths import get_module_template_dir
from core.image_codecs import (
    PIL_AVAILABLE,
    ensure_pillow_heif_registered,
    thumbnail_to_jpeg_bytes,
)
from core.database.manager import slugify_label
from core.image_paths import evidence_workspace_root, safe_relative_artifact_path
from core.safe_io import (
    ensure_dir_no_follow,
    write_bytes_no_follow,
)

logger = logging.getLogger(__name__)
_MD5_HEX_RE = re.compile(r"^[0-9a-fA-F]{32}$")

HAS_PIL = PIL_AVAILABLE

# Maximum number of SQL parameters per query (conservative, SQLite default is 999)
_SQL_CHUNK_SIZE = 500

# Number of parallel threads for thumbnail generation
_THUMB_WORKERS = 2


class AppendixDownloadListModule(BaseAppendixModule):
    """Appendix module for listing all downloaded images."""

    # Special filter values
    ALL = "all"
    ANY_TAG = "any_tag"

    # Thumbnail size
    THUMB_SIZE = (200, 200)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_download_list",
            name="Download List",
            description="Lists all downloaded images with URL, hash, and timestamp",
            category="Appendix",
            icon="📥",
        )

    def get_default_title(self) -> str:
        """Return default title for the appendix section."""
        return "Downloaded Images"

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="domain_filter",
                label="Domain",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All Domains"),
                ],
                help_text="Filter by source domain (specific domains loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All"),
                    (self.ANY_TAG, "Any Tag"),
                ],
                help_text="Filter by tag (specific tags loaded dynamically)",
                required=False,
            ),
            FilterField(
                key="include_url",
                label="Include URL",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the source URL under each image",
                required=False,
            ),
            FilterField(
                key="include_hash",
                label="Include Hash",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show the MD5/SHA256 hash under each image",
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
                    ("url_asc", "URL (A-Z)"),
                    ("url_desc", "URL (Z-A)"),
                ],
                help_text="Sort order for the images",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for domain and tag filters."""
        if key == "domain_filter":
            options: List[tuple] = [
                (self.ALL, "All Domains"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT domain
                    FROM downloads
                    WHERE file_type = 'image'
                      AND status = 'completed'
                      AND domain IS NOT NULL
                      AND domain != ''
                    ORDER BY domain
                    """
                )
                for (domain,) in cursor.fetchall():
                    if domain:
                        options.append((domain, domain))
            except Exception:
                pass
            return options

        if key == "tag_filter":
            options: List[tuple] = [
                (self.ALL, "All"),
                (self.ANY_TAG, "Any Tag"),
            ]
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'download'
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
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
        """Render the full download list for the appendix."""
        # Extract config values
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")

        domain_filter = config.get("domain_filter", self.ALL)
        tag_filter = config.get("tag_filter", self.ALL)
        include_url = bool(config.get("include_url", True))
        include_hash = bool(config.get("include_hash", True))
        sort_by = config.get("sort_by", "date_desc")

        # Context for path resolution (injected by ReportBuilder)
        case_folder = config.get("_case_folder")
        evidence_label = config.get("_evidence_label")

        # Optional progress / cancellation callbacks
        progress_cb: Optional[Callable[[int, str], None]] = config.get(
            "_progress_callback"
        )
        cancelled_fn: Optional[Callable[[], bool]] = config.get("_cancelled_fn")

        # Build and execute query
        query, params = self._build_query(
            evidence_id, domain_filter, tag_filter, sort_by
        )

        # ── Step 1: Load download rows ─────────────────────────────────
        downloads: List[Dict[str, Any]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                downloads.append(dict(row))
        except Exception as exc:
            return (
                f'<div class="module-error">'
                f"Error loading downloaded images: {exc}</div>"
            )

        if progress_cb:
            progress_cb(5, f"Loaded {len(downloads)} downloaded images")

        # ── Step 2: Generate thumbnails in parallel with disk caching ──
        thumb_cache_dir = self._get_thumb_cache_dir(case_folder)
        if HAS_PIL:
            ensure_pillow_heif_registered()

        thumbnail_map = self._generate_thumbnails_batch(
            downloads,
            case_folder,
            evidence_id,
            evidence_label,
            thumb_cache_dir,
            progress_cb=progress_cb,
            cancelled_fn=cancelled_fn,
        )

        if cancelled_fn and cancelled_fn():
            return '<div class="module-error">Report generation was cancelled.</div>'

        # ── Step 3: Build display data ─────────────────────────────────
        processed: List[Dict[str, Any]] = []
        for row in downloads:
            image_data = self._process_download(
                row,
                date_format,
                translations,
                thumbnail_map.get(row["id"], ""),
            )
            processed.append(image_data)

        if progress_cb:
            progress_cb(80, "Rendering template")

        # ── Step 4: Render template ────────────────────────────────────
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            images=processed,
            total_count=len(processed),
            include_url=include_url,
            include_hash=include_hash,
            t=translations,
            locale=locale,
        )

    # ── Query building ────────────────────────────────────────────────

    def _build_query(
        self,
        evidence_id: int,
        domain_filter: str,
        tag_filter: str,
        sort_by: str,
    ) -> tuple[str, list]:
        """Build SQL query based on filters."""
        params: list = [evidence_id]

        select = """
            SELECT DISTINCT
                d.id,
                d.url,
                d.domain,
                d.filename,
                d.dest_path,
                d.md5,
                d.size_bytes,
                d.completed_at_utc,
                d.width,
                d.height
            FROM downloads d
        """

        joins = []
        where = [
            "d.evidence_id = ?",
            "d.file_type = 'image'",
            "d.status = 'completed'",
        ]

        # Domain filter
        if domain_filter not in (self.ALL, None, ""):
            where.append("d.domain = ?")
            params.append(domain_filter)

        # Tag filter
        if tag_filter == self.ANY_TAG:
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = d.id
                    AND ta.artifact_type = 'download'
                """
            )
        elif tag_filter not in (self.ALL, None, ""):
            joins.append(
                """
                JOIN tag_associations ta ON ta.artifact_id = d.id
                    AND ta.artifact_type = 'download'
                JOIN tags t ON t.id = ta.tag_id
                """
            )
            where.append("t.name = ?")
            params.append(tag_filter)

        # Sort order
        order_map = {
            "date_desc": "d.completed_at_utc DESC NULLS LAST, d.filename ASC",
            "date_asc": "d.completed_at_utc ASC NULLS FIRST, d.filename ASC",
            "filename_asc": "d.filename ASC",
            "filename_desc": "d.filename DESC",
            "url_asc": "d.url ASC",
            "url_desc": "d.url DESC",
        }
        order = order_map.get(sort_by, "d.completed_at_utc DESC NULLS LAST")

        query = select + " ".join(joins)
        query += " WHERE " + " AND ".join(where)
        query += f" ORDER BY {order}"

        return query, params

    # ── Data processing ───────────────────────────────────────────────

    def _process_download(
        self,
        row: Dict[str, Any],
        date_format: str,
        t: Dict[str, str],
        thumbnail_ref: str,
    ) -> Dict[str, Any]:
        """Process a download row into display data."""
        size_bytes = row.get("size_bytes")
        size_display = self._format_size(size_bytes) if size_bytes else ""

        completed_at = format_datetime(
            row.get("completed_at_utc", ""),
            date_format,
            include_time=True,
            include_seconds=True,
        )

        return {
            "id": row["id"],
            "filename": row.get("filename", t.get("unknown", "Unknown")),
            "url": row.get("url", ""),
            "domain": row.get("domain", ""),
            "md5": row.get("md5", ""),
            "size_bytes": size_bytes,
            "size_display": size_display,
            "downloaded_at": completed_at,
            "width": row.get("width"),
            "height": row.get("height"),
            "thumbnail_src": thumbnail_ref,
        }

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size for display."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"

    # ── Thumbnail generation (parallel + cached) ──────────────────────

    @staticmethod
    def _get_thumb_cache_dir(case_folder: Optional[Path]) -> Optional[Path]:
        """Return (and create) thumbnail cache directory under the case folder."""
        if not case_folder:
            return None
        case_root = Path(case_folder).resolve()
        cache_root = case_root / "report_thumbs"
        try:
            ensure_dir_no_follow(cache_root, containment_root=case_root)
            resolved_cache_root = cache_root.resolve()
            if resolved_cache_root != cache_root:
                return None
            resolved_cache_root.relative_to(case_root)
            cache_dir = resolved_cache_root / "downloads"
            ensure_dir_no_follow(cache_dir, containment_root=case_root)
            resolved_cache_dir = cache_dir.resolve()
            if resolved_cache_dir != cache_dir:
                return None
            resolved_cache_dir.relative_to(case_root)
        except (OSError, ValueError):
            return None
        return resolved_cache_dir

    @staticmethod
    def _thumb_cache_key(
        download_id: int,
        md5: Optional[str],
        evidence_id: Optional[int] = None,
        dest_path: str = "",
    ) -> str:
        """Deterministic cache key for a thumbnail."""
        digest = md5.lower() if md5 and _MD5_HEX_RE.fullmatch(md5) else ""
        raw = f"download:{evidence_id}:{download_id}:{digest}:{dest_path}"
        return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()

    @staticmethod
    def _safe_thumb_cache_path(cache_dir: Path, key: str) -> Optional[Path]:
        """Return a cache path only if it resolves under ``cache_dir``."""
        try:
            cache_root = cache_dir.resolve()
            cache_path = cache_root / f"{key}.jpg"
            resolved_cache_path = cache_path.resolve()
            resolved_cache_path.relative_to(cache_root)
        except (OSError, ValueError):
            return None
        return cache_path

    def _generate_single_thumbnail(
        self,
        image_path: Optional[Path],
        cache_path: Optional[Path],
        cache_containment_root: Optional[Path] = None,
        source_containment_root: Optional[Path] = None,
    ) -> str:
        """Generate a thumbnail for one image from the selected source artifact.

        Returns an inline ``data:image/jpeg;base64,...`` string.
        """
        if not image_path:
            return ""

        try:
            thumb_bytes = thumbnail_to_jpeg_bytes(
                image_path,
                size=self.THUMB_SIZE,
                containment_root=source_containment_root,
            )
            if not thumb_bytes:
                return ""

            # Try to write to disk cache
            if cache_path:
                try:
                    write_bytes_no_follow(
                        cache_path,
                        thumb_bytes,
                        containment_root=cache_containment_root or cache_path.parent,
                        exclusive=True,
                    )
                except OSError:
                    pass

            # Fallback: inline base64
            b64 = base64.b64encode(thumb_bytes).decode("utf-8")
            return f"data:image/jpeg;base64,{b64}"
        except Exception:
            return ""

    def _generate_thumbnails_batch(
        self,
        download_rows: List[Dict[str, Any]],
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
        thumb_cache_dir: Optional[Path],
        progress_cb: Optional[Callable[[int, str], None]] = None,
        cancelled_fn: Optional[Callable[[], bool]] = None,
    ) -> Dict[int, str]:
        """Generate thumbnails for all downloads in parallel with disk caching."""
        result: Dict[int, str] = {}
        if not HAS_PIL or not download_rows:
            return result

        work_items: List[tuple] = []
        cache_containment_root = (
            thumb_cache_dir.parent.parent
            if thumb_cache_dir
            else (Path(case_folder).resolve() if case_folder else None)
        )
        source_containment_root = (
            evidence_workspace_root(
                case_folder=Path(case_folder),
                evidence_id=evidence_id,
                evidence_label=evidence_label,
            )
            if case_folder
            else None
        )
        for row in download_rows:
            dest_path = row.get("dest_path")
            if not dest_path:
                continue
            download_id = row["id"]

            cache_path: Optional[Path] = None
            if thumb_cache_dir:
                key = self._thumb_cache_key(
                    download_id,
                    row.get("md5"),
                    evidence_id,
                    dest_path,
                )
                cache_path = self._safe_thumb_cache_path(thumb_cache_dir, key)

            source_path = self._resolve_download_path(
                dest_path, case_folder, evidence_id, evidence_label
            )
            if not source_path or not source_path.exists():
                continue

            has_stable_cache_key = bool(row.get("md5") and _MD5_HEX_RE.match(str(row.get("md5"))))
            work_items.append(
                (
                    download_id,
                    source_path,
                    cache_path if has_stable_cache_key else None,
                    cache_containment_root,
                    source_containment_root,
                )
            )

        total = len(work_items)
        if total == 0:
            return result

        # Parallel generation
        done_count = 0
        workers = min(_THUMB_WORKERS, total)

        def _task(item: tuple) -> tuple:
            download_id, source_path, cache_path, cache_root, source_root = item
            ref = self._generate_single_thumbnail(
                source_path,
                cache_path,
                cache_root,
                source_root,
            )
            return (download_id, ref)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_task, item): item for item in work_items}
            for future in as_completed(futures):
                if cancelled_fn and cancelled_fn():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    download_id, ref = future.result()
                    result[download_id] = ref
                except Exception:
                    pass
                done_count += 1
                if progress_cb and done_count % 50 == 0:
                    pct = 10 + int(50 * done_count / total)
                    progress_cb(pct, f"Thumbnails: {done_count}/{total}")

        if progress_cb:
            progress_cb(60, f"Generated {len(result)} thumbnails")

        return result

    def _resolve_download_path(
        self,
        dest_path: str,
        case_folder: Optional[Path],
        evidence_id: int,
        evidence_label: Optional[str],
    ) -> Optional[Path]:
        """Resolve a download's destination path to its full filesystem path."""
        if not case_folder:
            return None

        try:
            case_root = Path(case_folder).resolve()
        except OSError:
            return None

        # Build evidence slug
        if evidence_label:
            evidence_slug = slugify_label(evidence_label, evidence_id)
        else:
            evidence_slug = f"evidence_{evidence_id}"

        evidence_dir = case_root / "evidences" / evidence_slug
        if evidence_dir.is_symlink():
            return None
        evidence_root = evidence_dir.resolve()
        if evidence_root != evidence_dir:
            return None

        raw_path = Path(dest_path)
        if raw_path.is_absolute():
            return None
        rel_path = safe_relative_artifact_path(dest_path)
        if rel_path is None:
            return None
        downloads_dir = evidence_dir / "_downloads"
        candidates = (
            downloads_dir / rel_path,
            evidence_dir / rel_path,
            case_root / rel_path,
        )
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
                resolved.relative_to(case_root)
                resolved.relative_to(evidence_root)
            except (OSError, ValueError):
                continue
            if resolved.exists():
                return resolved

        return None
