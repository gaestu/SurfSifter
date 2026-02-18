"""Image carving utilities for Safari cache entries."""

from __future__ import annotations

import gzip
import hashlib
import zlib
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Set

from PIL import Image

from core.logging import get_logger
from core.phash import compute_phash
from ....image_signatures import detect_image_type
from ._blob_parser import ResponseMetadata
from ._parser import SafariCacheEntry

LOGGER = get_logger("extractors.browser.safari.cache.image_carver")

try:
    import brotli

    BROTLI_AVAILABLE = True
except Exception:
    BROTLI_AVAILABLE = False

try:
    import zstandard

    ZSTD_AVAILABLE = True
except Exception:
    ZSTD_AVAILABLE = False


@dataclass
class CarvedImage:
    rel_path: str
    filename: str
    md5: str
    sha256: str
    phash: Optional[str]
    size_bytes: int
    format: str
    width: Optional[int]
    height: Optional[int]
    source_url: Optional[str]
    source_entry_id: Optional[int]
    source_type: str
    content_type: Optional[str]


def extract_body(data: bytes, content_encoding: Optional[str] = None) -> Optional[bytes]:
    """Decompress response body when content-encoding is known."""
    if not data:
        return None
    if not content_encoding:
        return data

    encoding = content_encoding.lower().strip()
    if "gzip" in encoding:
        try:
            return gzip.decompress(data)
        except Exception:
            return data
    if "br" in encoding:
        if BROTLI_AVAILABLE:
            try:
                return brotli.decompress(data)
            except Exception:
                return data
        return data
    if "zstd" in encoding:
        if ZSTD_AVAILABLE:
            try:
                dctx = zstandard.ZstdDecompressor()
                return dctx.decompress(data)
            except Exception:
                return data
        return data
    if "deflate" in encoding:
        try:
            return zlib.decompress(data, -zlib.MAX_WBITS)
        except Exception:
            return data
    return data


def carve_image_from_cache_entry(
    entry: SafariCacheEntry,
    response_meta: Optional[ResponseMetadata],
    run_dir: Path,
    fscached_dir: Optional[Path],
) -> Optional[CarvedImage]:
    """Carve one image from an inline or fsCachedData-backed cache entry."""
    content_type = response_meta.content_type if response_meta else None
    if content_type and not _content_type_allows_image(content_type):
        return None

    source_type = "inline"
    raw_body: Optional[bytes] = None
    if entry.is_data_on_fs:
        source_type = "fscached"
        fs_file = _find_fscached_file(entry.entry_id, fscached_dir)
        if fs_file and fs_file.is_file():
            raw_body = fs_file.read_bytes()
    else:
        raw_body = entry.inline_body

    if not raw_body:
        return None

    encoding = response_meta.all_headers.get("Content-Encoding") if response_meta else None
    body = extract_body(raw_body, encoding) or raw_body
    image_type = detect_image_type(body)
    if not image_type:
        return None

    return _save_carved_image(
        body=body,
        image_format=image_type[0],
        extension=image_type[1],
        run_dir=run_dir,
        stem=f"entry_{entry.entry_id}_{source_type}",
        source_url=entry.url,
        source_entry_id=entry.entry_id,
        source_type=source_type,
        content_type=content_type,
    )


def carve_orphan_images(
    fscached_dir: Path,
    known_entry_ids: Set[int],
    run_dir: Path,
) -> List[CarvedImage]:
    """Carve image files in fsCachedData that are not mapped to entry IDs."""
    carved: List[CarvedImage] = []
    if not fscached_dir.exists():
        return carved

    for file_path in sorted(fscached_dir.glob("*")):
        if not file_path.is_file():
            continue

        if _filename_matches_entry(file_path.name, known_entry_ids):
            continue

        data = file_path.read_bytes()
        image_type = detect_image_type(data)
        if not image_type:
            continue

        item = _save_carved_image(
            body=data,
            image_format=image_type[0],
            extension=image_type[1],
            run_dir=run_dir,
            stem=f"orphan_{file_path.stem}",
            source_url=None,
            source_entry_id=None,
            source_type="orphan",
            content_type=None,
        )
        if item:
            carved.append(item)

    return carved


def _save_carved_image(
    *,
    body: bytes,
    image_format: str,
    extension: str,
    run_dir: Path,
    stem: str,
    source_url: Optional[str],
    source_entry_id: Optional[int],
    source_type: str,
    content_type: Optional[str],
) -> Optional[CarvedImage]:
    images_dir = run_dir / "carved_images"
    images_dir.mkdir(parents=True, exist_ok=True)

    md5 = hashlib.md5(body).hexdigest()
    sha256 = hashlib.sha256(body).hexdigest()
    filename = f"{stem}_{sha256[:12]}{extension}"
    dest_path = images_dir / filename

    try:
        dest_path.write_bytes(body)
        phash = compute_phash(BytesIO(body))
        width, height = _get_dimensions(body)
        rel_path = str(dest_path.relative_to(run_dir.parent))
        return CarvedImage(
            rel_path=rel_path,
            filename=filename,
            md5=md5,
            sha256=sha256,
            phash=phash,
            size_bytes=len(body),
            format=image_format,
            width=width,
            height=height,
            source_url=source_url,
            source_entry_id=source_entry_id,
            source_type=source_type,
            content_type=content_type,
        )
    except Exception as exc:
        LOGGER.debug("Failed to save carved Safari cache image: %s", exc)
        return None


def _content_type_allows_image(content_type: str) -> bool:
    value = content_type.lower()
    if value.startswith("image/"):
        return True
    return value.startswith("application/octet-stream")


def _get_dimensions(data: bytes) -> tuple[Optional[int], Optional[int]]:
    try:
        with Image.open(BytesIO(data)) as img:
            return int(img.width), int(img.height)
    except Exception:
        return None, None


def _find_fscached_file(entry_id: int, fscached_dir: Optional[Path]) -> Optional[Path]:
    if not fscached_dir or not fscached_dir.exists():
        return None

    candidates = [
        str(entry_id),
        str(entry_id).upper(),
        f"{entry_id:x}",
        f"{entry_id:X}",
        f"{entry_id:08x}",
        f"{entry_id:08X}",
    ]
    for name in candidates:
        path = fscached_dir / name
        if path.exists():
            return path
    return None


def _filename_matches_entry(filename: str, entry_ids: Set[int]) -> bool:
    for entry_id in entry_ids:
        if filename == str(entry_id):
            return True
        if filename.lower() == f"{entry_id:x}":
            return True
        if filename.lower() == f"{entry_id:08x}":
            return True
    return False
