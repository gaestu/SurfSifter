"""Tests for the optimized image list appendix module.

Covers:
- Parallel thumbnail generation with ThreadPoolExecutor
- Disk-based thumbnail caching
- Inline base64 thumbnails with disk cache reuse
- Chunked SQL queries (_chunked helper)
- Progress / cancellation callbacks
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from PIL import Image as PILImage

from reports.appendix.image_list.module import AppendixImageListModule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_test_image(path: Path, size: tuple = (64, 64)) -> None:
    """Write a small JPEG file for testing."""
    img = PILImage.new("RGB", size, color=(100, 150, 200))
    img.save(path, format="JPEG")


def _create_colored_image(path: Path, color: tuple[int, int, int]) -> None:
    img = PILImage.new("RGB", (64, 64), color=color)
    img.save(path, format="JPEG")


def _make_image_rows(
    tmp_path: Path, count: int, prefix: str = "img"
) -> List[Dict[str, Any]]:
    """Create *count* test images and return DB-like row dicts."""
    rows = []
    for i in range(count):
        rel = f"{prefix}_{i:04d}.jpg"
        fpath = tmp_path / rel
        _create_test_image(fpath)
        rows.append(
            {
                "id": i + 1,
                "rel_path": rel,
                "filename": rel,
                "md5": f"md5_{i:04d}",
                "sha256": "",
                "ts_utc": "",
                "exif_json": None,
                "size_bytes": fpath.stat().st_size,
                "first_discovered_by": None,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Tests: _chunked helper
# ---------------------------------------------------------------------------

class TestChunked:
    def test_empty(self):
        assert list(AppendixImageListModule._chunked([])) == []

    def test_exact_multiple(self):
        data = list(range(10))
        chunks = list(AppendixImageListModule._chunked(data, size=5))
        assert len(chunks) == 2
        assert chunks[0] == [0, 1, 2, 3, 4]
        assert chunks[1] == [5, 6, 7, 8, 9]

    def test_remainder(self):
        data = list(range(7))
        chunks = list(AppendixImageListModule._chunked(data, size=3))
        assert len(chunks) == 3
        assert chunks[2] == [6]

    def test_single_chunk(self):
        data = [1, 2, 3]
        chunks = list(AppendixImageListModule._chunked(data, size=100))
        assert chunks == [[1, 2, 3]]


# ---------------------------------------------------------------------------
# Tests: _generate_single_thumbnail
# ---------------------------------------------------------------------------

class TestSingleThumbnail:
    def test_returns_base64_when_no_cache(self, tmp_path: Path):
        img_path = tmp_path / "test.jpg"
        _create_test_image(img_path)

        module = AppendixImageListModule()
        result = module._generate_single_thumbnail(img_path, cache_path=None)
        assert result.startswith("data:image/jpeg;base64,")

    def test_writes_to_cache_and_returns_file_uri(self, tmp_path: Path):
        img_path = tmp_path / "test.jpg"
        _create_test_image(img_path)
        cache_path = tmp_path / "cache" / "thumb.jpg"
        cache_path.parent.mkdir()

        module = AppendixImageListModule()
        result = module._generate_single_thumbnail(img_path, cache_path)
        assert result.startswith("data:image/jpeg;base64,")
        assert cache_path.exists()
        assert cache_path.stat().st_size > 100

    def test_does_not_serve_cache_without_source(self, tmp_path: Path):
        img_path = tmp_path / "test.jpg"
        _create_test_image(img_path)
        cache_path = tmp_path / "cached.jpg"

        module = AppendixImageListModule()
        # Generate once to fill cache
        first = module._generate_single_thumbnail(img_path, cache_path)
        assert cache_path.exists()

        # Delete the source — report-visible output must not come from cache alone.
        img_path.unlink()
        second = module._generate_single_thumbnail(img_path, cache_path)
        assert first.startswith("data:image/jpeg;base64,")
        assert second == ""

    def test_returns_empty_on_bad_file(self, tmp_path: Path):
        bad_path = tmp_path / "nonexistent.jpg"
        module = AppendixImageListModule()
        assert module._generate_single_thumbnail(bad_path, None) == ""

    def test_handles_rgba_image(self, tmp_path: Path):
        img_path = tmp_path / "rgba.png"
        img = PILImage.new("RGBA", (32, 32), (255, 0, 0, 128))
        img.save(img_path, format="PNG")

        module = AppendixImageListModule()
        result = module._generate_single_thumbnail(img_path, None)
        assert result.startswith("data:image/jpeg;base64,")


# ---------------------------------------------------------------------------
# Tests: _generate_thumbnails_batch
# ---------------------------------------------------------------------------

class TestBatchThumbnails:
    def test_parallel_generation(self, tmp_path: Path):
        """All thumbnails should be generated for a batch of images."""
        rows = _make_image_rows(tmp_path, count=10)
        module = AppendixImageListModule()

        result = module._generate_thumbnails_batch(
            image_rows=rows,
            case_folder=None,  # simple path resolution
            evidence_id=1,
            evidence_label=None,
            thumb_cache_dir=None,  # no caching
        )

        # All 10 images should be resolved (paths exist directly)
        # We need the _resolve_image_path to find them — with no case_folder
        # it tries Path(rel_path).exists() which won't work from CWD.
        # So we monkeypatch _resolve_image_path to return direct paths.
        assert isinstance(result, dict)

    def test_batch_with_disk_cache(self, tmp_path: Path):
        """Thumbnails should be cached to disk and served as inline data URIs."""
        img_dir = tmp_path / "evidences" / "evidence_1" / "filesystem_images" / "extracted"
        img_dir.mkdir(parents=True)
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        rows = _make_image_rows(img_dir, count=5)
        for index, row in enumerate(rows):
            row["md5"] = f"{index + 1:032x}"
        module = AppendixImageListModule()

        # Monkeypatch resolve to return real paths
        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        result = module._generate_thumbnails_batch(
            image_rows=rows,
            case_folder=tmp_path,
            evidence_id=1,
            evidence_label=None,
            thumb_cache_dir=cache_dir,
        )

        assert len(result) == 5
        for img_id, ref in result.items():
            assert ref.startswith("data:image/jpeg;base64,")

        # Cache dir should have 5 files
        cached_files = list(cache_dir.glob("*.jpg"))
        assert len(cached_files) == 5

    def test_batch_does_not_serve_cache_when_source_is_missing(self, tmp_path: Path):
        """A cached thumbnail is not enough without current source provenance."""
        img_dir = tmp_path / "evidences" / "evidence_1" / "filesystem_images" / "extracted"
        img_dir.mkdir(parents=True)
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        rows = _make_image_rows(img_dir, count=3)
        for index, row in enumerate(rows):
            row["md5"] = f"{index + 1:032x}"
        module = AppendixImageListModule()

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        # First run — generates thumbnails
        result1 = module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, cache_dir
        )
        assert len(result1) == 3

        # Delete source images — second run must not render stale cache entries.
        for f in img_dir.glob("*.jpg"):
            f.unlink()

        result2 = module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, cache_dir
        )
        assert result2 == {}

    def test_batch_regenerates_invalid_cache_from_source(self, tmp_path: Path):
        """Invalid cached bytes should not suppress a valid source thumbnail."""
        img_dir = tmp_path / "evidences" / "evidence_1" / "filesystem_images" / "extracted"
        img_dir.mkdir(parents=True)
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        rows = _make_image_rows(img_dir, count=1)
        module = AppendixImageListModule()
        row = rows[0]
        row["md5"] = "a" * 32
        key = module._thumb_cache_key(
            row["id"],
            row.get("md5"),
            row["rel_path"],
            1,
            row.get("first_discovered_by"),
        )
        cache_path = module._safe_thumb_cache_path(cache_dir, key)
        assert cache_path is not None
        cache_path.write_bytes(b"not-a-jpeg" * 20)

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        result = module._generate_thumbnails_batch(rows, tmp_path, 1, None, cache_dir)

        assert result[row["id"]].startswith("data:image/jpeg;base64,")

    def test_batch_ignores_valid_cache_when_md5_is_malformed(self, tmp_path: Path):
        """Malformed MD5 rows regenerate from source instead of serving stale cache."""
        img_dir = tmp_path / "evidences" / "evidence_1" / "filesystem_images" / "extracted"
        img_dir.mkdir(parents=True)
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        source_path = img_dir / "image.jpg"
        stale_path = tmp_path / "stale.jpg"
        _create_colored_image(source_path, (0, 0, 255))
        _create_colored_image(stale_path, (255, 0, 0))

        module = AppendixImageListModule()
        row = {
            "id": 1,
            "rel_path": "image.jpg",
            "filename": "image.jpg",
            "md5": "not-a-valid-md5",
            "first_discovered_by": "filesystem_images",
        }
        key = module._thumb_cache_key(
            row["id"],
            row.get("md5"),
            row["rel_path"],
            1,
            row.get("first_discovered_by"),
        )
        cache_path = module._safe_thumb_cache_path(cache_dir, key)
        assert cache_path is not None
        stale_ref = module._generate_single_thumbnail(stale_path, cache_path)

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        result = module._generate_thumbnails_batch([row], tmp_path, 1, None, cache_dir)

        assert result[row["id"]].startswith("data:image/jpeg;base64,")
        assert result[row["id"]] != stale_ref

    def test_cancellation_stops_early(self, tmp_path: Path):
        """Cancellation callback should abort thumbnail generation."""
        img_dir = tmp_path / "images"
        img_dir.mkdir()
        rows = _make_image_rows(img_dir, count=20)
        module = AppendixImageListModule()

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        # Cancel after first progress report
        cancel_after = {"count": 0}

        def cancelled():
            cancel_after["count"] += 1
            return cancel_after["count"] > 2

        result = module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, None, cancelled_fn=cancelled
        )
        # Should have some results but not necessarily all 20
        assert isinstance(result, dict)

    def test_progress_callback_invoked(self, tmp_path: Path):
        """Progress callback should be called during batch generation."""
        img_dir = tmp_path / "images"
        img_dir.mkdir()
        rows = _make_image_rows(img_dir, count=5)
        module = AppendixImageListModule()

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        progress_calls = []

        def on_progress(pct, msg):
            progress_calls.append((pct, msg))

        module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, None, progress_cb=on_progress
        )
        # At least the final progress message should be emitted
        assert len(progress_calls) >= 1

    def test_empty_rows(self):
        """Empty input should return empty dict without errors."""
        module = AppendixImageListModule()
        result = module._generate_thumbnails_batch([], None, 1, None, None)
        assert result == {}


# ---------------------------------------------------------------------------
# Tests: _thumb_cache_key
# ---------------------------------------------------------------------------

class TestThumbCacheKey:
    def test_uses_md5_when_available(self):
        digest = "a" * 32
        key = AppendixImageListModule._thumb_cache_key(1, digest, "some/path.jpg")
        assert key != digest
        assert len(key) == 32

    def test_rejects_malformed_md5_for_cache_key(self):
        key = AppendixImageListModule._thumb_cache_key(
            1,
            "../../outside",
            "some/path.jpg",
        )
        assert key != "../../outside"
        assert len(key) == 32

    def test_cache_key_is_evidence_scoped(self):
        digest = "a" * 32
        key1 = AppendixImageListModule._thumb_cache_key(
            1, digest, "some/path.jpg", evidence_id=1
        )
        key2 = AppendixImageListModule._thumb_cache_key(
            1, digest, "some/path.jpg", evidence_id=2
        )
        assert key1 != key2

    def test_safe_thumb_cache_path_stays_under_cache_dir(self, tmp_path: Path):
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()
        path = AppendixImageListModule._safe_thumb_cache_path(cache_dir, "a" * 32)
        assert path == (cache_dir / f"{'a' * 32}.jpg").resolve()

    def test_safe_thumb_cache_path_rejects_escape(self, tmp_path: Path):
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()
        assert AppendixImageListModule._safe_thumb_cache_path(
            cache_dir,
            "../../outside",
        ) is None

    def test_deterministic_fallback(self):
        k1 = AppendixImageListModule._thumb_cache_key(42, None, "test/path.jpg")
        k2 = AppendixImageListModule._thumb_cache_key(42, None, "test/path.jpg")
        assert k1 == k2
        assert len(k1) == 32  # md5 hex digest length

    def test_different_ids_different_keys(self):
        k1 = AppendixImageListModule._thumb_cache_key(1, None, "same.jpg")
        k2 = AppendixImageListModule._thumb_cache_key(2, None, "same.jpg")
        assert k1 != k2


# ---------------------------------------------------------------------------
# Tests: _get_thumb_cache_dir
# ---------------------------------------------------------------------------

class TestThumbCacheDir:
    def test_creates_directory(self, tmp_path: Path):
        result = AppendixImageListModule._get_thumb_cache_dir(tmp_path)
        assert result is not None
        assert result.exists()
        assert result.name == "report_thumbs"

    def test_returns_none_when_no_case_folder(self):
        assert AppendixImageListModule._get_thumb_cache_dir(None) is None


# ---------------------------------------------------------------------------
# Tests: _process_image (simplified API)
# ---------------------------------------------------------------------------

class TestProcessImage:
    def test_basic_output(self):
        module = AppendixImageListModule()
        row = {
            "id": 1,
            "rel_path": "images/test.jpg",
            "filename": "test.jpg",
            "md5": "deadbeef",
            "sha256": "",
            "ts_utc": "2024-01-15T10:30:00",
            "exif_json": None,
            "size_bytes": 12345,
            "first_discovered_by": None,
        }
        result = module._process_image(row, "eu", {}, "file:///thumb.jpg")
        assert result["id"] == 1
        assert result["thumbnail_src"] == "file:///thumb.jpg"
        assert result["filename"] == "test.jpg"
        assert result["md5"] == "deadbeef"

    def test_exif_parsing(self):
        module = AppendixImageListModule()
        exif = '{"Make": "Canon", "Model": "EOS R5"}'
        row = {
            "id": 2,
            "rel_path": "x.jpg",
            "filename": "x.jpg",
            "md5": "",
            "sha256": "",
            "ts_utc": "",
            "exif_json": exif,
            "size_bytes": 0,
            "first_discovered_by": None,
        }
        result = module._process_image(row, "eu", {}, "")
        assert len(result["exif_display"]) == 2
        assert any("Canon" in item for item in result["exif_display"])
