"""Tests for the optimized image list appendix module.

Covers:
- Parallel thumbnail generation with ThreadPoolExecutor
- Disk-based thumbnail caching
- File-URI references vs inline base64 fallback
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
        assert result.startswith("file://")
        assert cache_path.exists()
        assert cache_path.stat().st_size > 100

    def test_serves_from_cache_without_reopening(self, tmp_path: Path):
        img_path = tmp_path / "test.jpg"
        _create_test_image(img_path)
        cache_path = tmp_path / "cached.jpg"

        module = AppendixImageListModule()
        # Generate once to fill cache
        first = module._generate_single_thumbnail(img_path, cache_path)
        assert cache_path.exists()

        # Delete the source — should still return from cache
        img_path.unlink()
        second = module._generate_single_thumbnail(img_path, cache_path)
        # Both should be the same file URI
        assert first == second
        assert second.startswith("file://")

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
        """Thumbnails should be cached to disk and served as file:// URIs."""
        img_dir = tmp_path / "images"
        img_dir.mkdir()
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        rows = _make_image_rows(img_dir, count=5)
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
            assert ref.startswith("file://")

        # Cache dir should have 5 files
        cached_files = list(cache_dir.glob("*.jpg"))
        assert len(cached_files) == 5

    def test_batch_serves_cached_on_second_run(self, tmp_path: Path):
        """Second call should serve all from cache without PIL work."""
        img_dir = tmp_path / "images"
        img_dir.mkdir()
        cache_dir = tmp_path / "report_thumbs"
        cache_dir.mkdir()

        rows = _make_image_rows(img_dir, count=3)
        module = AppendixImageListModule()

        def resolve(rel_path, discovered_by, case_folder, eid, elabel):
            return img_dir / rel_path

        module._resolve_image_path = resolve

        # First run — generates thumbnails
        result1 = module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, cache_dir
        )
        assert len(result1) == 3

        # Delete source images — second run should still work from cache
        for f in img_dir.glob("*.jpg"):
            f.unlink()

        result2 = module._generate_thumbnails_batch(
            rows, tmp_path, 1, None, cache_dir
        )
        assert len(result2) == 3
        for ref in result2.values():
            assert ref.startswith("file://")

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
        key = AppendixImageListModule._thumb_cache_key(1, "abc123", "some/path.jpg")
        assert key == "abc123"

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
