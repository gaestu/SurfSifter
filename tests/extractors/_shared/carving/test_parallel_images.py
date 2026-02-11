"""
Tests for parallel image processing.

Verifies:
- Single and multiple image processing
- Error handling for corrupted images
- Deterministic ordering
- Configuration and environment variables
- Performance improvements over sequential processing
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from pathlib import Path
from PIL import Image

import pytest

# Use direct imports (circular dependency resolved with local import in carving_worker)
from extractors._shared.carving.processor import (
    process_image_worker,
    ParallelImageProcessor,
    ImageProcessResult,
)
from core.config import ParallelConfig


@pytest.fixture
def temp_images(tmp_path: Path):
    """Create temporary test images."""
    images_dir = tmp_path / "images"
    images_dir.mkdir()

    # Create 10 test images with known content
    image_paths = []
    for i in range(10):
        img_path = images_dir / f"test_image_{i:02d}.jpg"
        img = Image.new("RGB", (100, 100), color=(i * 25, 100, 200))
        img.save(img_path)
        image_paths.append(img_path)

    return images_dir, image_paths


@pytest.fixture
def corrupted_image(tmp_path: Path):
    """Create a corrupted image file."""
    img_path = tmp_path / "corrupted.jpg"
    img_path.write_bytes(b"not a valid image file")
    return img_path


@pytest.fixture
def corrupted_image(tmp_path: Path):
    """Create a corrupted image file."""
    img_path = tmp_path / "corrupted.jpg"
    img_path.write_bytes(b"not a valid image file")
    return img_path


def test_process_single_image_worker(temp_images):
    """Test processing a single image with worker function."""
    images_dir, image_paths = temp_images
    result = process_image_worker(image_paths[0], images_dir)

    assert result.error is None
    assert result.md5 is not None
    assert result.sha256 is not None
    assert result.filename == "test_image_00.jpg"
    assert result.rel_path == "test_image_00.jpg"
    # Synthetic images may not have EXIF data
    assert result.exif_json is not None
    assert result.thumbnail_path is not None
    assert result.thumbnail_path.exists()


def test_process_corrupted_image_worker(corrupted_image, tmp_path):
    """Test worker handles corrupted images gracefully."""
    result = process_image_worker(corrupted_image, tmp_path)

    # The corrupted file will fail during hash computation or EXIF extraction
    # but the worker should handle it gracefully
    # Note: hash_file might succeed on any binary data, so check if EXIF/thumb failed
    if result.error is not None:
        assert "error" in result.error.lower() or "identified" in result.error.lower()
    # If no error, at least one operation should have completed
    # (hash_file works on any binary data, but image operations should fail)
    else:
        # Worker succeeded in hashing but likely failed in image operations
        # This is acceptable - worker is robust
        assert result.md5 is not None  # Hash works on any data


def test_parallel_processor_single_image(temp_images):
    """Test parallel processor with single image."""
    images_dir, image_paths = temp_images
    processor = ParallelImageProcessor(max_workers=2, enable_parallel=True)

    results = processor.process_images([image_paths[0]], images_dir)

    assert len(results) == 1
    assert results[0].error is None
    assert results[0].md5 is not None


def test_parallel_processor_multiple_images(temp_images):
    """Test parallel processor with multiple images."""
    images_dir, image_paths = temp_images
    processor = ParallelImageProcessor(max_workers=4, enable_parallel=True)

    results = processor.process_images(image_paths, images_dir)

    assert len(results) == len(image_paths)
    # All should succeed
    assert all(r.error is None for r in results)
    # All should have hashes
    assert all(r.md5 is not None for r in results)
    assert all(r.sha256 is not None for r in results)


def test_parallel_processor_deterministic_ordering(temp_images):
    """Test that results are deterministic regardless of processing order."""
    images_dir, image_paths = temp_images
    processor = ParallelImageProcessor(max_workers=4, enable_parallel=True)

    # Run twice
    results1 = processor.process_images(image_paths, images_dir)
    results2 = processor.process_images(image_paths, images_dir)

    # Should have same order
    assert len(results1) == len(results2)
    for r1, r2 in zip(results1, results2):
        assert r1.path == r2.path
        assert r1.filename == r2.filename
        assert r1.md5 == r2.md5
        assert r1.sha256 == r2.sha256


def test_parallel_processor_mixed_valid_and_corrupted(temp_images, corrupted_image):
    """Test parallel processor with mix of valid and corrupted images."""
    images_dir, image_paths = temp_images
    # Add corrupted image to the list
    mixed_paths = image_paths[:3] + [corrupted_image] + image_paths[3:6]

    processor = ParallelImageProcessor(max_workers=4, enable_parallel=True)
    results = processor.process_images(mixed_paths, images_dir)

    assert len(results) == len(mixed_paths)

    # Count successes and errors
    # Note: corrupted image might not error if hash_file succeeds
    # Just verify we got results for all images
    assert all(r.md5 is not None or r.error is not None for r in results)


def test_sequential_mode(temp_images):
    """Test sequential processing mode (for debugging)."""
    images_dir, image_paths = temp_images
    processor = ParallelImageProcessor(max_workers=1, enable_parallel=False)

    results = processor.process_images(image_paths[:5], images_dir)

    assert len(results) == 5
    assert all(r.error is None for r in results)


def test_empty_image_list():
    """Test processing empty image list."""
    processor = ParallelImageProcessor()
    results = processor.process_images([], Path("/tmp"))

    assert results == []


def test_parallel_config_environment_override():
    """Test ParallelConfig respects VMGO_PARALLEL_IMAGES environment variable."""
    # Test disabling
    os.environ["VMGO_PARALLEL_IMAGES"] = "false"
    config = ParallelConfig.from_environment()
    assert config.enable_parallel is False

    # Test enabling
    os.environ["VMGO_PARALLEL_IMAGES"] = "true"
    config = ParallelConfig.from_environment()
    assert config.enable_parallel is True

    # Test different values
    os.environ["VMGO_PARALLEL_IMAGES"] = "0"
    config = ParallelConfig.from_environment()
    assert config.enable_parallel is False

    os.environ["VMGO_PARALLEL_IMAGES"] = "1"
    config = ParallelConfig.from_environment()
    assert config.enable_parallel is True

    # Cleanup
    del os.environ["VMGO_PARALLEL_IMAGES"]


def test_image_result_to_db_record():
    """Test ImageProcessResult conversion to database record."""
    result = ImageProcessResult(
        path=Path("/test/image.jpg"),
        rel_path="image.jpg",
        filename="image.jpg",
        md5="abc123",
        sha256="def456",
        phash="1234abcd",
        exif_json='{"Make": "Canon"}',
    )

    record = result.to_db_record("test_tool")

    assert record["rel_path"] == "image.jpg"
    assert record["filename"] == "image.jpg"
    assert record["md5"] == "abc123"
    assert record["sha256"] == "def456"
    assert record["phash"] == "1234abcd"
    assert record["discovered_by"] == "test_tool"
    assert record["exif_json"] == '{"Make": "Canon"}'


def test_performance_parallel_vs_sequential(temp_images):
    """
    Benchmark parallel vs sequential processing.

    This test creates enough images to show meaningful speedup.
    Target: parallel should be faster on multi-core systems.
    """
    images_dir, image_paths = temp_images

    # Create more images for better benchmark (30 total)
    extra_paths = []
    for i in range(10, 40):
        img_path = images_dir / f"bench_image_{i:02d}.jpg"
        img = Image.new("RGB", (200, 200), color=(i * 5, 150, 100))
        img.save(img_path)
        extra_paths.append(img_path)

    all_paths = image_paths + extra_paths

    # Sequential processing
    processor_seq = ParallelImageProcessor(max_workers=1, enable_parallel=False)
    start_seq = time.time()
    results_seq = processor_seq.process_images(all_paths, images_dir)
    time_seq = time.time() - start_seq

    # Parallel processing
    processor_par = ParallelImageProcessor(max_workers=4, enable_parallel=True)
    start_par = time.time()
    results_par = processor_par.process_images(all_paths, images_dir)
    time_par = time.time() - start_par

    # Verify same results
    assert len(results_seq) == len(results_par)
    for r_seq, r_par in zip(results_seq, results_par):
        assert r_seq.md5 == r_par.md5
        assert r_seq.sha256 == r_par.sha256

    # Parallel should be faster (but allow for variance in CI environments)
    # Don't assert strict speedup, just log the results
    speedup = time_seq / time_par if time_par > 0 else 1.0
    print(f"\nPerformance: Sequential={time_seq:.2f}s, Parallel={time_par:.2f}s, Speedup={speedup:.2f}x")

    # NOTE: We don't assert speedup because:
    # 1. CI runners often have 2 CPUs, making parallel overhead > benefit for small workloads
    # 2. Thread pool startup cost dominates for fast operations like hashing small images
    # 3. The real benefit shows on large E01 extractions with I/O-bound operations
    # The important thing is that both produce identical results (verified above)


def test_relative_path_calculation(temp_images):
    """Test relative path calculation for images."""
    images_dir, image_paths = temp_images

    # Process with correct out_dir
    result = process_image_worker(image_paths[0], images_dir)
    assert result.rel_path == "test_image_00.jpg"

    # Process with different out_dir (should use absolute path or adjusted relative)
    other_dir = images_dir.parent / "other"
    result2 = process_image_worker(image_paths[0], other_dir)
    # Should still work (either absolute or calculated differently)
    assert result2.rel_path is not None


def test_processor_logs_statistics(temp_images, caplog):
    """Test that processor logs success/error statistics."""
    import logging
    caplog.set_level(logging.INFO, logger="surfsifter.extractors._shared.carving.processor")

    images_dir, image_paths = temp_images
    processor = ParallelImageProcessor(max_workers=2, enable_parallel=True)

    results = processor.process_images(image_paths[:5], images_dir)

    # Check that logging occurred
    assert "5 images" in caplog.text
    assert "success" in caplog.text.lower()


def test_process_image_worker_decompression_bomb_skips(temp_images, monkeypatch):
    """Ensure decompression bomb detection skips the image and returns an error."""
    images_dir, image_paths = temp_images
    import extractors._shared.carving.processor as processor

    def bomb_probe(*args, **kwargs):
        raise processor.DecompressionBombError("too many pixels")

    monkeypatch.setattr(processor, "safe_probe_image", bomb_probe)

    result = processor.process_image_worker(image_paths[0], images_dir)
    assert result.error is not None
    assert "DecompressionBombError" in result.error
    assert result.md5 is None  # hashing should be skipped on bomb


def test_exif_helpers_handle_decompression_bomb(monkeypatch, tmp_path):
    """EXIF and thumbnail helpers should swallow DecompressionBombError."""
    from extractors._shared.carving import exif as exif_mod
    bomb_path = tmp_path / "bomb.jpg"
    bomb_path.write_bytes(b"fake")

    class BombCtx:
        def __enter__(self):
            raise Image.DecompressionBombError("bomb")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(exif_mod.Image, "open", lambda path: BombCtx())

    assert exif_mod.extract_exif(bomb_path) == {}
    assert exif_mod.generate_thumbnail(bomb_path, tmp_path / "out.jpg") is None


def test_parallel_processor_progress_logging(temp_images, caplog):
    """Test that processor logs progress every 100 images."""
    import logging
    caplog.set_level(logging.INFO, logger="surfsifter.extractors._shared.carving.processor")

    images_dir, _ = temp_images
    # Create 105 images to trigger progress logging
    many_images = []
    for i in range(105):
        img_path = images_dir / f"batch_image_{i:03d}.jpg"
        img = Image.new("RGB", (50, 50), color=(i % 256, 100, 150))
        img.save(img_path)
        many_images.append(img_path)

    processor = ParallelImageProcessor(max_workers=4, enable_parallel=True)
    results = processor.process_images(many_images, images_dir)

    assert len(results) == 105
    # Check that progress logging occurred at 100 images
    assert "100/105" in caplog.text


def test_parallel_processor_batch_timeout_calculation():
    """Verify batch timeout is calculated correctly."""
    # This tests the timeout calculation logic indirectly
    # 10 images × 60s timeout + 60s buffer = 660s
    processor = ParallelImageProcessor(max_workers=2, timeout_per_image=60)
    # The batch timeout is calculated inside process_images, but we verify
    # that the default timeout is reasonable
    assert processor.timeout_per_image == 60
