"""Tests for perceptual hashing functionality."""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image

from app.data.case_data import CaseDataAccess
from core.database import insert_images
from core.database import DatabaseManager
from core.phash import compute_phash, hamming_distance, IMAGEHASH_AVAILABLE


@pytest.fixture
def case_with_images(tmp_path: Path) -> tuple[CaseDataAccess, int, Path]:
    """Create a case with test images for similarity testing."""
    case_folder = tmp_path / "case"
    case_db_path = case_folder / "test_surfsifter.sqlite"
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)

    # Create case and evidence in case DB
    case_conn = manager.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-PHASH", "Perceptual Hash Test", "2024-01-01T00:00:00"),
        )
        cur = case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EV-PHASH", "test.e01", "2024-01-01T00:00:00"),
        )
    evidence_id = int(cur.lastrowid)

    # Create test images directory
    images_dir = case_folder / "images"
    images_dir.mkdir()

    case_data = CaseDataAccess(case_folder)
    return case_data, evidence_id, images_dir


def test_compute_phash_for_image(tmp_path: Path) -> None:
    """Test computing perceptual hash for a simple image."""
    img_path = tmp_path / "test.jpg"
    img = Image.new("RGB", (100, 100), color=(255, 0, 0))
    img.save(img_path)

    phash = compute_phash(img_path)

    if IMAGEHASH_AVAILABLE:
        assert phash is not None
        assert isinstance(phash, str)
        assert len(phash) == 16  # 64-bit hash as 16-char hex string
    else:
        assert phash is None


def test_compute_phash_from_stream(tmp_path: Path) -> None:
    """Test computing phash from binary stream."""
    img_path = tmp_path / "test.png"
    img = Image.new("RGB", (100, 100), color=(0, 255, 0))
    img.save(img_path)

    with open(img_path, "rb") as f:
        phash = compute_phash(f)

    if IMAGEHASH_AVAILABLE:
        assert phash is not None
        assert len(phash) == 16
    else:
        assert phash is None


def test_compute_phash_returns_none_for_invalid_image(tmp_path: Path) -> None:
    """Test that phash gracefully handles invalid images."""
    bad_file = tmp_path / "not_an_image.txt"
    bad_file.write_text("This is not an image")

    phash = compute_phash(bad_file)
    assert phash is None


def test_hamming_distance_identical_hashes() -> None:
    """Test that identical hashes have distance 0."""
    hash1 = "1234567890abcdef"
    hash2 = "1234567890abcdef"

    distance = hamming_distance(hash1, hash2)

    if IMAGEHASH_AVAILABLE:
        assert distance == 0
    else:
        assert distance == 64  # Max distance when unavailable


def test_hamming_distance_different_hashes() -> None:
    """Test that different hashes have non-zero distance."""
    hash1 = "0000000000000000"
    hash2 = "ffffffffffffffff"

    distance = hamming_distance(hash1, hash2)

    if IMAGEHASH_AVAILABLE:
        assert distance == 64  # All bits differ
    else:
        assert distance == 64


def test_hamming_distance_one_bit_diff() -> None:
    """Test single bit difference detection."""
    hash1 = "0000000000000000"
    hash2 = "0000000000000001"

    distance = hamming_distance(hash1, hash2)

    if IMAGEHASH_AVAILABLE:
        assert distance == 1
    else:
        assert distance == 64


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_similar_images_have_low_hamming_distance(tmp_path: Path) -> None:
    """Test that similar images (same content, different size) have low Hamming distance."""
    # Create original image
    orig_path = tmp_path / "original.jpg"
    img = Image.new("RGB", (100, 100), color=(128, 64, 192))
    img.save(orig_path)

    # Create resized version
    resized_path = tmp_path / "resized.jpg"
    img_resized = img.resize((50, 50))
    img_resized.save(resized_path)

    phash1 = compute_phash(orig_path)
    phash2 = compute_phash(resized_path)

    assert phash1 is not None
    assert phash2 is not None

    distance = hamming_distance(phash1, phash2)
    # Similar images should have low distance (typically 0-5 for resizing)
    assert distance <= 10


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_different_images_have_high_hamming_distance(tmp_path: Path) -> None:
    """Test that completely different images have high Hamming distance."""
    # Create image with horizontal pattern
    img1_path = tmp_path / "pattern1.jpg"
    img1 = Image.new("RGB", (100, 100), color=(255, 255, 255))
    for y in range(0, 100, 20):
        for x in range(100):
            img1.putpixel((x, y), (0, 0, 0))
    img1.save(img1_path)

    # Create image with vertical pattern
    img2_path = tmp_path / "pattern2.jpg"
    img2 = Image.new("RGB", (100, 100), color=(255, 255, 255))
    for x in range(0, 100, 20):
        for y in range(100):
            img2.putpixel((x, y), (0, 0, 0))
    img2.save(img2_path)

    phash1 = compute_phash(img1_path)
    phash2 = compute_phash(img2_path)

    assert phash1 is not None
    assert phash2 is not None

    distance = hamming_distance(phash1, phash2)
    # Different patterns should have higher distance
    assert distance > 5


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_find_similar_images_in_database(case_with_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test finding similar images using database query."""
    case_data, evidence_id, images_dir = case_with_images

    # Create reference image with pattern
    ref_path = images_dir / "reference.jpg"
    ref_img = Image.new("RGB", (100, 100), color=(255, 255, 255))
    # Add diagonal pattern
    for i in range(100):
        ref_img.putpixel((i, i), (0, 0, 0))
        if i < 99:
            ref_img.putpixel((i, i+1), (0, 0, 0))
    ref_img.save(ref_path)
    ref_phash = compute_phash(ref_path)
    assert ref_phash is not None

    # Create similar image (resized version of reference)
    similar_path = images_dir / "similar.jpg"
    similar_img = ref_img.resize((80, 80))
    similar_img.save(similar_path)
    similar_phash = compute_phash(similar_path)

    # Create different image with horizontal pattern
    diff_path = images_dir / "different.jpg"
    diff_img = Image.new("RGB", (100, 100), color=(255, 255, 255))
    for y in range(0, 100, 10):
        for x in range(100):
            diff_img.putpixel((x, y), (0, 0, 0))
    diff_img.save(diff_path)
    diff_phash = compute_phash(diff_path)

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-PHASH")
    insert_images(
        evidence_conn,
        evidence_id,
        [
            {
                "rel_path": "images/reference.jpg",
                "filename": "reference.jpg",
                "md5": "ref_md5",
                "sha256": "ref_sha256",
                "phash": ref_phash,
                "discovered_by": "test",
            },
            {
                "rel_path": "images/similar.jpg",
                "filename": "similar.jpg",
                "md5": "sim_md5",
                "sha256": "sim_sha256",
                "phash": similar_phash,
                "discovered_by": "test",
            },
            {
                "rel_path": "images/different.jpg",
                "filename": "different.jpg",
                "md5": "diff_md5",
                "sha256": "diff_sha256",
                "phash": diff_phash,
                "discovered_by": "test",
            },
        ],
    )
    evidence_conn.close()

    # Find similar images to reference
    results = case_data.find_similar_images(evidence_id, ref_phash, threshold=10)

    # Should find reference (distance=0) and similar (distance<=10)
    assert len(results) >= 2
    assert results[0]["filename"] == "reference.jpg"
    assert results[0]["hamming_distance"] == 0

    # Similar image should be in results
    similar_found = any(r["filename"] == "similar.jpg" for r in results)
    assert similar_found

    # Different image should NOT be in results (distance > 10)
    different_found = any(r["filename"] == "different.jpg" for r in results)
    assert not different_found


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_find_similar_with_no_matches(case_with_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that find_similar returns empty list when no similar images exist."""
    case_data, evidence_id, images_dir = case_with_images

    # Insert one image with diagonal pattern
    img_path = images_dir / "single.jpg"
    img = Image.new("RGB", (100, 100), color=(255, 255, 255))
    for i in range(100):
        img.putpixel((i, i), (0, 0, 0))
    img.save(img_path)
    phash = compute_phash(img_path)

    # Insert image into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-PHASH")
    insert_images(
        evidence_conn,
        evidence_id,
        [
            {
                "rel_path": "images/single.jpg",
                "filename": "single.jpg",
                "phash": phash,
                "discovered_by": "test",
            }
        ],
    )
    evidence_conn.close()

    # Search with completely different phash (horizontal pattern would be very different)
    fake_img = Image.new("RGB", (100, 100), color=(255, 255, 255))
    for y in range(0, 100, 5):
        for x in range(100):
            fake_img.putpixel((x, y), (0, 0, 0))
    fake_path = images_dir / "fake.jpg"
    fake_img.save(fake_path)
    fake_phash = compute_phash(fake_path)

    results = case_data.find_similar_images(evidence_id, fake_phash, threshold=5)

    # Should find nothing with strict threshold
    assert len(results) == 0


def test_find_similar_images_handles_null_phash(case_with_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that images without phash are gracefully skipped."""
    case_data, evidence_id, images_dir = case_with_images

    # Insert image without phash into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-PHASH")
    insert_images(
        evidence_conn,
        evidence_id,
        [
            {
                "rel_path": "images/no_phash.jpg",
                "filename": "no_phash.jpg",
                "phash": None,
                "discovered_by": "test",
            }
        ],
    )
    evidence_conn.close()

    # Search should not crash
    results = case_data.find_similar_images(evidence_id, "1234567890abcdef", threshold=10)
    assert results == []


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_results_sorted_by_distance(case_with_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that results are sorted by Hamming distance (closest first)."""
    case_data, evidence_id, images_dir = case_with_images

    # Create reference image
    ref_img = Image.new("RGB", (50, 50), color=(100, 150, 200))
    ref_path = images_dir / "ref.jpg"
    ref_img.save(ref_path)
    ref_phash = compute_phash(ref_path)

    # Create 3 images with varying similarity
    images_to_insert = []
    for i, color in enumerate([(100, 150, 200), (101, 151, 201), (50, 75, 100)]):
        img = Image.new("RGB", (50, 50), color=color)
        path = images_dir / f"img{i}.jpg"
        img.save(path)
        phash = compute_phash(path)
        images_to_insert.append(
            {
                "rel_path": f"images/img{i}.jpg",
                "filename": f"img{i}.jpg",
                "phash": phash,
                "discovered_by": "test",
            }
        )

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-PHASH")
    insert_images(evidence_conn, evidence_id, images_to_insert)
    evidence_conn.close()

    # Find similar images
    results = case_data.find_similar_images(evidence_id, ref_phash, threshold=20)

    # Verify results are sorted by distance
    for i in range(len(results) - 1):
        assert results[i]["hamming_distance"] <= results[i + 1]["hamming_distance"]
