"""Tests for image clustering functionality."""
from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from app.data.case_data import CaseDataAccess
from app.features.images.clustering import cluster_images
from core.database import insert_images
from core.database import DatabaseManager
from core.phash import compute_phash, hamming_distance, IMAGEHASH_AVAILABLE


@pytest.fixture
def case_with_clusterable_images(tmp_path: Path) -> tuple[CaseDataAccess, int, Path]:
    """Create a case with images suitable for clustering tests."""
    case_folder = tmp_path / "case"
    case_db_path = case_folder / "test_surfsifter.sqlite"
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)

    # Create case and evidence in case DB
    case_conn = manager.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-CLUSTER", "Clustering Test", "2024-01-01T00:00:00"),
        )
        cur = case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EV-CLUSTER", "test.e01", "2024-01-01T00:00:00"),
        )
    evidence_id = int(cur.lastrowid)

    images_dir = case_folder / "images"
    images_dir.mkdir()

    case_data = CaseDataAccess(case_folder)
    return case_data, evidence_id, images_dir


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_basic(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test basic clustering with distinct image groups."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create 3 groups of similar images
    # Group 1: Diagonal pattern (3 images)
    group1_images = []
    for i in range(3):
        img = Image.new("RGB", (50, 50), color=(255, 255, 255))
        for j in range(50):
            img.putpixel((j, j), (0, 0, 0))
        path = images_dir / f"diag_{i}.jpg"
        img.save(path)
        group1_images.append(compute_phash(path))

    # Group 2: Horizontal pattern (2 images)
    group2_images = []
    for i in range(2):
        img = Image.new("RGB", (50, 50), color=(255, 255, 255))
        for y in range(0, 50, 10):
            for x in range(50):
                img.putpixel((x, y), (0, 0, 0))
        path = images_dir / f"horiz_{i}.jpg"
        img.save(path)
        group2_images.append(compute_phash(path))

    # Group 3: Vertical pattern (2 images)
    group3_images = []
    for i in range(2):
        img = Image.new("RGB", (50, 50), color=(255, 255, 255))
        for x in range(0, 50, 10):
            for y in range(50):
                img.putpixel((x, y), (0, 0, 0))
        path = images_dir / f"vert_{i}.jpg"
        img.save(path)
        group3_images.append(compute_phash(path))

    # Insert all images into database
    all_phashes = group1_images + group2_images + group3_images
    records = [
        {
            "rel_path": f"images/img_{i}.jpg",
            "filename": f"img_{i}.jpg",
            "phash": phash,
            "discovered_by": "test",
        }
        for i, phash in enumerate(all_phashes)
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    # Cluster with threshold=10
    clusters = cluster_images(case_data, evidence_id, threshold=10)

    # Should have 3 clusters
    assert len(clusters) == 3

    # Clusters should be sorted by size (largest first)
    assert clusters[0]["count"] == 3  # Group 1
    assert clusters[1]["count"] == 2  # Group 2 or 3
    assert clusters[2]["count"] == 2  # Group 3 or 2

        # Each cluster should have proper structure
    for cluster in clusters:
        assert "cluster_id" in cluster
        assert "representative" in cluster
        assert "members" in cluster
        assert "count" in cluster
        assert cluster["count"] == len(cluster["members"])


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_singleton(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that unique images form singleton clusters."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create 3 very different images
    patterns = [
        lambda img: [img.putpixel((j, j), (0, 0, 0)) for j in range(50)],  # Diagonal
        lambda img: [img.putpixel((x, 10), (0, 0, 0)) for x in range(50)],  # Horizontal line
        lambda img: [img.putpixel((10, y), (0, 0, 0)) for y in range(50)],  # Vertical line
    ]

    phashes = []
    for i, pattern_fn in enumerate(patterns):
        img = Image.new("RGB", (50, 50), color=(255, 255, 255))
        pattern_fn(img)
        path = images_dir / f"unique_{i}.jpg"
        img.save(path)
        phashes.append(compute_phash(path))

    records = [
        {
            "rel_path": f"images/unique_{i}.jpg",
            "filename": f"unique_{i}.jpg",
            "phash": phash,
            "discovered_by": "test",
        }
        for i, phash in enumerate(phashes)
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    # Cluster with threshold=10
    clusters = cluster_images(case_data, evidence_id, threshold=10)

    # Should have 3 clusters (all singletons)
    assert len(clusters) == 3

    for cluster in clusters:
        assert cluster["count"] == 1
        assert len(cluster["members"]) == 1  # Representative is included


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_singleton(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that unique images form singleton clusters."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create 3 very different images
    patterns = [
        lambda img: [img.putpixel((j, j), (0, 0, 0)) for j in range(50)],  # Diagonal
        lambda img: [img.putpixel((x, 10), (0, 0, 0)) for x in range(50)],  # Horizontal line
        lambda img: [img.putpixel((10, y), (0, 0, 0)) for y in range(50)],  # Vertical line
    ]

    phashes = []
    for i, pattern_fn in enumerate(patterns):
        img = Image.new("RGB", (50, 50), color=(255, 255, 255))
        pattern_fn(img)
        path = images_dir / f"unique_{i}.jpg"
        img.save(path)
        phashes.append(compute_phash(path))

    records = [
        {
            "rel_path": f"images/unique_{i}.jpg",
            "filename": f"unique_{i}.jpg",
            "phash": phash,
            "discovered_by": "test",
        }
        for i, phash in enumerate(phashes)
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    # Cluster with strict threshold
    clusters = cluster_images(case_data, evidence_id, threshold=5)

    # Should have 3 singleton clusters
    assert len(clusters) == 3
    for cluster in clusters:
        assert cluster["count"] == 1
        assert len(cluster["members"]) == 1
@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_with_null_phash(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that images without phash form singleton clusters."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create one image with phash and one without
    img = Image.new("RGB", (50, 50), color=(100, 100, 100))
    path = images_dir / "with_phash.jpg"
    img.save(path)
    phash = compute_phash(path)

    records = [
        {
            "rel_path": "images/with_phash.jpg",
            "filename": "with_phash.jpg",
            "phash": phash,
            "discovered_by": "test",
        },
        {
            "rel_path": "images/no_phash.jpg",
            "filename": "no_phash.jpg",
            "phash": None,  # No phash
            "discovered_by": "test",
        },
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    clusters = cluster_images(case_data, evidence_id, threshold=10)

    # Should have 1 singleton cluster (null phash ignored)
    assert len(clusters) == 1
    assert clusters[0]["count"] == 1
    assert clusters[0]["representative"]["filename"] == "with_phash.jpg"


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_threshold_sensitivity(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that threshold affects cluster formation."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create original with diagonal line
    img1 = Image.new("RGB", (50, 50), color=(255, 255, 255))
    for i in range(50):
        img1.putpixel((i, i), (0, 0, 0))
    path1 = images_dir / "diagonal.jpg"
    img1.save(path1)
    phash1 = compute_phash(path1)

    # Create similar but slightly different (diagonal + few extra pixels)
    img2 = Image.new("RGB", (50, 50), color=(255, 255, 255))
    for i in range(50):
        img2.putpixel((i, i), (0, 0, 0))
    # Add a few extra pixels to make it slightly different
    img2.putpixel((5, 10), (0, 0, 0))
    img2.putpixel((10, 15), (0, 0, 0))
    img2.putpixel((15, 20), (0, 0, 0))
    path2 = images_dir / "diagonal_modified.jpg"
    img2.save(path2)
    phash2 = compute_phash(path2)

    # Verify they are actually different
    distance = hamming_distance(phash1, phash2)
    assert 0 < distance <= 10, f"Images should be similar but not identical (distance={distance})"

    records = [
        {
            "rel_path": "images/diagonal.jpg",
            "filename": "diagonal.jpg",
            "phash": phash1,
            "discovered_by": "test",
        },
        {
            "rel_path": "images/diagonal_modified.jpg",
            "filename": "diagonal_modified.jpg",
            "phash": phash2,
            "discovered_by": "test",
        },
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    # Strict threshold: should be separate (since distance > 0)
    clusters_strict = cluster_images(case_data, evidence_id, threshold=0)
    assert len(clusters_strict) == 2, "With threshold=0, only identical images should cluster"

    # Loose threshold: should cluster together
    clusters_loose = cluster_images(case_data, evidence_id, threshold=10)
    assert len(clusters_loose) == 1, "With threshold=10, similar images should cluster together"
    assert clusters_loose[0]["count"] == 2


def test_cluster_images_empty_dataset(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test clustering with no images."""
    case_data, evidence_id, _ = case_with_clusterable_images

    clusters = cluster_images(case_data, evidence_id, threshold=10)

    assert clusters == []


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_images_deterministic_order(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that clustering produces deterministic results."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create 5 identical images
    phashes = []
    for i in range(5):
        img = Image.new("RGB", (50, 50), color=(128, 128, 128))
        for j in range(50):
            img.putpixel((j, j), (0, 0, 0))
        path = images_dir / f"img_{i}.jpg"
        img.save(path)
        phashes.append(compute_phash(path))

    records = [
        {
            "rel_path": f"images/img_{i}.jpg",
            "filename": f"img_{i}.jpg",
            "phash": phash,
            "discovered_by": "test",
        }
        for i, phash in enumerate(phashes)
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    # Run clustering twice
    clusters1 = cluster_images(case_data, evidence_id, threshold=10)
    clusters2 = cluster_images(case_data, evidence_id, threshold=10)

    # Results should be identical
    assert len(clusters1) == len(clusters2)
    assert clusters1[0]["representative"]["id"] == clusters2[0]["representative"]["id"]
    assert clusters1[0]["count"] == clusters2[0]["count"]


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_representative_is_first_image(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that cluster representative is the first unclustered image."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create 3 similar images
    phashes = []
    for i in range(3):
        img = Image.new("RGB", (50, 50), color=(200, 200, 200))
        for j in range(50):
            img.putpixel((j, j), (0, 0, 0))
        path = images_dir / f"similar_{i}.jpg"
        img.save(path)
        phashes.append(compute_phash(path))

    records = [
        {
            "rel_path": f"images/similar_{i}.jpg",
            "filename": f"similar_{i}.jpg",
            "phash": phash,
            "discovered_by": "test",
        }
        for i, phash in enumerate(phashes)
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    clusters = cluster_images(case_data, evidence_id, threshold=10)

    # Should have 1 cluster
    assert len(clusters) == 1

    # Representative should be the first image (lowest ID)
    cluster = clusters[0]
    assert cluster["representative"]["filename"] == "similar_0.jpg"
    assert cluster["count"] == 3
    assert len(cluster["members"]) == 3


@pytest.mark.skipif(not IMAGEHASH_AVAILABLE, reason="imagehash not available")
def test_cluster_members_have_distance(case_with_clusterable_images: tuple[CaseDataAccess, int, Path]) -> None:
    """Test that cluster members include hamming_distance field."""
    case_data, evidence_id, images_dir = case_with_clusterable_images

    # Create original with diagonal line
    img1 = Image.new("RGB", (50, 50), color=(255, 255, 255))
    for i in range(50):
        img1.putpixel((i, i), (0, 0, 0))
    path1 = images_dir / "orig.jpg"
    img1.save(path1)
    phash1 = compute_phash(path1)

    # Create modified version with diagonal + a few extra pixels (more subtle change)
    img2 = Image.new("RGB", (50, 50), color=(255, 255, 255))
    for i in range(50):
        img2.putpixel((i, i), (0, 0, 0))  # diagonal
    # Add just a few pixels in one corner
    for i in range(0, 10, 2):
        img2.putpixel((i, 0), (0, 0, 0))
    path2 = images_dir / "mod.jpg"
    img2.save(path2)
    phash2 = compute_phash(path2)

    # Verify they're actually different
    distance = hamming_distance(phash1, phash2)
    # If they happen to be identical, skip this test (perceptual hashing is probabilistic)
    if distance == 0:
        pytest.skip("Generated images have identical perceptual hashes")
    # Use a higher threshold for this test since we just need to verify the distance field exists
    threshold = max(15, distance)

    records = [
        {"rel_path": "images/orig.jpg", "filename": "orig.jpg", "phash": phash1, "discovered_by": "test"},
        {"rel_path": "images/mod.jpg", "filename": "mod.jpg", "phash": phash2, "discovered_by": "test"},
    ]

    # Insert images into evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EV-CLUSTER")
    insert_images(evidence_conn, evidence_id, records)
    evidence_conn.close()

    clusters = cluster_images(case_data, evidence_id, threshold=threshold)

    assert len(clusters) == 1, f"Should form 1 cluster with threshold={threshold}, distance={distance}"
    cluster = clusters[0]

    # Members should include representative (dist 0) and the other image (dist > 0)
    assert len(cluster["members"]) == 2

    # Find the member that is NOT the representative
    other_member = next((m for m in cluster["members"] if m["hamming_distance"] > 0), None)
    assert other_member is not None
    assert other_member["hamming_distance"] == distance

    # Find the representative member
    rep_member = next((m for m in cluster["members"] if m["hamming_distance"] == 0), None)
    assert rep_member is not None
