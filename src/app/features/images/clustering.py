"""Image clustering algorithm using perceptual hashes.

This module provides pHash-based image clustering for the Images tab:
- Greedy clustering algorithm with Hamming distance threshold
- Deterministic ordering for reproducible results

Extracted from case_data.py for separation of algorithmic logic from data access.
"""
from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

from core.phash import hamming_distance

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


def cluster_images(
    case_data: "CaseDataAccess",
    evidence_id: int,
    threshold: int = 10,
) -> List[Dict[str, Any]]:
    """
    Group images into clusters based on perceptual hash similarity.

    Uses a greedy clustering algorithm:
    1. Fetch all images with phash from database
    2. Sort by phash for deterministic order
    3. For each unassigned image, create a cluster with it as representative
    4. Add all unassigned images within threshold Hamming distance

    Args:
        case_data: CaseDataAccess instance for database queries
        evidence_id: Evidence ID
        threshold: Maximum Hamming distance to include in cluster (default 10)

    Returns:
        List of cluster dicts with keys:
        - cluster_id: Sequential cluster number (1-indexed)
        - count: Number of images in cluster
        - representative: The first image in the cluster
        - members: List of all images with hamming_distance field
        - has_browser_source: True if any member came from browser cache

    Use v_image_sources view for first_discovered_by.
    Extracted to separate module from case_data.py.
    """
    # 1. Fetch all images with phash
    sql = """
        SELECT i.id, i.rel_path, i.filename, i.md5, i.sha256, i.phash,
               v.first_discovered_by as discovered_by, i.ts_utc, i.notes,
               COALESCE(v.has_browser_source, 0) AS has_browser_source,
               v.browser_sources
        FROM images i
        LEFT JOIN v_image_sources v ON v.image_id = i.id
        WHERE i.evidence_id = ? AND i.phash IS NOT NULL AND i.phash != ''
    """
    with case_data._use_evidence_conn(evidence_id):
        with case_data._connect() as conn:
            cursor = conn.execute(sql, (evidence_id,))
            images = [dict(row) for row in cursor.fetchall()]

    if not images:
        return []

    # 2. Greedy clustering
    clusters: List[Dict[str, Any]] = []
    # Sort by phash to have deterministic order
    images.sort(key=lambda x: x['phash'])

    assigned_ids = set()

    for img in images:
        if img['id'] in assigned_ids:
            continue

        # Start new cluster with this image as representative
        cluster = {
            "cluster_id": len(clusters) + 1,
            "representative": img,
            "members": [],
            "count": 0
        }

        # Add representative to members (distance 0)
        img_with_dist = img.copy()
        img_with_dist['hamming_distance'] = 0
        cluster['members'].append(img_with_dist)
        assigned_ids.add(img['id'])

        # Find other members
        rep_phash = img['phash']

        for candidate in images:
            if candidate['id'] in assigned_ids:
                continue

            dist = hamming_distance(rep_phash, candidate['phash'])
            if dist <= threshold:
                cand_with_dist = candidate.copy()
                cand_with_dist['hamming_distance'] = dist
                cluster['members'].append(cand_with_dist)
                assigned_ids.add(candidate['id'])

        cluster['count'] = len(cluster['members'])
        cluster['has_browser_source'] = any(
            member.get("has_browser_source") for member in cluster["members"]
        )
        clusters.append(cluster)

    # Sort clusters by size (descending)
    clusters.sort(key=lambda c: c['count'], reverse=True)

    return clusters
