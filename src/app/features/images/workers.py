"""
Images feature workers.

Provides QThread workers for background image operations.

Extracted from features/images/tab.py
ClusterLoadWorker now uses clustering module instead of CaseDataAccess.cluster_images()
"""

from __future__ import annotations

import logging
from typing import List, TYPE_CHECKING

from PySide6.QtCore import QThread, Signal

from app.features.images.clustering import cluster_images

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess

logger = logging.getLogger(__name__)


class HashCheckWorker(QThread):
    """Worker thread for checking images against hash lists."""

    progress = Signal(int, int)  # current, total
    finished = Signal(dict)  # results: {list_name: match_count}
    error = Signal(str)  # error message

    def __init__(self, db_manager, evidence_id: int, selected_hashlists: List[str]):
        super().__init__()
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.selected_hashlists = selected_hashlists

    def run(self):
        """Run hash checking in background thread."""
        import sqlite3
        from datetime import datetime, timezone
        from core.database import insert_hash_matches
        from core.matching import ReferenceListManager

        try:
            with sqlite3.connect(self.db_manager.case_db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT label FROM evidences WHERE id = ?",
                    (self.evidence_id,),
                ).fetchone()
                label = row["label"] if row and row["label"] else f"EV-{self.evidence_id:03d}"

            evidence_conn = self.db_manager.get_evidence_conn(self.evidence_id, label=label)
            ref_manager = ReferenceListManager()

            cursor = evidence_conn.execute(
                """SELECT id, md5, sha256 FROM images
                WHERE evidence_id = ? AND (md5 IS NOT NULL OR sha256 IS NOT NULL)""",
                (self.evidence_id,),
            )
            images = cursor.fetchall()
            total_images = len(images)

            results = {}
            matched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for list_idx, hashlist_name in enumerate(self.selected_hashlists):
                try:
                    hashes = ref_manager.load_hashlist(hashlist_name)
                    if not hashes:
                        results[hashlist_name] = 0
                        continue

                    matches = []
                    for img_idx, img_row in enumerate(images):
                        image_id, md5, sha256 = img_row

                        matched = False
                        matched_hash = None
                        if md5 and md5.lower() in hashes:
                            matched = True
                            matched_hash = md5.lower()
                        elif sha256 and sha256.lower() in hashes:
                            matched = True
                            matched_hash = sha256.lower()

                        if matched:
                            matches.append({
                                "image_id": image_id,
                                "db_name": hashlist_name,
                                "db_md5": matched_hash,
                                "matched_at_utc": matched_at,
                                "list_name": hashlist_name,
                                "list_version": None,
                                "note": None,
                                "hash_sha256": sha256,
                            })

                        if (img_idx + 1) % 50 == 0:
                            overall_progress = (list_idx * total_images) + img_idx + 1
                            overall_total = len(self.selected_hashlists) * total_images
                            self.progress.emit(overall_progress, overall_total)

                    if matches:
                        insert_hash_matches(evidence_conn, self.evidence_id, matches)

                    results[hashlist_name] = len(matches)

                except FileNotFoundError:
                    results[hashlist_name] = 0
                except Exception as e:
                    results[hashlist_name] = f"Error: {e}"

            evidence_conn.commit()
            evidence_conn.close()

            self.finished.emit(results)

        except Exception as e:
            self.error.emit(str(e))


class ClusterLoadWorker(QThread):
    """Background worker for loading image clusters.

    Uses clustering.cluster_images() instead of CaseDataAccess.cluster_images().
    """

    finished = Signal(list)
    error = Signal(str)

    def __init__(self, case_data: "CaseDataAccess", evidence_id: int, threshold: int = 10):
        super().__init__()
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.threshold = threshold

    def run(self):
        try:
            clusters = cluster_images(self.case_data, int(self.evidence_id), threshold=self.threshold)
            self.finished.emit(clusters)
        except Exception as e:
            logger.warning(f"ClusterLoadWorker error: {e}")
            self.error.emit(str(e))


class ImageFilterLoadWorker(QThread):
    """Background worker for loading image filter dropdown data."""

    finished = Signal(dict)
    MAX_EXTENSIONS = 100
    MAX_SOURCES = 50

    def __init__(self, case_data: "CaseDataAccess", evidence_id: int):
        super().__init__()
        self.case_data = case_data
        self.evidence_id = evidence_id

    def run(self):
        try:
            result = {
                "sources": [],
                "extensions": [],
                "extension_count": 0,
                "extensions_truncated": False,
                "hash_matches": [],
                "tags": [],
                "total_images": 0,
            }

            if hasattr(self.case_data, "list_image_sources_counts"):
                sources = self.case_data.list_image_sources_counts(int(self.evidence_id))
                result["sources"] = sources[:self.MAX_SOURCES]
                result["total_images"] = sum(count or 0 for _, count in sources)
            else:
                sources = [(src, None) for src in self.case_data.list_image_sources(int(self.evidence_id))]
                result["sources"] = sources[:self.MAX_SOURCES]

            if hasattr(self.case_data, "list_image_extensions_counts"):
                extensions = self.case_data.list_image_extensions_counts(int(self.evidence_id))
                result["extension_count"] = len(extensions)
                result["extensions"] = extensions[:self.MAX_EXTENSIONS]
                result["extensions_truncated"] = len(extensions) > self.MAX_EXTENSIONS

            if hasattr(self.case_data, "list_hash_match_lists"):
                result["hash_matches"] = self.case_data.list_hash_match_lists(int(self.evidence_id))

            tags = self.case_data.list_tags(self.evidence_id)
            result["tags"] = tags

            self.finished.emit(result)

        except Exception as e:
            logger.warning(f"ImageFilterLoadWorker error: {e}")
            self.finished.emit({
                "sources": [],
                "extensions": [],
                "extension_count": 0,
                "extensions_truncated": False,
                "hash_matches": [],
                "tags": [],
                "total_images": 0,
            })
