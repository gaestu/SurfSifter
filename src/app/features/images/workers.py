"""
Images feature workers.

Provides QThread workers for background image operations.

Extracted from features/images/tab.py
ClusterLoadWorker now uses clustering module instead of CaseDataAccess.cluster_images()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from PySide6.QtCore import QThread, Signal

from app.features.images.clustering import cluster_images

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess

logger = logging.getLogger(__name__)


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
