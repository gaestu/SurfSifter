"""
Image clusters model for perceptual hash clustering.

Displays clusters of similar images sorted by cluster size.
Phase 2: Checkbox support for batch tagging entire clusters.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt
from PySide6.QtGui import QIcon

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess

from app.features.images.clustering import cluster_images
from app.services.thumbnailer import ensure_thumbnail


class ImageClustersModel(QAbstractListModel):
    """
    Model for displaying image clusters sorted by size.

    Phase 2: Checkbox support - checking a cluster checks all member images.
    """

    def __init__(
        self,
        case_data: Optional[CaseDataAccess] = None,
        *,
        case_folder: Optional[Path] = None,
        thumb_size: int = 160,
        threshold: int = 10,
    ) -> None:
        super().__init__()
        self.case_data = case_data
        self.case_folder = case_folder
        self.thumb_size = thumb_size
        self.threshold = threshold
        self.evidence_id: Optional[int] = None
        self._clusters: List[Dict[str, Any]] = []
        self._thumb_cache: Dict[int, QIcon] = {}
        # Phase 2: Reference to shared checked IDs set (owned by ImagesTab)
        self._checked_ids: Optional[set] = None
        self._check_callback = None

    def set_checked_ids(self, checked_ids: set) -> None:
        """Set reference to external checked IDs set (owned by ImagesTab)."""
        self._checked_ids = checked_ids

    def set_check_callback(self, callback) -> None:
        """Set callback for check state changes."""
        self._check_callback = callback

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._clusters)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or not (0 <= index.row() < len(self._clusters)):
            return None
        cluster = self._clusters[index.row()]
        representative = cluster.get("representative", {})

        if role == Qt.DisplayRole:
            # Display cluster info: "Cluster N (M images)"
            cluster_id = cluster.get("cluster_id", index.row() + 1)
            count = cluster.get("count", 0)
            return f"Cluster {cluster_id} ({count} images)"

        if role == Qt.DecorationRole:
            # Show thumbnail of representative image
            image_id = representative.get("id")
            if image_id is not None:
                icon = self._thumb_cache.get(image_id)
                if icon:
                    return icon
                thumb = self._ensure_thumbnail(representative)
                if thumb is not None:
                    icon = QIcon(str(thumb))
                    self._thumb_cache[int(image_id)] = icon
                    return icon

        # Phase 2: Checkbox - checked if ALL cluster members are checked
        if role == Qt.CheckStateRole and self._checked_ids is not None:
            all_ids = self._get_cluster_image_ids(cluster)
            if all_ids and all(img_id in self._checked_ids for img_id in all_ids):
                return Qt.Checked
            elif all_ids and any(img_id in self._checked_ids for img_id in all_ids):
                return Qt.PartiallyChecked
            return Qt.Unchecked

        if role == Qt.UserRole:
            # Return full cluster data
            return cluster

        return None

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:  # noqa: N802
        """Handle checkbox state changes - check/uncheck all cluster members."""
        if role == Qt.CheckStateRole and self._checked_ids is not None:
            if not index.isValid() or not (0 <= index.row() < len(self._clusters)):
                return False

            cluster = self._clusters[index.row()]
            all_ids = self._get_cluster_image_ids(cluster)

            is_checked = value == Qt.Checked
            for img_id in all_ids:
                if is_checked:
                    self._checked_ids.add(img_id)
                else:
                    self._checked_ids.discard(img_id)

                # Notify parent widget for each
                if self._check_callback:
                    self._check_callback(img_id, is_checked)

            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        return False

    def _get_cluster_image_ids(self, cluster: Dict[str, Any]) -> List[int]:
        """Get all image IDs in a cluster (representative + members)."""
        ids = []
        representative = cluster.get("representative", {})
        if representative.get("id"):
            ids.append(representative["id"])
        for member in cluster.get("members", []):
            if member.get("id"):
                ids.append(member["id"])
        return ids

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        # Note: We don't set ItemIsUserCheckable because the custom delegate
        # (ImageThumbnailDelegate) draws and handles checkboxes. Setting this
        # flag would cause Qt to draw a duplicate checkbox.
        return super().flags(index)

    def set_case_data(self, case_data: Optional[CaseDataAccess], *, case_folder: Optional[Path] = None) -> None:
        self.case_data = case_data
        if case_folder is not None:
            self.case_folder = case_folder
        self.reload()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load clusters for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        self.evidence_id = evidence_id
        if reload:
            self.reload()

    def set_threshold(self, threshold: int) -> None:
        """Update clustering threshold and reload."""
        self.threshold = threshold
        self.reload()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._clusters = []
            self._thumb_cache.clear()
            self.endResetModel()
            return

        self.beginResetModel()
        self._clusters = cluster_images(self.case_data, int(self.evidence_id), threshold=self.threshold)
        self._thumb_cache.clear()  # Clear cache on reload
        self.endResetModel()

    def get_cluster(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        """Get cluster data at index."""
        if not index.isValid() or not (0 <= index.row() < len(self._clusters)):
            return None
        return self._clusters[index.row()]

    def export_to_csv(self, output_path: Path) -> None:
        """Export clusters to CSV with representative and all members."""
        if not self._clusters:
            return

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            # Header
            writer.writerow([
                "cluster_id",
                "cluster_size",
                "role",
                "filename",
                "rel_path",
                "phash",
                "hamming_distance",
                "md5",
                "sha256",
            ])

            # Write each cluster
            for cluster in self._clusters:
                cluster_id = cluster.get("cluster_id", 0)
                count = cluster.get("count", 0)
                representative = cluster.get("representative", {})
                members = cluster.get("members", [])

                # Write representative
                writer.writerow([
                    cluster_id,
                    count,
                    "representative",
                    representative.get("filename", ""),
                    representative.get("rel_path", ""),
                    representative.get("phash", ""),
                    0,  # distance to itself is 0
                    representative.get("md5", ""),
                    representative.get("sha256", ""),
                ])

                # Write members
                for member in members:
                    writer.writerow([
                        cluster_id,
                        count,
                        "member",
                        member.get("filename", ""),
                        member.get("rel_path", ""),
                        member.get("phash", ""),
                        member.get("hamming_distance", ""),
                        member.get("md5", ""),
                        member.get("sha256", ""),
                    ])

    def _ensure_thumbnail(self, row: Dict[str, Any]) -> Optional[Path]:
        """Generate thumbnail for image if needed."""
        if not self.case_data:
            return None
        cache_base = self.case_folder or self.case_data.case_folder
        if cache_base is None:
            return None
        rel_path = row.get("rel_path")
        if not rel_path:
            return None
        # Pass evidence_id and discovered_by for proper path resolution
        image_path = self.case_data.resolve_image_path(
            rel_path,
            evidence_id=self.evidence_id,
            discovered_by=row.get("discovered_by"),
        )
        if not image_path.exists():
            return None
        cache_dir = cache_base / ".thumbs"
        return ensure_thumbnail(image_path, cache_dir, size=(self.thumb_size, self.thumb_size))
