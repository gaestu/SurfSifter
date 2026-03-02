"""
Images table model for sortable table view.

Provides sortable, paginated image listing with checkbox support.
Added sorting and table view support.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QIcon

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess

from app.services.thumbnailer import ensure_thumbnail


class ImagesTableModel(QAbstractTableModel):
    """
    Table model for displaying images with sortable columns.

    Columns: Thumbnail, Filename, Size, Sources, Original Path, Timestamp, MD5
    Supports checkbox selection for batch operations and sorting.
    """

    HEADERS = [
        "",  # Thumbnail column (empty header)
        "Filename",
        "Size",
        "Sources",
        "Original Path",
        "Timestamp",
        "MD5",
    ]

    # Column indices for sorting
    COL_THUMB = 0
    COL_FILENAME = 1
    COL_SIZE = 2
    COL_SOURCES = 3
    COL_PATH = 4
    COL_TIMESTAMP = 5
    COL_MD5 = 6

    def __init__(
        self,
        case_data: Optional[CaseDataAccess] = None,
        *,
        case_folder: Optional[Path] = None,
        page_size: int = 200,
        thumb_size: int = 32,
    ) -> None:
        super().__init__()
        self.case_data = case_data
        self.case_folder = case_folder
        self.page_size = page_size
        self.thumb_size = thumb_size
        self.evidence_id: Optional[int] = None
        self.page = 0
        self._rows: List[Dict[str, Any]] = []
        self._thumb_cache: Dict[int, QIcon] = {}
        self._filters: Dict[str, Any] = {
            "tags": "%",
            "sources": None,
            "extension": None,
            "hash_match": None,
            "url_text": None,
            "min_size_bytes": None,
            "max_size_bytes": None,
        }
        # Sorting state
        self._sort_column: int = self.COL_FILENAME
        self._sort_order: Qt.SortOrder = Qt.AscendingOrder
        # Loading flag to prevent thumbnail generation during model reset
        self._loading = False
        # Checkbox support
        self._checked_ids: Optional[set] = None
        self._check_callback = None

    def set_checked_ids(self, checked_ids: set) -> None:
        """Set reference to external checked IDs set."""
        self._checked_ids = checked_ids

    def set_check_callback(self, callback) -> None:
        """Set callback for check state changes."""
        self._check_callback = callback

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row = self._rows[index.row()]
        col = index.column()

        # Thumbnail column (col 0)
        if col == self.COL_THUMB:
            if role == Qt.DecorationRole:
                # Skip thumbnail generation while loading to prevent hangs
                if self._loading:
                    return None
                image_id = row.get("id")
                if image_id is not None:
                    icon = self._thumb_cache.get(image_id)
                    if icon:
                        return icon
                    thumb = self._ensure_thumbnail(row)
                    if thumb is not None:
                        icon = QIcon(str(thumb))
                        self._thumb_cache[int(image_id)] = icon
                        return icon
            # Checkbox on thumbnail column
            if role == Qt.CheckStateRole and self._checked_ids is not None:
                image_id = row.get("id")
                if image_id is not None and image_id in self._checked_ids:
                    return Qt.Checked
                return Qt.Unchecked
            return None

        if role == Qt.DisplayRole:
            if col == self.COL_FILENAME:
                return row.get("filename", "")
            elif col == self.COL_SIZE:
                size_bytes = row.get("size_bytes")
                if size_bytes is None:
                    return "—"
                if size_bytes < 1024:
                    return f"{size_bytes} B"
                elif size_bytes < 1024 * 1024:
                    return f"{size_bytes / 1024:.1f} KB"
                else:
                    return f"{size_bytes / (1024 * 1024):.1f} MB"
            elif col == self.COL_SOURCES:
                sources = row.get("sources", "") or row.get("discovered_by", "")
                source_count = row.get("source_count", 1) or 1
                if source_count > 1:
                    return f"{sources} ({source_count})"
                return sources
            elif col == self.COL_PATH:
                return row.get("fs_path", "") or "—"
            elif col == self.COL_TIMESTAMP:
                return row.get("ts_utc", "") or "—"
            elif col == self.COL_MD5:
                md5 = row.get("md5", "")
                return md5[:16] + "..." if md5 and len(md5) > 16 else md5 or "—"

        # Tooltip for full values
        if role == Qt.ToolTipRole:
            if col == self.COL_PATH:
                return row.get("fs_path", "")
            elif col == self.COL_MD5:
                return row.get("md5", "")
            elif col == self.COL_SOURCES:
                sources = row.get("sources", "")
                fs_path = row.get("fs_path", "")
                lines = [f"Sources: {sources}"]
                if fs_path:
                    lines.append(f"Path: {fs_path}")
                return "\n".join(lines)
            elif col == self.COL_FILENAME:
                return row.get("filename", "")

        if role == Qt.UserRole:
            return row

        return None

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:
        """Handle checkbox state changes."""
        if role == Qt.CheckStateRole and index.column() == self.COL_THUMB and self._checked_ids is not None:
            if not index.isValid() or not (0 <= index.row() < len(self._rows)):
                return False

            row = self._rows[index.row()]
            image_id = row.get("id")
            if image_id is None:
                return False

            is_checked = value == Qt.Checked
            if is_checked:
                self._checked_ids.add(image_id)
            else:
                self._checked_ids.discard(image_id)

            if self._check_callback:
                self._check_callback(image_id, is_checked)

            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        return False

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        flags = super().flags(index)
        if index.column() == self.COL_THUMB and self._checked_ids is not None:
            flags |= Qt.ItemIsUserCheckable
        return flags

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        """Sort the table by the given column."""
        if column == self.COL_THUMB:
            return  # Can't sort by thumbnail

        self._sort_column = column
        self._sort_order = order

        # Define sort key based on column
        def get_sort_key(row: Dict[str, Any]) -> Any:
            if column == self.COL_FILENAME:
                return (row.get("filename") or "").lower()
            elif column == self.COL_SIZE:
                return row.get("size_bytes") or 0
            elif column == self.COL_SOURCES:
                return row.get("source_count") or 0
            elif column == self.COL_PATH:
                return (row.get("fs_path") or "").lower()
            elif column == self.COL_TIMESTAMP:
                return row.get("ts_utc") or ""
            elif column == self.COL_MD5:
                return row.get("md5") or ""
            return ""

        self.layoutAboutToBeChanged.emit()
        self._rows.sort(key=get_sort_key, reverse=(order == Qt.DescendingOrder))
        self.layoutChanged.emit()

    def set_case_data(self, case_data: Optional[CaseDataAccess], *, case_folder: Optional[Path] = None) -> None:
        self.case_data = case_data
        if case_folder is not None:
            self.case_folder = case_folder
        self.page = 0
        self.evidence_id = None
        self._thumb_cache.clear()
        self.reload()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        self.evidence_id = evidence_id
        self.page = 0
        self._thumb_cache.clear()
        if reload:
            self.reload()

    def set_filters(
        self,
        *,
        tags: Optional[str] = None,
        sources: Optional[Tuple[str, ...]] = None,
        extension: Optional[str] = None,
        hash_match: Optional[str] = None,
        url_text: Optional[str] = None,
        min_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
    ) -> None:
        if tags is not None:
            self._filters["tags"] = f"%{tags}%" if tags else "%"
        if sources is not None:
            self._filters["sources"] = tuple(sources) if sources else None
        if extension is not None:
            self._filters["extension"] = extension if extension else None
        if hash_match is not None:
            self._filters["hash_match"] = hash_match if hash_match else None
        if url_text is not None:
            self._filters["url_text"] = url_text if url_text else None
        if min_size_bytes is not None or max_size_bytes is not None:
            self._filters["min_size_bytes"] = min_size_bytes
            self._filters["max_size_bytes"] = max_size_bytes
        self.page = 0
        self._thumb_cache.clear()
        self.reload()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
            self._thumb_cache.clear()
            self.endResetModel()
            return

        # Set loading flag to prevent thumbnail generation during reset
        self._loading = True
        self._thumb_cache.clear()
        self.beginResetModel()
        self._rows = self.case_data.iter_images(
            int(self.evidence_id),
            tag_like=self._filters["tags"],
            discovered_by=self._filters["sources"],
            extension=self._filters.get("extension"),
            hash_match=self._filters.get("hash_match"),
            url_text=self._filters.get("url_text"),
            min_size_bytes=self._filters.get("min_size_bytes"),
            max_size_bytes=self._filters.get("max_size_bytes"),
            limit=self.page_size,
            offset=self.page * self.page_size,
        )
        self.endResetModel()
        self._loading = False

    def page_up(self) -> None:
        if self.page == 0:
            return
        self.page -= 1
        self.reload()

    def page_down(self) -> None:
        if len(self._rows) < self.page_size:
            return
        self.page += 1
        self.reload()

    def get_row(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        return self._rows[index.row()]

    def _ensure_thumbnail(self, row: Dict[str, Any]) -> Optional[Path]:
        """Generate or retrieve thumbnail for image row."""
        if not self.case_data:
            return None
        cache_base = self.case_folder or self.case_data.case_folder
        if cache_base is None:
            return None
        rel_path = row.get("rel_path")
        if not rel_path:
            return None
        # Support both aliased discovered_by and raw first_discovered_by
        discovered_by = row.get("discovered_by") or row.get("first_discovered_by")
        # Pass evidence_id and discovered_by for proper path resolution
        image_path = self.case_data.resolve_image_path(
            rel_path,
            evidence_id=self.evidence_id,
            discovered_by=discovered_by,
        )
        if not image_path.exists():
            return None
        cache_dir = cache_base / ".thumbs"
        return ensure_thumbnail(image_path, cache_dir, size=(self.thumb_size, self.thumb_size))
