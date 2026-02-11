"""
Screenshots Tab Model

Qt model for displaying screenshots in a table view.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QSize
from PySide6.QtGui import QIcon, QPixmap

from core.database.manager import slugify_label

logger = logging.getLogger(__name__)


class ScreenshotsTableModel(QAbstractTableModel):
    """
    Table model for screenshots with checkbox support.

    Columns:
    - Checkbox (selection)
    - Thumbnail (preview)
    - Title
    - Caption
    - Sequence
    - Source
    - Date
    """

    COLUMNS = [
        ("", "checkbox"),           # 0: Checkbox
        ("Preview", "thumbnail"),   # 1: Thumbnail
        ("Title", "title"),         # 2: Title
        ("Caption", "caption"),     # 3: Caption
        ("Sequence", "sequence_name"),  # 4: Sequence
        ("Source", "source"),       # 5: Source
        ("Captured", "captured_at_utc"),  # 6: Date
    ]

    THUMB_SIZE = QSize(60, 60)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data: List[Dict[str, Any]] = []
        self._checked: set = set()  # Set of checked row indices
        self._thumbnail_cache: Dict[str, QIcon] = {}  # path -> icon cache
        self._case_folder: Optional[Path] = None
        self._evidence_label: Optional[str] = None
        self._evidence_id: Optional[int] = None

    def set_paths(self, case_folder: Path, evidence_label: str, evidence_id: int) -> None:
        """Set paths needed for thumbnail loading."""
        self._case_folder = case_folder
        self._evidence_label = evidence_label
        self._evidence_id = evidence_id
        self._thumbnail_cache.clear()  # Clear cache when paths change

    def load_data(self, screenshots: List[Dict[str, Any]]) -> None:
        """Load screenshot data into the model."""
        self.beginResetModel()
        self._data = list(screenshots)
        self._checked.clear()
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._data)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.COLUMNS):
                return self.COLUMNS[section][0]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._data):
            return None

        record = self._data[row]
        col_key = self.COLUMNS[col][1]

        if role == Qt.CheckStateRole and col == 0:
            return Qt.Checked if row in self._checked else Qt.Unchecked

        if role == Qt.DecorationRole and col == 1:
            # Thumbnail column - return icon
            return self._get_thumbnail(record)

        if role == Qt.DisplayRole:
            if col == 0:  # Checkbox column - no text
                return None
            elif col == 1:  # Thumbnail - icon via DecorationRole
                return None
            elif col_key == "caption":
                # Truncate caption for display
                caption = record.get("caption") or ""
                return caption[:80] + "..." if len(caption) > 80 else caption
            elif col_key == "captured_at_utc":
                # Format date
                ts = record.get("captured_at_utc") or ""
                if ts:
                    return ts[:19].replace("T", " ")  # YYYY-MM-DD HH:MM:SS
                return ""
            else:
                return record.get(col_key) or ""

        if role == Qt.UserRole:
            return record

        if role == Qt.ToolTipRole:
            if col_key == "caption":
                return record.get("caption")
            elif col_key == "title":
                return record.get("captured_url")

        return None

    def _get_thumbnail(self, record: Dict[str, Any]) -> Optional[QIcon]:
        """Load or retrieve cached thumbnail for a screenshot."""
        if not self._case_folder or not self._evidence_label or not self._evidence_id:
            return None

        dest_path = record.get("dest_path")
        if not dest_path:
            return None

        # Check cache first
        if dest_path in self._thumbnail_cache:
            return self._thumbnail_cache[dest_path]

        try:
            slug = slugify_label(self._evidence_label, self._evidence_id)
            image_path = self._case_folder / "evidences" / slug / dest_path

            if not image_path.exists():
                logger.debug("Screenshot file not found: %s", image_path)
                return None

            pixmap = QPixmap(str(image_path))
            if pixmap.isNull():
                logger.debug("Could not load screenshot: %s", image_path)
                return None

            # Scale to thumbnail size
            scaled = pixmap.scaled(
                self.THUMB_SIZE,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )

            icon = QIcon(scaled)
            self._thumbnail_cache[dest_path] = icon
            return icon

        except Exception as e:
            logger.debug("Error loading thumbnail: %s", e)
            return None

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole) -> bool:
        if not index.isValid():
            return False

        row = index.row()
        col = index.column()

        if role == Qt.CheckStateRole and col == 0:
            if value == Qt.Checked:
                self._checked.add(row)
            else:
                self._checked.discard(row)
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        return False

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags

        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable

        if index.column() == 0:  # Checkbox column
            flags |= Qt.ItemIsUserCheckable

        return flags

    def get_screenshot(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        """Get screenshot data for given index."""
        if not index.isValid():
            return None
        row = index.row()
        if 0 <= row < len(self._data):
            return self._data[row]
        return None

    def get_checked_screenshots(self) -> List[Dict[str, Any]]:
        """Get all checked screenshots."""
        return [self._data[i] for i in sorted(self._checked) if i < len(self._data)]

    def get_checked_count(self) -> int:
        """Get count of checked screenshots."""
        return len(self._checked)

    def select_all(self) -> None:
        """Check all screenshots."""
        self._checked = set(range(len(self._data)))
        if self._data:
            self.dataChanged.emit(
                self.index(0, 0),
                self.index(len(self._data) - 1, 0),
                [Qt.CheckStateRole]
            )

    def deselect_all(self) -> None:
        """Uncheck all screenshots."""
        self._checked.clear()
        if self._data:
            self.dataChanged.emit(
                self.index(0, 0),
                self.index(len(self._data) - 1, 0),
                [Qt.CheckStateRole]
            )
