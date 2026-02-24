"""
Images list model for grid/list view.

Phase 1: Pure visual thumbnails (no text labels in grid).
Phase 2: Checkbox support for tagging workflow.
Phase 3: Size filtering support.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt
from PySide6.QtGui import QIcon

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess

from app.services.thumbnailer import ensure_thumbnail


class ImagesListModel(QAbstractListModel):
    """
    Model for displaying images in a grid/list view.

    Phase 1: Pure visual thumbnails (no text labels in grid).
    Phase 2: Checkbox support for tagging workflow.
    Phase 3: Size filtering support.
    """

    # Signal emitted when checkbox state changes (image_id, is_checked)
    checkStateChanged = None  # Will be connected by parent widget

    def __init__(
        self,
        case_data: Optional[CaseDataAccess] = None,
        *,
        case_folder: Optional[Path] = None,
        page_size: int = 200,
        thumb_size: int = 160,
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
        # Loading flag to prevent thumbnail generation during model reset
        self._loading = False
        # Phase 2: Reference to shared checked IDs set (owned by ImagesTab)
        self._checked_ids: Optional[set] = None
        self._check_callback = None  # Callback when check state changes

    def set_checked_ids(self, checked_ids: set) -> None:
        """Set reference to external checked IDs set (owned by ImagesTab)."""
        self._checked_ids = checked_ids

    def set_check_callback(self, callback) -> None:
        """Set callback for check state changes: callback(image_id, is_checked)."""
        self._check_callback = callback

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        row = self._rows[index.row()]
        image_id = row.get("id")

        # Phase 1: Don't show filename in grid (pure visual)
        if role == Qt.DisplayRole:
            return ""  # Empty string = no text under thumbnail

        if role == Qt.DecorationRole and image_id is not None:
            # Skip thumbnail generation while loading to prevent hangs
            if self._loading:
                return None
            icon = self._thumb_cache.get(image_id)
            if icon:
                return icon
            thumb = self._ensure_thumbnail(row)
            if thumb is not None:
                icon = QIcon(str(thumb))
                self._thumb_cache[int(image_id)] = icon
                return icon
            # Return placeholder icon when thumbnail unavailable
            return self._get_placeholder_icon()

        # Phase 2: Checkbox state
        if role == Qt.CheckStateRole and self._checked_ids is not None:
            if image_id is not None and image_id in self._checked_ids:
                return Qt.Checked
            return Qt.Unchecked

        if role == Qt.UserRole:
            return row

        # Tooltip shows filename, sources, and original path
        if role == Qt.ToolTipRole:
            return self._build_tooltip(row)

        return None

    def _build_tooltip(self, row: Dict[str, Any]) -> str:
        """Build rich tooltip with multi-source provenance info."""
        lines = [row.get("filename", "")]

        # Show source count and list
        source_count = row.get("source_count", 1) or 1
        sources = row.get("sources", "") or row.get("discovered_by", "")
        if source_count > 1:
            lines.append(f"🔗 {source_count} sources: {sources}")
        elif sources:
            lines.append(f"Source: {sources}")

        # Show original filesystem path if available
        fs_path = row.get("fs_path")
        if fs_path:
            lines.append(f"📁 {fs_path}")

        # Show browser badge if applicable
        if row.get("has_browser_source"):
            browser_sources = row.get("browser_sources", "")
            lines.append(f"🌐 Browser: {browser_sources}")

        return "\n".join(lines)

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:  # noqa: N802
        """Handle checkbox state changes."""
        if role == Qt.CheckStateRole and self._checked_ids is not None:
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

            # Notify parent widget
            if self._check_callback:
                self._check_callback(image_id, is_checked)

            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        return False

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        # Note: We don't set ItemIsUserCheckable because the custom delegate
        # (ImageThumbnailDelegate) draws and handles checkboxes. Setting this
        # flag would cause Qt to draw a duplicate checkbox.
        return super().flags(index)

    def set_case_data(self, case_data: Optional[CaseDataAccess], *, case_folder: Optional[Path] = None) -> None:
        self.case_data = case_data
        if case_folder is not None:
            self.case_folder = case_folder
        self.page = 0
        self.evidence_id = None
        self._thumb_cache.clear()
        self.reload()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load images for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        self.evidence_id = evidence_id
        self.page = 0
        self._thumb_cache.clear()
        if reload:
            self.reload()

    def set_filters(
        self,
        *,
        tags: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        extension: Optional[str] = None,
        hash_match: Optional[str] = None,
        url_text: Optional[str] = None,
        min_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
    ) -> None:
        """
        Update filters for image listing.

        Args:
            tags: Tag name filter (SQL LIKE pattern)
            sources: List of discovered_by source filters
            extension: File extension filter (e.g., 'jpg', 'gif')
            hash_match: Hash list name filter (only show images matching this list)
            url_text: Case-insensitive URL substring filter
            min_size_bytes: Minimum file size in bytes (Phase 3)
            max_size_bytes: Maximum file size in bytes (Phase 3)
        """
        if tags is not None:
            self._filters["tags"] = f"%{tags}%" if tags else "%"
        if sources is not None:
            self._filters["sources"] = tuple(sources) if sources else None
        # Extension filter
        if extension is not None:
            self._filters["extension"] = extension if extension else None
        # Hash match filter
        if hash_match is not None:
            self._filters["hash_match"] = hash_match if hash_match else None
        # URL text filter
        if url_text is not None:
            self._filters["url_text"] = url_text if url_text else None
        # Phase 3: Size filtering
        # Always update size filters when explicitly passed (even if None)
        # Use special sentinel _UNSET to detect when parameter was not passed at all
        self._filters["min_size_bytes"] = min_size_bytes
        self._filters["max_size_bytes"] = max_size_bytes
        self.page = 0
        self.reload()

    def set_thumbnail_size(self, size: int) -> None:
        if size == self.thumb_size:
            return
        self.thumb_size = size
        self._thumb_cache.clear()
        if self._rows:
            top = self.index(0, 0)
            bottom = self.index(len(self._rows) - 1, 0)
            self.dataChanged.emit(top, bottom, [Qt.DecorationRole])

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
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

    def current_page(self) -> int:
        return self.page

    def get_row(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        return self._rows[index.row()]

    def _ensure_thumbnail(self, row: Dict[str, Any]) -> Optional[Path]:
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

    def _get_placeholder_icon(self) -> QIcon:
        """
        Get placeholder icon for images without thumbnails.

        Returns a styled placeholder icon when thumbnail is unavailable.
        Uses lazy initialization and caching for efficiency.
        """
        # Cache placeholder at class level to avoid recreating
        if not hasattr(ImagesListModel, "_placeholder_icon"):
            from PySide6.QtGui import QPixmap, QPainter, QColor, QFont
            from PySide6.QtCore import Qt

            size = 160
            pixmap = QPixmap(size, size)
            pixmap.fill(QColor(240, 240, 240))  # Light gray background

            painter = QPainter(pixmap)
            painter.setRenderHint(QPainter.Antialiasing, True)

            # Draw "no image" icon (broken image indicator)
            painter.setPen(QColor(180, 180, 180))
            painter.setBrush(QColor(220, 220, 220))

            # Draw a simple "no image" rectangle with X
            margin = 30
            inner_rect = pixmap.rect().adjusted(margin, margin, -margin, -margin)
            painter.drawRect(inner_rect)

            # Draw diagonal lines (X) to indicate missing image
            painter.setPen(QColor(160, 160, 160))
            painter.drawLine(inner_rect.topLeft(), inner_rect.bottomRight())
            painter.drawLine(inner_rect.topRight(), inner_rect.bottomLeft())

            # Draw text label
            painter.setPen(QColor(120, 120, 120))
            font = QFont()
            font.setPixelSize(11)
            painter.setFont(font)
            painter.drawText(
                pixmap.rect().adjusted(0, size - 25, 0, 0),
                Qt.AlignHCenter | Qt.AlignTop,
                "No preview"
            )

            painter.end()
            ImagesListModel._placeholder_icon = QIcon(pixmap)

        return ImagesListModel._placeholder_icon
