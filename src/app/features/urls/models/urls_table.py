"""
URLs table model for flat URL listing.

Provides paginated, filterable URL listing with on-demand tag/match loading.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class UrlsTableModel(QAbstractTableModel):
    headers = [
        "URL",
        "Domain",
        "Scheme",
        "Source",
        "First Seen (UTC)",
        "Last Seen (UTC)",
        "Source Path",
        "Occurrences",  # Duplicate count after deduplication
        "Tags",
        "Match",  #
    ]

    # Performance optimization: Large page size like FileListModel
    PAGE_SIZE = 10000  # Load 10k rows at a time

    def __init__(self, case_data: Optional[CaseDataAccess] = None, page_size: Optional[int] = None) -> None:
        super().__init__()
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.page_size = page_size or self.PAGE_SIZE
        self.page = 0
        self._rows: List[Dict[str, Any]] = []
        self._total_count: int = 0  # Total URLs matching filters
        self._matches_cache: Dict[int, str] = {}  # url_id -> matched_lists string (on-demand loading)
        self._tags_cache: Dict[int, str] = {}  # url_id -> tags string (on-demand loading)
        self._filters: Dict[str, Any] = {
            "domain": "%",
            "url": "%",
            "sources": None,
            "artifact_type": None,  # NEW in
            "match_filter": None,  # "all", "matched", "unmatched", or specific list name
        }

    # Qt interface -------------------------------------------------------

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return len(self.headers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        row = self._rows[index.row()]
        column = index.column()
        if role in (Qt.DisplayRole, Qt.EditRole):
            if column == 0:
                return row.get("url") or ""
            if column == 1:
                return row.get("domain") or ""
            if column == 2:
                return row.get("scheme") or ""
            if column == 3:
                return row.get("discovered_by") or ""
            if column == 4:
                return row.get("first_seen_utc") or ""
            if column == 5:
                return row.get("last_seen_utc") or ""
            if column == 6:
                # Truncate long source_path lists in display
                source_path = row.get("source_path") or ""
                if source_path and ", " in source_path:
                    paths = source_path.split(", ")
                    if len(paths) > 3:
                        return f"{paths[0]}, {paths[1]}, {paths[2]}, … (+{len(paths) - 3} more)"
                return source_path
            if column == 7:  # Occurrences column
                count = row.get("occurrence_count") or 1
                return str(count) if count > 1 else ""
            if column == 8:  # Tags column (on-demand loading)
                return self._get_tags(row.get("id")) or ""
            if column == 9:  # Match column (on-demand loading)
                return self._get_matches(row.get("id")) or ""
        return None

    def _get_tags(self, url_id: Optional[int]) -> str:
        """
        Load tags for a URL on-demand (performance optimization ).

        Args:
            url_id: URL ID

        Returns:
            Comma-separated list of tag names, or empty string if no tags
        """
        if not url_id or not self.case_data or self.evidence_id is None:
            return ""

        if url_id not in self._tags_cache:
            # Fetch tags on first access
            tags_str = self.case_data.get_artifact_tags_str(int(self.evidence_id), 'url', url_id)
            self._tags_cache[url_id] = tags_str

        return self._tags_cache[url_id]

    def _get_matches(self, url_id: Optional[int]) -> str:
        """
        Load matches for a URL on-demand (performance optimization).

        Args:
            url_id: URL ID

        Returns:
            Comma-separated list of matched list names, or "—" if no matches
        """
        if not url_id or not self.case_data or self.evidence_id is None:
            return "—"

        if url_id not in self._matches_cache:
            # Fetch matches on first access
            matched_lists = self.case_data.get_url_matches(int(self.evidence_id), url_id)
            self._matches_cache[url_id] = matched_lists if matched_lists else "—"

        return self._matches_cache[url_id]

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        base = super().flags(index)
        if index.column() == 7:
            return base | Qt.ItemIsEditable
        return base

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:  # noqa: N802
        if role != Qt.EditRole or index.column() != 7 or not self.case_data or self.evidence_id is None:
            return False
        row = self._rows[index.row()]
        url_id = row.get("id")
        if not url_id:
            return False
        tags = str(value).strip()
        self.case_data.update_url_tags(int(self.evidence_id), int(url_id), tags)
        # Update tags cache instead of row dict (tags no longer in iter_urls result)
        self._tags_cache[url_id] = tags
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        return True

    # Data loading -------------------------------------------------------

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        self.page = 0
        self.evidence_id = None
        self._rows.clear()
        self.layoutChanged.emit()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load URLs for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        self.evidence_id = evidence_id
        self.page = 0
        if reload:
            self.reload()

    def set_filters(
        self,
        *,
        domain: Optional[str] = None,
        text: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,  #
        tag: Optional[str] = None,  #
        reload: bool = True,  # Allow setting filters without immediate reload
    ) -> None:
        """
        Update filters for URL listing.

        Args:
            domain: Domain filter pattern
            text: URL text filter pattern
            sources: List of source filters
            match_filter: Match filter ("all", "matched", "unmatched", or list name)
            tag: Tag filter pattern
            reload: If True (default), immediately reload data. Set to False when
                   using background DataLoadWorker.
        """
        if domain is not None:
            self._filters["domain"] = f"%{domain}%" if domain and domain != "*" else "%"
        if text is not None:
            self._filters["url"] = f"%{text}%" if text else "%"
        if sources is not None:
            self._filters["sources"] = tuple(sources) if sources else None
        if match_filter is not None:  #
            self._filters["match_filter"] = match_filter if match_filter and match_filter != "all" else None
        if tag is not None:  #
            self._filters["tag"] = tag if tag and tag != "*" else "%"
        self.page = 0
        self._matches_cache.clear()  # Clear cache when filters change
        self._tags_cache.clear()
        if reload:
            self.reload()

    def get_filters(self) -> Dict[str, Any]:
        """
        Get current filter settings (for background worker).

        Used by DataLoadWorker to load data with matching filters.
        """
        return dict(self._filters)

    def set_loaded_data(self, rows: List[Dict[str, Any]], total_count: int) -> None:
        """
        Set data loaded by background worker.

        Phase 2.1 - Allows DataLoadWorker to update model with loaded data.

        Args:
            rows: List of URL row dictionaries
            total_count: Total count of matching URLs
        """
        self.beginResetModel()
        self._rows = rows
        self._total_count = total_count
        self._matches_cache.clear()
        self._tags_cache.clear()
        self.endResetModel()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
            self._total_count = 0
            self._matches_cache.clear()
            self._tags_cache.clear()
            self.endResetModel()
            return

        self.beginResetModel()

        # Performance: Count first (fast query without GROUP_CONCAT)
        self._total_count = self.case_data.count_urls(
            int(self.evidence_id),
            domain_like=self._filters["domain"],
            url_like=self._filters["url"],
            tag_like=self._filters.get("tag", "%"),
            discovered_by=self._filters["sources"],
            match_filter=self._filters["match_filter"],
        )

        # Then load page data (matches loaded on-demand)
        self._rows = self.case_data.iter_urls(
            int(self.evidence_id),
            domain_like=self._filters["domain"],
            url_like=self._filters["url"],
            tag_like=self._filters.get("tag", "%"),
            discovered_by=self._filters["sources"],
            match_filter=self._filters["match_filter"],  #
            limit=self.page_size,
            offset=self.page * self.page_size,
        )

        # Clear matches and tags cache when reloading (new URLs may be visible)
        self._matches_cache.clear()
        self._tags_cache.clear()

        self.endResetModel()

    def total_count(self) -> int:
        """Get total count of URLs matching current filters."""
        return self._total_count

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

    def export_to_csv(self, output_path: Path) -> None:
        if not self.case_data or self.evidence_id is None:
            return
        rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            batch = self.case_data.iter_urls(
                int(self.evidence_id),
                domain_like=self._filters["domain"],
                url_like=self._filters["url"],
                tag_like=self._filters.get("tag", "%"),
                discovered_by=self._filters["sources"],
                match_filter=self._filters["match_filter"],  #
                limit=self.page_size,
                offset=offset,
            )
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < self.page_size:
                break
            offset += self.page_size

        rows_sorted = sorted(
            rows,
            key=lambda row: (
                row.get("first_seen_utc") or row.get("last_seen_utc") or "\uffff",
                row.get("url") or "",
                row.get("id") or 0,
            ),
        )

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            writer.writerow(self.headers)
            for row in rows_sorted:
                # Fetch matches and tags on-demand for CSV export
                url_id = row.get("id")
                matched_lists = self.case_data.get_url_matches(int(self.evidence_id), url_id) if url_id else ""
                tags = self.case_data.get_artifact_tags_str(int(self.evidence_id), 'url', url_id) if url_id else ""

                writer.writerow(
                    [
                        row.get("url", ""),
                        row.get("domain", ""),
                        row.get("scheme", ""),
                        row.get("discovered_by", ""),
                        row.get("first_seen_utc", ""),
                        row.get("last_seen_utc", ""),
                        row.get("source_path", ""),
                        row.get("occurrence_count") or 1,  # Occurrence count
                        tags,  # on-demand tag loading
                        matched_lists or "—",  # on-demand match loading
                    ]
                )

    def get_row(self, row: int) -> Optional[Dict[str, Any]]:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None
