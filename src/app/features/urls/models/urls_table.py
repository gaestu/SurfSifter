"""
URLs table model for flat URL listing.

Provides paginated, filterable URL listing. Tag/match strings come from
batch-loaded caches; display methods never query the database (issue #60).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from app.data import UrlPage, UrlPageQuery

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
        self._matches_cache: Dict[int, str] = {}  # url_id -> matched_lists string (batch-loaded)
        self._tags_cache: Dict[int, str] = {}  # url_id -> tags string (batch-loaded)
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
            if column == 8:  # Tags column (batch-loaded cache)
                return self._get_tags(row.get("id"))
            if column == 9:  # Match column (batch-loaded cache)
                return self._get_matches(row.get("id"))
        return None

    def _get_tags(self, url_id: Optional[int]) -> str:
        """Return cached tags for a URL ("" if none). Never queries the database."""
        if not url_id:
            return ""
        return self._tags_cache.get(url_id) or ""

    def _get_matches(self, url_id: Optional[int]) -> str:
        """Return cached matched list names for a URL ("—" if none). Never queries the database."""
        if not url_id:
            return "—"
        return self._matches_cache.get(url_id) or "—"

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
        self._clear_rows()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load URLs for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        if evidence_id != self.evidence_id:
            # Never show rows/caches of a different evidence.
            self._clear_rows()
        self.evidence_id = evidence_id
        self.page = 0
        if reload:
            self.reload()

    def _clear_rows(self) -> None:
        self.beginResetModel()
        self._rows = []
        self._total_count = 0
        self._tags_cache.clear()
        self._matches_cache.clear()
        self.endResetModel()

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
                   loading in the background (see ``build_query``).
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
        # Tag/match caches are replaced wholesale when the next page arrives;
        # clearing them here would make the still-visible rows render blank.
        if reload:
            self.reload()

    def get_filters(self) -> Dict[str, Any]:
        """Get current filter settings."""
        return dict(self._filters)

    def build_query(self, page: Optional[int] = None) -> UrlPageQuery:
        """Build the background-load request for the current filters.

        Args:
            page: Page index to request (defaults to the current page)
        """
        if self.evidence_id is None:
            raise ValueError("No evidence selected")
        sources = self._filters.get("sources")
        return UrlPageQuery(
            evidence_id=int(self.evidence_id),
            page=self.page if page is None else max(0, page),
            page_size=self.page_size,
            domain_like=self._filters["domain"],
            url_like=self._filters["url"],
            tag_like=self._filters.get("tag", "%"),
            discovered_by=tuple(sources) if sources else None,
            match_filter=self._filters["match_filter"],
        )

    def set_loaded_page(self, page: UrlPage) -> None:
        """Apply a page prepared off the GUI thread (no database access)."""
        self.beginResetModel()
        self.page = page.query.page
        self._rows = list(page.rows)
        self._total_count = page.total_count
        self._tags_cache = dict(page.tags)
        self._matches_cache = dict(page.matches)
        self.endResetModel()

    def reload(self) -> None:
        """Synchronously reload the current page ON THE CALLING THREAD.

        Kept for backward compatibility and tests only. GUI code must not use
        it (nor ``reload=True`` on ``set_evidence``/``set_filters``); the URLs
        tab loads pages in the background via ``build_query`` +
        ``set_loaded_page`` (issue #60).
        """
        if not self.case_data or self.evidence_id is None:
            self._clear_rows()
            return
        self.set_loaded_page(self.case_data.load_url_page(self.build_query()))

    def total_count(self) -> int:
        """Get total count of URLs matching current filters."""
        return self._total_count

    def has_next_page(self) -> bool:
        return len(self._rows) >= self.page_size and (self.page + 1) * self.page_size < self._total_count

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

        # Issue #47: Batch load all tags and matches upfront instead of N+1 queries
        evidence_id = int(self.evidence_id)
        all_url_ids = [row["id"] for row in rows_sorted if row.get("id")]
        tags_batch = self.case_data.get_tag_strings_for_artifacts(
            evidence_id, "url", all_url_ids
        ) if all_url_ids else {}
        matches_batch = self.case_data.get_url_matches_batch(
            evidence_id, all_url_ids
        ) if all_url_ids else {}

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            writer.writerow(self.headers)
            for row in rows_sorted:
                url_id = row.get("id")
                # Use batch-loaded tags and matches
                tags = tags_batch.get(url_id, "") if url_id else ""
                matched_lists = matches_batch.get(url_id, "") if url_id else ""

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
                        tags,  # batch-loaded tags
                        matched_lists or "—",  # batch-loaded matches
                    ]
                )

    def get_row(self, row: int) -> Optional[Dict[str, Any]]:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None
