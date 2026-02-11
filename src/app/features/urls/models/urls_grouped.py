"""
URLs grouped model for hierarchical domain-grouped URL view.

Provides tree-structured URL listing grouped by domain.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class UrlsGroupedModel(QStandardItemModel):
    def __init__(self, case_data: Optional[CaseDataAccess] = None) -> None:
        super().__init__()
        self.case_data = case_data
        self._filters: Dict[str, Any] = {
            "domain": "%",
            "url": "%",
            "sources": None,
            "match_filter": None,  #
        }
        self.evidence_id: Optional[int] = None
        self.setHorizontalHeaderLabels(
            [
                "URL",
                "Domain",
                "Scheme",
                "Source",
                "First Seen (UTC)",
                "Last Seen (UTC)",
                "Source Path",
                "Tags",
            ]
        )

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        if case_data is None:
            self.clear()
            self.setHorizontalHeaderLabels(
                [
                    "URL",
                    "Domain",
                    "Scheme",
                    "Source",
                    "First Seen (UTC)",
                    "Last Seen (UTC)",
                    "Source Path",
                    "Tags",
                ]
            )

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load grouped URLs for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        self.evidence_id = evidence_id
        if reload:
            self.reload()

    def set_filters(
        self,
        *,
        domain: Optional[str] = None,
        url: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,  #
        tag: Optional[str] = None,  #
        reload: bool = True,  # Allow deferring reload when view not visible
    ) -> None:
        if domain is not None:
            self._filters["domain"] = f"%{domain}%" if domain and domain != "*" else "%"
        if url is not None:
            self._filters["url"] = f"%{url}%" if url else "%"
        if sources is not None:
            self._filters["sources"] = tuple(sources) if sources else None
        if match_filter is not None:  #
            self._filters["match_filter"] = match_filter if match_filter != "all" else None
        if tag is not None:  #
            self._filters["tag"] = tag if tag and tag != "*" else "%"
        if reload:
            self.reload()

    def reload(self) -> None:
        self.clear()
        self.setHorizontalHeaderLabels(
            [
                "URL",
                "Domain",
                "Scheme",
                "Source",
                "First Seen (UTC)",
                "Last Seen (UTC)",
                "Source Path",
                "Tags",
            ]
        )
        if not self.case_data or self.evidence_id is None:
            return
        rows = self.case_data.iter_urls(
            int(self.evidence_id),
            domain_like=self._filters["domain"],
            url_like=self._filters["url"],
            tag_like=self._filters.get("tag", "%"),
            discovered_by=self._filters["sources"],
            match_filter=self._filters["match_filter"],  #
            limit=10_000,
            offset=0,
        )
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            domain = row.get("domain") or "<none>"
            grouped.setdefault(domain, []).append(row)
        for domain, children in grouped.items():
            parent_item = QStandardItem(domain)
            parent_row = [parent_item]
            for _ in range(7):
                parent_row.append(QStandardItem(""))
            self.appendRow(parent_row)
            for child in children:
                child_items = [
                    QStandardItem(str(child.get("url", ""))),
                    QStandardItem(str(child.get("domain", ""))),
                    QStandardItem(str(child.get("scheme", ""))),
                    QStandardItem(str(child.get("discovered_by", ""))),
                    QStandardItem(str(child.get("first_seen_utc", ""))),
                    QStandardItem(str(child.get("last_seen_utc", ""))),
                    QStandardItem(str(child.get("source_path", ""))),
                    QStandardItem(str(child.get("tags", ""))),
                ]
                for idx, item in enumerate(child_items):
                    editable = idx == 7
                    item.setEditable(editable)
                    item.setData(child.get("id"), Qt.UserRole)
                    if idx == 0:
                        item.setData(child, Qt.UserRole + 1)
                parent_item.appendRow(child_items)
