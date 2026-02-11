"""Timeline query operations for UI layer.

This module provides timeline-specific queries for the UI:
- Paginated timeline listing with filters (kind, confidence, date range, tag)
- Statistics (total, earliest/latest, by_kind, by_confidence)
- Filter dropdown data (kinds, confidences)
- CSV export
- Tag update wrapper

Extracted from case_data.py for modular repository pattern.

Database: Evidence DB (`timeline`, `tag_associations`)
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._base import BaseDataAccess


class TimelineQueryMixin(BaseDataAccess):
    """Mixin providing timeline query operations for UI views.

    Features:
    - Paginated timeline listing with filters
    - Statistics aggregation
    - Filter dropdowns (kinds, confidences)
    - CSV export
    - Tag update wrapper

    All methods operate on the evidence database.

    Extracted from CaseDataAccess for feature-local data access.
    """

    # -------------------------------------------------------------------------
    # Timeline Queries (Evidence DB)
    # -------------------------------------------------------------------------

    def iter_timeline(
        self,
        evidence_id: int,
        *,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Retrieve paginated timeline events with optional filters.

        Args:
            evidence_id: Evidence ID to filter
            filters: Dict with optional keys: kind, confidence, start_date, end_date, tag
            page: 1-indexed page number
            page_size: Number of events per page

        Returns:
            List of timeline event dicts with keys:
            id, evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note, tags
        """
        filters = filters or {}
        where = ["tl.evidence_id = ?"]
        params: List[Any] = [evidence_id]

        if filters.get("kind"):
            where.append("tl.kind = ?")
            params.append(filters["kind"])

        if filters.get("confidence"):
            where.append("tl.confidence = ?")
            params.append(filters["confidence"])

        if filters.get("tag"):
            where.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'timeline'
                    AND ta.artifact_id = tl.id
                    AND t.name LIKE ?
                )
            """)
            params.append(filters["tag"])

        if filters.get("start_date"):
            where.append("tl.ts_utc >= ?")
            params.append(filters["start_date"])

        if filters.get("end_date"):
            # Inclusive - add 1 day to end_date
            where.append("tl.ts_utc < datetime(?, '+1 day')")
            params.append(filters["end_date"])

        offset = (max(1, page) - 1) * page_size
        sql = f"""
            SELECT tl.id, tl.evidence_id, tl.ts_utc, tl.kind, tl.ref_table, tl.ref_id, tl.confidence, tl.note,
                   GROUP_CONCAT(t.name, ', ') as tags
            FROM timeline tl
            LEFT JOIN tag_associations ta ON ta.artifact_type = 'timeline' AND ta.artifact_id = tl.id
            LEFT JOIN tags t ON ta.tag_id = t.id
            WHERE {' AND '.join(where)}
            GROUP BY tl.id
            ORDER BY tl.ts_utc DESC, tl.kind, tl.ref_table, tl.ref_id
            LIMIT ? OFFSET ?
        """
        params.extend([page_size, offset])

        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_timeline_stats(self, evidence_id: int) -> Dict[str, Any]:
        """Get timeline statistics for an evidence item.

        Returns:
            Dict with keys: total_events, earliest, latest, by_kind, by_confidence
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                # Total events
                total = self._scalar(
                    conn,
                    "SELECT COUNT(*) FROM timeline WHERE evidence_id = ?",
                    (evidence_id,)
                )

                # Time range
                row = conn.execute(
                    """
                    SELECT MIN(ts_utc) AS earliest, MAX(ts_utc) AS latest
                    FROM timeline
                    WHERE evidence_id = ?
                    """,
                    (evidence_id,)
                ).fetchone()
                earliest = row["earliest"] if row else None
                latest = row["latest"] if row else None

                # Kind counts
                kind_cursor = conn.execute(
                    """
                    SELECT kind, COUNT(*) AS cnt
                    FROM timeline
                    WHERE evidence_id = ?
                    GROUP BY kind
                    ORDER BY cnt DESC
                    """,
                    (evidence_id,)
                )
                kind_counts = {row["kind"]: row["cnt"] for row in kind_cursor.fetchall()}

                # Confidence counts
                conf_cursor = conn.execute(
                    """
                    SELECT confidence, COUNT(*) AS cnt
                    FROM timeline
                    WHERE evidence_id = ?
                    GROUP BY confidence
                    ORDER BY cnt DESC
                    """,
                    (evidence_id,)
                )
                confidence_counts = {row["confidence"]: row["cnt"] for row in conf_cursor.fetchall()}

                return {
                    "total_events": total,
                    "earliest": earliest,
                    "latest": latest,
                    "by_kind": kind_counts,
                    "by_confidence": confidence_counts,
                }

    def get_timeline_kinds(self, evidence_id: int) -> List[str]:
        """Get all distinct event kinds for filtering."""
        sql = """
            SELECT DISTINCT kind
            FROM timeline
            WHERE evidence_id = ?
            ORDER BY kind
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                return [row[0] for row in cursor.fetchall()]

    def get_timeline_confidences(self, evidence_id: int) -> List[str]:
        """Get all distinct confidence levels for filtering."""
        sql = """
            SELECT DISTINCT confidence
            FROM timeline
            WHERE evidence_id = ?
            ORDER BY
                CASE confidence
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4
                END
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                return [row[0] for row in cursor.fetchall()]

    def export_timeline_csv(
        self,
        evidence_id: int,
        output_path: Path,
        *,
        filters: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Export timeline events to CSV file.

        Args:
            evidence_id: Evidence ID to export
            output_path: Path to write CSV file
            filters: Optional filters (same as iter_timeline)

        Returns:
            Number of events exported
        """
        filters = filters or {}
        where = ["tl.evidence_id = ?"]
        params: List[Any] = [evidence_id]

        if filters.get("kind"):
            where.append("tl.kind = ?")
            params.append(filters["kind"])

        if filters.get("confidence"):
            where.append("tl.confidence = ?")
            params.append(filters["confidence"])

        if filters.get("tag"):
            where.append("""
                EXISTS (
                    SELECT 1 FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'timeline'
                    AND ta.artifact_id = tl.id
                    AND t.name LIKE ?
                )
            """)
            params.append(filters["tag"])

        if filters.get("start_date"):
            where.append("tl.ts_utc >= ?")
            params.append(filters["start_date"])

        if filters.get("end_date"):
            where.append("tl.ts_utc < datetime(?, '+1 day')")
            params.append(filters["end_date"])

        sql = f"""
            SELECT tl.ts_utc, tl.kind, tl.confidence, tl.ref_table, tl.ref_id, tl.note,
                   GROUP_CONCAT(t.name, ', ') as tags
            FROM timeline tl
            LEFT JOIN tag_associations ta ON ta.artifact_type = 'timeline' AND ta.artifact_id = tl.id
            LEFT JOIN tags t ON ta.tag_id = t.id
            WHERE {' AND '.join(where)}
            GROUP BY tl.id
            ORDER BY tl.ts_utc DESC, tl.kind, tl.ref_table, tl.ref_id
        """

        count = 0
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                with open(output_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Timestamp (UTC)", "Event Type", "Confidence", "Source Table", "Source ID", "Note", "Tags"])
                    for row in cursor:
                        writer.writerow([
                            row["ts_utc"],
                            row["kind"],
                            row["confidence"],
                            row["ref_table"],
                            row["ref_id"],
                            row["note"],
                            row["tags"] or "",
                        ])
                        count += 1

        return count

    def update_timeline_tags(self, evidence_id: int, timeline_id: int, tags_str: str) -> None:
        """Update tags for a timeline event.

        Parses comma-separated string and updates tag associations.

        Args:
            evidence_id: Evidence ID
            timeline_id: Timeline event ID
            tags_str: Comma-separated list of tag names
        """
        # 1. Get current tags
        current_tags = {t['name'] for t in self.get_artifact_tags(evidence_id, 'timeline', timeline_id)}

        # 2. Parse new tags
        new_tags = {t.strip() for t in tags_str.split(',') if t.strip()}

        # 3. Determine changes
        to_add = new_tags - current_tags
        to_remove = current_tags - new_tags

        # 4. Apply changes
        for tag in to_add:
            self.tag_artifact(evidence_id, tag, 'timeline', timeline_id)

        for tag in to_remove:
            self.untag_artifact(evidence_id, tag, 'timeline', timeline_id)
