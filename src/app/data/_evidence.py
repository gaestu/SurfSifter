"""Evidence metadata data access.

This module provides CRUD operations for evidence metadata
stored in the case database and evidence counts from evidence databases.

Extracted from _case.py for modular repository pattern.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ._base import BaseDataAccess


@dataclass(frozen=True)
class EvidenceCounts:
    """Summary counts for an evidence item."""
    urls: int
    images: int
    indicators: int
    last_run_utc: Optional[str]


class EvidenceMetadataMixin(BaseDataAccess):
    """Mixin providing evidence metadata operations.

    Methods operate on the case database (evidences table),
    except get_evidence_counts() which queries the evidence database.

    Extracted from CaseMetadataMixin for modular architecture.
    """

    # -------------------------------------------------------------------------
    # Evidence Metadata (Case DB)
    # -------------------------------------------------------------------------

    def list_evidences(self) -> List[Dict[str, Any]]:
        """List all evidences.

        Note: evidences table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            cursor = conn.execute(
                """
                SELECT id, label, source_path, added_at_utc, read_only,
                       partition_info, partition_selections, scan_slack_space
                FROM evidences
                ORDER BY added_at_utc ASC
                """
            )
            rows = [dict(row) for row in cursor.fetchall()]
        return rows

    def get_evidence(self, evidence_id: int) -> Optional[Dict[str, Any]]:
        """Get evidence by ID.

        Note: evidences table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            row = conn.execute(
                """
                SELECT id, case_id, label, source_path, size, ewf_info_json,
                       added_at_utc, read_only, partition_index, partition_info,
                       partition_selections, scan_slack_space
                FROM evidences
                WHERE id = ?
                """,
                (evidence_id,),
            ).fetchone()
            return dict(row) if row else None

    def update_partition_selections(
        self,
        evidence_id: int,
        partition_selections: Optional[str],
        scan_slack_space: bool = False,
    ) -> None:
        """Update which partitions to scan for an evidence.

        Note: evidences table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            conn.execute(
                """
                UPDATE evidences
                SET partition_selections = ?,
                    scan_slack_space = ?
                WHERE id = ?
                """,
                (partition_selections, 1 if scan_slack_space else 0, evidence_id)
            )
            conn.commit()

    def update_partition_info(self, evidence_id: int, partition_info: str) -> None:
        """Update partition information JSON for an evidence.

        Note: evidences table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            conn.execute(
                """
                UPDATE evidences
                SET partition_info = ?
                WHERE id = ?
                """,
                (partition_info, evidence_id)
            )
            conn.commit()

    # -------------------------------------------------------------------------
    # Evidence Counts (Evidence DB)
    # -------------------------------------------------------------------------

    def get_evidence_counts(self, evidence_id: int) -> EvidenceCounts:
        """Get summary counts for an evidence item.

        Queries the evidence database for URL, image, and indicator counts,
        plus the timestamp of the last extraction run.

        Args:
            evidence_id: Evidence ID to get counts for

        Returns:
            EvidenceCounts with urls, images, indicators, and last_run_utc
        """
        # Guard: return empty counts if evidence DB doesn't exist yet
        if not self._evidence_db_exists(evidence_id):
            return EvidenceCounts(urls=0, images=0, indicators=0, last_run_utc=None)
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                urls = self._scalar(
                    conn, "SELECT COUNT(*) FROM urls WHERE evidence_id = ?", (evidence_id,)
                )
                images = self._scalar(
                    conn, "SELECT COUNT(*) FROM images WHERE evidence_id = ?", (evidence_id,)
                )
                indicators = self._scalar(
                    conn, "SELECT COUNT(*) FROM os_indicators WHERE evidence_id = ?", (evidence_id,)
                )
                last_run = self._scalar(
                    conn,
                    """
                    SELECT MAX(finished_at_utc)
                    FROM process_log
                    WHERE evidence_id = ?
                    """,
                    (evidence_id,),
                )
        last_run_str = str(last_run) if last_run else None
        return EvidenceCounts(
            urls=urls, images=images, indicators=indicators, last_run_utc=last_run_str
        )
