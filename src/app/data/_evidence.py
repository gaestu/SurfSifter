"""Evidence metadata data access.

This module provides CRUD operations for evidence metadata
stored in the case database and evidence counts from evidence databases.

Extracted from _case.py for modular repository pattern.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Hashable, List, Optional, Tuple

from core.database.helpers.url_search_index import URL_SEARCH_INDEX_BUILD_TASK

from ._base import BaseDataAccess
from ._stats_cache import (
    EVIDENCE_COUNTS_CATEGORIES,
    OVERVIEW_STATS_CATEGORIES,
    TAG_STATISTICS_CATEGORIES,
)


@dataclass(frozen=True)
class EvidenceCounts:
    """Summary counts for an evidence item."""
    urls: int
    images: int
    indicators: int
    last_run_utc: Optional[str]


@dataclass(frozen=True)
class OverviewStats:
    """Everything the evidence Overview sub-tab shows (issue #63)."""
    counts: EvidenceCounts
    tag_count: int
    tagged_artifact_count: int


_EVIDENCE_COUNTS_KEY = ("evidence_counts",)
_TAG_STATISTICS_KEY = ("tag_statistics",)


def _empty_evidence_counts() -> EvidenceCounts:
    return EvidenceCounts(urls=0, images=0, indicators=0, last_run_utc=None)


def _empty_tag_statistics() -> Dict[str, Any]:
    return {"tag_count": 0, "tagged_artifact_count": 0, "breakdown": {}}


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

    def get_evidence_counts(self, evidence_id: int, *, use_cache: bool = True) -> EvidenceCounts:
        """Get summary counts for an evidence item.

        Queries the evidence database for URL, image, and indicator counts,
        plus the timestamp of the last extraction run. Served from the shared
        case statistics cache while the underlying data is unchanged.

        Args:
            evidence_id: Evidence ID to get counts for
            use_cache: False forces a fresh read (the result is still cached)

        Returns:
            EvidenceCounts with urls, images, indicators, and last_run_utc
        """
        return self._cached_stat(
            evidence_id,
            _EVIDENCE_COUNTS_KEY,
            EVIDENCE_COUNTS_CATEGORIES,
            lambda: self._read_evidence_counts(evidence_id),
            use_cache=use_cache,
            # Not cached while the evidence DB does not exist yet.
            empty=_empty_evidence_counts,
        )

    def _read_evidence_counts(self, evidence_id: int) -> EvidenceCounts:
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
                      AND task IS NOT ?
                    """,
                    (evidence_id, URL_SEARCH_INDEX_BUILD_TASK),
                )
        last_run_str = str(last_run) if last_run else None
        return EvidenceCounts(
            urls=urls, images=images, indicators=indicators, last_run_utc=last_run_str
        )

    def get_tag_statistics(self, evidence_id: int, *, use_cache: bool = True) -> Dict[str, Any]:
        """Get tag statistics for an evidence.

        Returns dict with keys: tag_count, tagged_artifact_count, breakdown
        (dict of artifact_type -> count). Served from the shared case
        statistics cache while tags are unchanged.
        """
        return self._cached_stat(
            evidence_id,
            _TAG_STATISTICS_KEY,
            TAG_STATISTICS_CATEGORIES,
            lambda: self._read_tag_statistics(evidence_id),
            use_cache=use_cache,
            empty=_empty_tag_statistics,
        )

    def _read_tag_statistics(self, evidence_id: int) -> Dict[str, Any]:
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                tag_count = self._scalar(
                    conn,
                    "SELECT COUNT(*) FROM tags WHERE evidence_id = ?",
                    (evidence_id,),
                )
                tagged_artifact_count = self._scalar(
                    conn,
                    "SELECT COUNT(*) FROM tag_associations WHERE evidence_id = ?",
                    (evidence_id,),
                )
                cursor = conn.execute(
                    "SELECT artifact_type, COUNT(*) AS cnt "
                    "FROM tag_associations WHERE evidence_id = ? "
                    "GROUP BY artifact_type ORDER BY cnt DESC",
                    (evidence_id,),
                )
                breakdown = {row["artifact_type"]: row["cnt"] for row in cursor.fetchall()}
        return {
            "tag_count": tag_count,
            "tagged_artifact_count": tagged_artifact_count,
            "breakdown": breakdown,
        }

    # -------------------------------------------------------------------------
    # Overview Statistics (issue #63)
    # -------------------------------------------------------------------------

    @staticmethod
    def _compose_overview(counts: EvidenceCounts, tag_stats: Dict[str, Any]) -> OverviewStats:
        return OverviewStats(
            counts=counts,
            tag_count=int(tag_stats["tag_count"]),
            tagged_artifact_count=int(tag_stats["tagged_artifact_count"]),
        )

    def overview_stats_token(self, evidence_id: int) -> Hashable:
        """Revision token of everything ``get_overview_stats`` depends on."""
        return self.stats_cache.token(evidence_id, OVERVIEW_STATS_CATEGORIES)

    def is_overview_stats_current(self, evidence_id: int, token: Hashable) -> bool:
        """True if the overview data did not change since ``token`` was taken."""
        return self.stats_cache.is_current(evidence_id, OVERVIEW_STATS_CATEGORIES, token)

    def get_overview_stats(self, evidence_id: int, *, use_cache: bool = True) -> OverviewStats:
        """Counts and tag statistics shown on the evidence Overview."""
        return self.load_overview_stats(evidence_id, use_cache=use_cache)[0]

    def load_overview_stats(
        self, evidence_id: int, *, use_cache: bool = True
    ) -> Tuple[OverviewStats, Hashable]:
        """Like ``get_overview_stats`` but also returns the token taken before the reads.

        Background loaders pass the token to ``is_overview_stats_current`` on
        the GUI thread to drop results that were superseded while loading.
        """
        token = self.overview_stats_token(evidence_id)
        counts = self.get_evidence_counts(evidence_id, use_cache=use_cache)
        tag_stats = self.get_tag_statistics(evidence_id, use_cache=use_cache)
        return self._compose_overview(counts, tag_stats), token

    def peek_overview_stats(self, evidence_id: int) -> Optional[OverviewStats]:
        """Cached overview statistics, or None on a miss.

        Consults only the in-memory cache (no database or file system access),
        so it is safe on the GUI thread.
        """
        hit, counts = self._peek_stat(evidence_id, _EVIDENCE_COUNTS_KEY)
        if not hit:
            return None
        hit, tag_stats = self._peek_stat(evidence_id, _TAG_STATISTICS_KEY)
        if not hit:
            return None
        return self._compose_overview(counts, tag_stats)
