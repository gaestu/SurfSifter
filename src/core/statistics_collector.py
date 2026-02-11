"""
Extractor Statistics Collector

Central API for extractors to report run statistics. Provides:
- In-memory cache for fast access
- Database persistence for durability
- Qt signals for UI updates

Initial implementation
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, Signal

from .logging import get_logger

LOGGER = get_logger("core.statistics_collector")


@dataclass
class ExtractorRunStats:
    """Statistics for a single extractor run."""

    evidence_id: int
    evidence_label: str  # Required for db_manager access
    extractor_name: str
    run_id: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    status: str = "running"  # running, success, partial, failed, cancelled, skipped
    discovered: Dict[str, int] = field(default_factory=dict)
    ingested: Dict[str, int] = field(default_factory=dict)
    failed: Dict[str, int] = field(default_factory=dict)
    skipped: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "evidence_id": self.evidence_id,
            "extractor_name": self.extractor_name,
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "discovered": json.dumps(self.discovered),
            "ingested": json.dumps(self.ingested),
            "failed": json.dumps(self.failed),
            "skipped": json.dumps(self.skipped),
        }

    @classmethod
    def from_row(cls, row: Dict[str, Any], evidence_label: str = "") -> "ExtractorRunStats":
        """Create from database row."""
        return cls(
            evidence_id=row["evidence_id"],
            evidence_label=evidence_label,
            extractor_name=row["extractor_name"],
            run_id=row["run_id"],
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
            duration_seconds=row["duration_seconds"],
            status=row["status"],
            discovered=json.loads(row["discovered"] or "{}"),
            ingested=json.loads(row["ingested"] or "{}"),
            failed=json.loads(row["failed"] or "{}"),
            skipped=json.loads(row["skipped"] or "{}"),
        )


class StatisticsCollector(QObject):
    """
    Singleton collector for extractor statistics.

    Follows the same singleton management pattern as other core services.

    Usage in extractors:
        from core.statistics_collector import StatisticsCollector

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, "browser_history", run_id)

            # During extraction
            collector.report_discovered(evidence_id, "browser_history", urls=150, images=42)

            # During ingestion
            collector.report_ingested(evidence_id, "browser_history", urls=148, images=40)
            collector.report_failed(evidence_id, "browser_history", urls=2, images=2)

            # When done
            collector.finish_run(evidence_id, "browser_history", status="success")
    """

    # Signals for UI updates
    stats_updated = Signal(int, str)  # evidence_id, extractor_name
    run_started = Signal(int, str)    # evidence_id, extractor_name
    run_finished = Signal(int, str)   # evidence_id, extractor_name

    _instance: Optional["StatisticsCollector"] = None
    _initialized: bool = False

    def __init__(self, db_manager=None):
        super().__init__()
        self._db_manager = db_manager
        self._cache: Dict[tuple, ExtractorRunStats] = {}  # (evidence_id, extractor_name) -> stats

    @classmethod
    def install(cls, db_manager=None) -> "StatisticsCollector":
        """
        Install global statistics collector (singleton).

        If already installed, updates the db_manager and clears the cache
        (for case switching).

        Args:
            db_manager: DatabaseManager instance for persistence

        Returns:
            StatisticsCollector instance
        """
        if cls._instance is None or not cls._initialized:
            cls._instance = cls(db_manager=db_manager)
            cls._initialized = True
            LOGGER.info("StatisticsCollector installed (db_manager=%s)", db_manager is not None)
        else:
            # Update db_manager and clear cache on reinstall (case switch)
            cls._instance._db_manager = db_manager
            cls._instance._cache.clear()
            LOGGER.info("StatisticsCollector updated for new case (db_manager=%s)", db_manager is not None)
        return cls._instance

    @classmethod
    def get_instance(cls) -> Optional["StatisticsCollector"]:
        """Get current collector instance (if installed)."""
        return cls._instance

    # Alias for convenience
    instance = get_instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton state (for testing only)."""
        cls._instance = None
        cls._initialized = False

    def _cache_key(self, evidence_id: int, extractor_name: str) -> tuple:
        return (evidence_id, extractor_name)

    def start_run(
        self,
        evidence_id: int,
        evidence_label: str,
        extractor_name: str,
        run_id: str
    ) -> None:
        """Start tracking a new extractor run."""
        stats = ExtractorRunStats(
            evidence_id=evidence_id,
            evidence_label=evidence_label,
            extractor_name=extractor_name,
            run_id=run_id,
            started_at=datetime.now(),
            status="running",
        )
        key = self._cache_key(evidence_id, extractor_name)
        self._cache[key] = stats
        self._persist(stats)
        self.run_started.emit(evidence_id, extractor_name)
        LOGGER.debug("Started run: evidence=%d, extractor=%s, run_id=%s",
                     evidence_id, extractor_name, run_id)

    def continue_run(
        self,
        evidence_id: int,
        evidence_label: str,
        extractor_name: str,
        run_id: str
    ) -> None:
        """
        Continue tracking an existing extractor run (for ingestion phase).

        Behavior:
        1. If run exists in cache: continue it (preserving discovery stats)
        2. If not in cache but exists in DB: load and continue it
        3. If not in DB: start fresh (ingestion-only scenario)

        If the existing run was already finished (e.g., extraction completed),
        resets status to "running" and clears finished_at to indicate active ingestion.

        Use this instead of start_run() for ingestion to preserve extraction stats.
        """
        key = self._cache_key(evidence_id, extractor_name)

        # Check cache first
        if key in self._cache:
            existing = self._cache[key]
            self._continue_existing_run(existing, run_id)
            return

        # Cache miss - try loading from database (only if evidence_label is available)
        if self._db_manager and evidence_label:
            from .database import get_extractor_statistics_by_name
            try:
                row = get_extractor_statistics_by_name(
                    self._db_manager, evidence_id, evidence_label, extractor_name
                )
                if row:
                    # Found in DB - load into cache and continue
                    existing = ExtractorRunStats.from_row(row, evidence_label)
                    self._cache[key] = existing
                    self._continue_existing_run(existing, run_id)
                    LOGGER.debug("Loaded and continuing run from DB: evidence=%d, extractor=%s",
                                evidence_id, extractor_name)
                    return
            except Exception as exc:
                LOGGER.warning("Failed to load stats from DB for continue_run: %s", exc)

        # No existing run anywhere - start fresh (ingestion-only scenario)
        self.start_run(evidence_id, evidence_label, extractor_name, run_id)

    def _continue_existing_run(self, stats: ExtractorRunStats, run_id: str) -> None:
        """
        Continue an existing run, resetting status if it was finished.

        Preserves discovery stats and start time, but resets finished state
        so UI shows "running" during ingestion phase.
        """
        # Update run_id if different
        if stats.run_id != run_id:
            stats.run_id = run_id

        # If run was already finished (extraction complete), reset to running
        # This ensures UI shows active state during ingestion
        if stats.status != "running":
            stats.status = "running"
            stats.finished_at = None
            stats.duration_seconds = None
            LOGGER.debug("Reset finished run to running for ingestion: extractor=%s",
                        stats.extractor_name)

        self._persist(stats)
        self.stats_updated.emit(stats.evidence_id, stats.extractor_name)
        LOGGER.debug("Continuing existing run: evidence=%d, extractor=%s, run_id=%s",
                    stats.evidence_id, stats.extractor_name, run_id)

    def finish_run(
        self,
        evidence_id: int,
        extractor_name: str,
        status: str = "success"
    ) -> None:
        """Mark a run as finished."""
        key = self._cache_key(evidence_id, extractor_name)
        if key not in self._cache:
            LOGGER.warning("finish_run called for unknown run: evidence=%d, extractor=%s",
                          evidence_id, extractor_name)
            return

        stats = self._cache[key]
        stats.finished_at = datetime.now()
        stats.status = status
        if stats.started_at:
            stats.duration_seconds = (stats.finished_at - stats.started_at).total_seconds()

        self._persist(stats)
        self._sync_process_log(stats)  # Sync counts to process_log
        self.run_finished.emit(evidence_id, extractor_name)
        LOGGER.debug("Finished run: evidence=%d, extractor=%s, status=%s, duration=%.1fs",
                     evidence_id, extractor_name, status, stats.duration_seconds or 0)

    def complete_run(
        self,
        extractor_name: str,
        evidence_id: int,
        status: str = "success",
        *,
        records: int = 0,
        error: str = "",
        discovered: Optional[Dict[str, int]] = None,
        ingested: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Alias for finish_run with extractor-friendly argument order.

        This method exists because many extractors were written using:
            stats.complete_run(self.metadata.name, evidence_id, status, records=N)

        Args:
            extractor_name: Name of the extractor
            evidence_id: Evidence ID
            status: Final status (success, failed, cancelled, skipped, completed, etc.)
            records: Number of records processed (shorthand for ingested={"records": N})
            error: Error message if failed (logged only)
            discovered: Dict of discovered counts (e.g., {"files": 10, "urls": 50})
            ingested: Dict of ingested counts (e.g., {"records": 100})
        """
        if error:
            LOGGER.debug("complete_run: extractor=%s, status=%s, error=%s",
                        extractor_name, status, error)

        # Report discovered counts if provided
        if discovered:
            self.report_discovered(evidence_id, extractor_name, **discovered)

        # Report ingested counts - use explicit dict or records shorthand
        if ingested:
            self.report_ingested(evidence_id, extractor_name, **ingested)
        elif records > 0:
            # Backwards compatibility: records param becomes ingested.records
            self.report_ingested(evidence_id, extractor_name, records=records)

        self.finish_run(evidence_id, extractor_name, status)

    def report_discovered(
        self,
        evidence_id: int,
        extractor_name: str,
        **counts: int
    ) -> None:
        """Report discovered item counts (additive)."""
        self._update_counts(evidence_id, extractor_name, "discovered", counts)

    def report_ingested(
        self,
        evidence_id: int,
        extractor_name: str,
        **counts: int
    ) -> None:
        """Report ingested item counts (additive)."""
        self._update_counts(evidence_id, extractor_name, "ingested", counts)

    def report_failed(
        self,
        evidence_id: int,
        extractor_name: str,
        **counts: int
    ) -> None:
        """Report failed item counts (additive)."""
        self._update_counts(evidence_id, extractor_name, "failed", counts)

    def report_skipped(
        self,
        evidence_id: int,
        extractor_name: str,
        **counts: int
    ) -> None:
        """Report skipped item counts (additive)."""
        self._update_counts(evidence_id, extractor_name, "skipped", counts)

    def _update_counts(
        self,
        evidence_id: int,
        extractor_name: str,
        category: str,
        counts: Dict[str, int]
    ) -> None:
        """Update counts for a category (additive)."""
        key = self._cache_key(evidence_id, extractor_name)
        if key not in self._cache:
            LOGGER.warning("_update_counts called for unknown run: evidence=%d, extractor=%s",
                          evidence_id, extractor_name)
            return

        stats = self._cache[key]
        target = getattr(stats, category)
        for item_type, count in counts.items():
            target[item_type] = target.get(item_type, 0) + count

        self._persist(stats)
        self.stats_updated.emit(evidence_id, extractor_name)

    def get_stats(
        self,
        evidence_id: int,
        extractor_name: str
    ) -> Optional[ExtractorRunStats]:
        """Get statistics for a specific extractor run."""
        return self._cache.get(self._cache_key(evidence_id, extractor_name))

    def get_all_stats_for_evidence(
        self,
        evidence_id: int
    ) -> List[ExtractorRunStats]:
        """Get all extractor statistics for an evidence."""
        return [
            stats for (eid, _), stats in self._cache.items()
            if eid == evidence_id
        ]

    def get_aggregated_totals(
        self,
        evidence_id: int
    ) -> Dict[str, Dict[str, int]]:
        """
        Get aggregated totals across all extractors for an evidence.

        Returns:
            {
                "discovered": {"urls": 500, "images": 200, ...},
                "ingested": {"urls": 480, "images": 195, ...},
                "failed": {"urls": 20, "images": 5, ...},
                "skipped": {...}
            }
        """
        totals: Dict[str, Dict[str, int]] = {
            "discovered": {},
            "ingested": {},
            "failed": {},
            "skipped": {},
        }

        for stats in self.get_all_stats_for_evidence(evidence_id):
            for category in totals.keys():
                source = getattr(stats, category)
                for item_type, count in source.items():
                    totals[category][item_type] = totals[category].get(item_type, 0) + count

        return totals

    def _persist(self, stats: ExtractorRunStats) -> None:
        """Persist statistics to database."""
        if not self._db_manager:
            return

        # Guard: evidence_label is required for database access
        if not stats.evidence_label:
            LOGGER.debug(
                "Skipping statistics persistence: empty evidence_label "
                "(evidence_id=%d, extractor=%s)",
                stats.evidence_id, stats.extractor_name
            )
            return

        from .database import upsert_extractor_statistics
        try:
            upsert_extractor_statistics(self._db_manager, stats)
        except Exception as exc:
            LOGGER.warning("Failed to persist statistics: %s", exc)

    def _sync_process_log(self, stats: ExtractorRunStats) -> None:
        """
        Sync extractor statistics to process_log for audit trail consistency.

        This ensures the process_log records_extracted and records_ingested
        columns reflect the accurate counts from extractor_statistics.

        Added for Option B audit sync.
        """
        if not self._db_manager:
            return

        if not stats.evidence_label:
            return

        from .database import sync_process_log_from_statistics
        try:
            sync_process_log_from_statistics(
                self._db_manager,
                stats.evidence_id,
                stats.evidence_label,
                stats.extractor_name,
            )
        except Exception as exc:
            LOGGER.debug("Failed to sync process_log from statistics: %s", exc)

    def clear_evidence_stats(self, evidence_id: int, evidence_label: str) -> None:
        """Clear all statistics for an evidence (e.g., on purge)."""
        keys_to_remove = [
            key for key in self._cache.keys()
            if key[0] == evidence_id
        ]
        for key in keys_to_remove:
            del self._cache[key]

        if self._db_manager:
            from .database import delete_extractor_statistics_by_evidence
            try:
                delete_extractor_statistics_by_evidence(self._db_manager, evidence_id, evidence_label)
            except Exception as exc:
                LOGGER.warning("Failed to delete statistics from database: %s", exc)

    def load_evidence_stats(self, evidence_id: int, evidence_label: str) -> None:
        """
        Load statistics for a specific evidence into cache (on evidence tab open).

        Clears existing cache entries for this evidence before loading to ensure
        stale data from purged or removed extractors is not displayed.
        """
        if not self._db_manager:
            return

        # Clear stale cache entries for this evidence first
        keys_to_remove = [
            key for key in self._cache.keys()
            if key[0] == evidence_id
        ]
        for key in keys_to_remove:
            del self._cache[key]

        from .database import get_extractor_statistics_by_evidence
        try:
            rows = get_extractor_statistics_by_evidence(self._db_manager, evidence_id, evidence_label)
            for row in rows:
                stats = ExtractorRunStats.from_row(row, evidence_label)
                self._cache[(stats.evidence_id, stats.extractor_name)] = stats
            LOGGER.debug("Loaded %d statistics records for evidence %d (cleared stale cache first)",
                        len(rows), evidence_id)
        except Exception as exc:
            LOGGER.warning("Failed to load statistics from database: %s", exc)
