"""Background loader for the evidence Overview statistics (issue #63).

The Overview shows exact counts (``COUNT(*)`` over ``urls``, ``images``, ...)
that are expensive on large evidences. ``OverviewStatsLoader`` keeps them off
the GUI thread:

- ``request(evidence_id)`` first consults the shared case statistics cache
  (``CaseDataAccess.peek_overview_stats``, memory only). A hit is returned
  immediately and no worker activity happens.
- On a miss, one ``CancellableQueryWorker`` (latest request wins, cancellable
  reads) runs ``load_overview_stats`` on the *background* ``ReadLimiter``, so
  the counts never take the interactive slots used by URL page loads.
- Requests for the evidence already being loaded are coalesced while its data
  is unchanged.
- A result is emitted (``stats_ready``) only if its revision token is still
  current on arrival; otherwise the data changed while loading and the same
  evidence is loaded again.

One loader per open case; it lives on the GUI thread.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Hashable, Optional, Tuple

from PySide6.QtCore import QObject, Signal

from app.data import OverviewStats
from app.services.query_loader import (
    CancellableQueryWorker,
    QueryRequest,
    QueryResult,
    ReadLimiter,
    background_read_limiter,
)

logger = logging.getLogger(__name__)


def _load_overview(data_access: Any, request: QueryRequest) -> Tuple[OverviewStats, Hashable]:
    """Worker handler: overview stats plus the token taken before the reads."""
    return data_access.load_overview_stats(request.evidence_id)


class OverviewStatsLoader(QObject):
    """Loads Overview statistics for one case on a background worker.

    Signals (delivered on the GUI thread):
        stats_ready(evidence_id, OverviewStats): fresh stats for a request.
        stats_failed(evidence_id, message): the latest request failed.
    """

    stats_ready = Signal(int, object)
    stats_failed = Signal(int, str)

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        case_data: Any,
        parent: Optional[QObject] = None,
        *,
        limiter: Optional[ReadLimiter] = None,
        data_access_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        """
        Args:
            case_folder: Case workspace folder.
            case_db_path: Case database path.
            case_data: The GUI thread's ``CaseDataAccess``; only used for
                cache peeks and token checks (never for database reads).
            parent: Optional Qt parent.
            limiter: Read limiter (default: the shared background limiter).
            data_access_factory: Optional factory for the worker-owned data
                access (tests).
        """
        super().__init__(parent)
        self._case_data = case_data
        self._worker = CancellableQueryWorker(
            Path(case_folder),
            Path(case_db_path),
            _load_overview,
            limiter=limiter or background_read_limiter(),
            data_access_factory=data_access_factory,
            name="OverviewStatsLoader",
        )
        self._worker.result_ready.connect(self._on_result)
        self._worker.failed.connect(self._on_failed)
        self._generation = 0
        # Evidence of the queued/running request and the overview token seen
        # on the GUI thread when it was submitted (for coalescing).
        self._pending_evidence_id: Optional[int] = None
        self._pending_token: Optional[Hashable] = None
        self._shut_down = False

    # ------------------------------------------------------------------
    # GUI-thread API
    # ------------------------------------------------------------------

    @property
    def pending_evidence_id(self) -> Optional[int]:
        """Evidence whose stats are being loaded, or None when idle."""
        return self._pending_evidence_id

    def request(self, evidence_id: int) -> Optional[OverviewStats]:
        """Return cached stats, or start (or keep) a background load.

        Returns the stats on a cache hit (no worker activity). Otherwise
        returns None; ``stats_ready`` or ``stats_failed`` follows for this
        evidence unless another evidence is requested or ``cancel()`` /
        ``shutdown()`` is called first.
        """
        evidence_id = int(evidence_id)
        cached = self._case_data.peek_overview_stats(evidence_id)
        if cached is not None:
            return cached
        if self._shut_down:
            return None
        if (
            self._pending_evidence_id == evidence_id
            and self._pending_token is not None
            and self._case_data.is_overview_stats_current(evidence_id, self._pending_token)
        ):
            return None  # same evidence, unchanged data: already loading
        self._submit(evidence_id)
        return None

    def cancel(self) -> None:
        """Drop the queued request and cancel the running one.

        Results of earlier requests are ignored afterwards.
        """
        self._generation += 1
        self._clear_pending()
        if not self._shut_down:
            self._worker.cancel()

    def shutdown(self, timeout_ms: int = 2000) -> bool:
        """Stop the worker; later results are ignored.

        Returns True if the worker thread has finished (see
        ``CancellableQueryWorker.stop``).
        """
        self._shut_down = True
        self._generation += 1
        self._clear_pending()
        return self._worker.stop(timeout_ms)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _submit(self, evidence_id: int) -> None:
        self._generation += 1
        self._pending_evidence_id = evidence_id
        self._pending_token = self._case_data.overview_stats_token(evidence_id)
        self._worker.submit(QueryRequest(self._generation, evidence_id, None))

    def _clear_pending(self) -> None:
        self._pending_evidence_id = None
        self._pending_token = None

    def _on_result(self, result: QueryResult) -> None:
        if self._shut_down or result.generation != self._generation:
            return
        evidence_id = result.evidence_id
        self._clear_pending()
        stats, token = result.payload
        if self._case_data.is_overview_stats_current(evidence_id, token):
            self.stats_ready.emit(evidence_id, stats)
            return
        # The data changed while loading: load the same evidence again.
        logger.debug("Overview stats for evidence %s changed while loading; reloading", evidence_id)
        self._submit(evidence_id)

    def _on_failed(self, generation: int, evidence_id: int, message: str) -> None:
        if self._shut_down or generation != self._generation:
            return
        self._clear_pending()
        self.stats_failed.emit(evidence_id, message)
