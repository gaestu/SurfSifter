"""Background build of the URL substring search index (issue #62).

``UrlSearchIndexWorker`` runs ``CaseDataAccess.build_url_search_index`` for one
evidence on its own thread with a worker-owned data access object (the SQLite
connection is created, used, and closed on the worker thread only).

- Cancellation is checked between batches; every batch is its own write
  transaction, so cancelling loses at most one batch and the next worker
  resumes where this one stopped.
- Search results never depend on the build: URLs that are not indexed yet are
  scanned. The signals only drive the status display.
"""
from __future__ import annotations

import logging
import threading
import time
import weakref
from pathlib import Path
from typing import Any, Callable, Optional

from PySide6.QtCore import QThread, Signal

from app.data import CaseDataAccess, UrlSearchIndexStatus
from app.services.query_loader import wait_or_retire

logger = logging.getLogger(__name__)

# Workers whose run() has not returned (weak: lifetime is owned elsewhere).
_running_workers: "weakref.WeakSet[UrlSearchIndexWorker]" = weakref.WeakSet()
_running_lock = threading.Lock()


def wait_for_url_search_index_builds(timeout_ms: int) -> bool:
    """Wait up to ``timeout_ms`` for every running build, including stopped ones.

    Callers cancel the builds first (e.g. ``UrlsTab.pause_search_index_build``);
    this also covers builds of tabs that were already closed.

    Returns:
        True if no build is running anymore.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    with _running_lock:
        workers = list(_running_workers)
    for worker in workers:
        remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
        if remaining_ms:
            worker.wait(remaining_ms)
    with _running_lock:
        return not any(worker.isRunning() for worker in _running_workers)


class UrlSearchIndexWorker(QThread):
    """Builds the URL search index of one evidence until ready or cancelled."""

    progress = Signal(int, object)  # evidence_id, UrlSearchIndexStatus
    completed = Signal(int, object)  # evidence_id, UrlSearchIndexStatus (last state)
    failed = Signal(int, str)  # evidence_id, message

    # Minimum seconds between progress signals.
    PROGRESS_INTERVAL_S = 0.25

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        evidence_id: int,
        *,
        data_access_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        super().__init__()
        self.evidence_id = evidence_id
        self._data_access_factory = data_access_factory or (
            lambda: CaseDataAccess(Path(case_folder), Path(case_db_path))
        )
        self._cancel = threading.Event()

    def cancel(self) -> None:
        """Stop after the running batch."""
        self._cancel.set()

    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def stop(self, timeout_ms: int = 2000) -> bool:
        """Cancel and wait up to ``timeout_ms`` (0: do not wait) for the thread.

        Returns True if the thread has finished. Otherwise the worker is kept
        alive until it finishes on its own (no forced termination, so the
        running batch commits or rolls back and the worker closes its own
        connections).
        """
        self.cancel()
        return wait_or_retire(self, timeout_ms, "UrlSearchIndexWorker")

    def run(self) -> None:
        with _running_lock:
            _running_workers.add(self)
        try:
            self._run()
        finally:
            with _running_lock:
                _running_workers.discard(self)

    def _run(self) -> None:
        data_access = None
        last_emit = 0.0

        def on_progress(status: UrlSearchIndexStatus) -> None:
            nonlocal last_emit
            now = time.monotonic()
            if now - last_emit >= self.PROGRESS_INTERVAL_S and not self.is_cancelled():
                last_emit = now
                self.progress.emit(self.evidence_id, status)

        try:
            data_access = self._data_access_factory()
            status = data_access.build_url_search_index(
                self.evidence_id, is_cancelled=self.is_cancelled, progress_callback=on_progress
            )
            if not self.is_cancelled():
                self.completed.emit(self.evidence_id, status)
        except Exception as exc:
            logger.error("URL search index build failed for evidence %s: %s", self.evidence_id, exc, exc_info=True)
            if not self.is_cancelled():
                self.failed.emit(self.evidence_id, str(exc))
        finally:
            if data_access is not None:
                try:
                    data_access.close()
                except Exception as exc:
                    logger.warning("UrlSearchIndexWorker failed to close connections: %s", exc)
