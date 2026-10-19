"""Cancellable, coalescing background query worker shared by feature tabs.

Contract (issue #60):
- A tab submits ``QueryRequest`` objects carrying a request generation, the
  evidence identity, and a feature-specific payload (filters, page state).
- One long-lived ``CancellableQueryWorker`` per tab runs a handler with a
  worker-owned ``CaseDataAccess``; the SQLite connection is created, reused,
  and closed on the worker thread only and never shared with other threads.
- The latest request wins: submitting replaces any queued request and cancels
  the running one. Cancellation is propagated into running SQLite reads via
  ``BaseDataAccess.cancellable_reads`` (progress handler).
- A process-wide ``ReadLimiter`` bounds concurrent reads across tabs.
- Results arrive as ``QueryResult`` on the GUI thread; the receiver must still
  reject results whose generation/evidence no longer match its state.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Set

from PySide6.QtCore import QThread, Signal

from app.data import CaseDataAccess, QueryCancelledError

logger = logging.getLogger(__name__)


class ReadLimiter:
    """Bounds the number of concurrently running background reads."""

    def __init__(self, max_concurrent: int) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self.max_concurrent = max_concurrent
        self._semaphore = threading.BoundedSemaphore(max_concurrent)

    def acquire(self, should_abort: Callable[[], bool], poll_interval_s: float = 0.05) -> bool:
        """Block until a slot is free; return False if ``should_abort`` turns true first."""
        while True:
            if should_abort():
                return False
            if self._semaphore.acquire(timeout=poll_interval_s):
                return True

    def release(self) -> None:
        self._semaphore.release()


# Default limiter shared by all workers that do not pass their own: at most
# this many interactive reads run at once across evidence tabs. Long-running
# bulk consumers (e.g. exports) should pass a separate ReadLimiter so they do
# not starve interactive page loads.
DEFAULT_MAX_CONCURRENT_READS = 2
_default_read_limiter = ReadLimiter(DEFAULT_MAX_CONCURRENT_READS)


def default_read_limiter() -> ReadLimiter:
    return _default_read_limiter


# Separate limiter for long-running background reads that should not hold the
# interactive slots, e.g. exact result totals (issue #61).
DEFAULT_MAX_CONCURRENT_BACKGROUND_READS = 2
_background_read_limiter = ReadLimiter(DEFAULT_MAX_CONCURRENT_BACKGROUND_READS)


def background_read_limiter() -> ReadLimiter:
    return _background_read_limiter


@dataclass(frozen=True)
class QueryRequest:
    generation: int
    evidence_id: int
    payload: Any


@dataclass(frozen=True)
class QueryResult:
    generation: int
    evidence_id: int
    payload: Any


QueryHandler = Callable[[Any, QueryRequest], Any]

# Workers that did not stop within the shutdown timeout are kept referenced
# here until their thread finishes (prevents QThread destruction while running).
_retiring_workers: Set["CancellableQueryWorker"] = set()


class CancellableQueryWorker(QThread):
    """Long-lived worker that runs the latest submitted request.

    Signals are emitted from the worker thread and delivered queued to
    receivers living on the GUI thread.
    """

    result_ready = Signal(object)  # QueryResult
    failed = Signal(int, int, str)  # generation, evidence_id, message

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        handler: QueryHandler,
        *,
        limiter: Optional[ReadLimiter] = None,
        data_access_factory: Optional[Callable[[], Any]] = None,
        name: str = "CancellableQueryWorker",
    ) -> None:
        super().__init__()
        self._handler = handler
        self._limiter = limiter or default_read_limiter()
        self._data_access_factory = data_access_factory or (
            lambda: CaseDataAccess(Path(case_folder), Path(case_db_path))
        )
        self._name = name
        self._cond = threading.Condition()
        self._pending: Optional[QueryRequest] = None
        self._active_cancel: Optional[threading.Event] = None
        self._stopping = False

    # GUI-thread API ----------------------------------------------------

    def submit(self, request: QueryRequest) -> None:
        """Queue ``request``, replacing any queued one and cancelling the running one."""
        with self._cond:
            if self._stopping:
                return
            self._pending = request
            if self._active_cancel is not None:
                self._active_cancel.set()
            self._cond.notify_all()
        if not self.isRunning() and not self.isFinished():
            self.start()

    def cancel(self) -> None:
        """Drop the queued request and cancel the running one."""
        with self._cond:
            self._pending = None
            if self._active_cancel is not None:
                self._active_cancel.set()

    def stop(self, timeout_ms: int = 2000) -> bool:
        """Cancel all work, end the thread, and wait for it.

        Returns True if the thread has finished. Otherwise the worker is kept
        alive until it finishes on its own (no forced termination, so the
        worker always closes its own connections).
        """
        with self._cond:
            self._stopping = True
            self._pending = None
            if self._active_cancel is not None:
                self._active_cancel.set()
            self._cond.notify_all()
        if not self.isRunning():
            return True
        if self.wait(timeout_ms):
            return True
        logger.warning("%s still stopping after %dms; keeping it alive until it ends", self._name, timeout_ms)
        _retiring_workers.add(self)
        self.finished.connect(lambda worker=self: _retiring_workers.discard(worker))
        if self.isFinished():
            # Finished between wait() and connect(): finished was already emitted.
            _retiring_workers.discard(self)
            return True
        return False

    # Worker thread -----------------------------------------------------

    def run(self) -> None:
        data_access = None
        try:
            while True:
                with self._cond:
                    while self._pending is None and not self._stopping:
                        self._cond.wait()
                    if self._stopping:
                        return
                    request = self._pending
                    self._pending = None
                    cancel_event = threading.Event()
                    self._active_cancel = cancel_event
                try:
                    if data_access is None:
                        data_access = self._data_access_factory()
                    self._execute(data_access, request, cancel_event)
                except Exception as exc:  # factory failure
                    logger.error("%s setup failed: %s", self._name, exc, exc_info=True)
                    if not self._is_cancelled(cancel_event):
                        self.failed.emit(request.generation, request.evidence_id, str(exc))
                finally:
                    with self._cond:
                        self._active_cancel = None
        finally:
            if data_access is not None:
                try:
                    data_access.close()
                except Exception as exc:
                    logger.warning("%s failed to close connections: %s", self._name, exc)

    def _is_cancelled(self, cancel_event: threading.Event) -> bool:
        return self._stopping or cancel_event.is_set()

    def _execute(self, data_access: Any, request: QueryRequest, cancel_event: threading.Event) -> None:
        def is_cancelled() -> bool:
            return self._is_cancelled(cancel_event)

        if not self._limiter.acquire(is_cancelled):
            logger.debug("%s: request gen %d superseded while queued", self._name, request.generation)
            return
        try:
            if is_cancelled():
                return
            with data_access.cancellable_reads(is_cancelled):
                payload = self._handler(data_access, request)
        except QueryCancelledError:
            logger.debug("%s: request gen %d interrupted", self._name, request.generation)
            return
        except Exception as exc:
            if is_cancelled():
                return
            logger.error("%s: request gen %d failed: %s", self._name, request.generation, exc, exc_info=True)
            self.failed.emit(request.generation, request.evidence_id, str(exc))
            return
        finally:
            self._limiter.release()

        if is_cancelled():
            return
        self.result_ready.emit(QueryResult(request.generation, request.evidence_id, payload))
