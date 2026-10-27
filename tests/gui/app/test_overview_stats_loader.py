"""GUI tests for OverviewStatsLoader (issue #63).

The worker-owned data access is a gated wrapper around a real
``CaseDataAccess`` of the fixture case (same shared statistics cache), so the
tests decide when a background load finishes.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List

import pytest

from app.data import STATS_TAGS, STATS_URL_MATCHES, STATS_URLS, CaseDataAccess, discard_case_stats_cache
from app.services.overview_stats import OverviewStatsLoader
from app.services.query_loader import ReadLimiter
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000


class _GatedDataAccess:
    """Worker data access: records loads and blocks each one on a gate if set."""

    def __init__(self, case, log: Dict[str, Any]) -> None:
        self._data = CaseDataAccess(case.case_folder, case.case_db_path)
        self._log = log

    @contextmanager
    def cancellable_reads(self, is_cancelled):
        with self._data.cancellable_reads(is_cancelled):
            yield

    def load_overview_stats(self, evidence_id: int):
        with self._log["lock"]:
            self._log["calls"].append(evidence_id)
            gate = self._log["gates"].pop(0) if self._log["gates"] else None
            gate_after_read = self._log.get("gate_after_read", False)
        if self._log.get("fail"):
            raise RuntimeError(self._log["fail"])
        if gate is not None and not gate_after_read:
            # Blocked before reading: nothing is cached meanwhile.
            self._log["waiting"].set()
            assert gate.wait(10), "test gate never opened"
        result = self._data.load_overview_stats(evidence_id)
        if gate is not None and gate_after_read:
            # Read (and cached) already; the result is still on its way.
            self._log["waiting"].set()
            assert gate.wait(10), "test gate never opened"
        return result

    def close(self) -> None:
        self._data.close()


@pytest.fixture
def loader_env(qtbot, tmp_path: Path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=40, evidence_count=2, tag_every=4, domain_count=5
    )
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    log: Dict[str, Any] = {
        "lock": threading.Lock(),
        "calls": [],
        "gates": [],
        "waiting": threading.Event(),
    }
    loader = OverviewStatsLoader(
        case.case_folder,
        case.case_db_path,
        data,
        limiter=ReadLimiter(1),
        data_access_factory=lambda: _GatedDataAccess(case, log),
    )
    ready: List[tuple] = []
    failed: List[tuple] = []
    loader.stats_ready.connect(lambda eid, stats: ready.append((eid, stats)))
    loader.stats_failed.connect(lambda eid, msg: failed.append((eid, msg)))
    submits: List[int] = []
    original_submit = loader._worker.submit

    def counting_submit(request):
        submits.append(request.evidence_id)
        original_submit(request)

    loader._worker.submit = counting_submit
    yield case, data, loader, log, ready, failed, submits
    loader.shutdown()
    data.close()
    discard_case_stats_cache(case.case_db_path)


def _gate(log) -> threading.Event:
    gate = threading.Event()
    log["waiting"].clear()
    log["gates"].append(gate)
    return gate


def test_load_emits_stats_and_second_request_is_a_cache_hit(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]

    assert loader.request(evidence_id) is None
    assert loader.pending_evidence_id == evidence_id
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    eid, stats = ready[0]
    assert eid == evidence_id
    assert stats.counts.urls == 40 and stats.tag_count == 2
    assert loader.pending_evidence_id is None
    assert submits == [evidence_id]

    # Cache hit: returned directly, no worker activity, no signal.
    assert loader.request(evidence_id) == stats
    qtbot.wait(50)
    assert submits == [evidence_id]
    assert log["calls"] == [evidence_id]
    assert len(ready) == 1 and failed == []


def test_duplicate_requests_are_coalesced(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    gate = _gate(log)

    loader.request(evidence_id)
    assert log["waiting"].wait(5)
    for _ in range(3):
        assert loader.request(evidence_id) is None
    assert submits == [evidence_id]

    gate.set()
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert log["calls"] == [evidence_id]
    assert len(ready) == 1


def test_request_after_unrelated_invalidation_still_coalesces(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    gate = _gate(log)
    loader.request(evidence_id)
    assert log["waiting"].wait(5)
    data.invalidate_stats(evidence_id, STATS_URL_MATCHES)  # not an overview category
    data.invalidate_stats(case.evidence_ids[1])  # other evidence
    loader.request(evidence_id)
    assert submits == [evidence_id]
    gate.set()
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert log["calls"] == [evidence_id]


def test_result_made_stale_while_loading_is_dropped_and_reloaded(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    log["gate_after_read"] = True
    gate = _gate(log)

    loader.request(evidence_id)
    assert log["waiting"].wait(5)
    # Data changes while the (already read) result is on its way.
    data.tag_artifact(evidence_id, "Fresh", "url", 1)
    gate.set()

    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    assert log["calls"] == [evidence_id, evidence_id]
    assert submits == [evidence_id, evidence_id]
    eid, stats = ready[0]
    assert eid == evidence_id
    assert stats.tag_count == 3  # the reload saw the new tag
    assert loader.request(evidence_id) == stats  # and it is cached


def test_request_with_changed_data_while_loading_resubmits(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    gate = _gate(log)
    loader.request(evidence_id)
    assert log["waiting"].wait(5)
    data.invalidate_stats(evidence_id, STATS_URLS)
    loader.request(evidence_id)  # token changed: not coalesced
    assert submits == [evidence_id, evidence_id]
    gate.set()
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert len(ready) == 1 and ready[0][0] == evidence_id


def test_switching_evidence_while_loading_drops_older_result(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    ev1, ev2 = case.evidence_ids
    gate = _gate(log)

    loader.request(ev1)
    assert log["waiting"].wait(5)
    loader.request(ev2)
    assert loader.pending_evidence_id == ev2
    gate.set()

    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(100)
    assert [eid for eid, _stats in ready] == [ev2]
    assert ev1 in log["calls"] and ev2 in log["calls"]


def test_cancel_drops_running_request(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    gate = _gate(log)
    loader.request(evidence_id)
    assert log["waiting"].wait(5)
    loader.cancel()
    assert loader.pending_evidence_id is None
    gate.set()
    qtbot.wait(200)
    assert ready == [] and failed == []

    # The loader stays usable after a cancel (the cancelled read may have
    # completed and filled the cache: start from changed data).
    data.invalidate_stats(evidence_id)
    assert loader.request(evidence_id) is None
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)


def test_failure_emits_stats_failed_for_latest_request_only(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    log["fail"] = "disk on fire"
    loader.request(evidence_id)
    qtbot.waitUntil(lambda: len(failed) == 1, timeout=TIMEOUT_MS)
    assert failed[0][0] == evidence_id and "disk on fire" in failed[0][1]
    assert ready == []
    assert loader.pending_evidence_id is None

    # A later request after the failure loads again.
    log["fail"] = None
    assert loader.request(evidence_id) is None
    qtbot.waitUntil(lambda: len(ready) == 1, timeout=TIMEOUT_MS)


def test_shutdown_ignores_late_results_and_further_requests(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    ev1, ev2 = case.evidence_ids
    gate = _gate(log)
    loader.request(ev1)
    assert log["waiting"].wait(5)

    stopper = threading.Timer(0.1, gate.set)
    stopper.start()
    try:
        assert loader.shutdown(timeout_ms=5000)
    finally:
        stopper.cancel()
        gate.set()
    qtbot.wait(100)
    assert ready == [] and failed == []
    assert loader.request(ev2) is None
    qtbot.wait(100)
    assert submits == [ev1]
    assert ready == []


def test_shutdown_still_serves_cache_hits(qtbot, loader_env):
    case, data, loader, log, ready, failed, submits = loader_env
    evidence_id = case.evidence_ids[0]
    expected = data.get_overview_stats(evidence_id)
    loader.shutdown()
    assert loader.request(evidence_id) == expected
    assert submits == []
