"""Opt-in case-open benchmark (issue #63).

Opens a deterministic synthetic case (``tests/fixtures/url_fixtures.py``:
1000 domains, 4 sources, tags on every 100th URL, matches on every 20th URL
over 3 lists, plus a few image and OS indicator rows) in a real ``MainWindow``
(offscreen) and measures, per evidence count, foreground latency separately
from background completion:

- ``case_load_ms``: ``_open_case`` call until ``_apply_loaded_case`` starts
  (background ``CaseLoadTask``),
- ``apply_ui_ms``: wall time inside ``_apply_loaded_case`` (builds the evidence tabs),
- ``max_stall_ms`` / ``stall_total_ms``: longest gap between 5 ms heartbeat
  ticks on the GUI thread, and the sum of (gap - 5 ms) over gaps > 50 ms,
  from the open until settle,
- ``first_overview_ms``: from ``_apply_loaded_case`` start until the visible
  (first) evidence's Overview URL count shows a number,
- ``settle_ms``: until the overview is populated and the process is quiet,
  i.e. no stall > 50 ms, no active ``QThreadPool`` thread and < 5% of one core
  of process CPU during a 1.5 s window; ``settle_ms`` is the start of that
  window (capped at CASE_OPEN_BENCH_SETTLE_CAP_S, default 600 s;
  ``settle_timed_out`` otherwise),
- ``background_cpu_s``: process CPU seconds (all threads) from the open
  until settle,
- ``rss_mb_after_open`` / ``rss_delta_mb``: VmRSS after settle, and its growth
  since just before the open,
- with more than one evidence: ``switch_first_ms`` (switch to the last evidence
  until its Overview URL count shows a number) and ``switch_repeat_ms`` (back to
  the first and again to the last), each with the max stall from the switch
  until 250 ms after the number appeared,
- ``url_first_rows_ms`` / ``url_filters_ms``: selecting the URLs sub-tab of the
  current evidence until the first rows are shown / the domain dropdown is
  populated, with the max stall until 250 ms after both.

After each run the benchmark waits until the process is quiet (as for
settle), closes the window and waits again (``pre_close_wait_ms``,
``close_wait_ms``), so runs do not overlap. Every evidence count gets one
unmeasured warm-up open (CASE_OPEN_BENCH_WARMUP) (runs any pending
evidence DB migrations and warms the OS page cache), then ``REPEAT`` measured
opens, each in a fresh ``MainWindow``; medians are reported. The URL search
index build (issue #62) is disabled so the fixture case stays unchanged and
adds no background load. ``MainWindow`` gets a temporary ``base_dir`` with a
copy of the shipped ``config/`` (without the user's ``settings.json``), so the
user's settings, window geometry and logs are not touched. Only correctness
invariants are asserted (overview counts, URL rows shown), never timings.

Opt-in: skipped unless CASE_OPEN_BENCH_ROWS is set. Run explicitly:

    CASE_OPEN_BENCH_ROWS=1000000 QT_QPA_PLATFORM=offscreen poetry run pytest \\
        tests/gui/app/test_case_open_benchmark.py -m "slow and gui_offscreen" -s

To compare against an older commit, run from a git worktree of that commit
with ``PYTHONPATH=<worktree>/src:<worktree>`` and the same CASE_OPEN_BENCH_DIR;
the code under test (``app.__file__``, git commit) is recorded in the JSON.

Environment:
    CASE_OPEN_BENCH_ROWS       URL rows per evidence (required; 1_000_000 standard)
    CASE_OPEN_BENCH_EVIDENCES  comma-separated evidence counts (default "1,4,8")
    CASE_OPEN_BENCH_DIR        work folder; fixture cases in it are reused across runs
    CASE_OPEN_BENCH_OUTPUT     write JSON results to this path
    CASE_OPEN_BENCH_REPEAT     measured opens per evidence count (default 3)
    CASE_OPEN_BENCH_WARMUP     unmeasured warm-up opens per evidence count (default 1;
                               reported under "warmups")
    CASE_OPEN_BENCH_SETTLE_CAP_S  settle cap in seconds from the open (default 600)
"""
from __future__ import annotations

import bisect
import gc
import json
import os
import platform
import resource
import shutil
import sqlite3
import statistics
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
from PySide6.QtCore import QCoreApplication, QEvent, QEventLoop, Qt, QThreadPool, QTimer

from core.database import DatabaseManager
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = [
    pytest.mark.slow,
    pytest.mark.gui_offscreen,
    pytest.mark.skipif(
        not os.environ.get("CASE_OPEN_BENCH_ROWS"), reason="set CASE_OPEN_BENCH_ROWS to run the benchmark"
    ),
]

ROWS = int(os.environ.get("CASE_OPEN_BENCH_ROWS") or 0)
EVIDENCE_COUNTS = tuple(
    int(part) for part in (os.environ.get("CASE_OPEN_BENCH_EVIDENCES") or "1,4,8").split(",") if part.strip()
)
REPEAT = int(os.environ.get("CASE_OPEN_BENCH_REPEAT") or 3)
WARMUP = int(os.environ.get("CASE_OPEN_BENCH_WARMUP") or 1)
FIXTURE = dict(timestamp_every=10, tag_every=100, match_every=20, domain_count=1000)
IMAGE_ROWS = 2000  # per evidence
INDICATOR_ROWS = 200  # per evidence
FIXTURE_VERSION = 1  # bump when the fixture contents change

HEARTBEAT_MS = 5
STALL_MS = 50
QUIET_S = 1.5
QUIET_CPU_FRACTION = 0.05
# The old code (before issue #63) spends 25-70 s in _apply_loaded_case alone
# with 4-8 x 1M URLs and ~4 min in background work, so 60 s is too short.
SETTLE_CAP_S = float(os.environ.get("CASE_OPEN_BENCH_SETTLE_CAP_S") or 600)
STEP_TIMEOUT_S = 120.0
TAIL_S = 0.25
CLOSE_WAIT_CAP_S = 600.0

METRICS = (
    "case_load_ms",
    "apply_ui_ms",
    "max_stall_ms",
    "stall_total_ms",
    "first_overview_ms",
    "settle_ms",
    "background_cpu_s",
    "rss_mb_after_open",
    "rss_delta_mb",
    "switch_first_ms",
    "switch_first_max_stall_ms",
    "switch_repeat_ms",
    "switch_repeat_max_stall_ms",
    "url_first_rows_ms",
    "url_filters_ms",
    "url_max_stall_ms",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Heartbeat:
    """5 ms GUI-thread timer recording (wall, process CPU) per tick."""

    def __init__(self) -> None:
        self.walls: List[float] = []
        self.cpus: List[float] = []
        self.last_stall_end = 0.0
        self._timer = QTimer()
        self._timer.setTimerType(Qt.PreciseTimer)
        self._timer.setInterval(HEARTBEAT_MS)
        self._timer.timeout.connect(self.tick)

    def start(self) -> None:
        self.walls.clear()
        self.cpus.clear()
        self.last_stall_end = 0.0
        self.tick()
        self._timer.start()

    def stop(self) -> None:
        self._timer.stop()

    def tick(self) -> None:
        now = time.perf_counter()
        if self.walls and (now - self.walls[-1]) * 1000 > STALL_MS:
            self.last_stall_end = now
        self.walls.append(now)
        self.cpus.append(time.process_time())

    def stalls(self, t0: float, t1: float) -> Tuple[float, float]:
        """(max gap, sum of (gap - 5 ms) over gaps > 50 ms) in ms within [t0, t1]."""
        lo = bisect.bisect_left(self.walls, t0)
        hi = bisect.bisect_right(self.walls, t1)
        points = [t0] + self.walls[lo:hi] + [t1]
        gaps = [(b - a) * 1000 for a, b in zip(points, points[1:])]
        max_gap = max(gaps) if gaps else 0.0
        total = sum(g - HEARTBEAT_MS for g in gaps if g > STALL_MS)
        return round(max_gap, 1), round(total, 1)

    def cpu_at(self, wall: float) -> Tuple[float, float]:
        """(wall, cpu) of the last tick at or before ``wall`` (first tick if none)."""
        index = max(0, bisect.bisect_right(self.walls, wall) - 1)
        return self.walls[index], self.cpus[index]


def _pump(seconds: float) -> None:
    """Run the event loop for ``seconds`` (the heartbeat guarantees wake-ups)."""
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        QCoreApplication.processEvents(QEventLoop.AllEvents | QEventLoop.WaitForMoreEvents)


def _pump_until(predicate: Callable[[], bool], timeout_s: float, what: str) -> float:
    """Run the event loop until ``predicate()``; return the time it became true."""
    deadline = time.perf_counter() + timeout_s
    while True:
        if predicate():
            return time.perf_counter()
        if time.perf_counter() > deadline:
            raise AssertionError(f"timed out after {timeout_s:.0f}s waiting for {what}")
        QCoreApplication.processEvents(QEventLoop.AllEvents | QEventLoop.WaitForMoreEvents)


def _label_int(label: Any) -> Optional[int]:
    """Integer shown by a count label ("1,000,000"), None for "—" / "Loading…"."""
    if label is None:
        return None
    text = label.text().strip()
    for sep in (",", "'", "’", " ", "\xa0", " ", "."):
        text = text.replace(sep, "")
    return int(text) if text.isdigit() else None


def _overview_label(evidence_tabs: Any, name: str = "urls_count_label") -> Any:
    overview = evidence_tabs.property("overview_widget") if evidence_tabs is not None else None
    return overview.property(name) if overview is not None else None


def _overview_urls(evidence_tabs: Any) -> Optional[int]:
    return _label_int(_overview_label(evidence_tabs))


def _domain_filters_ready(urls_tab: Any) -> bool:
    """Domain dropdown populated: enabled, "All domains" ("*") first, plus domains."""
    combo = urls_tab.domain_combo
    return combo.isEnabled() and combo.count() > 1 and combo.itemData(0) == "*"


def _rss_mb() -> float:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)  # peak, KiB on Linux


def _ms(seconds: Optional[float]) -> Optional[float]:
    return None if seconds is None else round(seconds * 1000, 1)


def _median(values: List[Any]) -> Optional[float]:
    present = [v for v in values if v is not None]
    return round(statistics.median(present), 2) if present else None


def _code_root() -> Path:
    import app

    return Path(app.__file__).resolve().parents[2]


def _git(*args: str) -> Optional[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(_code_root()), *args], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _db_files_state(folder: Path) -> Dict[str, Tuple[int, int]]:
    return {
        str(path.relative_to(folder)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(folder.rglob("*.sqlite*"))
    }


def _folder_bytes(folder: Path) -> int:
    return sum(path.stat().st_size for path in folder.rglob("*") if path.is_file())


# ---------------------------------------------------------------------------
# Fixture cases
# ---------------------------------------------------------------------------


@dataclass
class _BenchCase:
    folder: Path
    db_path: Path
    evidence_count: int
    build_s: Optional[float]
    reused: bool
    bytes: int = 0
    evidence_ids: List[int] = field(default_factory=list)


def _add_images_and_indicators(folder: Path, db_path: Path) -> None:
    manager = DatabaseManager(folder, case_db_path=db_path)
    try:
        evidences = manager.get_case_conn().execute("SELECT id, label FROM evidences ORDER BY id").fetchall()
        for evidence_id, label in evidences:
            conn = manager.get_evidence_conn(evidence_id, label)
            with conn:
                conn.executemany(
                    "INSERT INTO images(evidence_id, rel_path, filename, md5, sha256, size_bytes, "
                    "first_discovered_by, first_discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        (evidence_id, f"Users/u/Pictures/img{i}.jpg", f"img{i}.jpg", f"{i:032x}",
                         f"{i:064x}", 1000 + i, "filesystem_images", "2025-01-01T00:00:00Z")
                        for i in range(IMAGE_ROWS)
                    ),
                )
                conn.executemany(
                    "INSERT INTO os_indicators(evidence_id, type, name, value, path, confidence, "
                    "detected_at_utc, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        (evidence_id, f"type{i % 5}", f"indicator{i}", f"value{i}", f"HKLM\\Bench\\{i}",
                         "high", "2025-01-01T00:00:00Z", "bench")
                        for i in range(INDICATOR_ROWS)
                    ),
                )
    finally:
        manager.close_all()


def _bench_case(work_dir: Path, evidence_count: int) -> _BenchCase:
    folder = work_dir / f"rows{ROWS}_ev{evidence_count}"
    db_path = folder / "bench_surfsifter.sqlite"
    marker = folder / ".case_open_bench_ok"
    spec = {"version": FIXTURE_VERSION, "rows": ROWS, "evidences": evidence_count, "fixture": FIXTURE,
            "images": IMAGE_ROWS, "indicators": INDICATOR_ROWS}
    info: Dict[str, Any] = {}
    if marker.exists():
        info = json.loads(marker.read_text())
        if info.get("spec") != spec:
            info = {}
    reused = bool(info)
    if not reused:
        if folder.exists():
            shutil.rmtree(folder)
        start = time.perf_counter()
        build_url_fixture_case(folder, rows_per_evidence=ROWS, evidence_count=evidence_count, **FIXTURE)
        _add_images_and_indicators(folder, db_path)
        info = {"spec": spec, "build_s": round(time.perf_counter() - start, 1)}
        marker.write_text(json.dumps(info))
        print(f"\n[bench] built {evidence_count} x {ROWS:,} URL rows in {info['build_s']}s at {folder}")
    case = _BenchCase(folder=folder, db_path=db_path, evidence_count=evidence_count,
                      build_s=info.get("build_s"), reused=reused, bytes=_folder_bytes(folder))
    conn = sqlite3.connect(db_path)
    try:
        case.evidence_ids = [row[0] for row in conn.execute("SELECT id FROM evidences ORDER BY id")]
    finally:
        conn.close()
    assert len(case.evidence_ids) == evidence_count
    return case


def _make_base_dir(tmp_path: Path) -> Path:
    """Temporary MainWindow base_dir with the shipped config (not the user's settings.json)."""
    base_dir = tmp_path / "base_dir"
    config_src = _code_root() / "config"
    config_dst = base_dir / "config"
    config_dst.mkdir(parents=True)
    for name in ("config.yml", "settings.defaults.json"):
        if (config_src / name).exists():
            shutil.copy2(config_src / name, config_dst / name)
    if (config_src / "branding").is_dir():
        shutil.copytree(config_src / "branding", config_dst / "branding")
    return base_dir


# ---------------------------------------------------------------------------
# One open
# ---------------------------------------------------------------------------


class _ApplyRecorder:
    def __init__(self) -> None:
        self.spans: List[Tuple[float, float]] = []


def _wait_quiet(heartbeat: _Heartbeat, cap_s: float) -> float:
    """Pump until no pool thread is active and process CPU < 5% of a core for 1.5 s.

    CPU-based on purpose: QThread workers are invisible to the pool, and idle
    loader threads wait inside app code, so thread inspection cannot tell busy
    from idle. Returns the seconds waited.
    """
    start = time.perf_counter()
    while time.perf_counter() - start < cap_s:
        _pump(0.2)
        now = time.perf_counter()
        if now - start < QUIET_S or QThreadPool.globalInstance().activeThreadCount():
            continue
        then_wall, then_cpu = heartbeat.cpu_at(now - QUIET_S)
        if (time.process_time() - then_cpu) / max(1e-9, now - then_wall) < QUIET_CPU_FRACTION:
            break
    return time.perf_counter() - start


def _close_window(window: Any, heartbeat: _Heartbeat) -> Dict[str, Any]:
    """Wait for background work, close the window, wait again, then delete it.

    Leftover workers would otherwise overlap (and slow down) the next run.
    Before issue #63 the Download tab's count workers (QThreads) have no
    shutdown and outlive closeEvent; closing or deleting the window while
    they still ran crashed the process (segfault in a native thread).
    """
    pre_close_wait = _wait_quiet(heartbeat, CLOSE_WAIT_CAP_S)
    window.close()
    _pump(0.1)
    start = time.perf_counter()
    QThreadPool.globalInstance().waitForDone(30000)
    _wait_quiet(heartbeat, CLOSE_WAIT_CAP_S)
    close_wait = time.perf_counter() - start
    window.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)
    _pump(0.1)
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)
    gc.collect()
    return {"pre_close_wait_ms": _ms(pre_close_wait), "close_wait_ms": _ms(close_wait)}


def _open_once(case: _BenchCase, base_dir: Path, heartbeat: _Heartbeat, recorder: _ApplyRecorder) -> Dict[str, Any]:
    from app.main import MainWindow

    gc.collect()
    window = MainWindow(base_dir)
    window.show()
    m: Dict[str, Any] = {}
    try:
        heartbeat.start()
        _pump(0.3)
        QThreadPool.globalInstance().waitForDone(30000)
        _pump(0.5)
        idle_wall, idle_cpu = heartbeat.walls[-1], heartbeat.cpus[-1]
        _pump(0.5)
        m["idle_cpu_fraction"] = round(
            (heartbeat.cpus[-1] - idle_cpu) / max(1e-9, heartbeat.walls[-1] - idle_wall), 4)
        rss_before = _rss_mb()
        recorder.spans.clear()

        # --- open ---------------------------------------------------------
        t_open = time.perf_counter()
        cpu_open = time.process_time()
        window._open_case(case.folder, case.db_path)

        t_overview: Optional[float] = None
        t_settle: Optional[float] = None
        deadline = t_open + SETTLE_CAP_S
        while True:
            now = time.perf_counter()
            if t_overview is None and recorder.spans and window.main_tabs.count() > 1:
                if _overview_urls(window.main_tabs.widget(1)) is not None:
                    t_overview = now
            if t_overview is not None and now - max(heartbeat.last_stall_end, t_open) >= QUIET_S:
                if QThreadPool.globalInstance().activeThreadCount() == 0:
                    then_wall, then_cpu = heartbeat.cpu_at(now - QUIET_S)
                    fraction = (time.process_time() - then_cpu) / max(1e-9, now - then_wall)
                    if fraction < QUIET_CPU_FRACTION:
                        t_settle = max(now - QUIET_S, t_overview)
                        break
            if now > deadline:
                break
            QCoreApplication.processEvents(QEventLoop.AllEvents | QEventLoop.WaitForMoreEvents)

        assert recorder.spans, "_apply_loaded_case was never called (case load failed?)"
        assert t_overview is not None, "overview URL count of the first evidence never showed a number"
        apply_start, apply_end = recorder.spans[0]
        m["case_load_ms"] = _ms(apply_start - t_open)
        m["apply_ui_ms"] = _ms(apply_end - apply_start)
        m["first_overview_ms"] = _ms(t_overview - apply_start)
        m["settle_timed_out"] = t_settle is None
        window_end = t_settle if t_settle is not None else time.perf_counter()
        m["settle_ms"] = _ms(t_settle - t_open) if t_settle is not None else None
        m["max_stall_ms"], m["stall_total_ms"] = heartbeat.stalls(t_open, window_end)
        # Timed out: the loop may end right after a long stall, before any new tick.
        cpu_settle = heartbeat.cpu_at(window_end)[1] if t_settle is not None else time.process_time()
        m["background_cpu_s"] = round(cpu_settle - cpu_open, 3)
        m["rss_mb_after_open"] = _rss_mb()
        m["rss_delta_mb"] = round(m["rss_mb_after_open"] - rss_before, 1)

        tabs = window.main_tabs
        assert tabs.count() == 1 + case.evidence_count, f"expected {case.evidence_count} evidence tabs"
        first = tabs.widget(1)
        assert _overview_urls(first) == ROWS, f"overview URL count {_overview_urls(first)} != {ROWS}"
        for name, expected in (("images_count_label", IMAGE_ROWS), ("indicators_count_label", INDICATOR_ROWS)):
            shown = _label_int(_overview_label(first, name))
            if shown is not None:
                assert shown == expected, f"overview {name} {shown} != {expected}"

        # --- evidence switching -----------------------------------------
        if case.evidence_count > 1:
            last_index = tabs.count() - 1

            def switch(index: int, what: str) -> Tuple[float, float]:
                target = tabs.widget(index)
                t0 = time.perf_counter()
                tabs.setCurrentIndex(index)
                t_done = _pump_until(lambda: _overview_urls(target) is not None, STEP_TIMEOUT_S, what)
                _pump(TAIL_S)
                assert _overview_urls(target) == ROWS, f"{what}: URL count {_overview_urls(target)} != {ROWS}"
                return _ms(t_done - t0), heartbeat.stalls(t0, time.perf_counter())[0]

            m["switch_first_ms"], m["switch_first_max_stall_ms"] = switch(last_index, "switch to last evidence")
            _pump(0.5)
            switch(1, "switch back to first evidence")
            _pump(0.5)
            m["switch_repeat_ms"], m["switch_repeat_max_stall_ms"] = switch(last_index, "repeat switch to last")
            _pump(0.5)

        # --- URL tab of the current evidence ----------------------------
        evidence_tabs = tabs.currentWidget()
        urls_tab = evidence_tabs.property("urls_tab")
        assert urls_tab is not None, "evidence tab has no urls_tab property"
        t0 = time.perf_counter()
        evidence_tabs.setCurrentWidget(urls_tab)
        t_rows: Optional[float] = None
        t_filters: Optional[float] = None

        def url_tab_done() -> bool:
            nonlocal t_rows, t_filters
            now = time.perf_counter()
            if t_rows is None and urls_tab.model.rowCount() > 0:
                t_rows = now
            if t_filters is None and _domain_filters_ready(urls_tab):
                t_filters = now
            return t_rows is not None and t_filters is not None

        _pump_until(url_tab_done, STEP_TIMEOUT_S, "URL tab first rows and domain filters")
        _pump(TAIL_S)
        m["url_first_rows_ms"] = _ms(t_rows - t0)
        m["url_filters_ms"] = _ms(t_filters - t0)
        m["url_max_stall_ms"] = heartbeat.stalls(t0, time.perf_counter())[0]
        assert 0 < urls_tab.model.rowCount() <= ROWS
        m["url_rows_shown"] = urls_tab.model.rowCount()
        m["domain_items"] = urls_tab.domain_combo.count()
    finally:
        m.update(_close_window(window, heartbeat))
        heartbeat.stop()
    return m


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def work_dir(tmp_path_factory) -> Path:
    folder_env = os.environ.get("CASE_OPEN_BENCH_DIR")
    folder = Path(folder_env) if folder_env else tmp_path_factory.mktemp("case_open_bench")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def test_case_open_benchmark(qapp, monkeypatch, tmp_path, work_dir: Path):
    from PySide6.QtWidgets import QMessageBox

    from app.features.urls.tab import UrlsTab
    from app.main import MainWindow

    # No URL search index build: keeps the fixture unchanged, adds no load.
    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)

    # Unexpected modal message boxes fail the run instead of blocking it.
    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))

    recorder = _ApplyRecorder()
    original_apply = MainWindow._apply_loaded_case

    def timed_apply(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            return original_apply(self, *args, **kwargs)
        finally:
            recorder.spans.append((start, time.perf_counter()))

    monkeypatch.setattr(MainWindow, "_apply_loaded_case", timed_apply)

    base_dir = _make_base_dir(tmp_path)
    heartbeat = _Heartbeat()
    results: Dict[str, Any] = {}
    fixtures: Dict[str, Any] = {}
    for evidence_count in EVIDENCE_COUNTS:
        case = _bench_case(work_dir, evidence_count)
        state_before = _db_files_state(case.folder)
        warmups = [_open_once(case, base_dir, heartbeat, recorder) for _ in range(WARMUP)]
        state_after_warmup = _db_files_state(case.folder)
        runs = [_open_once(case, base_dir, heartbeat, recorder) for _ in range(REPEAT)]
        state_after = _db_files_state(case.folder)
        results[str(evidence_count)] = {
            "median": {key: _median([run.get(key) for run in runs]) for key in METRICS},
            "runs": runs,
            "warmups": warmups,
        }
        fixtures[str(evidence_count)] = {
            "folder": str(case.folder),
            "bytes": case.bytes,
            "build_s": case.build_s,
            "reused": case.reused,
            "db_files_changed_by_warmup": sorted(
                k for k in set(state_before) | set(state_after_warmup)
                if state_before.get(k) != state_after_warmup.get(k)),
            "db_files_changed_by_runs": sorted(
                k for k in set(state_after_warmup) | set(state_after)
                if state_after_warmup.get(k) != state_after.get(k)),
        }

    import app

    output = {
        "environment": {
            "rows_per_evidence": ROWS,
            "evidence_counts": list(EVIDENCE_COUNTS),
            "repeat": REPEAT,
            "warmup": WARMUP,
            "settle_cap_s": SETTLE_CAP_S,
            "fixture": FIXTURE,
            "image_rows": IMAGE_ROWS,
            "indicator_rows": INDICATOR_ROWS,
            "app_file": app.__file__,
            "git_commit": _git("rev-parse", "HEAD"),
            "git_dirty_src": bool(_git("status", "--porcelain", "--", "src")),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "qt_platform": os.environ.get("QT_QPA_PLATFORM"),
        },
        "fixtures": fixtures,
        "results": results,
    }
    _print_report(output)
    path = os.environ.get("CASE_OPEN_BENCH_OUTPUT")
    if path:
        Path(path).write_text(json.dumps(output, indent=2))


def _print_report(output: Dict[str, Any]) -> None:
    print("\n[bench] environment:", json.dumps(output["environment"]))
    for count, info in output["fixtures"].items():
        print(f"[bench] fixture ev={count}: {json.dumps(info)}")
    counts = list(output["results"])
    print("[bench] " + f"{'metric (median)':28}" + "".join(f"{'ev=' + c:>12}" for c in counts))
    for key in METRICS:
        cells = []
        for c in counts:
            value = output["results"][c]["median"].get(key)
            cells.append(f"{'-' if value is None else value:>12}")
        print("[bench] " + f"{key:28}" + "".join(cells))
    for c in counts:
        timed_out = sum(1 for run in output["results"][c]["runs"] if run.get("settle_timed_out"))
        runs = output["results"][c]["runs"]
        idle = [run.get("idle_cpu_fraction") for run in runs]
        waits = [(run.get("pre_close_wait_ms"), run.get("close_wait_ms")) for run in runs]
        print(f"[bench] ev={c}: settle timeouts {timed_out}/{len(runs)}, idle cpu fraction {idle}, "
              f"wait ms before/after close {waits}")
