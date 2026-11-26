"""File list discovery with all Chromium browsers selected.

With many browsers the path patterns of one artifact exceed what SQLite accepts
in one query (sessions: 1,800 patterns, cache: 2,016). The file list query must
still find files on other partitions, and a failed query must end the run with
an error instead of a search of the open partition.
"""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from extractors._shared import file_list_discovery as discovery_module
from extractors._shared.file_list_discovery import (
    FileListDiscoveryResult,
    FileListQueryError,
    MAX_PATTERNS_PER_QUERY,
    discover_from_file_list,
    glob_to_sql_like,
)
from extractors.browser.chromium._patterns import (
    CHROMIUM_ARTIFACTS,
    get_all_browsers,
    get_artifact_patterns,
    get_stable_browsers,
)
from extractors.browser.chromium.cache import extractor as cache_module
from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
from extractors.browser.chromium.sessions import extractor as sessions_module
from extractors.browser.chromium.sessions.extractor import ChromiumSessionsExtractor
from tests.fixtures.file_list_discovery import (
    EWF_PATHS,
    PartitionFS,
    add_to_file_list,
    make_partition_roots,
    mock_callbacks,
    patch_open_ewf_partition,
    QueryRecorder,
    write_file,
)

CHROME = "Users/alice/AppData/Local/Google/Chrome/User Data/Default"
SESSION_PATH = f"{CHROME}/Sessions/Session_13300000000000000"
CACHE_INDEX_PATH = f"{CHROME}/Cache/Cache_Data/index"
SESSION_CONTENT = b"SNSS\x03\x00\x00\x00"


def all_browser_patterns(artifact: str) -> list[str]:
    patterns = set()
    for browser in get_all_browsers():
        for pattern in get_artifact_patterns(browser, artifact):
            patterns.add(glob_to_sql_like(pattern))
    return sorted(patterns)


@pytest.fixture
def callbacks():
    return mock_callbacks()


@pytest.fixture
def roots(tmp_path):
    """Directories of partitions 1 and 2."""
    return make_partition_roots(tmp_path)


@pytest.fixture
def opened_partitions(monkeypatch, roots):
    """Replace open_ewf_partition; the dict maps partition index -> opened filesystem."""
    return patch_open_ewf_partition(monkeypatch, roots)


def failed_discovery(*args, **kwargs):
    return FileListDiscoveryResult(error="database is locked"), []


class TestPatternCounts:
    """No artifact's pattern count may make the file list query fail."""

    def test_sessions_and_cache_need_more_than_one_query(self):
        """The case this test file is about still exists."""
        assert len(all_browser_patterns("sessions")) > 1000
        assert len(all_browser_patterns("cache")) > 1000
        assert MAX_PATTERNS_PER_QUERY < 1000

    @pytest.mark.parametrize("artifact", sorted(CHROMIUM_ARTIFACTS))
    def test_query_for_all_browsers_runs(self, artifact, evidence_db, case_context):
        patterns = all_browser_patterns(artifact)
        if not patterns:
            pytest.skip(f"no path patterns for {artifact}")

        result = discover_from_file_list(evidence_db, case_context.evidence_id, path_patterns=patterns)

        assert result.error is None


class TestSameResultInOneOrSeveralQueries:
    """Splitting the patterns over several queries does not change the result."""

    def test_chunked_result_equals_single_query(self, evidence_db, case_context, monkeypatch):
        # Four browsers: below the limit, so one query runs; lower limits split it.
        patterns = sorted({
            glob_to_sql_like(pattern)
            for browser in ("chrome", "edge", "brave", "opera")
            for pattern in get_artifact_patterns(browser, "sessions")
        })
        assert 400 < len(patterns) < MAX_PATTERNS_PER_QUERY
        names = ["Current Session", "Last Session", "Session_*", "Tabs_*"]
        # Rows of several browsers and partitions, interleaved, plus rows that match nothing
        browsers_paths = [
            "Users/a/AppData/Local/Google/Chrome/User Data/Default/Sessions/Session_1",
            "Users/a/AppData/Local/Microsoft/Edge/User Data/Profile 1/Sessions/Tabs_2",
            "Users/a/AppData/Roaming/Opera Software/Opera Stable/Sessions/Session_3",
            "Users/a/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Current Session",
            "Users/a/Documents/Session_4",
            "Users/a/AppData/Local/Google/Chrome/User Data/Default/History",
        ]
        for index, path in enumerate(browsers_paths * 3):
            add_to_file_list(evidence_db, case_context.evidence_id, f"/{path}", 1 + index // len(browsers_paths))

        def run(limit):
            monkeypatch.setattr(discovery_module, "MAX_PATTERNS_PER_QUERY", limit)
            recorder = QueryRecorder(evidence_db)
            result = discover_from_file_list(
                recorder, case_context.evidence_id, filename_patterns=names, path_patterns=patterns
            )
            assert result.error is None
            return result, len(recorder.queries)

        single, single_queries = run(10_000)
        for limit in (7, 100, len(patterns) - 1):
            chunked, chunked_queries = run(limit)
            assert single_queries == 1 and chunked_queries > 1
            assert chunked.partitions_with_matches == single.partitions_with_matches
            assert chunked.total_matches == single.total_matches == 12
            for partition in single.partitions_with_matches:
                assert [(m.file_path, m.partition_index) for m in chunked.matches_by_partition[partition]] == [
                    (m.file_path, m.partition_index) for m in single.matches_by_partition[partition]
                ]


class TestNumberOfQueries:
    """Selections of up to 850 patterns, including the defaults, run one query."""

    @pytest.mark.parametrize(
        "artifact, browsers, names, queries",
        [
            ("sessions", get_stable_browsers(), ["Current Session", "Session_*"], 1),
            ("cache", get_stable_browsers(), ["index", "f_*"], 1),
            ("sessions", get_all_browsers(), ["Current Session", "Session_*"], 3),
            ("cache", get_all_browsers(), ["index", "f_*"], 3),
        ],
    )
    def test_queries_per_selection(self, artifact, browsers, names, queries, evidence_db, case_context):
        patterns = sorted({
            glob_to_sql_like(pattern)
            for browser in browsers
            for pattern in get_artifact_patterns(browser, artifact)
        })
        recorder = QueryRecorder(evidence_db)

        result = discover_from_file_list(
            recorder, case_context.evidence_id, filename_patterns=names, path_patterns=patterns
        )

        assert result.error is None
        assert len(recorder.queries) == queries


class TestSessionsAllBrowsers:
    """Chromium Session Restore with its default selection of all browsers."""

    def test_session_file_on_second_partition_is_extracted(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions
    ):
        write_file(roots[2], SESSION_PATH, SESSION_CONTENT)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 2)
        extractor = ChromiumSessionsExtractor()
        assert len(extractor.SUPPORTED_BROWSERS) >= 14
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = extractor.run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert ok is True
        assert manifest["status"] == "ok"
        assert manifest["partitions_scanned"] == [2]
        assert [(f["logical_path"], f["partition_index"]) for f in manifest["files"]] == [
            (f"/{SESSION_PATH}", 2)
        ]
        assert list(opened_partitions) == [2]
        assert opened_partitions[2].read_paths == [f"/{SESSION_PATH}"]
        assert fs.read_paths == [], "nothing may be read from the open partition"
        assert fs.scanned_patterns == [], "the file list had a match, so nothing is scanned"

    def test_failed_query_ends_with_error_and_scans_nothing(
        self, roots, tmp_path, callbacks, evidence_db, case_context, monkeypatch
    ):
        # The file is on the open partition: a search would find it.
        write_file(roots[1], SESSION_PATH, SESSION_CONTENT)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 1)
        monkeypatch.setattr(sessions_module, "discover_artifacts_with_embedded_roots", failed_discovery)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert ok is False
        assert manifest["status"] == "error"
        assert manifest["files"] == []
        assert len(manifest["notes"]) == 1
        assert "database is locked" in manifest["notes"][0]
        callbacks.on_error.assert_called_once()
        assert callbacks.on_error.call_args.args[1] == manifest["notes"][0]
        assert fs.scanned_patterns == []
        assert fs.read_paths == []

        # The error run is not output that Run All may reuse, and it is not ingested.
        extractor = ChromiumSessionsExtractor()
        assert extractor.has_existing_output(output_dir) is False
        can_ingest, reason = extractor.can_run_ingestion(output_dir)
        assert can_ingest is False
        assert "database is locked" in reason
        ingest_callbacks = mock_callbacks()
        counts = extractor.run_ingestion(output_dir, evidence_db, case_context.evidence_id, {}, ingest_callbacks)
        assert counts == {"windows": 0, "tabs": 0}
        ingest_callbacks.on_error.assert_called_once()
        assert "database is locked" in ingest_callbacks.on_error.call_args.args[1]

    def test_partition_that_cannot_be_opened_gives_partial(
        self, roots, tmp_path, callbacks, evidence_db, case_context, monkeypatch
    ):
        def failing_open(ewf_paths, partition_index=-1):
            raise RuntimeError("partition is encrypted")

        monkeypatch.setattr("core.evidence_fs.open_ewf_partition", failing_open)
        write_file(roots[1], SESSION_PATH, SESSION_CONTENT)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert ok is True
        assert manifest["status"] == "partial"
        assert [(f["logical_path"], f["partition_index"]) for f in manifest["files"]] == [
            (f"/{SESSION_PATH}", 1)
        ]
        assert len(manifest["notes"]) == 1
        assert "partition 2" in manifest["notes"][0]
        assert "partition is encrypted" in manifest["notes"][0]
        assert ChromiumSessionsExtractor().has_existing_output(output_dir) is True

    def test_file_that_cannot_be_copied_gives_partial(
        self, roots, tmp_path, callbacks, evidence_db, case_context
    ):
        """In the file list, but not on the partition."""
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert manifest["status"] == "partial"
        assert [f["copy_status"] for f in manifest["files"]] == ["error"]
        assert len(manifest["notes"]) == 1
        assert SESSION_PATH in manifest["notes"][0]

    def test_cancel_between_partitions_gives_cancelled(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions
    ):
        """Cancelled after the last file of partition 1: partition 2 is not processed."""
        for partition in (1, 2):
            write_file(roots[partition], SESSION_PATH, SESSION_CONTENT)
            add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", partition)
        # First check: before the file of partition 1; every later check: cancelled
        callbacks.is_cancelled = Mock(side_effect=[False] + [True] * 20)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert manifest["status"] == "cancelled"
        assert [f["partition_index"] for f in manifest["files"]] == [1]
        assert manifest["notes"] == ["Extraction cancelled by user"]
        assert opened_partitions == {}

    def test_embedded_root_too_long_for_a_query_gives_partial(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions
    ):
        long_root = "ProgramData/" + "_" * 25_000 + "/User Data"
        for name in ("History", "Cookies", "Sessions/Session_1"):
            add_to_file_list(evidence_db, case_context.evidence_id, f"{long_root}/Default/{name}", 2)
        write_file(roots[2], SESSION_PATH, SESSION_CONTENT)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{SESSION_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert ok is True
        assert manifest["status"] == "partial"
        assert [f["logical_path"] for f in manifest["files"]] == [f"/{SESSION_PATH}"]
        assert len(manifest["notes"]) == 1
        assert "too long" in manifest["notes"][0]
        assert "partition 2" in manifest["notes"][0]
        warnings = [c.args[0] for c in callbacks.on_log.call_args_list if c.args[1:] == ("warning",)]
        assert manifest["notes"][0] in warnings

    def test_no_match_still_searches_the_open_partition(
        self, roots, tmp_path, callbacks, evidence_db, case_context
    ):
        """Unchanged: a file list without a session file falls back to a search."""
        write_file(roots[1], SESSION_PATH, SESSION_CONTENT)
        add_to_file_list(evidence_db, case_context.evidence_id, "/Windows/notepad.exe", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = ChromiumSessionsExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id, "browsers": ["chrome"]},
            callbacks,
        )

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert ok is True
        assert [f["logical_path"] for f in manifest["files"]] == [SESSION_PATH]
        assert fs.scanned_patterns != []


class TestCacheAllBrowsers:
    """Chromium Cache: the partitions to open come from the file list."""

    def test_cache_on_second_partition_is_found(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{CACHE_INDEX_PATH}", 2)

        partitions = CacheSimpleExtractor()._discover_cache_partitions(
            evidence_conn=evidence_db,
            evidence_id=case_context.evidence_id,
            browsers=get_all_browsers(),
            include_disk_cache=True,
        )

        assert partitions == [2]

    def test_run_extracts_cache_from_second_partition(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions, monkeypatch
    ):
        """The whole run with all browsers: only partition 2 is opened and searched."""
        cache_dir = CACHE_INDEX_PATH.rsplit("/", 1)[0]
        for name in ("index", "f_000001"):
            write_file(roots[2], f"{cache_dir}/{name}", b"cache " + name.encode())
            add_to_file_list(evidence_db, case_context.evidence_id, f"/{cache_dir}/{name}", 2)

        handed_over = []

        def fake_concurrent(self, evidence_fs, files_to_extract, *args, **kwargs):
            handed_over.append((evidence_fs.partition_index, sorted(f["path"] for f, _ in files_to_extract)))

        monkeypatch.setattr(CacheSimpleExtractor, "_run_concurrent_extraction", fake_concurrent)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok = CacheSimpleExtractor().run_extraction(
            fs, tmp_path / "output",
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id, "browsers": get_all_browsers()},
            callbacks,
        )

        assert ok is True
        assert list(opened_partitions) == [2]
        assert opened_partitions[2].scanned_patterns != []
        assert fs.scanned_patterns == [], "the open partition has no cache in the file list"
        assert len(handed_over) == 1
        partition, paths = handed_over[0]
        assert partition == 2
        assert [p.rsplit("/", 1)[-1] for p in paths] == ["f_000001", "index"]

    def test_failed_embedded_root_query_ends_the_run_with_error(
        self, roots, tmp_path, callbacks, evidence_db, case_context
    ):
        """The first query of the run looks for embedded Chromium roots."""
        write_file(roots[1], CACHE_INDEX_PATH, b"index")
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{CACHE_INDEX_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = CacheSimpleExtractor().run_extraction(
            fs, output_dir,
            {
                "evidence_conn": QueryRecorder(evidence_db, fail_on_query=1),
                "evidence_id": case_context.evidence_id,
                "browsers": ["chrome"],
            },
            callbacks,
        )

        assert ok is False
        manifest = json.loads(next(output_dir.glob("*/manifest.json")).read_text())
        assert manifest["status"] == "error"
        assert "database is locked" in manifest["notes"][0]
        assert fs.scanned_patterns == []

        can_ingest, reason = CacheSimpleExtractor().can_run_ingestion(output_dir)
        assert can_ingest is False
        assert "database is locked" in reason
        ingest_callbacks = mock_callbacks()
        counts = CacheSimpleExtractor().run_ingestion(
            output_dir, evidence_db, case_context.evidence_id, {}, ingest_callbacks
        )
        assert counts == {"urls": 0, "images": 0, "records": 0}
        ingest_callbacks.on_error.assert_called_once()
        assert "database is locked" in ingest_callbacks.on_error.call_args.args[1]

    def test_failed_query_raises(self, evidence_db, case_context, monkeypatch):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{CACHE_INDEX_PATH}", 2)
        monkeypatch.setattr(cache_module, "discover_artifacts_with_embedded_roots", failed_discovery)

        with pytest.raises(FileListQueryError, match="database is locked"):
            CacheSimpleExtractor()._discover_cache_partitions(
                evidence_conn=evidence_db,
                evidence_id=case_context.evidence_id,
                browsers=["chrome"],
                include_disk_cache=True,
            )

    def test_failed_query_ends_the_run_with_error_and_scans_nothing(
        self, roots, tmp_path, callbacks, evidence_db, case_context, monkeypatch
    ):
        # The cache is on the open partition: a search would find it.
        write_file(roots[1], CACHE_INDEX_PATH, b"index")
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{CACHE_INDEX_PATH}", 1)
        monkeypatch.setattr(cache_module, "discover_artifacts_with_embedded_roots", failed_discovery)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = CacheSimpleExtractor().run_extraction(
            fs, output_dir,
            {"evidence_conn": evidence_db, "evidence_id": case_context.evidence_id, "browsers": ["chrome"]},
            callbacks,
        )

        assert ok is False
        manifests = list(output_dir.glob("*/manifest.json"))
        assert len(manifests) == 1
        manifest = json.loads(manifests[0].read_text())
        assert manifest["status"] == "error"
        assert manifest["files"] == []
        assert len(manifest["notes"]) == 1
        assert "database is locked" in manifest["notes"][0]
        callbacks.on_error.assert_called_once()
        assert callbacks.on_error.call_args.args[1] == manifest["notes"][0]
        assert fs.scanned_patterns == []
        assert fs.read_paths == []
