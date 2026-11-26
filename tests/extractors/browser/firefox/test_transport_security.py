"""Tests for the Firefox HSTS extractor (SiteSecurityServiceState.txt).

Discovery uses the file list only. Two directories stand for partitions 1 and
2 of an image: ``PartitionFS`` reports a partition index and EWF paths, and
``core.evidence_fs.open_ewf_partition`` is replaced so that it returns the
directory of the requested partition.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from core.database.helpers.extracted_files import get_extracted_files
from core.database.helpers.extraction_warnings import get_extraction_warnings_by_run
from core.database.helpers.hsts import get_hsts_entries
from core.evidence_fs import MountedFS
from extractors._shared.file_list_discovery import FileListDiscoveryResult
from extractors.browser.firefox.transport_security import FirefoxTransportSecurityExtractor
from extractors.browser.firefox.transport_security import extractor as extractor_module
from extractors.browser.firefox.transport_security._parsers import parse_transport_security_file
from tests.fixtures.file_list_discovery import (
    EWF_PATHS,
    PartitionFS,
    add_to_file_list,
    make_partition_roots,
    mock_callbacks,
    patch_open_ewf_partition,
    write_file,
)

PROFILES = "Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles"
HSTS_PATH = f"{PROFILES}/abc123.default-release/SiteSecurityServiceState.txt"
ESR_PATH = f"{PROFILES}/xyz789.default-esr/SiteSecurityServiceState.txt"
TOR_PATH = (
    "Users/alice/Desktop/Tor Browser/Browser/TorBrowser/Data/Browser/"
    "profile.default/SiteSecurityServiceState.txt"
)

CONTENT_P1 = b"partition-one.example:HSTS\t0\t19000\t1893456000000,1,0\n"
CONTENT_P2 = b"partition-two.example:HSTS\t0\t19001\t1893456000000,1,1\n"


@pytest.fixture
def extractor():
    return FirefoxTransportSecurityExtractor()


@pytest.fixture
def callbacks():
    return mock_callbacks()


@pytest.fixture
def roots(tmp_path):
    """Directories of partitions 1 and 2."""
    return make_partition_roots(tmp_path)


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "output"


@pytest.fixture
def opened_partitions(monkeypatch, roots):
    """Replace open_ewf_partition; the dict maps partition index -> opened filesystem."""
    return patch_open_ewf_partition(monkeypatch, roots, fs_class=PartitionFS)


def run_extraction(extractor, evidence_fs, output_dir, callbacks, evidence_conn, evidence_id, **config):
    ok = extractor.run_extraction(
        evidence_fs,
        output_dir,
        {"evidence_conn": evidence_conn, "evidence_id": evidence_id, **config},
        callbacks,
    )
    manifest = json.loads((output_dir / "manifest.json").read_text())
    return ok, manifest


def logged(callbacks, level: str) -> list[str]:
    """Messages passed to callbacks.on_log with the given level."""
    messages = []
    for call in callbacks.on_log.call_args_list:
        call_level = call.args[1] if len(call.args) > 1 else call.kwargs.get("level", "info")
        if call_level == level:
            messages.append(call.args[0])
    return messages


class TestFileListRequired:
    """Without a file list the run ends with an error and scans nothing."""

    def test_empty_file_list_is_an_error(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is False
        assert manifest["status"] == "error"
        assert manifest["files"] == []
        assert any("File List extractor" in note for note in manifest["notes"])
        callbacks.on_error.assert_called_once()
        assert "File List extractor" in callbacks.on_error.call_args.args[1]
        assert fs.scanned_patterns == []
        assert fs.read_paths == []

    def test_no_database_connection_is_an_error(self, extractor, roots, output_dir, callbacks):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(extractor, fs, output_dir, callbacks, None, 1)

        assert ok is False
        assert manifest["status"] == "error"
        assert any("File List extractor" in note for note in manifest["notes"])
        callbacks.on_error.assert_called_once()
        assert fs.scanned_patterns == []
        assert fs.read_paths == []

    def test_file_list_without_match_is_skipped(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        """The file is on disk but not in the file list: no fallback scan."""
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, "/Windows/notepad.exe", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "skipped"
        assert manifest["files"] == []
        assert any("found in the file list" in note for note in manifest["notes"])
        callbacks.on_error.assert_not_called()
        assert fs.scanned_patterns == []
        assert fs.read_paths == []


class TestExtractionPerPartition:
    """Every file is read from the partition the file list gives for it."""

    def test_file_on_open_partition(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "ok"
        assert manifest["notes"] == []
        assert len(manifest["files"]) == 1

        entry = manifest["files"][0]
        assert entry["copy_status"] == "ok"
        assert entry["logical_path"] == f"/{HSTS_PATH}"
        assert entry["partition_index"] == 1
        assert entry["browser"] == "firefox"
        assert entry["user"] == "alice"
        assert entry["profile"] == "abc123.default-release"
        assert entry["md5"] == hashlib.md5(CONTENT_P1).hexdigest()
        assert entry["sha256"] == hashlib.sha256(CONTENT_P1).hexdigest()
        assert entry["fs_type"] == "fs-of-partition-1"
        assert entry["forensic_path"] == f"p1:/{HSTS_PATH}"
        assert Path(entry["extracted_path"]).read_bytes() == CONTENT_P1
        assert Path(entry["extracted_path"]).name == entry["local_filename"]
        assert manifest["skipped_files"] == []

        assert opened_partitions == {}, "the open partition must not be opened a second time"
        assert fs.closed is False, "the caller's filesystem must stay open"
        assert fs.scanned_patterns == []

    def test_file_only_on_second_partition(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        write_file(roots[2], HSTS_PATH, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "ok"
        assert len(manifest["files"]) == 1

        entry = manifest["files"][0]
        assert entry["copy_status"] == "ok"
        assert entry["partition_index"] == 2
        assert entry["md5"] == hashlib.md5(CONTENT_P2).hexdigest()
        assert entry["fs_type"] == "fs-of-partition-2"
        assert entry["forensic_path"] == f"p2:/{HSTS_PATH}"

        assert list(opened_partitions) == [2]
        assert opened_partitions[2].read_paths == [f"/{HSTS_PATH}"]
        assert opened_partitions[2].closed is True
        assert fs.read_paths == [], "nothing may be read from the open partition"

    def test_same_path_on_both_partitions(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        """Different content under the same path: each hash belongs to its partition."""
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        write_file(roots[2], HSTS_PATH, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "ok"
        by_partition = {entry["partition_index"]: entry for entry in manifest["files"]}
        assert sorted(by_partition) == [1, 2]
        assert by_partition[1]["md5"] == hashlib.md5(CONTENT_P1).hexdigest()
        assert by_partition[2]["md5"] == hashlib.md5(CONTENT_P2).hexdigest()
        assert by_partition[1]["extracted_path"] != by_partition[2]["extracted_path"]
        assert Path(by_partition[1]["extracted_path"]).read_bytes() == CONTENT_P1
        assert Path(by_partition[2]["extracted_path"]).read_bytes() == CONTENT_P2

    def test_ingestion_records_partition_index(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        write_file(roots[2], HSTS_PATH, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        run_extraction(extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id)

        counts = extractor.run_ingestion(
            output_dir, evidence_db, case_context.evidence_id, {}, callbacks
        )

        assert counts == {"urls": 1, "hsts_entries": 1}
        rows = get_hsts_entries(evidence_db, case_context.evidence_id)
        assert len(rows) == 1
        assert rows[0]["decoded_host"] == "partition-two.example"
        assert rows[0]["mode"] == "force-https"
        assert rows[0]["include_subdomains"] == 1
        assert rows[0]["partition_index"] == 2
        assert rows[0]["fs_type"] == "fs-of-partition-2"
        assert rows[0]["logical_path"] == f"/{HSTS_PATH}"
        assert rows[0]["forensic_path"] == f"p2:/{HSTS_PATH}"

        url_notes = [row[0] for row in evidence_db.execute("SELECT notes FROM urls")]
        assert len(url_notes) == 1
        assert "partition: 2" in url_notes[0]

        # Ingesting the same run again replaces its rows instead of adding to them
        extractor.run_ingestion(output_dir, evidence_db, case_context.evidence_id, {}, callbacks)
        assert len(get_hsts_entries(evidence_db, case_context.evidence_id)) == 1
        assert evidence_db.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 1

    def test_ingestion_records_schema_warnings(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        """A line that cannot be parsed is recorded as an extraction warning."""
        write_file(roots[1], HSTS_PATH, CONTENT_P1 + b"not a valid line\n")
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        counts = extractor.run_ingestion(
            output_dir, evidence_db, case_context.evidence_id, {}, callbacks
        )

        assert counts == {"urls": 1, "hsts_entries": 1}
        warnings = get_extraction_warnings_by_run(
            evidence_db, case_context.evidence_id, extractor.metadata.name, manifest["run_id"]
        )
        assert len(warnings) == 1
        assert warnings[0]["warning_type"] == "format_error"
        assert warnings[0]["source_file"] == f"/{HSTS_PATH}"
        rows = get_hsts_entries(evidence_db, case_context.evidence_id)
        assert [row["partition_index"] for row in rows] == [1]

    def test_browser_comes_from_the_path_and_selection_filters(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        for path in (HSTS_PATH, ESR_PATH, TOR_PATH):
            write_file(roots[1], path, CONTENT_P1)
            add_to_file_list(evidence_db, case_context.evidence_id, f"/{path}", 1)

        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )
        assert {e["logical_path"]: (e["browser"], e["user"], e["profile"]) for e in manifest["files"]} == {
            f"/{HSTS_PATH}": ("firefox", "alice", "abc123.default-release"),
            f"/{ESR_PATH}": ("firefox_esr", "alice", "xyz789.default-esr"),
            f"/{TOR_PATH}": ("tor", "alice", "profile.default"),
        }

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id,
            browsers=["tor"],
        )
        assert [e["logical_path"] for e in manifest["files"]] == [f"/{TOR_PATH}"]


class TestPartitionCannotBeOpened:
    """A path is never read from the open filesystem for another partition."""

    def test_other_partition_without_ewf_paths_is_skipped(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        # The same path exists on the open filesystem and must not be copied.
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        write_file(roots[2], HSTS_PATH, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        fs = PartitionFS(roots[1], 1, ewf_paths=None)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "partial"
        assert manifest["files"] == []
        assert len(manifest["notes"]) == 1
        assert "partition 2" in manifest["notes"][0]
        assert "no EWF paths" in manifest["notes"][0]
        assert manifest["notes"][0] in logged(callbacks, "warning")
        assert [(f["logical_path"], f["partition_index"]) for f in manifest["skipped_files"]] == [
            (f"/{HSTS_PATH}", 2)
        ]
        assert fs.read_paths == []
        assert opened_partitions == {}

    def test_opened_filesystem_reports_another_partition(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch
    ):
        """The filesystem that was opened is not the partition that was asked for."""
        def open_wrong_partition(ewf_paths, partition_index=-1):
            return PartitionFS(roots[1], 0, ewf_paths)

        monkeypatch.setattr("core.evidence_fs.open_ewf_partition", open_wrong_partition)

        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "partial"
        assert manifest["files"] == []
        assert "reports partition 0" in manifest["notes"][0]
        assert len(manifest["skipped_files"]) == 1


class TestRowsWithoutPartitionNumber:
    """Rows without a partition number are looked up on the open filesystem."""

    @pytest.mark.parametrize("listed_partition", [-1, 0])
    @pytest.mark.parametrize("listed_path", [HSTS_PATH, f"/{HSTS_PATH}"], ids=["relative", "absolute"])
    def test_directory_evidence(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context,
        listed_partition, listed_path,
    ):
        """A mounted directory reads both path forms and reports partition 0."""
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, listed_path, listed_partition)
        fs = MountedFS(roots[1])

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "ok"
        assert len(manifest["files"]) == 1
        entry = manifest["files"][0]
        assert entry["copy_status"] == "ok"
        assert entry["logical_path"] == listed_path
        assert entry["partition_index"] == 0
        assert entry["user"] == "alice"
        assert entry["md5"] == hashlib.md5(CONTENT_P1).hexdigest()
        # 0 is the mounted directory's own index; only -1 is "no number" here.
        assert len(manifest["notes"]) == (1 if listed_partition == -1 else 0)

    def test_ewf_evidence_records_the_partition_that_was_read(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        write_file(roots[2], HSTS_PATH, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", -1, inode="111")
        fs = PartitionFS(roots[2], 2, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "ok"
        assert len(manifest["files"]) == 1
        entry = manifest["files"][0]
        assert entry["partition_index"] == 2
        assert entry["listed_partition_index"] == -1
        assert entry["inode"] is None, "the row does not say which partition its inode belongs to"
        assert entry["forensic_path"] == f"p2:/{HSTS_PATH}"
        assert entry["fs_type"] == "fs-of-partition-2"
        assert entry["md5"] == hashlib.md5(CONTENT_P2).hexdigest()
        assert opened_partitions == {}, "no partition can be opened for a row without a number"
        assert len(manifest["notes"]) == 1
        assert "no partition number" in manifest["notes"][0]
        assert manifest["notes"][0] in logged(callbacks, "warning")

    def test_file_listed_with_and_without_number_is_copied_once(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, HSTS_PATH, -1, inode="999")
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1, inode="4711-128-3")
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert len(manifest["files"]) == 1
        entry = manifest["files"][0]
        assert entry["partition_index"] == 1
        # The numbered row is the one that is kept.
        assert entry["listed_partition_index"] == 1
        assert entry["logical_path"] == f"/{HSTS_PATH}"
        assert entry["inode"] == 4711
        assert manifest["notes"] == []


class TestPartitionOpenFailure:
    """A partition that fails to open does not stop the other partitions."""

    def test_partition_that_fails_to_open_is_skipped(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch
    ):
        def failing_open(ewf_paths, partition_index=-1):
            raise RuntimeError("partition is encrypted")

        monkeypatch.setattr("core.evidence_fs.open_ewf_partition", failing_open)

        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        write_file(roots[1], ESR_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{ESR_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "partial"
        # The open partition's file is still extracted; partition 2's path is not read.
        assert [e["logical_path"] for e in manifest["files"]] == [f"/{HSTS_PATH}"]
        assert fs.read_paths == [f"/{HSTS_PATH}"]
        assert len(manifest["notes"]) == 1
        assert "partition 2" in manifest["notes"][0]
        assert "partition is encrypted" in manifest["notes"][0]
        assert [(f["logical_path"], f["partition_index"]) for f in manifest["skipped_files"]] == [
            (f"/{ESR_PATH}", 2)
        ]
        assert manifest["notes"][0] in logged(callbacks, "warning")


class TestFailuresAreVisible:
    """Failures reach the evidence log and the manifest."""

    @pytest.mark.parametrize(
        "discovery",
        [
            lambda *args, **kwargs: FileListDiscoveryResult(error="no such column: file_name"),
            Mock(side_effect=TypeError("unexpected keyword argument 'pattern'")),
        ],
        ids=["failed-query", "exception"],
    )
    def test_failed_discovery(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch, discovery
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        monkeypatch.setattr(extractor_module, "discover_from_file_list", discovery)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is False
        assert manifest["status"] == "error"
        assert manifest["files"] == []
        assert len(manifest["notes"]) == 1
        assert "discovery" in manifest["notes"][0]
        callbacks.on_error.assert_called_once()
        assert callbacks.on_error.call_args.args[1] == manifest["notes"][0]
        assert fs.scanned_patterns == []

    def test_file_that_cannot_be_copied(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        """In the file list but not readable: not `ok` without a note."""
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "partial"
        assert len(manifest["files"]) == 1
        entry = manifest["files"][0]
        assert entry["copy_status"] == "error"
        assert entry["partition_index"] == 1
        assert entry["artifact_type"] == "transport_security"
        assert entry["forensic_path"] == f"p1:/{HSTS_PATH}"
        assert len(manifest["notes"]) == 1
        assert HSTS_PATH in manifest["notes"][0]
        assert any(HSTS_PATH in message for message in logged(callbacks, "error"))

    def test_failed_copies_on_two_partitions_are_both_audited(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, opened_partitions
    ):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 2)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert manifest["status"] == "partial"
        audited = get_extracted_files(evidence_db, case_context.evidence_id)
        assert sorted(row["partition_index"] for row in audited) == [1, 2]
        assert {row["status"] for row in audited} == {"error"}
        assert {row["file_type"] for row in audited} == {"transport_security"}

    def test_file_above_the_size_limit_is_not_copied(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch
    ):
        monkeypatch.setattr(extractor_module, "MAX_HSTS_FILE_BYTES", len(CONTENT_P1) - 1)
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "partial"
        assert manifest["files"][0]["copy_status"] == "error"
        assert "safety limit" in manifest["notes"][0]
        assert fs.read_paths == [], "the size is checked before the file is read"
        assert [path.name for path in output_dir.iterdir()] == ["manifest.json"]

    def test_copy_above_the_size_limit_is_not_ingested(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch
    ):
        write_file(roots[1], HSTS_PATH, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{HSTS_PATH}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        run_extraction(extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id)
        callbacks.on_error.assert_not_called()

        monkeypatch.setattr(extractor_module, "MAX_HSTS_FILE_BYTES", len(CONTENT_P1) - 1)
        counts = extractor.run_ingestion(
            output_dir, evidence_db, case_context.evidence_id, {}, callbacks
        )

        assert counts == {"urls": 0, "hsts_entries": 0}
        callbacks.on_error.assert_called_once()
        assert "safety limit" in callbacks.on_error.call_args.args[0]


class TestOutputNames:
    """Copies never overwrite each other and names stay within file name limits."""

    def test_repeated_name_gets_a_counter(self):
        used: set[str] = set()
        unique = FirefoxTransportSecurityExtractor._unique_filename

        assert unique("p1_firefox_alice_prof_abcd1234", used) == (
            "p1_firefox_alice_prof_abcd1234_SiteSecurityServiceState.txt"
        )
        assert unique("p1_firefox_alice_prof_abcd1234", used) == (
            "p1_firefox_alice_prof_abcd1234_2_SiteSecurityServiceState.txt"
        )
        # Letter case does not make a name new: the workspace can be case-insensitive.
        assert unique("P1_firefox_ALICE_prof_abcd1234", used) == (
            "P1_firefox_ALICE_prof_abcd1234_3_SiteSecurityServiceState.txt"
        )

    def test_same_name_for_two_files_keeps_both_copies(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context, monkeypatch
    ):
        """Two paths whose path hashes collide are both kept."""
        first = f"{PROFILES}/prof/a/SiteSecurityServiceState.txt"
        second = f"{PROFILES}/prof/b/SiteSecurityServiceState.txt"
        write_file(roots[1], first, CONTENT_P1)
        write_file(roots[1], second, CONTENT_P2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{first}", 1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{second}", 1)

        real_sha256 = hashlib.sha256
        colliding = {f"/{first}".encode(), f"/{second}".encode()}

        def sha256_with_collision(data=b""):
            return real_sha256(b"same" if data in colliding else data)

        monkeypatch.setattr(extractor_module.hashlib, "sha256", sha256_with_collision)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )
        monkeypatch.undo()

        assert manifest["status"] == "ok"
        paths = [entry["extracted_path"] for entry in manifest["files"]]
        assert len(set(paths)) == 2
        for entry in manifest["files"]:
            assert hashlib.sha256(Path(entry["extracted_path"]).read_bytes()).hexdigest() == entry["sha256"]

    def test_long_profile_name_is_shortened(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        long_path = f"{PROFILES}/{'p' * 230}/SiteSecurityServiceState.txt"
        write_file(roots[1], long_path, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{long_path}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert manifest["status"] == "ok"
        entry = manifest["files"][0]
        assert entry["profile"] == "p" * 230, "the manifest keeps the full profile name"
        assert len(Path(entry["extracted_path"]).name) < 150

    def test_non_ascii_names_stay_within_the_file_name_limit(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        user = "\u7528" * 60
        profile = "\u6237" * 60
        path = f"Users/{user}/AppData/Roaming/Mozilla/Firefox/Profiles/{profile}/SiteSecurityServiceState.txt"
        write_file(roots[1], path, CONTENT_P1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{path}", 1)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert manifest["status"] == "ok"
        entry = manifest["files"][0]
        assert (entry["user"], entry["profile"]) == (user, profile)
        name = Path(entry["extracted_path"]).name
        assert name.isascii()
        assert len(name.encode()) < 150


class TestExistingOutput:
    """An error manifest is not output that a later run may reuse."""

    def test_error_manifest_is_not_existing_output(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        assert extractor.has_existing_output(output_dir) is False

        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )
        assert manifest["status"] == "error"
        assert extractor.has_existing_output(output_dir) is False

        add_to_file_list(evidence_db, case_context.evidence_id, "/Windows/notepad.exe", 1)
        _, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )
        assert manifest["status"] == "skipped"
        assert extractor.has_existing_output(output_dir) is True

    def test_cancelled_run_is_not_existing_output(
        self, extractor, roots, output_dir, callbacks, evidence_db, case_context
    ):
        for path in (HSTS_PATH, ESR_PATH):
            write_file(roots[1], path, CONTENT_P1)
            add_to_file_list(evidence_db, case_context.evidence_id, f"/{path}", 1)
        # Cancel after the first file: checked per partition, then per file.
        callbacks.is_cancelled = Mock(side_effect=[False, False, True])
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok, manifest = run_extraction(
            extractor, fs, output_dir, callbacks, evidence_db, case_context.evidence_id
        )

        assert ok is True
        assert manifest["status"] == "cancelled"
        assert len(manifest["files"]) == 1
        assert "cancelled after 1 of 2 file(s)" in manifest["notes"][-1]
        assert extractor.has_existing_output(output_dir) is False


class TestStateValues:
    """State values of Firefox's SecurityPropertyState enum."""

    @pytest.mark.parametrize(
        "state, mode",
        [(0, "unset"), (1, "force-https"), (2, "knockout"), (3, "negative"), (9, None)],
    )
    def test_state_to_mode(self, state, mode):
        content = f"example.com:HSTS\t0\t19000\t1893456000000,{state},0\n"

        entries = parse_transport_security_file(content, "SiteSecurityServiceState.txt")

        assert len(entries) == 1
        assert entries[0]["state"] == state
        assert entries[0]["mode"] == mode
