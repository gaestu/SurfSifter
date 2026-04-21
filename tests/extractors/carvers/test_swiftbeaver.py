"""Tests for SwiftBeaver carver extractor."""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from extractors.carvers.swiftbeaver.extractor import (
    SwiftbeaverExtractor,
    DEFAULT_IMAGE_TYPES,
    DEFAULT_MIN_SIZE_BYTES,
    DEFAULT_SCAN_URLS,
    METADATA_DIR,
    CARVED_DIR,
    CARVED_FILES_JSONL,
    STRING_ARTEFACTS_JSONL,
    RUN_SUMMARY_JSONL,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def extractor():
    return SwiftbeaverExtractor()


@pytest.fixture
def mock_tools():
    with patch("extractors.carvers.swiftbeaver.extractor.discover_tools") as mock_discover:
        mock_tool = Mock()
        mock_tool.available = True
        mock_tool.path = Path("/usr/bin/swiftbeaver")
        mock_discover.return_value = {"swiftbeaver": mock_tool}
        yield mock_discover


@pytest.fixture
def mock_callbacks():
    cb = Mock(spec_set=["on_step", "on_log", "on_progress", "on_error", "is_cancelled"])
    cb.is_cancelled.return_value = False
    return cb


@pytest.fixture
def run_dir(tmp_path):
    """Create a minimal SwiftBeaver run directory structure."""
    rd = tmp_path / "20250101_120000_abcd1234"
    metadata = rd / METADATA_DIR
    carved = rd / CARVED_DIR
    metadata.mkdir(parents=True)
    carved.mkdir(parents=True)
    return rd


def _write_jsonl(path: Path, entries: list[dict]):
    """Helper to write JSONL lines."""
    with open(path, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_metadata_name(self, extractor):
        assert extractor.metadata.name == "swiftbeaver"

    def test_metadata_requires_tools(self, extractor):
        assert "swiftbeaver" in extractor.metadata.requires_tools

    def test_metadata_can_extract_and_ingest(self, extractor):
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True


# ---------------------------------------------------------------------------
# can_run_extraction tests
# ---------------------------------------------------------------------------


class TestCanRunExtraction:
    def test_tool_available(self, extractor, mock_tools, tmp_path):
        evidence = tmp_path / "evidence.E01"
        evidence.touch()
        can_run, reason = extractor.can_run_extraction(evidence)
        assert can_run is True
        assert reason == ""

    def test_tool_not_available(self, extractor, tmp_path):
        with patch("extractors.carvers.swiftbeaver.extractor.discover_tools") as mock_discover:
            mock_discover.return_value = {}
            evidence = tmp_path / "evidence.E01"
            evidence.touch()
            can_run, reason = extractor.can_run_extraction(evidence)
            assert can_run is False
            assert "not installed" in reason

    def test_evidence_not_exists(self, extractor, mock_tools, tmp_path):
        missing = tmp_path / "nonexistent.E01"
        can_run, reason = extractor.can_run_extraction(missing)
        assert can_run is False
        assert "not found" in reason

    def test_evidence_none(self, extractor, mock_tools):
        can_run, reason = extractor.can_run_extraction(None)
        assert can_run is False


# ---------------------------------------------------------------------------
# can_run_ingestion tests
# ---------------------------------------------------------------------------


class TestCanRunIngestion:
    def test_valid_run_dir(self, extractor, run_dir, tmp_path):
        # tmp_path is the parent (output_dir)
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        # run_dir has metadata/ but no JSONL yet
        assert can_run is False

    def test_valid_with_carved_jsonl(self, extractor, run_dir, tmp_path):
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, [{"file_path": "carved/img.jpg"}])
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_valid_with_strings_jsonl(self, extractor, run_dir, tmp_path):
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, [{"artefact_kind": "url"}])
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_nonexistent_dir(self, extractor, tmp_path):
        can_run, reason = extractor.can_run_ingestion(tmp_path / "nonexistent")
        assert can_run is False


# ---------------------------------------------------------------------------
# has_existing_output tests
# ---------------------------------------------------------------------------


class TestHasExistingOutput:
    def test_true_with_run_dir(self, extractor, run_dir, tmp_path):
        assert extractor.has_existing_output(tmp_path) is True

    def test_false_without_run_dir(self, extractor, tmp_path):
        assert extractor.has_existing_output(tmp_path) is False

    def test_false_nonexistent(self, extractor, tmp_path):
        assert extractor.has_existing_output(tmp_path / "nonexistent") is False


# ---------------------------------------------------------------------------
# _find_latest_run_dir tests
# ---------------------------------------------------------------------------


class TestFindLatestRunDir:
    def test_returns_latest(self, extractor, tmp_path):
        # Create two run dirs
        older = tmp_path / "20250101_100000_aaaa0000"
        newer = tmp_path / "20250102_100000_bbbb1111"
        for d in [older, newer]:
            (d / METADATA_DIR).mkdir(parents=True)

        result = extractor._find_latest_run_dir(tmp_path)
        assert result == newer

    def test_ignores_dirs_without_metadata(self, extractor, tmp_path):
        valid = tmp_path / "20250101_100000_aaaa0000"
        (valid / METADATA_DIR).mkdir(parents=True)

        invalid = tmp_path / "20250102_100000_bbbb1111"
        invalid.mkdir(parents=True)  # No metadata/ subdir

        result = extractor._find_latest_run_dir(tmp_path)
        assert result == valid

    def test_empty_dir(self, extractor, tmp_path):
        assert extractor._find_latest_run_dir(tmp_path) is None

    def test_nonexistent_dir(self, extractor, tmp_path):
        assert extractor._find_latest_run_dir(tmp_path / "nonexistent") is None


# ---------------------------------------------------------------------------
# get_output_dir tests
# ---------------------------------------------------------------------------


class TestGetOutputDir:
    def test_output_dir_path(self, extractor, tmp_path):
        result = extractor.get_output_dir(tmp_path, "my_evidence")
        assert result == tmp_path / "evidences" / "my_evidence" / "swiftbeaver"


# ---------------------------------------------------------------------------
# run_extraction tests (subprocess mocking)
# ---------------------------------------------------------------------------


class TestRunExtraction:
    def test_successful_extraction(self, extractor, mock_tools, mock_callbacks, tmp_path):
        output_dir = tmp_path / "output"
        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "image_types": ["jpeg", "png"],
            "scan_urls": True,
            "num_workers": 2,
            "min_size_bytes": 4096,
            "output_reuse_policy": "overwrite",
            "evidence_id": 1,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.subprocess.Popen") as mock_popen, \
             patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None  # Disable stats

            mock_process = Mock()
            # Simulate progress output
            progress_lines = [
                json.dumps({"progress_percent": 50.0, "message": "Processing..."}) + "\n",
                json.dumps({"progress_percent": 100.0, "message": "Done"}) + "\n",
            ]
            mock_process.stdout = iter(progress_lines)
            mock_process.stderr = Mock()
            mock_process.stderr.read.return_value = ""
            mock_process.wait.return_value = 0
            mock_popen.return_value = mock_process

            result = extractor.run_extraction(evidence, output_dir, config, mock_callbacks)

            assert result is True
            mock_popen.assert_called_once()
            cmd = mock_popen.call_args[0][0]
            assert "--input" in cmd
            assert "--types" in cmd
            assert "jpeg,png" in cmd
            assert "--scan-strings" in cmd
            assert "--scan-urls" in cmd

    def test_extraction_without_urls(self, extractor, mock_tools, mock_callbacks, tmp_path):
        output_dir = tmp_path / "output"
        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "scan_urls": False,
            "num_workers": 2,
            "output_reuse_policy": "overwrite",
            "evidence_id": 1,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.subprocess.Popen") as mock_popen, \
             patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            mock_process = Mock()
            mock_process.stdout = iter([])
            mock_process.stderr = Mock()
            mock_process.stderr.read.return_value = ""
            mock_process.wait.return_value = 0
            mock_popen.return_value = mock_process

            extractor.run_extraction(evidence, output_dir, config, mock_callbacks)

            cmd = mock_popen.call_args[0][0]
            assert "--no-scan-strings" in cmd

    def test_extraction_reuse_policy(self, extractor, mock_tools, mock_callbacks, tmp_path):
        """Test that existing output is reused when policy is 'reuse'."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "20250101_120000_abcd1234"
        (run_dir / METADATA_DIR).mkdir(parents=True)

        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "output_reuse_policy": "reuse",
            "evidence_id": 1,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            result = extractor.run_extraction(evidence, output_dir, config, mock_callbacks)
            assert result is True
            # No subprocess should have been called
            mock_callbacks.on_log.assert_any_call(
                "✓ Reusing existing SwiftBeaver output (run: 20250101_120000_abcd1234)",
                "info"
            )

    def test_extraction_failure(self, extractor, mock_tools, mock_callbacks, tmp_path):
        output_dir = tmp_path / "output"
        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "output_reuse_policy": "overwrite",
            "evidence_id": 1,
            "num_workers": 1,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.subprocess.Popen") as mock_popen, \
             patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            mock_process = Mock()
            mock_process.stdout = iter([])
            mock_process.stderr = Mock()
            mock_process.stderr.read.return_value = "fatal error"
            mock_process.wait.return_value = 1
            mock_popen.return_value = mock_process

            result = extractor.run_extraction(evidence, output_dir, config, mock_callbacks)
            assert result is False
            mock_callbacks.on_error.assert_called_once()

    def test_extraction_cancelled(self, extractor, mock_tools, mock_callbacks, tmp_path):
        output_dir = tmp_path / "output"
        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "output_reuse_policy": "overwrite",
            "evidence_id": 1,
            "num_workers": 1,
        }

        mock_callbacks.is_cancelled.return_value = True

        with patch("extractors.carvers.swiftbeaver.extractor.subprocess.Popen") as mock_popen, \
             patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            mock_process = Mock()
            mock_process.stdout = iter([json.dumps({"message": "start"}) + "\n"])
            mock_process.stderr = Mock()
            mock_process.stderr.read.return_value = ""
            mock_process.wait.return_value = -15
            mock_popen.return_value = mock_process

            result = extractor.run_extraction(evidence, output_dir, config, mock_callbacks)
            assert result is False
            mock_process.terminate.assert_called_once()

    def test_tool_not_available(self, extractor, mock_callbacks, tmp_path):
        output_dir = tmp_path / "output"
        evidence = tmp_path / "evidence.E01"
        evidence.touch()

        config = {
            "output_reuse_policy": "overwrite",
            "evidence_id": 1,
            "num_workers": 1,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.discover_tools") as mock_discover, \
             patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None
            mock_discover.return_value = {}

            result = extractor.run_extraction(evidence, output_dir, config, mock_callbacks)
            assert result is False


# ---------------------------------------------------------------------------
# JSONL Parsing tests
# ---------------------------------------------------------------------------


class TestJsonlParsing:
    """Test JSONL parsing within _ingest_carved_images."""

    def test_carved_files_parsing(self, extractor, run_dir, tmp_path):
        """Verify carved_files.jsonl entries are parsed correctly."""
        carved_entries = [
            {
                "file_path": "carved/img001.jpg",
                "file_type": "jpeg",
                "size": 8192,
                "md5": "d41d8cd98f00b204e9800998ecf8427e",
                "sha256": "e3b0c44298fc1c149afbf4c8996fb924",
                "global_start": 1024,
                "global_end": 9216,
                "validated": True,
            },
            {
                "file_path": "carved/img002.png",
                "file_type": "png",
                "size": 16384,
                "md5": "abc123",
                "sha256": "def456",
                "global_start": 10000,
                "global_end": 26384,
                "validated": True,
            },
        ]

        # Create the actual carved files
        for entry in carved_entries:
            f = run_dir / entry["file_path"]
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"\x00" * entry["size"])

        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, carved_entries)

        # Parse manually to verify
        jsonl_path = run_dir / METADATA_DIR / CARVED_FILES_JSONL
        parsed = []
        with open(jsonl_path, "r") as f:
            for line in f:
                parsed.append(json.loads(line.strip()))

        assert len(parsed) == 2
        assert parsed[0]["file_type"] == "jpeg"
        assert parsed[0]["sha256"] == "e3b0c44298fc1c149afbf4c8996fb924"
        assert parsed[1]["global_start"] == 10000

    def test_min_size_filtering(self, extractor, run_dir, mock_callbacks):
        """Verify images below min_size_bytes are skipped."""
        entries = [
            {"file_path": "carved/small.jpg", "file_type": "jpeg", "size": 100},
            {"file_path": "carved/large.jpg", "file_type": "jpeg", "size": 8192,
             "md5": "abc", "sha256": "def", "global_start": 0, "global_end": 8192},
        ]

        for entry in entries:
            f = run_dir / entry["file_path"]
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"\x00" * entry["size"])

        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, entries)

        # Use the _ingest_carved_images method with mocked dependencies
        mock_conn = Mock()
        mock_conn.cursor.return_value.fetchone.return_value = (0,)

        mock_result = Mock()
        mock_result.error = None
        mock_result.path = run_dir / "carved" / "large.jpg"
        mock_result.sha256 = "def"
        mock_result.md5 = "abc"
        mock_result.size_bytes = 8192
        mock_result.to_db_record.return_value = {
            "file_type": "jpeg",
            "size_bytes": 8192,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.ParallelImageProcessor") as MockProcessor, \
             patch("extractors.carvers.swiftbeaver.extractor.ingest_with_enrichment") as mock_ingest, \
             patch("extractors.carvers.swiftbeaver.extractor.delete_discoveries_by_run") as mock_delete, \
             patch("extractors.carvers.swiftbeaver.extractor.validate_image_carving_manifest"), \
             patch("extractors.carvers.swiftbeaver.extractor.record_carved_files"):
            mock_delete.return_value = 0
            mock_processor_inst = MockProcessor.return_value
            mock_processor_inst.process_images.return_value = [mock_result]
            mock_ingest.return_value = (1, True)

            stats = extractor._ingest_carved_images(
                run_dir, run_dir / METADATA_DIR, mock_conn, 1,
                "test_run", 4096, mock_callbacks
            )

            # Only the large image should be processed (1 image, not 2)
            paths_arg = mock_processor_inst.process_images.call_args[0][0]
            assert len(paths_arg) == 1
            assert "large.jpg" in str(paths_arg[0])

    def test_malformed_jsonl_lines_skipped(self, extractor, run_dir, mock_callbacks):
        """Verify malformed JSONL lines are skipped without crashing."""
        jsonl_path = run_dir / METADATA_DIR / CARVED_FILES_JSONL
        with open(jsonl_path, "w") as f:
            f.write('{"file_path": "carved/valid.jpg", "file_type": "jpeg", "size": 8192, "md5": "a", "sha256": "b", "global_start": 0, "global_end": 8192}\n')
            f.write("not json at all\n")
            f.write('{"incomplete": true\n')
            f.write("\n")  # blank line
            f.write('{"file_path": "carved/valid2.jpg", "file_type": "png", "size": 16384, "md5": "c", "sha256": "d", "global_start": 8192, "global_end": 24576}\n')

        # Create the carved files
        for name, size in [("valid.jpg", 8192), ("valid2.jpg", 16384)]:
            f = run_dir / CARVED_DIR / name
            f.write_bytes(b"\x00" * size)

        mock_conn = Mock()
        mock_conn.cursor.return_value.fetchone.return_value = (0,)

        mock_result1 = Mock()
        mock_result1.error = None
        mock_result1.path = run_dir / CARVED_DIR / "valid.jpg"
        mock_result1.sha256 = "b"
        mock_result1.md5 = "a"
        mock_result1.size_bytes = 8192
        mock_result1.to_db_record.return_value = {"file_type": "jpeg", "size_bytes": 8192}

        mock_result2 = Mock()
        mock_result2.error = None
        mock_result2.path = run_dir / CARVED_DIR / "valid2.jpg"
        mock_result2.sha256 = "d"
        mock_result2.md5 = "c"
        mock_result2.size_bytes = 16384
        mock_result2.to_db_record.return_value = {"file_type": "png", "size_bytes": 16384}

        with patch("extractors.carvers.swiftbeaver.extractor.ParallelImageProcessor") as MockProcessor, \
             patch("extractors.carvers.swiftbeaver.extractor.ingest_with_enrichment") as mock_ingest, \
             patch("extractors.carvers.swiftbeaver.extractor.delete_discoveries_by_run") as mock_delete, \
             patch("extractors.carvers.swiftbeaver.extractor.validate_image_carving_manifest"), \
             patch("extractors.carvers.swiftbeaver.extractor.record_carved_files"):
            mock_delete.return_value = 0
            mock_processor_inst = MockProcessor.return_value
            mock_processor_inst.process_images.return_value = [mock_result1, mock_result2]
            mock_ingest.return_value = (1, True)

            stats = extractor._ingest_carved_images(
                run_dir, run_dir / METADATA_DIR, mock_conn, 1,
                "test_run", 4096, mock_callbacks
            )

        # 2 valid entries parsed out of 5 lines
        paths_arg = mock_processor_inst.process_images.call_args[0][0]
        assert len(paths_arg) == 2


# ---------------------------------------------------------------------------
# URL Ingestion tests
# ---------------------------------------------------------------------------


class TestUrlIngestion:
    def test_url_extraction(self, extractor, run_dir, mock_callbacks):
        """Test that URLs are correctly parsed and batch-inserted."""
        artefacts = [
            {"artefact_kind": "url", "content": "https://example.com/page", "global_start": 0, "global_end": 25},
            {"artefact_kind": "url", "content": "http://test.org/file.pdf", "global_start": 100, "global_end": 124},
            {"artefact_kind": "email", "content": "user@example.com", "global_start": 200, "global_end": 216},
        ]
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, artefacts)

        mock_conn = Mock()

        with patch("extractors.carvers.swiftbeaver.extractor.insert_urls") as mock_insert:
            mock_insert.return_value = 2

            count = extractor._ingest_urls(
                run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
            )

            assert count == 2
            mock_insert.assert_called_once()
            url_batch = mock_insert.call_args[0][2]
            assert len(url_batch) == 2
            assert url_batch[0]["url"] == "https://example.com/page"
            assert url_batch[0]["domain"] == "example.com"
            assert url_batch[0]["scheme"] == "https"
            assert url_batch[0]["discovered_by"] == "swiftbeaver:url"

    def test_url_filters_non_url_artefacts(self, extractor, run_dir, mock_callbacks):
        """Test that non-URL artefact kinds are skipped."""
        artefacts = [
            {"artefact_kind": "email", "content": "user@example.com"},
            {"artefact_kind": "phone", "content": "+1234567890"},
            {"artefact_kind": "url", "content": "https://example.com"},
        ]
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, artefacts)

        mock_conn = Mock()

        with patch("extractors.carvers.swiftbeaver.extractor.insert_urls") as mock_insert:
            mock_insert.return_value = 1

            count = extractor._ingest_urls(
                run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
            )

            url_batch = mock_insert.call_args[0][2]
            assert len(url_batch) == 1
            assert url_batch[0]["url"] == "https://example.com"

    def test_url_empty_content_skipped(self, extractor, run_dir, mock_callbacks):
        """Test that entries with empty URL content are skipped."""
        artefacts = [
            {"artefact_kind": "url", "content": ""},
            {"artefact_kind": "url", "content": "   "},
            {"artefact_kind": "url", "content": "https://real.com"},
        ]
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, artefacts)

        mock_conn = Mock()

        with patch("extractors.carvers.swiftbeaver.extractor.insert_urls") as mock_insert:
            mock_insert.return_value = 1

            count = extractor._ingest_urls(
                run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
            )

            url_batch = mock_insert.call_args[0][2]
            assert len(url_batch) == 1

    def test_url_batch_threshold(self, extractor, run_dir, mock_callbacks):
        """Test that URLs are inserted in batches of 1000."""
        # Create 1500 URL artefacts
        artefacts = [
            {"artefact_kind": "url", "content": f"https://example.com/{i}", "global_start": i}
            for i in range(1500)
        ]
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, artefacts)

        mock_conn = Mock()

        with patch("extractors.carvers.swiftbeaver.extractor.insert_urls") as mock_insert:
            mock_insert.return_value = 1000

            extractor._ingest_urls(
                run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
            )

            # Should be called twice: once with 1000, once with 500
            assert mock_insert.call_count == 2
            first_batch = mock_insert.call_args_list[0][0][2]
            second_batch = mock_insert.call_args_list[1][0][2]
            assert len(first_batch) == 1000
            assert len(second_batch) == 500

    def test_url_no_jsonl(self, extractor, run_dir, mock_callbacks):
        """Test graceful handling when no string_artefacts.jsonl exists."""
        mock_conn = Mock()

        count = extractor._ingest_urls(
            run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
        )

        assert count == 0

    def test_url_source_path_includes_offset(self, extractor, run_dir, mock_callbacks):
        """Verify source_path records the global_start offset for provenance."""
        artefacts = [
            {"artefact_kind": "url", "content": "https://example.com", "global_start": 42},
        ]
        _write_jsonl(run_dir / METADATA_DIR / STRING_ARTEFACTS_JSONL, artefacts)

        mock_conn = Mock()

        with patch("extractors.carvers.swiftbeaver.extractor.insert_urls") as mock_insert:
            mock_insert.return_value = 1

            extractor._ingest_urls(
                run_dir / METADATA_DIR, mock_conn, 1, "test_run", mock_callbacks
            )

            url_batch = mock_insert.call_args[0][2]
            assert url_batch[0]["source_path"] == "string_artefacts.jsonl:42"


# ---------------------------------------------------------------------------
# Image Ingestion integration
# ---------------------------------------------------------------------------


class TestImageIngestion:
    def test_precomputed_hashes_used(self, extractor, run_dir, mock_callbacks):
        """Verify pre-computed hashes from SwiftBeaver are passed through."""
        entries = [
            {
                "file_path": "carved/img.jpg",
                "file_type": "jpeg",
                "size": 8192,
                "md5": "computed_md5",
                "sha256": "computed_sha256",
                "global_start": 500,
                "global_end": 8692,
            }
        ]
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, entries)
        carved_file = run_dir / CARVED_DIR / "img.jpg"
        carved_file.write_bytes(b"\x00" * 8192)

        mock_conn = Mock()
        mock_conn.cursor.return_value.fetchone.return_value = (0,)

        mock_result = Mock()
        mock_result.error = None
        mock_result.path = carved_file
        mock_result.sha256 = "original_sha256"
        mock_result.md5 = "original_md5"
        mock_result.size_bytes = 8192
        mock_result.to_db_record.return_value = {"file_type": "jpeg", "size_bytes": 8192}

        with patch("extractors.carvers.swiftbeaver.extractor.ParallelImageProcessor") as MockProcessor, \
             patch("extractors.carvers.swiftbeaver.extractor.ingest_with_enrichment") as mock_ingest, \
             patch("extractors.carvers.swiftbeaver.extractor.delete_discoveries_by_run") as mock_delete, \
             patch("extractors.carvers.swiftbeaver.extractor.validate_image_carving_manifest"), \
             patch("extractors.carvers.swiftbeaver.extractor.record_carved_files"):
            mock_delete.return_value = 0
            MockProcessor.return_value.process_images.return_value = [mock_result]
            mock_ingest.return_value = (1, True)

            extractor._ingest_carved_images(
                run_dir, run_dir / METADATA_DIR, mock_conn, 1,
                "test_run", 4096, mock_callbacks
            )

            # Verify pre-computed hashes were set
            assert mock_result.sha256 == "computed_sha256"
            assert mock_result.md5 == "computed_md5"

            # Verify carved_offset_bytes passed to ingest_with_enrichment
            call_kwargs = mock_ingest.call_args[1]
            assert call_kwargs["carved_offset_bytes"] == 500
            assert call_kwargs["discovered_by"] == "swiftbeaver"

    def test_missing_carved_file_skipped(self, extractor, run_dir, mock_callbacks):
        """Verify entries referencing missing carved files are skipped."""
        entries = [
            {
                "file_path": "carved/missing.jpg",
                "file_type": "jpeg",
                "size": 8192,
                "md5": "a",
                "sha256": "b",
                "global_start": 0,
                "global_end": 8192,
            }
        ]
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, entries)
        # Note: NOT creating the actual carved file

        mock_conn = Mock()
        mock_conn.cursor.return_value.fetchone.return_value = (0,)

        with patch("extractors.carvers.swiftbeaver.extractor.ParallelImageProcessor") as MockProcessor, \
             patch("extractors.carvers.swiftbeaver.extractor.delete_discoveries_by_run") as mock_delete, \
             patch("extractors.carvers.swiftbeaver.extractor.validate_image_carving_manifest"), \
             patch("extractors.carvers.swiftbeaver.extractor.record_carved_files"):
            mock_delete.return_value = 0
            MockProcessor.return_value.process_images.return_value = []

            stats = extractor._ingest_carved_images(
                run_dir, run_dir / METADATA_DIR, mock_conn, 1,
                "test_run", 4096, mock_callbacks
            )

            # No images should be passed to processor (early return before calling it)
            MockProcessor.return_value.process_images.assert_not_called()

    def test_path_traversal_rejected(self, extractor, run_dir, mock_callbacks):
        """Verify file_path values with path traversal are rejected."""
        entries = [
            {
                "file_path": "../../etc/passwd",
                "file_type": "jpeg",
                "size": 8192,
                "md5": "a",
                "sha256": "b",
                "global_start": 0,
                "global_end": 8192,
            },
            {
                "file_path": "carved/../../../etc/shadow",
                "file_type": "png",
                "size": 16384,
                "md5": "c",
                "sha256": "d",
                "global_start": 100,
                "global_end": 16484,
            },
        ]
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, entries)

        mock_conn = Mock()
        mock_conn.cursor.return_value.fetchone.return_value = (0,)

        with patch("extractors.carvers.swiftbeaver.extractor.ParallelImageProcessor") as MockProcessor, \
             patch("extractors.carvers.swiftbeaver.extractor.delete_discoveries_by_run") as mock_delete, \
             patch("extractors.carvers.swiftbeaver.extractor.validate_image_carving_manifest"), \
             patch("extractors.carvers.swiftbeaver.extractor.record_carved_files"):
            mock_delete.return_value = 0
            MockProcessor.return_value.process_images.return_value = []

            stats = extractor._ingest_carved_images(
                run_dir, run_dir / METADATA_DIR, mock_conn, 1,
                "test_run", 4096, mock_callbacks
            )

            # No images should be passed to processor (all rejected, early return)
            MockProcessor.return_value.process_images.assert_not_called()

    def test_no_carved_jsonl(self, extractor, run_dir, mock_callbacks):
        """Verify graceful handling when carved_files.jsonl is missing."""
        mock_conn = Mock()

        stats = extractor._ingest_carved_images(
            run_dir, run_dir / METADATA_DIR, mock_conn, 1,
            "test_run", 4096, mock_callbacks
        )

        assert stats == {"inserted": 0, "enriched": 0, "errors": 0}


# ---------------------------------------------------------------------------
# run_ingestion tests (full flow)
# ---------------------------------------------------------------------------


class TestRunIngestion:
    def test_ingestion_no_run_dir(self, extractor, mock_callbacks, tmp_path):
        """Test ingestion when no run directory exists."""
        mock_conn = Mock()
        config = {"evidence_id": 1, "evidence_label": "test"}

        with patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            result = extractor.run_ingestion(tmp_path, mock_conn, 1, config, mock_callbacks)
            assert result == {}
            mock_callbacks.on_error.assert_called_once()

    def test_ingestion_cancel_mode(self, extractor, mock_callbacks, run_dir, tmp_path):
        """Test that overwrite_mode=cancel stops ingestion."""
        # Create a JSONL to pass can_run checks
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, [{"file_path": "x"}])

        mock_conn = Mock()
        cursor_mock = Mock()
        cursor_mock.fetchone.return_value = (5,)  # Existing data found
        mock_conn.cursor.return_value = cursor_mock

        config = {
            "evidence_id": 1,
            "evidence_label": "test",
            "overwrite_mode": "cancel",
        }

        with patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats:
            mock_stats.instance.return_value = None

            result = extractor.run_ingestion(tmp_path, mock_conn, 1, config, mock_callbacks)
            assert result == {}

    def test_ingestion_writes_process_log(self, extractor, mock_callbacks, run_dir, tmp_path):
        """Verify process_log entry is written during ingestion."""
        # Empty JSONL files (no actual data to ingest)
        _write_jsonl(run_dir / METADATA_DIR / CARVED_FILES_JSONL, [])

        mock_conn = Mock()
        cursor_mock = Mock()
        cursor_mock.fetchone.return_value = (0,)  # No existing data
        mock_conn.cursor.return_value = cursor_mock

        config = {
            "evidence_id": 1,
            "evidence_label": "test",
            "import_images": False,
            "import_urls": False,
        }

        with patch("extractors.carvers.swiftbeaver.extractor.StatisticsCollector") as mock_stats, \
             patch("extractors.carvers.swiftbeaver.extractor.insert_process_log") as mock_log:
            mock_stats.instance.return_value = None

            extractor.run_ingestion(tmp_path, mock_conn, 1, config, mock_callbacks)

            # At least one call for the ingestion phase process_log
            assert mock_log.call_count >= 1
            # Last call should be the ingestion log
            last_call_kwargs = mock_log.call_args_list[-1][1]
            assert last_call_kwargs["tool_name"] == "swiftbeaver"
            assert last_call_kwargs["extractor_name"] == "swiftbeaver"
            assert last_call_kwargs["exit_code"] == 0


# ---------------------------------------------------------------------------
# _check_existing_data and _delete tests
# ---------------------------------------------------------------------------


class TestExistingDataManagement:
    def test_check_existing_data(self, extractor):
        mock_conn = Mock()
        cursor = Mock()
        cursor.fetchone.side_effect = [(3,), (5,)]
        mock_conn.cursor.return_value = cursor

        counts = extractor._check_existing_data(mock_conn, 1)
        assert counts["url"] == 3
        assert counts["images"] == 5

    def test_check_no_existing_data(self, extractor):
        mock_conn = Mock()
        cursor = Mock()
        cursor.fetchone.side_effect = [(0,), (0,)]
        mock_conn.cursor.return_value = cursor

        counts = extractor._check_existing_data(mock_conn, 1)
        assert len(counts) == 0  # Empty dict when all counts are 0

    def test_delete_data(self, extractor):
        mock_conn = Mock()
        cursor = Mock()
        mock_conn.cursor.return_value = cursor

        extractor._delete_swiftbeaver_data(mock_conn, 1)

        # Two DELETE calls + commit
        assert cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# _generate_run_id tests
# ---------------------------------------------------------------------------


class TestGenerateRunId:
    def test_format(self, extractor):
        run_id = extractor._generate_run_id()
        # Format: YYYYMMDD_HHMMSS_<8-hex-chars>
        parts = run_id.split("_")
        assert len(parts) == 3
        assert len(parts[0]) == 8  # YYYYMMDD
        assert len(parts[1]) == 6  # HHMMSS
        assert len(parts[2]) == 8  # UUID hex

    def test_uniqueness(self, extractor):
        ids = {extractor._generate_run_id() for _ in range(10)}
        assert len(ids) == 10


# ---------------------------------------------------------------------------
# _build_image_manifest tests
# ---------------------------------------------------------------------------


class TestBuildManifest:
    def test_manifest_structure(self, extractor, tmp_path):
        files = [tmp_path / "a.jpg", tmp_path / "b.png"]
        for f in files:
            f.write_bytes(b"\x00" * 100)

        manifest = extractor._build_image_manifest(
            run_id="test_run",
            run_dir=tmp_path,
            files=files,
            inserted=2,
            errors=0,
            enriched=1,
        )

        assert manifest["extractor"] == "swiftbeaver"
        assert manifest["run_id"] == "test_run"
        assert manifest["schema_version"] == "1.0.0"
        assert manifest["stats"]["carved_total"] == 2
        assert manifest["ingestion"]["inserted"] == 2
        assert manifest["ingestion"]["enriched"] == 1
        assert len(manifest["carved_files"]) == 2


# ---------------------------------------------------------------------------
# Registry discovery
# ---------------------------------------------------------------------------


class TestRegistryDiscovery:
    def test_extractor_in_registry(self, extractor_registry_names):
        """SwiftBeaver should be auto-discovered by the extractor registry."""
        assert "swiftbeaver" in extractor_registry_names


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_default_image_types(self):
        assert "jpeg" in DEFAULT_IMAGE_TYPES
        assert "png" in DEFAULT_IMAGE_TYPES
        assert len(DEFAULT_IMAGE_TYPES) == 8

    def test_default_min_size(self):
        assert DEFAULT_MIN_SIZE_BYTES == 4096

    def test_default_scan_urls(self):
        assert DEFAULT_SCAN_URLS is True
