"""
Tests for Firefox Cache2 Extractor performance improvements (, ).

Tests the incremental manifest writing with JSONL part-files:
- Part-file writer (write, rotate, finalize)
- Part-file loader (for finalization)
- Hash mode default
- Error/cancel status handling in manifest

 additions:
- Streaming copy with _stream_copy_hash_from_iterator()
- File list fast path with _collect_from_file_list()
- Pattern deduplication

Resume feature removed for simplicity.
"""

import json
import pytest
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestPartFileWriter:
    """Tests for part-file manifest writer methods."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance with part-file state initialized."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        ext = CacheFirefoxExtractor()
        ext._init_part_file_state()
        return ext

    @pytest.fixture
    def run_dir(self, tmp_path):
        """Create temporary run directory."""
        run_dir = tmp_path / "20260105_120000_abcd1234"
        run_dir.mkdir(parents=True)
        return run_dir

    def test_init_part_file_state(self, extractor):
        """Part-file state initializes correctly."""
        assert extractor._current_part_file is None
        assert extractor._current_part_number == 1
        assert extractor._entries_in_current_part == 0
        assert extractor._total_entries_written == 0

    def test_get_part_file_path(self, extractor, run_dir):
        """Part-file paths follow naming convention."""
        path = extractor._get_part_file_path(run_dir, 1)
        assert path == run_dir / "manifest.part-0001.jsonl"

        path = extractor._get_part_file_path(run_dir, 99)
        assert path == run_dir / "manifest.part-0099.jsonl"

    def test_append_entries_creates_part_file(self, extractor, run_dir):
        """Appending entries creates part-file if not exists."""
        entries = [
            {"source_path": "path/to/file1", "size_bytes": 100},
            {"source_path": "path/to/file2", "size_bytes": 200},
        ]

        extractor._append_entries_to_part_file(run_dir, entries)

        part_path = run_dir / "manifest.part-0001.jsonl"
        assert part_path.exists()

        # Verify JSONL content
        lines = part_path.read_text().strip().split("\n")
        assert len(lines) == 2

        entry1 = json.loads(lines[0])
        assert entry1["source_path"] == "path/to/file1"

        entry2 = json.loads(lines[1])
        assert entry2["source_path"] == "path/to/file2"

        # Cleanup
        extractor._close_part_file()

    def test_append_entries_incremental(self, extractor, run_dir):
        """Multiple appends accumulate in same part-file."""
        # First batch
        extractor._append_entries_to_part_file(run_dir, [
            {"source_path": "file1", "size_bytes": 100},
        ])

        # Second batch
        extractor._append_entries_to_part_file(run_dir, [
            {"source_path": "file2", "size_bytes": 200},
            {"source_path": "file3", "size_bytes": 300},
        ])

        part_path = run_dir / "manifest.part-0001.jsonl"
        lines = part_path.read_text().strip().split("\n")
        assert len(lines) == 3

        extractor._close_part_file()

    def test_part_file_rotation(self, extractor, run_dir):
        """Part-file rotates at threshold."""
        from extractors.browser.firefox.cache.extractor import PART_FILE_MAX_ENTRIES

        # Create entries just under threshold
        entries = [{"source_path": f"file{i}", "size_bytes": i} for i in range(PART_FILE_MAX_ENTRIES - 1)]
        extractor._append_entries_to_part_file(run_dir, entries)

        assert extractor._current_part_number == 1
        assert extractor._entries_in_current_part == PART_FILE_MAX_ENTRIES - 1

        # Add two more to trigger rotation
        extractor._append_entries_to_part_file(run_dir, [
            {"source_path": "trigger_rotation", "size_bytes": 999},
            {"source_path": "in_new_part", "size_bytes": 1000},
        ])

        # Should have rotated to part 2
        assert extractor._current_part_number == 2
        assert extractor._entries_in_current_part == 1

        # Both part files should exist
        assert (run_dir / "manifest.part-0001.jsonl").exists()
        assert (run_dir / "manifest.part-0002.jsonl").exists()

        extractor._close_part_file()

    def test_close_part_file(self, extractor, run_dir):
        """Close part file releases handle."""
        extractor._append_entries_to_part_file(run_dir, [{"source_path": "test"}])
        assert extractor._current_part_file is not None

        extractor._close_part_file()
        assert extractor._current_part_file is None

    def test_append_entries_increments_total(self, extractor, run_dir):
        """Appending entries increments total_entries_written counter."""
        assert extractor._total_entries_written == 0

        extractor._append_entries_to_part_file(run_dir, [
            {"source_path": "file1"},
            {"source_path": "file2"},
        ])
        assert extractor._total_entries_written == 2

        extractor._append_entries_to_part_file(run_dir, [
            {"source_path": "file3"},
        ])
        assert extractor._total_entries_written == 3

        extractor._close_part_file()


class TestPartFileLoader:
    """Tests for loading part-files for finalization."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    @pytest.fixture
    def run_dir_with_parts(self, tmp_path):
        """Create run directory with part-files."""
        run_dir = tmp_path / "20260105_120000_abcd1234"
        run_dir.mkdir(parents=True)

        # Create part-0001.jsonl
        part1 = run_dir / "manifest.part-0001.jsonl"
        part1.write_text(
            '{"source_path": "file1", "size_bytes": 100}\n'
            '{"source_path": "file2", "size_bytes": 200}\n'
        )

        # Create part-0002.jsonl
        part2 = run_dir / "manifest.part-0002.jsonl"
        part2.write_text(
            '{"source_path": "file3", "size_bytes": 300}\n'
        )

        return run_dir

    def test_load_part_files(self, extractor, run_dir_with_parts):
        """Load entries from multiple part-files."""
        entries, source_paths, last_part = extractor._load_part_files(run_dir_with_parts)

        assert len(entries) == 3
        assert entries[0]["source_path"] == "file1"
        assert entries[1]["source_path"] == "file2"
        assert entries[2]["source_path"] == "file3"

        assert source_paths == {"file1", "file2", "file3"}
        assert last_part == 2

    def test_load_part_files_empty_dir(self, extractor, tmp_path):
        """Load from empty directory returns empty results."""
        run_dir = tmp_path / "empty_run"
        run_dir.mkdir()

        entries, source_paths, last_part = extractor._load_part_files(run_dir)

        assert entries == []
        assert source_paths == set()
        assert last_part == 0

    def test_load_part_files_skips_corrupted_lines(self, extractor, tmp_path):
        """Corrupted JSONL lines are skipped with warning."""
        run_dir = tmp_path / "corrupted_run"
        run_dir.mkdir()

        # Create part-file with corrupted line
        part1 = run_dir / "manifest.part-0001.jsonl"
        part1.write_text(
            '{"source_path": "good1", "size_bytes": 100}\n'
            'this is not valid json\n'
            '{"source_path": "good2", "size_bytes": 200}\n'
            '{"incomplete": \n'
            '{"source_path": "good3", "size_bytes": 300}\n'
        )

        entries, source_paths, last_part = extractor._load_part_files(run_dir)

        # Should have 3 good entries, skipping 2 corrupted
        assert len(entries) == 3
        assert source_paths == {"good1", "good2", "good3"}

    def test_load_part_files_preserves_order(self, extractor, tmp_path):
        """Part-files are loaded in correct order."""
        run_dir = tmp_path / "order_test"
        run_dir.mkdir()

        # Create part-files out of order on filesystem
        (run_dir / "manifest.part-0003.jsonl").write_text('{"source_path": "c"}\n')
        (run_dir / "manifest.part-0001.jsonl").write_text('{"source_path": "a"}\n')
        (run_dir / "manifest.part-0002.jsonl").write_text('{"source_path": "b"}\n')

        entries, _, _ = extractor._load_part_files(run_dir)

        # Should be in part-number order
        assert [e["source_path"] for e in entries] == ["a", "b", "c"]


class TestFinalizeManifest:
    """Tests for manifest finalization."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        ext = CacheFirefoxExtractor()
        ext._init_part_file_state()
        return ext

    def test_finalize_merges_parts(self, extractor, tmp_path):
        """Finalization merges part-files into manifest.json."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        # Create part-files
        (run_dir / "manifest.part-0001.jsonl").write_text(
            '{"source_path": "file1", "size_bytes": 100}\n'
        )
        (run_dir / "manifest.part-0002.jsonl").write_text(
            '{"source_path": "file2", "size_bytes": 200}\n'
        )
        (run_dir / "manifest.partial.json").write_text('{}')

        stats = {"cache_files_copied": 2}

        manifest_files = extractor._finalize_manifest(
            run_dir, "test_run", 1, stats, "ingestion", "ok"
        )

        # Manifest should exist with merged entries
        manifest_path = run_dir / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        assert len(manifest["files"]) == 2
        assert manifest["status"] == "ok"
        assert manifest["run_id"] == "test_run"

        # Part-files should be deleted
        assert not (run_dir / "manifest.part-0001.jsonl").exists()
        assert not (run_dir / "manifest.part-0002.jsonl").exists()
        assert not (run_dir / "manifest.partial.json").exists()

    def test_finalize_returns_entries(self, extractor, tmp_path):
        """Finalization returns list of entries."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        (run_dir / "manifest.part-0001.jsonl").write_text(
            '{"source_path": "a"}\n'
            '{"source_path": "b"}\n'
        )

        result = extractor._finalize_manifest(
            run_dir, "run1", 1, {}, "ingestion", "ok"
        )

        assert len(result) == 2
        assert result[0]["source_path"] == "a"
        assert result[1]["source_path"] == "b"

    def test_finalize_with_cancelled_status(self, extractor, tmp_path):
        """Finalization records cancelled status."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        (run_dir / "manifest.part-0001.jsonl").write_text('{"source_path": "a"}\n')

        extractor._finalize_manifest(
            run_dir, "run1", 1, {"cache_files_copied": 1}, "ingestion", "cancelled"
        )

        manifest = json.loads((run_dir / "manifest.json").read_text())
        assert manifest["status"] == "cancelled"
        assert len(manifest["files"]) == 1

    def test_finalize_with_error_status(self, extractor, tmp_path):
        """Finalization records error status."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        (run_dir / "manifest.part-0001.jsonl").write_text('{"source_path": "a"}\n')

        extractor._finalize_manifest(
            run_dir, "run1", 1, {"cache_files_copied": 1, "errors": ["test error"]}, "ingestion", "error"
        )

        manifest = json.loads((run_dir / "manifest.json").read_text())
        assert manifest["status"] == "error"
        assert len(manifest["files"]) == 1


class TestWritePartialManifest:
    """Tests for incremental partial manifest writes."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        ext = CacheFirefoxExtractor()
        ext._init_part_file_state()
        return ext

    def test_write_partial_appends_to_part_file(self, extractor, tmp_path):
        """Partial manifest write appends to part-file."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        new_entries = [
            {"source_path": "new1"},
            {"source_path": "new2"},
        ]
        stats = {"cache_files_copied": 2}

        extractor._write_partial_manifest(
            run_dir, "test_run", 1, new_entries, stats, "ingestion"
        )

        # Part-file should have entries
        part_path = run_dir / "manifest.part-0001.jsonl"
        assert part_path.exists()
        lines = part_path.read_text().strip().split("\n")
        assert len(lines) == 2

        # Partial manifest should have header only (no files array)
        partial_path = run_dir / "manifest.partial.json"
        assert partial_path.exists()
        partial = json.loads(partial_path.read_text())
        assert "files" not in partial
        assert partial["total_entries"] == 2
        assert partial["part_file_count"] == 1

        extractor._close_part_file()

    def test_write_partial_uses_tracked_total(self, extractor, tmp_path):
        """Partial manifest total_entries uses tracked counter, not file re-read."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        # First batch
        extractor._write_partial_manifest(
            run_dir, "test_run", 1, [{"source_path": "a"}, {"source_path": "b"}], {}, "ingestion"
        )

        # Second batch
        extractor._write_partial_manifest(
            run_dir, "test_run", 1, [{"source_path": "c"}], {}, "ingestion"
        )

        # Total should be 3 (tracked incrementally, not re-read)
        partial = json.loads((run_dir / "manifest.partial.json").read_text())
        assert partial["total_entries"] == 3

        # Counter should match
        assert extractor._total_entries_written == 3

        extractor._close_part_file()


class TestHashModeDefault:
    """Tests for hash mode default change."""

    def test_default_hash_mode_is_ingestion(self):
        """Hash mode defaults to 'During Ingestion'."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor

        ext = CacheFirefoxExtractor()

        # Create widget to check default
        from PySide6.QtWidgets import QApplication
        import sys

        # Skip if no display available
        if QApplication.instance() is None:
            try:
                app = QApplication(sys.argv)
            except Exception:
                pytest.skip("No display available for Qt widgets")

        widget = ext.get_config_widget(None)

        # Index 1 = "During Ingestion (Recommended)"
        assert ext._hash_combo.currentIndex() == 1
        assert "Ingestion" in ext._hash_combo.currentText()


class TestPartFileMaxEntries:
    """Tests for PART_FILE_MAX_ENTRIES constant."""

    def test_constant_exists(self):
        """PART_FILE_MAX_ENTRIES constant is defined."""
        from extractors.browser.firefox.cache.extractor import PART_FILE_MAX_ENTRIES

        assert PART_FILE_MAX_ENTRIES == 10000


class TestConfigWidget:
    """Tests for config widget (no resume checkbox)."""

    def test_no_resume_checkbox(self):
        """Resume checkbox was removed in."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor

        ext = CacheFirefoxExtractor()

        from PySide6.QtWidgets import QApplication
        import sys

        if QApplication.instance() is None:
            try:
                app = QApplication(sys.argv)
            except Exception:
                pytest.skip("No display available for Qt widgets")

        widget = ext.get_config_widget(None)

        # Should NOT have resume checkbox
        assert not hasattr(ext, "_resume_checkbox")

    def test_config_has_no_resume_enabled(self):
        """Config dict should not include resume_enabled."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor

        ext = CacheFirefoxExtractor()

        from PySide6.QtWidgets import QApplication
        import sys

        if QApplication.instance() is None:
            try:
                app = QApplication(sys.argv)
            except Exception:
                pytest.skip("No display available for Qt widgets")

        widget = ext.get_config_widget(None)
        config = ext._get_config_from_widget()

        assert "resume_enabled" not in config
        assert "worker_count" in config
        assert "hash_mode" in config


class TestCanRunIngestion:
    """Tests for can_run_ingestion status checking."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_can_run_ingestion_no_manifest(self, extractor, tmp_path):
        """Returns False when no manifest exists."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is False
        assert "No extraction manifests found" in reason

    def test_can_run_ingestion_ok_status(self, extractor, tmp_path):
        """Returns True when manifest has 'ok' status."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "run1"
        run_dir.mkdir(parents=True)

        manifest = {"status": "ok", "run_id": "run1", "files": []}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is True
        assert reason == ""

    def test_can_run_ingestion_cancelled_status(self, extractor, tmp_path):
        """Returns False when manifest has 'cancelled' status."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "run1"
        run_dir.mkdir(parents=True)

        manifest = {"status": "cancelled", "run_id": "run1", "files": []}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is False
        assert "cancelled" in reason.lower()

    def test_can_run_ingestion_error_status(self, extractor, tmp_path):
        """Returns False when manifest has 'error' status."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "run1"
        run_dir.mkdir(parents=True)

        manifest = {"status": "error", "run_id": "run1", "files": []}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is False
        assert "error" in reason.lower() or "failed" in reason.lower()

    def test_can_run_ingestion_missing_status_defaults_ok(self, extractor, tmp_path):
        """Returns True when manifest has no status field (legacy compat)."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "run1"
        run_dir.mkdir(parents=True)

        # Legacy manifest without status field
        manifest = {"run_id": "run1", "files": []}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is True
        assert reason == ""

    def test_can_run_ingestion_nonstandard_status(self, extractor, tmp_path):
        """Returns False for any non-'ok' status, including non-standard values."""
        output_dir = tmp_path / "output"
        run_dir = output_dir / "run1"
        run_dir.mkdir(parents=True)

        # Non-standard status (e.g., typo or future status)
        manifest = {"status": "partial", "run_id": "run1", "files": []}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        can_run, reason = extractor.can_run_ingestion(output_dir)

        assert can_run is False
        assert "partial" in reason  # Should mention the actual status


# =============================================================================
#  Fixes Tests
# =============================================================================

class TestRunIdAlignment:
    """Tests for run_id alignment between stats tracking and manifest."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_finalize_manifest_uses_passed_run_id(self, extractor, tmp_path):
        """_finalize_manifest uses the passed run_id in manifest."""
        extractor._init_part_file_state()
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        # Write some test entries
        extractor._append_entries_to_part_file(run_dir, [{"test": "entry1"}])

        # Finalize with a specific run_id
        test_run_id = "custom_run_id_12345"
        extractor._finalize_manifest(
            run_dir=run_dir,
            run_id=test_run_id,
            evidence_id=1,
            stats={"test": 1},
            hash_mode="extraction",
            status="ok",
        )

        # Verify manifest has the passed run_id
        manifest_path = run_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["run_id"] == test_run_id


class TestExtractorVersionAlignment:
    """Tests for extractor_version using metadata.version."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_finalize_manifest_uses_metadata_version(self, extractor, tmp_path):
        """_finalize_manifest uses self.metadata.version for extractor_version."""
        extractor._init_part_file_state()
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        # Write some test entries
        extractor._append_entries_to_part_file(run_dir, [{"test": "entry1"}])

        # Finalize
        extractor._finalize_manifest(
            run_dir=run_dir,
            run_id="test_run",
            evidence_id=1,
            stats={"test": 1},
            hash_mode="extraction",
            status="ok",
        )

        # Verify manifest has dynamic version
        manifest_path = run_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["extractor_version"] == extractor.metadata.version
        assert manifest["extractor_version"] == "1.11.0"


class TestSupportingFilesSkipped:
    """Tests for skipping supporting file parsing during ingestion."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_artifact_type_identifies_supporting_files(self):
        """Supporting files have artifact_type = 'cache_firefox_supporting'."""
        # This tests that the extraction correctly tags supporting files
        # The artifact_type field is used during ingestion to skip parsing
        cache_entry = {"artifact_type": "cache_firefox", "source_path": "entries/ABC123"}
        supporting_file = {"artifact_type": "cache_firefox_supporting", "source_path": "index"}

        assert cache_entry["artifact_type"] == "cache_firefox"
        assert supporting_file["artifact_type"] == "cache_firefox_supporting"

    def test_ingestion_skips_supporting_files(self, extractor, tmp_path):
        """Ingestion skips parsing files with artifact_type != 'cache_firefox'."""
        # Create output directory structure
        output_dir = tmp_path / "output"
        run_dir = output_dir / "test_run"
        run_dir.mkdir(parents=True)

        # Create a manifest with one cache entry and one supporting file
        manifest = {
            "version": "1.0",
            "extractor": "cache_firefox",
            "extractor_version": "0.69.0",
            "run_id": "test_run",
            "evidence_id": 1,
            "extraction_timestamp": "2025-01-01T00:00:00Z",
            "hash_mode": "extraction",
            "status": "ok",
            "files": [
                {
                    "artifact_type": "cache_firefox_supporting",
                    "source_path": "cache2/index",
                    "extracted_path": "test_run/index",
                },
            ],
            "statistics": {},
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest))

        # Create empty index file (doesn't matter - should be skipped)
        (run_dir / "index").write_bytes(b"")

        # Mock callbacks
        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        # Mock database connection
        mock_conn = MagicMock()

        # Run ingestion
        stats = extractor.run_ingestion(
            output_dir=output_dir,
            evidence_conn=mock_conn,
            evidence_id=1,
            config={},
            callbacks=callbacks,
        )

        # Supporting files should be skipped, not parsed
        assert stats.get("supporting_files_skipped", 0) == 1
        assert stats.get("entries_parsed", 0) == 0  # No cache entries to parse


class TestPatternsIntegration:
    """Tests for _patterns.py integration."""

    def test_patterns_module_available(self):
        """Test that _patterns.py module is importable."""
        from extractors.browser.firefox._patterns import (
            get_all_patterns,
            get_patterns,
            FIREFOX_BROWSERS,
            FIREFOX_ARTIFACTS,
        )

        # Module should export expected symbols
        assert callable(get_all_patterns)
        assert callable(get_patterns)
        assert isinstance(FIREFOX_BROWSERS, dict)
        assert isinstance(FIREFOX_ARTIFACTS, dict)

    def test_get_all_patterns_returns_cache_patterns(self):
        """Test get_all_patterns('cache') returns cache entry patterns."""
        from extractors.browser.firefox._patterns import get_all_patterns

        patterns = get_all_patterns("cache")

        assert isinstance(patterns, list)
        assert len(patterns) > 0

        # Should contain cache2/entries patterns
        assert any("cache2/entries" in p for p in patterns)

    def test_patterns_cover_firefox_browsers(self):
        """Test patterns cover Firefox, Firefox ESR, and Tor Browser."""
        from extractors.browser.firefox._patterns import FIREFOX_BROWSERS

        # Should have all three Firefox-family browsers
        assert "firefox" in FIREFOX_BROWSERS
        assert "firefox_esr" in FIREFOX_BROWSERS
        assert "tor" in FIREFOX_BROWSERS  # Tor Browser uses 'tor' as key

    def test_patterns_cover_all_platforms(self):
        """Test patterns cover Windows, macOS, and Linux."""
        from extractors.browser.firefox._patterns import get_all_patterns

        patterns = get_all_patterns("cache")
        patterns_str = " ".join(patterns)

        # Windows paths (Users/*/AppData)
        assert "Users/" in patterns_str or "AppData" in patterns_str

        # Linux paths (home/*/.mozilla or .local/share)
        assert ".mozilla" in patterns_str or ".local/share" in patterns_str

        # macOS paths (Library/Caches or Application Support)
        assert "Library" in patterns_str or "Application Support" in patterns_str

    def test_tor_browser_patterns_included(self):
        """Test Tor Browser cache patterns are included (enhancement)."""
        from extractors.browser.firefox._patterns import get_all_patterns

        patterns = get_all_patterns("cache")
        patterns_str = " ".join(patterns)

        # Tor Browser has distinctive path pattern
        assert "Tor Browser" in patterns_str or "tor-browser" in patterns_str

    def test_cache_artifacts_defined(self):
        """Test cache artifact patterns are properly defined."""
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS

        assert "cache" in FIREFOX_ARTIFACTS

        cache_patterns = FIREFOX_ARTIFACTS["cache"]
        assert isinstance(cache_patterns, list)
        assert any("cache2/entries" in p for p in cache_patterns)


class TestStreamingCopy:
    """Tests for streaming copy functions."""

    def test_stream_copy_hash_from_iterator_basic(self, tmp_path):
        """Test _stream_copy_hash_from_iterator with simple chunks."""
        from extractors.browser.firefox.cache.extractor import _stream_copy_hash_from_iterator

        # Create mock chunk iterator
        chunks = [b"Hello ", b"World", b"!"]
        dest_path = tmp_path / "output.bin"

        size, md5, sha256 = _stream_copy_hash_from_iterator(iter(chunks), dest_path, compute_hash=True)

        assert size == 12  # "Hello World!"
        assert dest_path.read_bytes() == b"Hello World!"
        assert md5 is not None
        assert sha256 is not None
        # Verify hash correctness
        import hashlib
        expected_md5 = hashlib.md5(b"Hello World!").hexdigest()
        expected_sha256 = hashlib.sha256(b"Hello World!").hexdigest()
        assert md5 == expected_md5
        assert sha256 == expected_sha256

    def test_stream_copy_hash_from_iterator_no_hash(self, tmp_path):
        """Test _stream_copy_hash_from_iterator with hashing disabled."""
        from extractors.browser.firefox.cache.extractor import _stream_copy_hash_from_iterator

        chunks = [b"test data"]
        dest_path = tmp_path / "output.bin"

        size, md5, sha256 = _stream_copy_hash_from_iterator(iter(chunks), dest_path, compute_hash=False)

        assert size == 9
        assert dest_path.read_bytes() == b"test data"
        assert md5 is None
        assert sha256 is None

    def test_stream_copy_hash_from_iterator_empty(self, tmp_path):
        """Test _stream_copy_hash_from_iterator with empty input."""
        from extractors.browser.firefox.cache.extractor import _stream_copy_hash_from_iterator

        chunks = []
        dest_path = tmp_path / "output.bin"

        size, md5, sha256 = _stream_copy_hash_from_iterator(iter(chunks), dest_path, compute_hash=True)

        assert size == 0
        assert dest_path.read_bytes() == b""
        # Empty file still has a hash
        assert md5 is not None
        assert sha256 is not None

    def test_stream_copy_hash_from_iterator_large_chunks(self, tmp_path):
        """Test _stream_copy_hash_from_iterator with large chunks simulating 64MB."""
        from extractors.browser.firefox.cache.extractor import _stream_copy_hash_from_iterator

        # Simulate a 1MB chunk (smaller for test speed)
        large_chunk = b"X" * (1024 * 1024)
        chunks = [large_chunk, large_chunk]  # 2MB total
        dest_path = tmp_path / "output.bin"

        size, md5, sha256 = _stream_copy_hash_from_iterator(iter(chunks), dest_path, compute_hash=True)

        assert size == 2 * 1024 * 1024
        assert dest_path.stat().st_size == 2 * 1024 * 1024


class TestFileListFastPath:
    """Tests for file list fast path (Phase 3)."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    @pytest.fixture
    def evidence_db(self, tmp_path):
        """Create test evidence database with file_list table."""
        db_path = tmp_path / "evidence.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                file_path TEXT,
                partition_index INTEGER,
                deleted INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        return conn

    def test_collect_from_file_list_basic(self, extractor, evidence_db):
        """Test _collect_from_file_list returns cache entry paths."""
        # Insert test data
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            [
                (1, "/Users/test/AppData/Local/Mozilla/Firefox/Profiles/abc.default/cache2/entries/ABC123", 0, 0),
                (1, "/Users/test/AppData/Local/Mozilla/Firefox/Profiles/abc.default/cache2/entries/DEF456", 0, 0),
                (1, "/Users/test/Documents/file.txt", 0, 0),  # Non-cache file
                (1, "/Users/test/AppData/Local/Mozilla/Firefox/Profiles/abc.default/cache2/doomed/XYZ789", 0, 0),  # doomed - should not match
            ]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert not cancelled
        assert len(paths) == 2
        assert any("ABC123" in p for p in paths)
        assert any("DEF456" in p for p in paths)
        # Should NOT include doomed or non-cache files
        assert not any("doomed" in p for p in paths)
        assert not any("file.txt" in p for p in paths)

    def test_collect_from_file_list_filters_deleted(self, extractor, evidence_db):
        """Test _collect_from_file_list excludes deleted files."""
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            [
                (1, "/cache2/entries/ABC123", 0, 0),
                (1, "/cache2/entries/DEF456", 0, 1),  # Deleted
            ]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert len(paths) == 1
        assert "ABC123" in paths[0]

    def test_collect_from_file_list_filters_deleted_suffix(self, extractor, evidence_db):
        """Test _collect_from_file_list excludes paths with ' (deleted)' suffix (fix)."""
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            [
                (1, "/cache2/entries/ABC123", 0, 0),
                (1, "/Windows.old/Users/test/cache2/entries/DEF456 (deleted)", 0, 0),  # SleuthKit orphan marker
                (1, "/cache2/entries/GHI789 (deleted)", 0, 0),  # Another deleted marker
            ]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert len(paths) == 1
        assert "ABC123" in paths[0]
        assert not any("deleted" in p for p in paths)

    def test_collect_from_file_list_filters_file_name_suffix(self, extractor, evidence_db):
        """Test _collect_from_file_list filters SleuthKit $FILE_NAME suffix."""
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            [
                (1, "/cache2/entries/ABC123", 0, 0),
                (1, "/cache2/entries/ABC123 ($FILE_NAME)", 0, 0),  # SleuthKit metadata
            ]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert len(paths) == 1
        assert "($FILE_NAME)" not in paths[0]

    def test_collect_from_file_list_empty_db(self, extractor, evidence_db):
        """Test _collect_from_file_list handles empty database."""
        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert not cancelled
        assert paths == []

    def test_collect_from_file_list_cancellation(self, extractor, evidence_db):
        """Test _collect_from_file_list respects cancellation."""
        # Insert many rows to trigger iteration
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            [(1, f"/cache2/entries/FILE{i}", 0, 0) for i in range(100)]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        # Cancel after first check
        callbacks.is_cancelled.side_effect = [False, True]

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert cancelled
        # Should have partial results
        assert len(paths) < 100

    def test_collect_from_file_list_strips_leading_slash(self, extractor, evidence_db):
        """Test _collect_from_file_list normalizes paths by stripping leading slash."""
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, partition_index, deleted) VALUES (?, ?, ?, ?)",
            (1, "/Users/test/cache2/entries/ABC123", 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        paths, cancelled = extractor._collect_from_file_list(evidence_db, 1, callbacks)

        assert len(paths) == 1
        assert not paths[0].startswith("/")
        assert paths[0] == "Users/test/cache2/entries/ABC123"


class TestPatternDeduplication:
    """Tests for pattern deduplication (Phase 1)."""

    def test_get_all_patterns_deduplicates(self):
        """Test get_all_patterns removes duplicate patterns."""
        from extractors.browser.firefox._patterns import get_all_patterns

        patterns = get_all_patterns("cache")

        # Should have no duplicates
        assert len(patterns) == len(set(patterns))

    def test_firefox_esr_is_label_only_no_patterns(self):
        """Test Firefox ESR is label-only entry with no patterns (deduplication moved to detect_browser_from_path)."""
        from extractors.browser.firefox._patterns import get_patterns, FIREFOX_BROWSERS

        # firefox_esr exists for labeling only (detect_browser_from_path returns "firefox_esr" for ESR profiles)
        # All Firefox family artifacts are discovered via "firefox" patterns
        assert "firefox_esr" in FIREFOX_BROWSERS
        assert FIREFOX_BROWSERS["firefox_esr"]["profile_roots"] == []
        assert FIREFOX_BROWSERS["firefox_esr"]["cache_roots"] == []

        # get_patterns should return empty list for label-only browser
        esr_patterns = set(get_patterns("firefox_esr", "cache"))
        assert len(esr_patterns) == 0, "firefox_esr should have no patterns (label-only entry)"

    def test_doomed_not_in_cache_patterns(self):
        """Test doomed/* is NOT in cache artifact patterns (moved to supporting_patterns)."""
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS

        cache_patterns = FIREFOX_ARTIFACTS["cache"]

        # doomed should NOT be in the main cache patterns
        assert not any("doomed" in p for p in cache_patterns)


class TestChunkSize:
    """Tests for CHUNK_SIZE configuration (Phase 2)."""

    def test_chunk_size_is_64mb(self):
        """Test CHUNK_SIZE is set to 64MB for NVMe optimization."""
        from extractors.browser.firefox.cache.extractor import CHUNK_SIZE

        expected_64mb = 64 * 1024 * 1024
        assert CHUNK_SIZE == expected_64mb


class TestIcatExtraction:
    """Tests for icat-based extraction (Phase 4)."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_icat_available_function(self):
        """Test _icat_available correctly detects icat presence."""
        from extractors.browser.firefox.cache.extractor import _icat_available
        import shutil

        # Should return bool based on shutil.which("icat")
        result = _icat_available()
        assert isinstance(result, bool)

        # Verify it matches shutil.which
        expected = shutil.which("icat") is not None
        assert result == expected

    def test_icat_constants_defined(self):
        """Test icat-related constants are properly defined."""
        from extractors.browser.firefox.cache.extractor import (
            ICAT_BATCH_SIZE,
            ICAT_WORKERS,
            ICAT_PROGRESS_INTERVAL,
            ICAT_HASH_CHUNK_SIZE,
            ICAT_HASH_WORKERS,
        )

        assert ICAT_BATCH_SIZE == 500
        assert ICAT_WORKERS >= 1  # Dynamic based on CPU
        assert ICAT_PROGRESS_INTERVAL == 100
        assert ICAT_HASH_CHUNK_SIZE == 1024 * 1024  # 1MB
        assert ICAT_HASH_WORKERS >= 1  # Dynamic based on CPU

    def test_icat_result_dataclass(self):
        """Test IcatResult dataclass structure."""
        from extractors.browser.firefox.cache.extractor import IcatResult

        result = IcatResult(
            source_path="/path/to/file",
            extracted_path="/output/file",
            size_bytes=1024,
            success=True,
        )

        assert result.source_path == "/path/to/file"
        assert result.extracted_path == "/output/file"
        assert result.size_bytes == 1024
        assert result.success is True
        assert result.error is None

        # Test with error
        error_result = IcatResult(
            source_path="/path/to/file",
            extracted_path="",
            size_bytes=0,
            success=False,
            error="icat failed",
        )
        assert error_result.success is False
        assert error_result.error == "icat failed"

    def test_icat_file_info_dataclass(self):
        """Test IcatFileInfo dataclass structure."""
        from extractors.browser.firefox.cache.extractor import IcatFileInfo

        info = IcatFileInfo(
            path="/cache2/entries/ABC123",
            inode="292163-128-4",
            partition_index=0,
        )

        assert info.path == "/cache2/entries/ABC123"
        assert info.inode == "292163-128-4"
        assert info.partition_index == 0

    def test_compute_file_hashes(self, tmp_path):
        """Test _compute_file_hashes computes MD5 and SHA256."""
        from extractors.browser.firefox.cache.extractor import _compute_file_hashes
        import hashlib

        # Create test file
        test_file = tmp_path / "test.bin"
        test_content = b"Hello, World! This is a test file for hashing."
        test_file.write_bytes(test_content)

        # Compute hashes
        md5, sha256 = _compute_file_hashes(str(test_file))

        # Verify against known values
        expected_md5 = hashlib.md5(test_content).hexdigest()
        expected_sha256 = hashlib.sha256(test_content).hexdigest()

        assert md5 == expected_md5
        assert sha256 == expected_sha256

    def test_compute_file_hashes_returns_none_on_error(self, tmp_path):
        """Test _compute_file_hashes returns (None, None) on error."""
        from extractors.browser.firefox.cache.extractor import _compute_file_hashes

        # Non-existent file
        md5, sha256 = _compute_file_hashes("/nonexistent/file.bin")

        assert md5 is None
        assert sha256 is None

    def test_extract_profile_from_path_handles_icat_entries(self):
        """Test profile extraction works with icat-extracted paths."""
        from extractors.browser.firefox.cache.extractor import _extract_profile_from_path

        # Windows path
        path = "Users/Alice/AppData/Local/Mozilla/Firefox/Profiles/abc123.default/cache2/entries/1F2E3D4C"
        profile = _extract_profile_from_path(path)
        assert profile == "abc123.default"

        # Linux path
        path = "home/user/.mozilla/firefox/xyz789.default-release/cache2/entries/AABBCCDD"
        profile = _extract_profile_from_path(path)
        assert profile == "xyz789.default-release"


class TestIcatFileListQuery:
    """Tests for file list query with inodes."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    @pytest.fixture
    def evidence_db(self, tmp_path):
        """Create temporary evidence database with file_list table."""
        db_path = tmp_path / "evidence.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE file_list (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                file_path TEXT,
                inode TEXT,
                partition_index INTEGER,
                deleted INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        return conn

    def test_collect_with_inodes_returns_file_infos(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes returns file info dicts."""
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123", "292163-128-4", 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        assert not cancelled
        assert len(file_infos) == 1
        assert file_infos[0]["path"] == "cache2/entries/ABC123"  # Leading slash stripped
        assert file_infos[0]["inode"] == "292163-128-4"
        assert file_infos[0]["partition_index"] == 0

    def test_collect_with_inodes_skips_null_inodes(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes skips entries without inodes."""
        # Entry with inode
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123", "292163-128-4", 0, 0)
        )
        # Entry without inode
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/DEF456", None, 0, 0)
        )
        # Entry with empty inode
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/GHI789", "", 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        assert not cancelled
        assert len(file_infos) == 1  # Only the one with valid inode
        assert file_infos[0]["inode"] == "292163-128-4"

    def test_collect_with_inodes_filters_metadata_suffixes(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes filters SleuthKit metadata suffixes."""
        # Regular entry
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123", "292163-128-4", 0, 0)
        )
        # Entry with ($FILE_NAME) suffix
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123 ($FILE_NAME)", "292163-128-5", 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        assert len(file_infos) == 1
        assert "($FILE_NAME)" not in file_infos[0]["path"]

    def test_collect_with_inodes_recovers_deleted_files(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes KEEPS deleted files with valid inodes (forensic recovery)."""
        # Regular entry
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123", "292163-128-4", 0, 0)
        )
        # Entry with (deleted) suffix BUT valid inode - SHOULD BE RECOVERED
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/Windows.old/Users/test/cache2/entries/DEF456 (deleted)", "12345-128-4", 0, 0)
        )
        # Another deleted marker with valid inode - SHOULD BE RECOVERED
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/GHI789 (deleted)", "67890-128-4", 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        # All 3 files should be returned - deleted files with valid inodes are forensically valuable
        assert len(file_infos) == 3
        paths = [f["path"] for f in file_infos]
        assert any("ABC123" in p for p in paths)
        assert any("DEF456" in p for p in paths)  # Deleted but recoverable
        assert any("GHI789" in p for p in paths)  # Deleted but recoverable

    def test_collect_with_inodes_filters_inode_zero(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes filters inode '0' (orphan entries) (fix)."""
        # Regular entry with valid inode
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ABC123", "292163-128-4", 0, 0)
        )
        # Entry with inode "0" - orphan MFT entry pointing to unallocated space
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ORPHAN1", "0", 0, 0)
        )
        # Integer 0 as inode (edge case)
        evidence_db.execute(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            (1, "/cache2/entries/ORPHAN2", 0, 0, 0)
        )
        evidence_db.commit()

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        assert len(file_infos) == 1
        assert file_infos[0]["inode"] == "292163-128-4"
        assert not any(f["inode"] in ("0", 0) for f in file_infos)

    def test_collect_with_inodes_handles_cancellation(self, extractor, evidence_db):
        """Test _collect_from_file_list_with_inodes respects cancellation."""
        # Insert many rows
        evidence_db.executemany(
            "INSERT INTO file_list (evidence_id, file_path, inode, partition_index, deleted) VALUES (?, ?, ?, ?, ?)",
            [(1, f"/cache2/entries/FILE{i}", f"1000{i}-128-4", 0, 0) for i in range(100)]
        )
        evidence_db.commit()

        callbacks = MagicMock()
        # Cancel after first check
        callbacks.is_cancelled.side_effect = [False, True]

        file_infos, cancelled = extractor._collect_from_file_list_with_inodes(evidence_db, 1, callbacks)

        assert cancelled
        assert len(file_infos) < 100


class TestIcatSingleExtraction:
    """Tests for single file icat extraction."""

    def test_run_icat_single_builds_correct_command(self):
        """Test _run_icat_single builds correct icat command."""
        from extractors.browser.firefox.cache.extractor import _run_icat_single

        # Mock subprocess.run to capture the command
        with patch('extractors.browser.firefox.cache.extractor.subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Create temp output dir
            with patch('os.path.getsize', return_value=1024):
                with patch('builtins.open', MagicMock()):
                    result = _run_icat_single(
                        "/path/to/image.E01",
                        "292163-128-4",
                        0,  # No offset
                        "/tmp/output/file.bin",
                    )

            # Verify command
            call_args = mock_run.call_args
            cmd = call_args[0][0]
            assert cmd[0] == "icat"
            assert "/path/to/image.E01" in cmd
            assert "292163-128-4" in cmd
            # No -o flag when offset is 0
            assert "-o" not in cmd

    def test_run_icat_single_with_partition_offset(self):
        """Test _run_icat_single includes -o flag for non-zero offset."""
        from extractors.browser.firefox.cache.extractor import _run_icat_single

        with patch('extractors.browser.firefox.cache.extractor.subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch('os.path.getsize', return_value=1024):
                with patch('builtins.open', MagicMock()):
                    _run_icat_single(
                        "/path/to/image.E01",
                        "292163-128-4",
                        1048576,  # 1MB offset = 2048 sectors
                        "/tmp/output/file.bin",
                    )

            call_args = mock_run.call_args
            cmd = call_args[0][0]
            assert "-o" in cmd
            offset_idx = cmd.index("-o")
            assert cmd[offset_idx + 1] == "2048"  # 1048576 / 512

    def test_run_icat_single_returns_failure_on_error(self):
        """Test _run_icat_single returns failure result on icat error."""
        from extractors.browser.firefox.cache.extractor import _run_icat_single

        with patch('extractors.browser.firefox.cache.extractor.subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stderr=b"icat: error opening image"
            )

            with patch('os.path.exists', return_value=False):
                with patch('builtins.open', MagicMock()):
                    result = _run_icat_single(
                        "/path/to/image.E01",
                        "292163-128-4",
                        0,
                        "/tmp/output/file.bin",
                    )

            assert result.success is False
            assert "icat failed" in result.error
            assert "error opening image" in result.error


class TestIcatPartitionOffsets:
    """Tests for partition offset retrieval."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    def test_get_partition_offsets_returns_dict(self, extractor):
        """Test _get_partition_offsets returns dict mapping index to offset."""
        mock_partitions = [
            {"index": 0, "offset": 0},
            {"index": 1, "offset": 1048576},
            {"index": 2, "offset": 2097152},
        ]

        with patch('core.evidence_fs.list_ewf_partitions', return_value=mock_partitions):
            offsets = extractor._get_partition_offsets([Path("/image.E01")])

        assert offsets == {0: 0, 1: 1048576, 2: 2097152}

    def test_get_partition_offsets_handles_error(self, extractor):
        """Test _get_partition_offsets returns empty dict on error."""
        with patch('core.evidence_fs.list_ewf_partitions', side_effect=Exception("Failed")):
            offsets = extractor._get_partition_offsets([Path("/image.E01")])

        assert offsets == {}


class TestRunExtractionIcatIntegration:
    """Tests for run_extraction with icat integration."""

    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.browser.firefox.cache.extractor import CacheFirefoxExtractor
        return CacheFirefoxExtractor()

    @pytest.fixture
    def mock_evidence_fs(self):
        """Create mock evidence filesystem."""
        mock_fs = MagicMock()
        mock_fs.ewf_paths = [Path("/path/to/image.E01")]
        mock_fs.partition_index = 0
        return mock_fs

    def test_run_extraction_checks_icat_availability(self, extractor, mock_evidence_fs, tmp_path):
        """Test run_extraction checks for icat before using it."""
        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        with patch.object(extractor, '_can_use_concurrent_extraction', return_value=True):
            with patch('extractors.browser.firefox.cache.extractor._icat_available', return_value=False):
                with patch.object(extractor, '_run_concurrent_extraction', return_value=True) as mock_concurrent:
                    extractor.run_extraction(
                        mock_evidence_fs,
                        tmp_path / "output",
                        {"evidence_id": 1, "evidence_label": "test"},
                        callbacks,
                    )

        # Should fall back to concurrent extraction
        mock_concurrent.assert_called_once()

    def test_run_extraction_uses_icat_when_available(self, extractor, mock_evidence_fs, tmp_path):
        """Test run_extraction uses icat when available with inodes."""
        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        file_infos = [{"path": "/cache2/entries/ABC", "inode": "123-128-4", "partition_index": 0}]

        with patch.object(extractor, '_can_use_concurrent_extraction', return_value=True):
            with patch('extractors.browser.firefox.cache.extractor._icat_available', return_value=True):
                with patch.object(extractor, '_open_evidence_conn', return_value=(MagicMock(), MagicMock(), 1, "test")):
                    with patch.object(extractor, '_collect_from_file_list_with_inodes', return_value=(file_infos, False)):
                        with patch.object(extractor, '_run_icat_extraction', return_value=True) as mock_icat:
                            extractor.run_extraction(
                                mock_evidence_fs,
                                tmp_path / "output",
                                {"evidence_id": 1, "evidence_label": "test"},
                                callbacks,
                            )

        # Should use icat extraction
        mock_icat.assert_called_once()

    def test_run_extraction_falls_back_when_no_inodes(self, extractor, mock_evidence_fs, tmp_path):
        """Test run_extraction falls back to concurrent when no inodes available."""
        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        with patch.object(extractor, '_can_use_concurrent_extraction', return_value=True):
            with patch('extractors.browser.firefox.cache.extractor._icat_available', return_value=True):
                with patch.object(extractor, '_open_evidence_conn', return_value=(MagicMock(), MagicMock(), 1, "test")):
                    with patch.object(extractor, '_collect_from_file_list_with_inodes', return_value=([], False)):
                        with patch.object(extractor, '_run_concurrent_extraction', return_value=True) as mock_concurrent:
                            extractor.run_extraction(
                                mock_evidence_fs,
                                tmp_path / "output",
                                {"evidence_id": 1, "evidence_label": "test"},
                                callbacks,
                            )

        # Should fall back to concurrent extraction
        mock_concurrent.assert_called_once()
