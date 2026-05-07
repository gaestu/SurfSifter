"""Tests for Chromium sessions manifest deduplication (Workstream G)."""

import pytest


class TestManifestDeduplication:
    """Verify manifest dedup logic removes duplicates by (logical_path, partition_index)."""

    @staticmethod
    def _dedup(files):
        """Replicate the dedup logic from ChromiumSessionsExtractor.run_extraction."""
        seen = set()
        unique = []
        for f in files:
            key = (f.get("logical_path", ""), f.get("partition_index", 0))
            if key not in seen:
                seen.add(key)
                unique.append(f)
        return unique

    def test_removes_exact_duplicates(self):
        files = [
            {"logical_path": "/Users/test/Current Session", "partition_index": 2},
            {"logical_path": "/Users/test/Current Session", "partition_index": 2},
            {"logical_path": "/Users/test/Last Session", "partition_index": 2},
        ]
        result = self._dedup(files)
        assert len(result) == 2
        paths = [f["logical_path"] for f in result]
        assert "/Users/test/Current Session" in paths
        assert "/Users/test/Last Session" in paths

    def test_preserves_unique_entries(self):
        files = [
            {"logical_path": "/a/Current Session", "partition_index": 1},
            {"logical_path": "/a/Last Session", "partition_index": 1},
            {"logical_path": "/a/Current Tabs", "partition_index": 1},
            {"logical_path": "/a/Last Tabs", "partition_index": 1},
        ]
        result = self._dedup(files)
        assert len(result) == 4

    def test_different_partitions_not_deduplicated(self):
        files = [
            {"logical_path": "/Users/test/Current Session", "partition_index": 1},
            {"logical_path": "/Users/test/Current Session", "partition_index": 2},
        ]
        result = self._dedup(files)
        assert len(result) == 2

    def test_empty_files_list(self):
        assert self._dedup([]) == []

    def test_missing_fields_use_defaults(self):
        files = [
            {"logical_path": "/a/file"},
            {"logical_path": "/a/file"},
        ]
        result = self._dedup(files)
        assert len(result) == 1

    def test_first_occurrence_preserved(self):
        files = [
            {"logical_path": "/a/file", "partition_index": 1, "size_bytes": 100},
            {"logical_path": "/a/file", "partition_index": 1, "size_bytes": 200},
        ]
        result = self._dedup(files)
        assert len(result) == 1
        assert result[0]["size_bytes"] == 100


class TestCollectUrlData:
    """Regression tests for _collect_url_data NameError bug (issue #55)."""

    @staticmethod
    def _make_extractor():
        from extractors.browser.chromium.sessions.extractor import ChromiumSessionsExtractor
        return ChromiumSessionsExtractor.__new__(ChromiumSessionsExtractor)

    def test_collect_url_data_no_name_error(self):
        """_collect_url_data must not raise NameError for normalized_logical_path."""
        extractor = self._make_extractor()
        file_entry = {
            "logical_path": "Users/x1/AppData/Local/Microsoft/Edge/User Data/Default/Sessions/Session_123",
            "browser": "edge",
            "profile": "Default",
            "partition_index": 0,
            "fs_type": "NTFS",
            "forensic_path": None,
        }
        tab_records = [
            {"url": "https://example.com", "last_accessed_utc": "2024-01-01T00:00:00Z"},
        ]
        history_records = [
            {"url": "https://news.example.org/article/1", "timestamp_utc": "2024-01-01T00:01:00Z"},
        ]
        # Must not raise NameError
        result = extractor._collect_url_data(
            tab_records, history_records, "edge", "Default", "run-abc", "test:1.0:run-abc", file_entry
        )
        assert len(result) == 2
        for row in result:
            assert "source_path" in row
            assert row["source_path"] is not None
            assert "normalized_logical_path" not in row

    def test_collect_url_data_skips_chrome_internal_urls(self):
        """Internal Chrome/browser URLs are filtered out."""
        extractor = self._make_extractor()
        file_entry = {
            "logical_path": "Users/x1/Sessions/Tabs_456",
            "browser": "chrome",
            "profile": "Default",
            "partition_index": 0,
            "fs_type": "NTFS",
            "forensic_path": None,
        }
        tab_records = [
            {"url": "chrome://newtab/"},
            {"url": "about:blank"},
            {"url": "https://valid.example.com"},
        ]
        result = extractor._collect_url_data(tab_records, [], "chrome", "Default", "run-1", "t:1:run-1", file_entry)
        assert len(result) == 1
        assert result[0]["url"] == "https://valid.example.com"

    def test_collect_url_data_empty_records(self):
        """Empty tab and history records produce empty URL list."""
        extractor = self._make_extractor()
        file_entry = {
            "logical_path": "Users/x1/Sessions/Session_789",
            "browser": "chrome",
            "profile": "Default",
            "partition_index": 0,
            "fs_type": "NTFS",
            "forensic_path": None,
        }
        result = extractor._collect_url_data([], [], "chrome", "Default", "run-2", "t:1:run-2", file_entry)
        assert result == []


class TestDiscoverFilesReturn:
    """Verify _discover_files_multi_partition has no dead code after final return."""

    def test_no_dead_code_after_return(self):
        """Check that no unreachable statements follow the final return."""
        import ast
        from pathlib import Path

        src = Path(__file__).resolve().parents[2] / "src" / "extractors" / "browser" / "chromium" / "sessions" / "extractor.py"
        tree = ast.parse(src.read_text())

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_discover_files_multi_partition":
                # Check direct children of the function body for consecutive
                # return statements (dead code pattern: return X; return Y)
                body = node.body
                for i, stmt in enumerate(body):
                    if isinstance(stmt, ast.Return) and i < len(body) - 1:
                        next_stmt = body[i + 1]
                        assert not isinstance(next_stmt, ast.Return), (
                            f"Dead code: return at line {next_stmt.lineno} is "
                            f"unreachable after return at line {stmt.lineno}"
                        )
                break
        else:
            pytest.fail("_discover_files_multi_partition not found in extractor.py")
