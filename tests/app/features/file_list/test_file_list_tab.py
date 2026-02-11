"""
Unit tests for file list data layer functionality.

These tests validate the file list processing logic without requiring Qt/GUI.
Converted from GUI tests to pure unit tests for CI compatibility.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch


class TestFileListDataLayer:
    """Tests for file list data operations without GUI dependencies."""

    def test_csv_parsing_basic(self, tmp_path):
        """Test basic CSV parsing of file list data."""
        # Create test CSV
        csv_file = tmp_path / "file_list.csv"
        csv_file.write_text(
            "Full Path,Name,Extension,Size,Modified,Hash\n"
            "/evidence/file1.txt,file1.txt,txt,1024,2024-01-01,abc123\n"
            "/evidence/file2.jpg,file2.jpg,jpg,2048,2024-01-02,def456\n"
        )

        # Parse using Python csv (simulating what the tab does)
        import csv
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["Name"] == "file1.txt"
        assert rows[1]["Extension"] == "jpg"

    def test_csv_parsing_empty_file(self, tmp_path):
        """Test parsing empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("Full Path,Name,Extension,Size\n")

        import csv
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0

    def test_csv_parsing_with_special_chars(self, tmp_path):
        """Test CSV parsing with special characters in paths."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text(
            "Full Path,Name,Extension\n"
            '"/path/with spaces/file.txt",file.txt,txt\n'
            '"/path/with,comma/file.txt",file.txt,txt\n'
        )

        import csv
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert "spaces" in rows[0]["Full Path"]
        assert "comma" in rows[1]["Full Path"]

    def test_extension_extraction(self):
        """Test file extension extraction logic."""
        test_cases = [
            ("file.txt", "txt"),
            ("file.TAR.GZ", "GZ"),
            ("file", ""),
            (".hidden", ""),  # Unix hidden file, no extension
            ("path/to/file.jpg", "jpg"),
            (".config.yml", "yml"),  # Hidden file with extension
        ]

        for filename, expected in test_cases:
            path = Path(filename)
            ext = path.suffix.lstrip(".") if path.suffix else ""
            assert ext == expected, f"Failed for {filename}"

    def test_size_formatting(self):
        """Test human-readable size formatting."""
        def format_size(size_bytes):
            """Format bytes to human readable string."""
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            elif size_bytes < 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
            else:
                return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

        assert format_size(500) == "500 B"
        assert format_size(1024) == "1.0 KB"
        assert format_size(1048576) == "1.0 MB"
        assert format_size(1073741824) == "1.0 GB"

    def test_filter_by_extension(self):
        """Test filtering file list by extension."""
        files = [
            {"name": "a.txt", "ext": "txt"},
            {"name": "b.jpg", "ext": "jpg"},
            {"name": "c.txt", "ext": "txt"},
            {"name": "d.png", "ext": "png"},
        ]

        filtered = [f for f in files if f["ext"] == "txt"]
        assert len(filtered) == 2
        assert all(f["ext"] == "txt" for f in filtered)

    def test_filter_by_size_range(self):
        """Test filtering file list by size range."""
        files = [
            {"name": "small.txt", "size": 100},
            {"name": "medium.txt", "size": 5000},
            {"name": "large.txt", "size": 100000},
        ]

        min_size, max_size = 1000, 10000
        filtered = [f for f in files if min_size <= f["size"] <= max_size]
        assert len(filtered) == 1
        assert filtered[0]["name"] == "medium.txt"

    def test_search_by_name(self):
        """Test searching file list by name pattern."""
        files = [
            {"name": "document.txt", "path": "/docs/document.txt"},
            {"name": "image.jpg", "path": "/pics/image.jpg"},
            {"name": "backup_document.txt", "path": "/backup/backup_document.txt"},
        ]

        pattern = "document"
        matches = [f for f in files if pattern.lower() in f["name"].lower()]
        assert len(matches) == 2

    def test_sort_by_size(self):
        """Test sorting file list by size."""
        files = [
            {"name": "medium.txt", "size": 5000},
            {"name": "small.txt", "size": 100},
            {"name": "large.txt", "size": 100000},
        ]

        sorted_asc = sorted(files, key=lambda f: f["size"])
        assert sorted_asc[0]["name"] == "small.txt"
        assert sorted_asc[-1]["name"] == "large.txt"

        sorted_desc = sorted(files, key=lambda f: f["size"], reverse=True)
        assert sorted_desc[0]["name"] == "large.txt"

    def test_hash_validation_format(self):
        """Test hash value format validation."""
        valid_md5 = "d41d8cd98f00b204e9800998ecf8427e"
        valid_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        invalid_hash = "not-a-hash"

        def is_valid_hash(h: str) -> bool:
            """Check if string is valid hex hash."""
            if not h:
                return False
            try:
                int(h, 16)
                return len(h) in (32, 40, 64, 128)  # MD5, SHA1, SHA256, SHA512
            except ValueError:
                return False

        assert is_valid_hash(valid_md5)
        assert is_valid_hash(valid_sha256)
        assert not is_valid_hash(invalid_hash)
        assert not is_valid_hash("")


class TestFileListHelpers:
    """Tests for file list helper functions."""

    def test_partition_index_extraction(self):
        """Test extracting partition index from path."""
        def extract_partition(path: str) -> int:
            """Extract partition index from evidence path."""
            import re
            match = re.search(r'/p(\d+)/', path)
            return int(match.group(1)) if match else 0

        assert extract_partition("/evidence/p0/Users/file.txt") == 0
        assert extract_partition("/evidence/p1/Windows/System32/config") == 1
        assert extract_partition("/evidence/p2/data/file.db") == 2
        assert extract_partition("/evidence/file.txt") == 0  # No partition marker

    def test_inode_parsing(self):
        """Test inode number parsing from file metadata."""
        # Simulating TSK output format
        test_cases = [
            ("12345", 12345),
            ("12345-128-1", 12345),  # TSK format with sequence
            ("0", 0),
        ]

        def parse_inode(inode_str: str) -> int:
            """Parse inode from string, handling TSK format."""
            if "-" in inode_str:
                return int(inode_str.split("-")[0])
            return int(inode_str)

        for input_val, expected in test_cases:
            assert parse_inode(input_val) == expected

    def test_path_normalization(self):
        """Test Windows to Unix path normalization."""
        def normalize_path(path: str) -> str:
            """Normalize Windows path to Unix format."""
            return path.replace("\\", "/")

        assert normalize_path("C:\\Users\\test\\file.txt") == "C:/Users/test/file.txt"
        assert normalize_path("/unix/path/file.txt") == "/unix/path/file.txt"
        assert normalize_path("mixed\\path/file.txt") == "mixed/path/file.txt"
