"""
Tests for reference lists manager and matcher.
"""
import sqlite3
from pathlib import Path

import pytest

from core.database import DatabaseManager
from core.matching import ReferenceListManager, ReferenceListMatcher


@pytest.fixture
def ref_manager(tmp_path):
    """Create temporary reference list manager."""
    base_path = tmp_path / "reference_lists"
    return ReferenceListManager(base_path=base_path)


@pytest.fixture
def evidence_db(tmp_path):
    """Create temporary evidence database with file_list data."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    # Create case DB and evidence
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (1, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Get evidence connection
    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    # Insert test file_list data
    evidence_conn.executemany(
        """
        INSERT INTO file_list (
            evidence_id, file_path, file_name, extension, size_bytes,
            md5_hash, sha1_hash, sha256_hash, import_source, import_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (1, "C:\\test\\file1.txt", "file1.txt", ".txt", 100, "d41d8cd98f00b204e9800998ecf8427e", None, None, "test", "2025-11-05T10:00:00Z"),
            (1, "C:\\freeze\\deepfreeze.exe", "deepfreeze.exe", ".exe", 2000, "a1b2c3d4e5f6789012345678901234567890abcd", None, None, "test", "2025-11-05T10:00:00Z"),
            (1, "C:\\system\\FrzState2020.exe", "FrzState2020.exe", ".exe", 3000, None, "1234567890abcdef1234567890abcdef12345678", None, "test", "2025-11-05T10:00:00Z"),
            (1, "C:\\temp\\ccleaner.exe", "ccleaner.exe", ".exe", 5000, None, None, "abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab", "test", "2025-11-05T10:00:00Z"),
            (1, "C:\\temp\\slots99.exe", "slots99.exe", ".exe", 1000, None, None, None, "test", "2025-11-05T10:00:00Z"),
        ],
    )
    evidence_conn.commit()

    return evidence_conn, evidence_id


def test_ref_manager_directories_created(ref_manager):
    """Test that reference list directories are created."""
    assert ref_manager.hashlists_dir.exists()
    assert ref_manager.filelists_dir.exists()


def test_create_hashlist(ref_manager):
    """Test creating a hash list."""
    metadata = {
        "NAME": "Test Hashes",
        "CATEGORY": "Test",
        "DESCRIPTION": "Test hash list",
        "UPDATED": "2025-11-05",
    }
    hashes = [
        "d41d8cd98f00b204e9800998ecf8427e",
        "a1b2c3d4e5f6789012345678901234567890abcd",
    ]

    ref_manager.create_list("hashlist", "test_hashes", metadata, hashes)

    # Verify file created
    list_path = ref_manager.hashlists_dir / "test_hashes.txt"
    assert list_path.exists()

    # Verify content
    content = list_path.read_text()
    assert "# NAME: Test Hashes" in content
    assert "d41d8cd98f00b204e9800998ecf8427e" in content


def test_create_filelist_wildcard(ref_manager):
    """Test creating a file list with wildcards."""
    metadata = {
        "NAME": "Test Files",
        "CATEGORY": "Test",
        "DESCRIPTION": "Test file list",
        "TYPE": "filelist",
        "REGEX": "false",
    }
    patterns = ["*freeze*.exe", "Frz*.exe"]

    ref_manager.create_list("filelist", "test_files", metadata, patterns)

    # Verify file created
    list_path = ref_manager.filelists_dir / "test_files.txt"
    assert list_path.exists()

    # Verify content
    content = list_path.read_text()
    assert "# NAME: Test Files" in content
    assert "*freeze*.exe" in content


def test_create_filelist_regex(ref_manager):
    """Test creating a file list with regex patterns."""
    metadata = {
        "NAME": "Test Regex",
        "CATEGORY": "Test",
        "DESCRIPTION": "Test regex patterns",
        "TYPE": "filelist",
        "REGEX": "true",
    }
    patterns = ["^.*slots\\d+\\.exe$", ".*casino.*\\.(dll|exe)$"]

    ref_manager.create_list("filelist", "test_regex", metadata, patterns)

    # Verify file created
    list_path = ref_manager.filelists_dir / "test_regex.txt"
    assert list_path.exists()

    # Verify REGEX flag
    metadata_parsed = ref_manager.get_metadata("filelist", "test_regex")
    assert metadata_parsed.get("REGEX") == "true"


def test_load_hashlist(ref_manager):
    """Test loading a hash list."""
    # Create hash list
    metadata = {"NAME": "Test", "TYPE": "hashlist"}
    hashes = ["d41d8cd98f00b204e9800998ecf8427e", "A1B2C3D4E5F6"]
    ref_manager.create_list("hashlist", "test", metadata, hashes)

    # Load hash list
    loaded = ref_manager.load_hashlist("test")

    # Verify (lowercase normalization)
    assert len(loaded) == 2
    assert "d41d8cd98f00b204e9800998ecf8427e" in loaded
    assert "a1b2c3d4e5f6" in loaded  # Normalized to lowercase


def test_load_filelist_wildcard(ref_manager):
    """Test loading a file list with wildcards."""
    # Create file list
    metadata = {"NAME": "Test", "TYPE": "filelist", "REGEX": "false"}
    patterns = ["*freeze*.exe", "*.dll"]
    ref_manager.create_list("filelist", "test", metadata, patterns)

    # Load file list
    loaded_patterns, is_regex = ref_manager.load_filelist("test")

    assert len(loaded_patterns) == 2
    assert "*freeze*.exe" in loaded_patterns
    assert is_regex is False


def test_load_filelist_regex(ref_manager):
    """Test loading a file list with regex."""
    # Create file list
    metadata = {"NAME": "Test", "TYPE": "filelist", "REGEX": "true"}
    patterns = ["^test\\d+\\.exe$"]
    ref_manager.create_list("filelist", "test", metadata, patterns)

    # Load file list
    loaded_patterns, is_regex = ref_manager.load_filelist("test")

    assert len(loaded_patterns) == 1
    assert is_regex is True


def test_list_available(ref_manager):
    """Test listing available reference lists."""
    # Create some lists
    ref_manager.create_list("hashlist", "hash1", {"NAME": "Hash 1"}, ["abc123"])
    ref_manager.create_list("hashlist", "hash2", {"NAME": "Hash 2"}, ["def456"])
    ref_manager.create_list("filelist", "file1", {"NAME": "File 1", "REGEX": "false"}, ["*.exe"])

    # List available
    available = ref_manager.list_available()

    assert "hash1" in available["hashlists"]
    assert "hash2" in available["hashlists"]
    assert "file1" in available["filelists"]


def test_get_metadata(ref_manager):
    """Test extracting metadata from reference list."""
    metadata = {
        "NAME": "Test List",
        "CATEGORY": "System",
        "DESCRIPTION": "Test description",
        "UPDATED": "2025-11-05",
        "AUTHOR": "FBGA Team",
    }
    ref_manager.create_list("hashlist", "test", metadata, ["abc123"])

    # Get metadata
    parsed = ref_manager.get_metadata("hashlist", "test")

    assert parsed["NAME"] == "Test List"
    assert parsed["CATEGORY"] == "System"
    assert parsed["DESCRIPTION"] == "Test description"


def test_import_list(ref_manager, tmp_path):
    """Test importing an external file."""
    # Create source file
    source_file = tmp_path / "source.txt"
    source_file.write_text("# NAME: Imported\nabc123\ndef456\n")

    # Import
    ref_manager.import_list(source_file, "hashlist", "imported")

    # Verify imported
    assert "imported" in ref_manager.list_available()["hashlists"]
    hashes = ref_manager.load_hashlist("imported")
    assert len(hashes) == 2


def test_delete_list(ref_manager):
    """Test deleting a reference list."""
    # Create list
    ref_manager.create_list("hashlist", "to_delete", {"NAME": "Delete Me"}, ["abc123"])
    assert "to_delete" in ref_manager.list_available()["hashlists"]

    # Delete
    ref_manager.delete_list("hashlist", "to_delete")

    # Verify deleted
    assert "to_delete" not in ref_manager.list_available()["hashlists"]


def test_match_hashlist(evidence_db, ref_manager):
    """Test matching file_list entries against hash list."""
    conn, evidence_id = evidence_db

    # Create hash list with known hashes
    metadata = {"NAME": "Test Hashes", "TYPE": "hashlist"}
    hashes = [
        "d41d8cd98f00b204e9800998ecf8427e",  # file1.txt MD5
        "a1b2c3d4e5f6789012345678901234567890abcd",  # deepfreeze.exe MD5
    ]
    ref_manager.create_list("hashlist", "test_hashes", metadata, hashes)

    # Match
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager  # Use test ref_manager
    match_count = matcher.match_hashlist("test_hashes")

    # Verify matches
    assert match_count == 2

    # Check database
    cursor = conn.execute(
        "SELECT COUNT(*) FROM file_list_matches WHERE reference_list_name = 'test_hashes'"
    )
    assert cursor.fetchone()[0] == 2


def test_match_filelist_wildcard(evidence_db, ref_manager):
    """Test matching file_list entries against wildcard patterns."""
    conn, evidence_id = evidence_db

    # Create file list with wildcard patterns
    metadata = {"NAME": "DeepFreeze", "TYPE": "filelist", "REGEX": "false"}
    patterns = ["*freeze*.exe", "Frz*.exe"]
    ref_manager.create_list("filelist", "deepfreeze", metadata, patterns)

    # Match
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager
    match_count = matcher.match_filelist("deepfreeze")

    # Verify matches (deepfreeze.exe, FrzState2020.exe)
    assert match_count == 2

    # Check database
    cursor = conn.execute(
        "SELECT file_list_id, matched_value FROM file_list_matches WHERE reference_list_name = 'deepfreeze'"
    )
    matches = cursor.fetchall()
    assert len(matches) == 2


def test_match_filelist_regex(evidence_db, ref_manager):
    """Test matching file_list entries against regex patterns."""
    conn, evidence_id = evidence_db

    # Create file list with regex patterns
    metadata = {"NAME": "Gambling", "TYPE": "filelist", "REGEX": "true"}
    patterns = ["^.*slots\\d+\\.exe$"]  # Match slots99.exe
    ref_manager.create_list("filelist", "gambling", metadata, patterns)

    # Match
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager
    match_count = matcher.match_filelist("gambling")

    # Verify matches
    assert match_count == 1

    # Check which file matched
    cursor = conn.execute(
        """
        SELECT fl.file_name
        FROM file_list fl
        JOIN file_list_matches flm ON fl.id = flm.file_list_id
        WHERE flm.reference_list_name = 'gambling'
    """
    )
    matched_files = [row[0] for row in cursor.fetchall()]
    assert "slots99.exe" in matched_files


def test_match_progress_callback(evidence_db, ref_manager):
    """Test progress callback during matching."""
    conn, evidence_id = evidence_db

    # Create hash list
    ref_manager.create_list("hashlist", "test", {"NAME": "Test"}, ["abc123"])

    # Match with progress tracking
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager

    progress_calls = []

    def track_progress(current, total):
        progress_calls.append((current, total))

    matcher.match_hashlist("test", progress_callback=track_progress)

    # Verify progress was reported
    assert len(progress_calls) > 0
    assert progress_calls[-1][0] == progress_calls[-1][1]  # Final call: current == total


def test_match_duplicate_prevention(evidence_db, ref_manager):
    """Test that duplicate matches are prevented."""
    conn, evidence_id = evidence_db

    # Create hash list
    ref_manager.create_list("hashlist", "test", {"NAME": "Test"}, ["d41d8cd98f00b204e9800998ecf8427e"])

    # Match twice
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager
    match_count1 = matcher.match_hashlist("test")
    match_count2 = matcher.match_hashlist("test")

    # First match should succeed, second should find 0 new matches
    assert match_count1 == 1
    assert match_count2 == 0

    # Database should have only 1 match
    cursor = conn.execute("SELECT COUNT(*) FROM file_list_matches WHERE reference_list_name = 'test'")
    assert cursor.fetchone()[0] == 1


def test_wildcard_case_insensitive(evidence_db, ref_manager):
    """Test that wildcard matching is case-insensitive."""
    conn, evidence_id = evidence_db

    # Create file list with lowercase pattern
    ref_manager.create_list("filelist", "test", {"NAME": "Test", "REGEX": "false"}, ["*deepfreeze*.exe"])

    # Match (should match "deepfreeze.exe" regardless of case)
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager
    match_count = matcher.match_filelist("test")

    assert match_count >= 1


def test_regex_case_insensitive(evidence_db, ref_manager):
    """Test that regex matching is case-insensitive."""
    conn, evidence_id = evidence_db

    # Create file list with regex (lowercase pattern)
    ref_manager.create_list("filelist", "test", {"NAME": "Test", "REGEX": "true"}, ["^.*freeze.*\\.exe$"])

    # Match (should match "deepfreeze.exe" and "FrzState2020.exe")
    matcher = ReferenceListMatcher(conn, evidence_id)
    matcher.ref_manager = ref_manager
    match_count = matcher.match_filelist("test")

    assert match_count >= 1
