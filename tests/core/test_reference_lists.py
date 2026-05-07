"""
Tests for reference lists manager and matcher.
"""
import sqlite3
from hashlib import sha256
from pathlib import Path

import pytest

import core.matching.manager as matching_manager
from core.database import DatabaseManager
from core.matching import ConflictPolicy, ReferenceListManager, ReferenceListMatcher


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


def test_ref_manager_no_create_mode_does_not_create_directories(tmp_path):
    """Read-only reference list access must not create config directories."""
    base_path = tmp_path / "reference_lists"

    manager = ReferenceListManager(base_path=base_path, create_dirs=False)

    assert manager.hashlists_dir == base_path / "hashlists"
    assert not base_path.exists()


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


def test_load_hashlist_with_version_uses_file_bytes(ref_manager):
    """Hash-list version is the SHA-256 of the exact text file bytes."""
    contents = b"# test\nDEADBEEF\n"
    ref_manager.hashlists_dir.mkdir(parents=True, exist_ok=True)
    (ref_manager.hashlists_dir / "versioned.txt").write_bytes(contents)

    hashes, version = ref_manager.load_hashlist_with_version("versioned")

    assert hashes == {"deadbeef"}
    assert version == sha256(contents).hexdigest()


@pytest.mark.parametrize(
    "name",
    [
        "",
        "   ",
        "../escape",
        "nested/name",
        "nested\\name",
        "bad\nname",
        "bad\u202ename",
        "bad\u200bname",
        "fullwidth\uff0fslash",
    ],
)
def test_load_hashlist_with_version_rejects_unsafe_names(ref_manager, name):
    """Hash-list loading rejects names that could escape or spoof paths."""
    with pytest.raises(ValueError):
        ref_manager.load_hashlist_with_version(name)


def test_load_hashlist_with_version_allows_empty_stored_list(ref_manager):
    """Stored empty hash lists can be loaded and viewed without import validation."""
    ref_manager.hashlists_dir.mkdir(parents=True, exist_ok=True)
    (ref_manager.hashlists_dir / "empty.txt").write_bytes(b"")

    hashes, version = ref_manager.load_hashlist_with_version("empty")

    assert hashes == set()
    assert version == sha256(b"").hexdigest()
    assert ref_manager.read_list_text("hashlist", "empty") == ""


def test_load_hashlist_with_version_rejects_symlinks(ref_manager, tmp_path):
    """Hash-list loading keeps the existing no-follow symlink protection."""
    target = tmp_path / "target.txt"
    target.write_text("deadbeef\n", encoding="utf-8")
    symlink = ref_manager.hashlists_dir / "linked.txt"
    symlink.symlink_to(target)

    with pytest.raises(ValueError, match="Symlinked files"):
        ref_manager.load_hashlist_with_version("linked")


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


def test_import_urllist_batch_generates_metadata(ref_manager, tmp_path):
    """URL batch import should generate metadata for plain text files."""
    first = tmp_path / "gambling.txt"
    second = tmp_path / "social.txt"
    first.write_text("*.casino.example\nexample.net/path\n", encoding="utf-8")
    second.write_text("# comment\nsocial.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [first, second],
        category="Investigation",
        description="Shared URL patterns",
        is_regex=True,
    )

    assert [result.status for result in results] == ["imported", "imported"]
    assert "gambling" in ref_manager.list_available()["urllists"]
    assert "social" in ref_manager.list_available()["urllists"]

    content = (ref_manager.urllists_dir / "gambling.txt").read_text(encoding="utf-8")
    assert "# NAME: gambling" in content
    assert "# CATEGORY: Investigation" in content
    assert "# DESCRIPTION: Shared URL patterns" in content
    assert "# TYPE: urllist" in content
    assert "# REGEX: true" in content
    assert "*.casino.example" in content

    patterns, is_regex = ref_manager.load_urllist("gambling")
    assert patterns == ["*.casino.example", "example.net/path"]
    assert is_regex is True


def test_import_urllist_batch_normalizes_multiline_metadata(ref_manager, tmp_path):
    """Generated metadata values should stay on comment lines."""
    source = tmp_path / "multiline.txt"
    source.write_text("*.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        category="Category",
        description="Line one\nLine two",
        is_regex=True,
    )

    assert results[0].status == "imported"
    metadata = ref_manager.get_metadata("urllist", "multiline")
    assert metadata["DESCRIPTION"] == "Line one Line two"
    assert metadata["REGEX"] == "true"

    patterns, is_regex = ref_manager.load_urllist("multiline")
    assert patterns == ["*.example"]
    assert is_regex is True


def test_import_urllist_batch_preserves_existing_metadata(ref_manager, tmp_path):
    """Existing SurfSifter URL-list headers should be copied through."""
    source = tmp_path / "preserved.txt"
    source.write_text(
        "# NAME: Original Name\n"
        "# CATEGORY: Existing\n"
        "# DESCRIPTION: Existing description\n"
        "# TYPE: urllist\n"
        "# REGEX: false\n"
        "\n"
        "*.preserved.example\n",
        encoding="utf-8",
    )

    results = ref_manager.import_urllist_batch(
        [source],
        category="Generated",
        description="Generated description",
        is_regex=True,
    )

    assert results[0].status == "imported"
    content = (ref_manager.urllists_dir / "preserved.txt").read_text(encoding="utf-8")
    assert "# NAME: Original Name" in content
    assert "# CATEGORY: Existing" in content
    assert "# DESCRIPTION: Existing description" in content
    assert "# REGEX: false" in content
    assert "Generated description" not in content


def test_import_urllist_batch_skip_conflict(ref_manager, tmp_path):
    """Skip policy should leave an existing URL list untouched."""
    ref_manager.create_list(
        "urllist",
        "conflict",
        {"NAME": "Existing", "TYPE": "urllist", "REGEX": "false"},
        ["existing.example"],
    )
    source = tmp_path / "conflict.txt"
    source.write_text("new.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        conflict_policy=ConflictPolicy.SKIP,
        category="Category",
        description="Description",
    )

    assert results[0].status == "skipped"
    patterns, _ = ref_manager.load_urllist("conflict")
    assert patterns == ["existing.example"]


def test_import_urllist_batch_overwrite_conflict(ref_manager, tmp_path):
    """Overwrite policy should replace an existing URL list."""
    ref_manager.create_list(
        "urllist",
        "conflict",
        {"NAME": "Existing", "TYPE": "urllist", "REGEX": "false"},
        ["existing.example"],
    )
    source = tmp_path / "conflict.txt"
    source.write_text("new.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        conflict_policy=ConflictPolicy.OVERWRITE,
        category="Category",
        description="Description",
    )

    assert results[0].status == "overwritten"
    patterns, _ = ref_manager.load_urllist("conflict")
    assert patterns == ["new.example"]


def test_import_urllist_batch_rename_conflict(ref_manager, tmp_path):
    """Rename policy should suffix imported URL list names."""
    ref_manager.create_list(
        "urllist",
        "conflict",
        {"NAME": "Existing", "TYPE": "urllist", "REGEX": "false"},
        ["existing.example"],
    )
    source = tmp_path / "conflict.txt"
    source.write_text("renamed.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        conflict_policy=ConflictPolicy.RENAME,
        category="Category",
        description="Description",
    )

    assert results[0].status == "renamed"
    assert results[0].dest_name == "conflict_1"
    patterns, _ = ref_manager.load_urllist("conflict_1")
    assert patterns == ["renamed.example"]


def test_import_urllist_batch_reports_invalid_utf8(ref_manager, tmp_path):
    """Invalid UTF-8 should be reported without aborting the batch."""
    bad = tmp_path / "bad.txt"
    good = tmp_path / "good.txt"
    bad.write_bytes(b"\xff\xfe\x00")
    good.write_text("good.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [bad, good],
        category="Category",
        description="Description",
    )

    assert [result.status for result in results] == ["error", "imported"]
    assert "Invalid UTF-8 encoding" in (results[0].error or "")
    assert "good" in ref_manager.list_available()["urllists"]


def test_import_urllist_batch_reports_empty_or_comment_only(ref_manager, tmp_path):
    """Empty/comment-only URL lists should be reported as invalid."""
    empty = tmp_path / "empty.txt"
    comments = tmp_path / "comments.txt"
    empty.write_text("", encoding="utf-8")
    comments.write_text("# only comments\n\n# still comments\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [empty, comments],
        category="Category",
        description="Description",
    )

    assert [result.status for result in results] == ["error", "error"]
    assert results[0].error == "File is empty"
    assert "No valid URL patterns found" in (results[1].error or "")


def test_import_urllist_batch_rejects_unsafe_stem(ref_manager, tmp_path):
    """Unsafe URL-list filenames should be rejected before destination paths are built."""
    source = tmp_path / "bad\u202ename.txt"
    source.write_text("bad.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        category="Category",
        description="Description",
    )

    assert results[0].status == "error"
    assert "Invalid reference list name" in (results[0].error or "")
    assert not (ref_manager.urllists_dir / "bad\u202ename.txt").exists()


def test_import_urllist_batch_rejects_oversized_file(ref_manager, tmp_path, monkeypatch):
    """Oversized URL lists should be rejected before import."""
    monkeypatch.setattr(matching_manager, "MAX_URLLIST_SIZE", 4)
    source = tmp_path / "large.txt"
    source.write_text("large.example\n", encoding="utf-8")

    results = ref_manager.import_urllist_batch(
        [source],
        category="Category",
        description="Description",
    )

    assert results[0].status == "error"
    assert "File too large" in (results[0].error or "")
    assert "large" not in ref_manager.list_available()["urllists"]


def test_import_urllist_batch_rejects_symlink(ref_manager, tmp_path):
    """Symlinked URL-list inputs should not be followed or copied."""
    target = tmp_path / "outside.txt"
    target.write_text("secret.example\n", encoding="utf-8")
    source = tmp_path / "linked.txt"
    source.symlink_to(target)

    results = ref_manager.import_urllist_batch(
        [source],
        category="Category",
        description="Description",
    )

    assert results[0].status == "error"
    assert "Symlinked files are not supported" in (results[0].error or "")
    assert "linked" not in ref_manager.list_available()["urllists"]


def test_import_urllist_batch_replaces_destination_symlink_without_following(ref_manager, tmp_path):
    """Overwrite should replace a destination symlink without writing through it."""
    source = tmp_path / "safe.txt"
    source.write_text("safe.example\n", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("unchanged\n", encoding="utf-8")
    dest = ref_manager.urllists_dir / "safe.txt"
    dest.symlink_to(outside)

    results = ref_manager.import_urllist_batch(
        [source],
        conflict_policy=ConflictPolicy.OVERWRITE,
        category="Category",
        description="Description",
    )

    assert results[0].status == "overwritten"
    assert outside.read_text(encoding="utf-8") == "unchanged\n"
    assert not dest.is_symlink()
    assert "safe.example" in dest.read_text(encoding="utf-8")


def test_import_hashlist_batch_rejects_symlink(ref_manager, tmp_path):
    """Symlinked hash-list inputs should not be followed or copied."""
    target = tmp_path / "outside_hashes.txt"
    target.write_text("d41d8cd98f00b204e9800998ecf8427e\n", encoding="utf-8")
    source = tmp_path / "linked_hashes.txt"
    source.symlink_to(target)

    results = ref_manager.import_hashlist_batch([source])

    assert results[0].status == "error"
    assert "Symlinked files are not supported" in (results[0].error or "")
    assert "linked_hashes" not in ref_manager.list_available()["hashlists"]


def test_import_hashlist_batch_rejects_unsafe_stem(ref_manager, tmp_path):
    """Unsafe hash-list filenames should be rejected before destination paths are built."""
    source = tmp_path / "bad\u202ename.txt"
    source.write_text("d41d8cd98f00b204e9800998ecf8427e\n", encoding="utf-8")

    results = ref_manager.import_hashlist_batch([source])

    assert results[0].status == "error"
    assert "Invalid reference list name" in (results[0].error or "")
    assert not (ref_manager.hashlists_dir / "bad\u202ename.txt").exists()


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
