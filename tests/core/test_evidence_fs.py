from pathlib import Path

import pytest

from core.evidence_fs import MountedFS, find_ewf_segments


def test_mounted_fs_iter_and_open(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    data_dir = mount / "Users" / "Alice" / "AppData"
    data_dir.mkdir(parents=True)
    file_path = data_dir / "example.txt"
    file_path.write_text("https://example.com", encoding="utf-8")

    fs = MountedFS(mount)
    paths = list(fs.iter_paths("Users/*/AppData/*.txt"))
    assert "Users/Alice/AppData/example.txt" in paths

    with fs.open_for_read("Users/Alice/AppData/example.txt") as handle:
        content = handle.read().decode("utf-8")
        assert content == "https://example.com"

    assert fs.list_users() == ["Alice"]


def test_mounted_fs_open_rejects_path_traversal(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    mount.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")

    fs = MountedFS(mount)
    with pytest.raises(ValueError, match="Path traversal attempt"):
        fs.open_for_read("../secret.txt")


def test_mounted_fs_stat_rejects_path_traversal(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    mount.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")

    fs = MountedFS(mount)
    with pytest.raises(ValueError, match="Path traversal attempt"):
        fs.stat("../secret.txt")


def test_find_ewf_segments_single(tmp_path: Path) -> None:
    """Test discovery of single-segment E01 file."""
    e01 = tmp_path / "evidence.E01"
    e01.write_text("fake ewf data")

    segments = find_ewf_segments(e01)
    assert len(segments) == 1
    assert segments[0] == e01


def test_find_ewf_segments_multiple_uppercase(tmp_path: Path) -> None:
    """Test discovery of multi-segment E01 files (uppercase)."""
    e01 = tmp_path / "image.E01"
    e02 = tmp_path / "image.E02"
    e03 = tmp_path / "image.E03"

    e01.write_text("segment 1")
    e02.write_text("segment 2")
    e03.write_text("segment 3")

    segments = find_ewf_segments(e01)
    assert len(segments) == 3
    assert segments == [e01, e02, e03]


def test_find_ewf_segments_multiple_lowercase(tmp_path: Path) -> None:
    """Test discovery of multi-segment e01 files (lowercase)."""
    e01 = tmp_path / "case.e01"
    e02 = tmp_path / "case.e02"

    e01.write_text("segment 1")
    e02.write_text("segment 2")

    segments = find_ewf_segments(e01)
    assert len(segments) == 2
    assert segments == [e01, e02]


def test_find_ewf_segments_gaps(tmp_path: Path) -> None:
    """Test that segment discovery stops at first gap."""
    e01 = tmp_path / "data.E01"
    e02 = tmp_path / "data.E02"
    e04 = tmp_path / "data.E04"  # Gap at E03

    e01.write_text("segment 1")
    e02.write_text("segment 2")
    e04.write_text("segment 4")

    segments = find_ewf_segments(e01)
    # Should stop at E03 (missing), not include E04
    assert len(segments) == 2
    assert segments == [e01, e02]


def test_find_ewf_segments_not_found(tmp_path: Path) -> None:
    """Test error when first segment doesn't exist."""
    missing = tmp_path / "missing.E01"

    with pytest.raises(FileNotFoundError, match="E01 segment not found"):
        find_ewf_segments(missing)


def test_find_ewf_segments_unusual_extension(tmp_path: Path) -> None:
    """Test handling of non-standard extensions (fallback to single file)."""
    unusual = tmp_path / "data.raw"
    unusual.write_text("raw disk image")

    # Should return single file (with warning logged)
    segments = find_ewf_segments(unusual)
    assert len(segments) == 1
    assert segments[0] == unusual


# =============================================================================
# Junction Loop Detection Tests (- Sync Data Hang Fix)
# =============================================================================


class TestMountedFSJunctionLoops:
    """Test junction/symlink loop handling in MountedFS.

    Tests verify that the filesystem walking doesn't hang on circular
    directory structures (symlinks pointing to parent directories).
    """

    def test_symlink_loop_detection(self, tmp_path: Path) -> None:
        """Test that symlink loops don't cause infinite iteration.

        Simulates NTFS junction like Application Data -> AppData/Roaming.
        """
        mount = tmp_path / "mount"
        users = mount / "Users" / "Alice" / "AppData" / "Roaming"
        users.mkdir(parents=True)

        # Create a file to find
        target_file = users / "test.txt"
        target_file.write_text("data")

        # Create a symlink loop (Linux equivalent of NTFS junction)
        # Application Data -> AppData/Roaming
        app_data_link = mount / "Users" / "Alice" / "Application Data"
        try:
            app_data_link.symlink_to(users, target_is_directory=True)
        except OSError:
            pytest.skip("Unable to create symlink (may need elevated permissions)")

        fs = MountedFS(mount)

        # Should complete without hanging, finding the file via direct path
        # but not infinitely recursing through symlink
        paths = list(fs.iter_paths("Users/*/AppData/Roaming/*.txt"))
        assert "Users/Alice/AppData/Roaming/test.txt" in paths

    def test_iter_paths_completes_with_loops(self, tmp_path: Path) -> None:
        """Test that iter_paths completes even with circular structures.

        Creates multiple levels of circular symlinks to stress-test.
        """
        mount = tmp_path / "mount"
        data_dir = mount / "data" / "level1" / "level2"
        data_dir.mkdir(parents=True)

        # Create a file
        (data_dir / "file.txt").write_text("content")

        # Create circular symlink: level2/back -> level1
        try:
            (data_dir / "back").symlink_to(mount / "data" / "level1", target_is_directory=True)
        except OSError:
            pytest.skip("Unable to create symlink")

        fs = MountedFS(mount)

        # Collect all paths with a timeout-like limit
        paths = []
        max_paths = 1000  # Safety limit
        for i, path in enumerate(fs.iter_paths("data/**/*.txt")):
            paths.append(path)
            if i > max_paths:
                pytest.fail("iter_paths did not terminate - likely infinite loop")

        # Should find the file once
        assert any("file.txt" in p for p in paths)


class TestMountedFSWalkDirectory:
    """Test walk_directory method on MountedFS.

    walk_directory is an optimized method for walking a known directory
    without pattern matching overhead.
    """

    def test_walk_directory_basic(self, tmp_path: Path) -> None:
        """Test basic walk_directory functionality."""
        mount = tmp_path / "mount"
        subdir = mount / "data" / "storage"
        subdir.mkdir(parents=True)

        # Create some files
        (subdir / "file1.txt").write_text("content1")
        (subdir / "file2.log").write_text("content2")

        fs = MountedFS(mount)

        files = list(fs.walk_directory("data/storage"))
        assert len(files) == 2
        assert any("file1.txt" in f for f in files)
        assert any("file2.log" in f for f in files)

    def test_walk_directory_nested(self, tmp_path: Path) -> None:
        """Test walk_directory with nested subdirectories."""
        mount = tmp_path / "mount"
        nested = mount / "data" / "level1" / "level2"
        nested.mkdir(parents=True)

        (mount / "data" / "root.txt").write_text("root")
        (mount / "data" / "level1" / "mid.txt").write_text("mid")
        (nested / "deep.txt").write_text("deep")

        fs = MountedFS(mount)

        files = list(fs.walk_directory("data"))
        assert len(files) == 3
        assert any("root.txt" in f for f in files)
        assert any("mid.txt" in f for f in files)
        assert any("deep.txt" in f for f in files)

    def test_walk_directory_empty(self, tmp_path: Path) -> None:
        """Test walk_directory on empty directory."""
        mount = tmp_path / "mount"
        empty = mount / "empty"
        empty.mkdir(parents=True)

        fs = MountedFS(mount)

        files = list(fs.walk_directory("empty"))
        assert len(files) == 0

    def test_walk_directory_missing(self, tmp_path: Path) -> None:
        """Test walk_directory on non-existent directory."""
        mount = tmp_path / "mount"
        mount.mkdir(parents=True)

        fs = MountedFS(mount)

        # Should return empty, not raise
        files = list(fs.walk_directory("nonexistent"))
        assert len(files) == 0

    def test_walk_directory_only_returns_files(self, tmp_path: Path) -> None:
        """Test that walk_directory only yields files, not directories."""
        mount = tmp_path / "mount"
        subdir = mount / "data" / "subdir"
        subdir.mkdir(parents=True)

        (mount / "data" / "file.txt").write_text("content")

        fs = MountedFS(mount)

        files = list(fs.walk_directory("data"))
        # Should only have the file, not 'subdir'
        assert len(files) == 1
        assert "file.txt" in files[0]


class TestPyEwfTskFSWalkCycleDetection:
    """Test cycle detection in PyEwfTskFS._walk() method.

    These tests use object.__new__ to instantiate PyEwfTskFS without calling
    __init__, then inject mocked _fs/_pytsk3 attributes. This exercises the
    actual production _walk method rather than a copied implementation.
    """

    def _create_mock_pyewftskfs(self, mock_fs, mock_pytsk3):
        """Create a PyEwfTskFS instance with injected mocks, bypassing __init__."""
        from core.evidence_fs import PyEwfTskFS

        # Create instance without calling __init__
        instance = object.__new__(PyEwfTskFS)

        # Inject the mocked dependencies that _walk needs
        instance._fs = mock_fs
        instance._pytsk3 = mock_pytsk3

        return instance

    def test_walk_detects_inode_cycle(self) -> None:
        """Test that _walk skips directories with already-visited inodes.

        Simulates NTFS junction where 'Application Data' points to same
        inode as 'AppData/Roaming', which would cause infinite recursion.
        """
        from unittest.mock import MagicMock

        # Mock pytsk3 module
        mock_pytsk3 = MagicMock()
        mock_pytsk3.TSK_FS_META_TYPE_DIR = 4  # Directory type constant
        mock_pytsk3.TSK_FS_META_TYPE_REG = 1  # Regular file type

        # Create mock directory entries
        def make_entry(name: str, is_dir: bool, inode: int):
            entry = MagicMock()
            entry.info.name.name = name.encode()
            entry.info.meta.type = mock_pytsk3.TSK_FS_META_TYPE_DIR if is_dir else mock_pytsk3.TSK_FS_META_TYPE_REG
            entry.info.meta.addr = inode
            return entry

        # Root directory has: Users (dir, inode 100)
        root_entries = [
            make_entry(".", True, 1),
            make_entry("..", True, 1),
            make_entry("Users", True, 100),
        ]

        # Users has: Alice (dir, inode 200)
        users_entries = [
            make_entry(".", True, 100),
            make_entry("..", True, 1),
            make_entry("Alice", True, 200),
        ]

        # Alice has: AppData (inode 300), Application Data (inode 300 - SAME! junction)
        alice_entries = [
            make_entry(".", True, 200),
            make_entry("..", True, 100),
            make_entry("AppData", True, 300),
            make_entry("Application Data", True, 300),  # Junction - same inode!
        ]

        # AppData has: file.txt (inode 400)
        appdata_entries = [
            make_entry(".", True, 300),
            make_entry("..", True, 200),
            make_entry("file.txt", False, 400),
        ]

        # Mock filesystem open_dir to return appropriate entries
        mock_fs = MagicMock()
        def mock_open_dir(path):
            dir_mock = MagicMock()
            if path == "/":
                dir_mock.__iter__ = lambda self: iter(root_entries)
            elif path == "/Users":
                dir_mock.__iter__ = lambda self: iter(users_entries)
            elif path == "/Users/Alice":
                dir_mock.__iter__ = lambda self: iter(alice_entries)
            elif path in ("/Users/Alice/AppData", "/Users/Alice/Application Data"):
                dir_mock.__iter__ = lambda self: iter(appdata_entries)
            else:
                raise IOError(f"No such directory: {path}")
            return dir_mock

        mock_fs.open_dir = mock_open_dir

        # Create PyEwfTskFS instance with injected mocks (bypassing __init__)
        walker = self._create_mock_pyewftskfs(mock_fs, mock_pytsk3)

        # Call the ACTUAL _walk method from production code
        paths = list(walker._walk("/"))

        # Should have walked these paths (order may vary due to BFS)
        assert "/Users" in paths
        assert "/Users/Alice" in paths
        assert "/Users/Alice/AppData" in paths
        assert "/Users/Alice/AppData/file.txt" in paths

        # The key test: Application Data should appear in output (it's yielded)
        # but its contents should NOT be walked again (cycle detected)
        assert "/Users/Alice/Application Data" in paths

        # Verify we didn't walk into Application Data (would have duplicate file.txt paths)
        file_txt_count = sum(1 for p in paths if p.endswith("/file.txt"))
        assert file_txt_count == 1, f"Expected 1 file.txt, got {file_txt_count}. Cycle detection failed!"

    def test_walk_handles_none_inode(self) -> None:
        """Test that _walk handles entries without inode gracefully.

        Some filesystem entries may not have valid inode numbers.
        """
        from unittest.mock import MagicMock

        mock_pytsk3 = MagicMock()
        mock_pytsk3.TSK_FS_META_TYPE_DIR = 4
        mock_pytsk3.TSK_FS_META_TYPE_REG = 1

        def make_entry(name: str, is_dir: bool, inode):
            entry = MagicMock()
            entry.info.name.name = name.encode()
            entry.info.meta.type = mock_pytsk3.TSK_FS_META_TYPE_DIR if is_dir else mock_pytsk3.TSK_FS_META_TYPE_REG
            entry.info.meta.addr = inode  # Can be None
            return entry

        root_entries = [
            make_entry(".", True, 1),
            make_entry("..", True, 1),
            make_entry("dir_with_inode", True, 100),
            make_entry("dir_no_inode", True, None),  # No inode - edge case
        ]

        dir_with_inode_entries = [
            make_entry(".", True, 100),
            make_entry("..", True, 1),
            make_entry("file1.txt", False, 200),
        ]

        dir_no_inode_entries = [
            make_entry(".", True, None),
            make_entry("..", True, 1),
            make_entry("file2.txt", False, 201),
        ]

        mock_fs = MagicMock()
        def mock_open_dir(path):
            dir_mock = MagicMock()
            if path == "/":
                dir_mock.__iter__ = lambda self: iter(root_entries)
            elif path == "/dir_with_inode":
                dir_mock.__iter__ = lambda self: iter(dir_with_inode_entries)
            elif path == "/dir_no_inode":
                dir_mock.__iter__ = lambda self: iter(dir_no_inode_entries)
            else:
                raise IOError(f"No such directory: {path}")
            return dir_mock

        mock_fs.open_dir = mock_open_dir

        # Create PyEwfTskFS instance with injected mocks (bypassing __init__)
        walker = self._create_mock_pyewftskfs(mock_fs, mock_pytsk3)

        # Call the ACTUAL _walk method from production code
        paths = list(walker._walk("/"))

        # Should walk both directories even if one has no inode
        assert "/dir_with_inode" in paths
        assert "/dir_no_inode" in paths
        assert "/dir_with_inode/file1.txt" in paths
        assert "/dir_no_inode/file2.txt" in paths
