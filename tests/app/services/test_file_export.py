"""Tests for evidence_access helpers and FileExportTask worker."""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.services.evidence_access import (
    EvidenceAccessError,
    evidence_has_source,
    open_evidence_fs,
)
from app.services.file_export_worker import (
    FileExportRequest,
    FileExportResult,
    FileExportTask,
    _safe_filename,
    _unique_dest,
)


# ---------------------------------------------------------------------------
# evidence_has_source
# ---------------------------------------------------------------------------


class TestEvidenceHasSource:
    def test_none_evidence(self):
        assert evidence_has_source(None) is False

    def test_empty_source_path(self):
        assert evidence_has_source({"source_path": ""}) is False

    def test_missing_source_path_key(self):
        assert evidence_has_source({"label": "foo"}) is False

    def test_nonexistent_path(self, tmp_path: Path):
        p = tmp_path / "does_not_exist.E01"
        assert evidence_has_source({"source_path": str(p)}) is False

    def test_existing_directory(self, tmp_path: Path):
        assert evidence_has_source({"source_path": str(tmp_path)}) is True

    def test_existing_file(self, tmp_path: Path):
        f = tmp_path / "image.E01"
        f.write_bytes(b"\x00")
        assert evidence_has_source({"source_path": str(f)}) is True


# ---------------------------------------------------------------------------
# open_evidence_fs – unit tests (no real E01 images)
# ---------------------------------------------------------------------------


class TestOpenEvidenceFs:
    def test_empty_source_path_raises(self):
        with pytest.raises(EvidenceAccessError, match="no source_path"):
            open_evidence_fs({"source_path": ""})

    def test_nonexistent_raises(self, tmp_path: Path):
        with pytest.raises(EvidenceAccessError, match="not found"):
            open_evidence_fs({"source_path": str(tmp_path / "nope.E01")})

    def test_directory_opens_mounted_fs(self, tmp_path: Path):
        with patch("app.services.evidence_access.MountedFS") as mock_cls:
            mock_cls.return_value = MagicMock()
            fs = open_evidence_fs({"source_path": str(tmp_path)})
            mock_cls.assert_called_once_with(tmp_path)
            assert fs is mock_cls.return_value

    def test_unsupported_format_raises(self, tmp_path: Path):
        f = tmp_path / "image.dd"
        f.write_bytes(b"\x00")
        with pytest.raises(EvidenceAccessError, match="Unsupported"):
            open_evidence_fs({"source_path": str(f)})

    def test_e01_opens_pyewf(self, tmp_path: Path):
        f = tmp_path / "image.E01"
        f.write_bytes(b"\x00")
        with (
            patch("app.services.evidence_access.find_ewf_segments", return_value=[f]),
            patch("app.services.evidence_access.PyEwfTskFS") as mock_cls,
        ):
            mock_cls.return_value = MagicMock(fs_type="NTFS")
            fs = open_evidence_fs({"source_path": str(f), "partition_index": 2})
            mock_cls.assert_called_once_with([f], partition_index=2)
            assert fs is mock_cls.return_value


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestSafeFilename:
    def test_clean_name_unchanged(self):
        assert _safe_filename("readme.txt") == "readme.txt"

    def test_special_chars_replaced(self):
        assert _safe_filename('a<b>c:d"e') == "a_b_c_d_e"

    def test_empty_becomes_unnamed(self):
        assert _safe_filename("") == "unnamed"

    def test_dots_only_becomes_unnamed(self):
        assert _safe_filename("...") == "unnamed"


class TestUniqueDest:
    def test_no_collision(self, tmp_path: Path):
        result = _unique_dest(tmp_path, "file.txt")
        assert result == tmp_path / "file.txt"

    def test_collision_appends_counter(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("existing")
        result = _unique_dest(tmp_path, "file.txt")
        assert result == tmp_path / "file_1.txt"

    def test_multiple_collisions(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("a")
        (tmp_path / "file_1.txt").write_text("b")
        result = _unique_dest(tmp_path, "file.txt")
        assert result == tmp_path / "file_2.txt"


# ---------------------------------------------------------------------------
# FileExportResult
# ---------------------------------------------------------------------------


class TestFileExportResult:
    def test_total(self):
        r = FileExportResult(exported=3, failed=2)
        assert r.total == 5

    def test_summary_all_ok(self):
        r = FileExportResult(exported=5)
        assert "5" in r.summary_message()
        assert "failed" not in r.summary_message().lower()

    def test_summary_with_errors(self):
        r = FileExportResult(exported=2, failed=1, errors=["bad: reason"])
        msg = r.summary_message()
        assert "2" in msg
        assert "1" in msg
        assert "bad: reason" in msg


# ---------------------------------------------------------------------------
# FileExportTask – integration-style tests with a mock EvidenceFS
# ---------------------------------------------------------------------------


class _FakeFS:
    """Minimal in-memory EvidenceFS stand-in."""

    def __init__(self, files: Dict[str, bytes]):
        self._files = files
        self.closed = False

    def open_for_stream(self, path: str, chunk_size: int = 65536) -> Iterator[bytes]:
        if path not in self._files:
            raise FileNotFoundError(path)
        data = self._files[path]
        for i in range(0, len(data), chunk_size):
            yield data[i: i + chunk_size]

    def close(self) -> None:
        self.closed = True


class TestFileExportTask:
    """Test FileExportTask.run_task() with a mock evidence factory."""

    @pytest.fixture()
    def evidence_dict(self, tmp_path: Path) -> Dict[str, Any]:
        # Point at a temporary dir so evidence_has_source would pass
        return {"source_path": str(tmp_path), "partition_index": None}

    def _run_task(
        self,
        evidence: Dict[str, Any],
        files: List[FileExportRequest],
        dest: Path,
        fake_fs: _FakeFS,
    ) -> FileExportResult:
        """Create a task, patch the FS factory, and run synchronously."""
        task = FileExportTask(evidence, files, dest)
        with patch(
            "app.services.file_export_worker.open_evidence_fs",
            return_value=fake_fs,
        ):
            return task.run_task()

    def test_export_single_file(self, evidence_dict, tmp_path):
        dest = tmp_path / "out"
        dest.mkdir()
        fake = _FakeFS({"/docs/hello.txt": b"Hello, world!"})
        result = self._run_task(
            evidence_dict,
            [FileExportRequest("/docs/hello.txt")],
            dest,
            fake,
        )
        assert result.exported == 1
        assert result.failed == 0
        assert (dest / "hello.txt").read_bytes() == b"Hello, world!"

    def test_export_multiple_files(self, evidence_dict, tmp_path):
        dest = tmp_path / "out"
        dest.mkdir()
        fake = _FakeFS({
            "/a.txt": b"AAA",
            "/b.txt": b"BBB",
        })
        result = self._run_task(
            evidence_dict,
            [FileExportRequest("/a.txt"), FileExportRequest("/b.txt")],
            dest,
            fake,
        )
        assert result.exported == 2
        assert (dest / "a.txt").read_bytes() == b"AAA"
        assert (dest / "b.txt").read_bytes() == b"BBB"

    def test_name_collision_deduplicates(self, evidence_dict, tmp_path):
        dest = tmp_path / "out"
        dest.mkdir()
        fake = _FakeFS({
            "/dir1/report.txt": b"one",
            "/dir2/report.txt": b"two",
        })
        result = self._run_task(
            evidence_dict,
            [
                FileExportRequest("/dir1/report.txt"),
                FileExportRequest("/dir2/report.txt"),
            ],
            dest,
            fake,
        )
        assert result.exported == 2
        assert (dest / "report.txt").read_bytes() == b"one"
        assert (dest / "report_1.txt").read_bytes() == b"two"

    def test_missing_file_counted_as_failed(self, evidence_dict, tmp_path):
        dest = tmp_path / "out"
        dest.mkdir()
        fake = _FakeFS({})
        result = self._run_task(
            evidence_dict,
            [FileExportRequest("/nope.bin")],
            dest,
            fake,
        )
        assert result.exported == 0
        assert result.failed == 1
        assert "not found" in result.errors[0]

    def test_cancellation(self, evidence_dict, tmp_path):
        """Task respects cancellation between files."""
        dest = tmp_path / "out"
        dest.mkdir()
        fake = _FakeFS({"/a.txt": b"A", "/b.txt": b"B"})

        task = FileExportTask(
            evidence_dict,
            [FileExportRequest("/a.txt"), FileExportRequest("/b.txt")],
            dest,
        )
        # Cancel before running
        task.cancel()

        with patch(
            "app.services.file_export_worker.open_evidence_fs",
            return_value=fake,
        ):
            from app.services.workers import TaskCancelled
            with pytest.raises(TaskCancelled):
                task.run_task()

    def test_evidence_access_error_fails_whole_group(self, evidence_dict, tmp_path):
        """If the evidence cannot be opened, all files in that partition fail."""
        dest = tmp_path / "out"
        dest.mkdir()
        task = FileExportTask(
            evidence_dict,
            [FileExportRequest("/a.txt"), FileExportRequest("/b.txt")],
            dest,
        )
        with patch(
            "app.services.file_export_worker.open_evidence_fs",
            side_effect=EvidenceAccessError("boom"),
        ):
            result = task.run_task()

        assert result.exported == 0
        assert result.failed == 2
        assert all("boom" in e for e in result.errors)

    def test_csv_evidence_has_no_source(self):
        """CSV-imported evidence (no source_path) → evidence_has_source is False."""
        assert evidence_has_source({"source_path": ""}) is False
        assert evidence_has_source({}) is False

    def test_partial_file_cleaned_up_on_failure(self, evidence_dict, tmp_path):
        """When streaming fails mid-write, the partial output file is removed."""
        dest = tmp_path / "out"
        dest.mkdir()

        class _FailMidStream:
            def open_for_stream(self, path, chunk_size=65536):
                yield b"partial content"
                raise OSError("disk read error")

            def close(self):
                pass

        result = self._run_task(
            evidence_dict,
            [FileExportRequest("/bad.bin")],
            dest,
            _FailMidStream(),
        )
        assert result.failed == 1
        # The partial file should have been cleaned up
        assert not (dest / "bad.bin").exists()

    def test_missing_file_no_leftover(self, evidence_dict, tmp_path):
        """FileNotFoundError also cleans up any empty file that was created."""
        dest = tmp_path / "out"
        dest.mkdir()

        class _RaiseOnStream:
            """FS that raises FileNotFoundError on stream open."""
            def open_for_stream(self, path, chunk_size=65536):
                raise FileNotFoundError(path)

            def close(self):
                pass

        result = self._run_task(
            evidence_dict,
            [FileExportRequest("/ghost.txt")],
            dest,
            _RaiseOnStream(),
        )
        assert result.failed == 1
        assert not (dest / "ghost.txt").exists()


# ---------------------------------------------------------------------------
# MountedFS path-traversal confinement
# ---------------------------------------------------------------------------


class TestMountedFsConfinement:
    """Verify _resolve_under_mount blocks escapes via open_for_stream / walk_directory."""

    def test_open_for_stream_blocks_traversal(self, tmp_path: Path):
        from core.evidence_fs import MountedFS

        mount = tmp_path / "evidence"
        mount.mkdir()
        (mount / "legit.txt").write_text("ok")

        # Create a file *outside* the mount
        (tmp_path / "secret.txt").write_text("do-not-read")

        fs = MountedFS(mount)

        with pytest.raises(ValueError, match="traversal"):
            list(fs.open_for_stream("../secret.txt"))

    def test_walk_directory_blocks_traversal(self, tmp_path: Path):
        from core.evidence_fs import MountedFS

        mount = tmp_path / "evidence"
        mount.mkdir()

        fs = MountedFS(mount)

        with pytest.raises(ValueError, match="traversal"):
            list(fs.walk_directory("../../etc"))
