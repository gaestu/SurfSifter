from __future__ import annotations

import os
from pathlib import Path

import pytest

from core import safe_io


def require_dir_fd_support() -> None:
    if not safe_io._DIR_FD_SUPPORTED:
        pytest.skip("no-follow dir-fd file I/O is unavailable on this platform")


@pytest.fixture(autouse=True)
def restore_dir_fd_support() -> None:
    original = safe_io._DIR_FD_SUPPORTED
    yield
    safe_io._DIR_FD_SUPPORTED = original


def test_write_and_read_bytes_no_follow(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    case_root.mkdir()
    target_dir = case_root / "reports"
    safe_io.ensure_dir_no_follow(target_dir, containment_root=case_root)

    output_path = target_dir / "export.xlsx"
    safe_io.write_bytes_no_follow(output_path, b"xlsx", containment_root=case_root, exclusive=True)

    assert safe_io.read_bytes_no_follow(output_path, containment_root=case_root) == b"xlsx"
    assert safe_io.read_bytes_no_follow(output_path, containment_root=case_root, max_bytes=4) == b"xlsx"


def test_read_bytes_no_follow_rejects_over_limit(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    case_root.mkdir()
    output_path = case_root / "cached.jpg"
    output_path.write_bytes(b"abcdef")

    with pytest.raises(OSError):
        safe_io.read_bytes_no_follow(output_path, containment_root=case_root, max_bytes=5)


def test_unlink_no_follow_removes_contained_file(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    case_root.mkdir()
    output_path = case_root / "reports" / "export.xlsx"
    safe_io.ensure_dir_no_follow(output_path.parent, containment_root=case_root)
    output_path.write_bytes(b"xlsx")

    safe_io.unlink_no_follow(output_path, containment_root=case_root)

    assert not output_path.exists()


def test_unlink_no_follow_rejects_parent_symlink(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    outside = tmp_path / "outside"
    outside.mkdir()
    case_root.mkdir()
    outside_file = outside / "export.xlsx"
    outside_file.write_bytes(b"outside")
    (case_root / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        safe_io.unlink_no_follow(case_root / "linked" / "export.xlsx", containment_root=case_root)

    assert outside_file.read_bytes() == b"outside"


def test_exclusive_write_refuses_overwrite(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    case_root.mkdir()
    target_dir = case_root / "reports"
    safe_io.ensure_dir_no_follow(target_dir, containment_root=case_root)
    output_path = target_dir / "export.xlsx"
    safe_io.write_bytes_no_follow(output_path, b"first", containment_root=case_root, exclusive=True)

    with pytest.raises(FileExistsError):
        safe_io.write_bytes_no_follow(output_path, b"second", containment_root=case_root, exclusive=True)


def test_rejects_parent_symlink(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    outside = tmp_path / "outside"
    outside.mkdir()
    case_root.mkdir()
    (case_root / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        safe_io.write_bytes_no_follow(
            case_root / "linked" / "escape.bin",
            b"escape",
            containment_root=case_root,
        )


def test_open_file_refuses_symlink_target(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    case_root.mkdir()
    (case_root / "linked.bin").symlink_to(outside)

    with pytest.raises(OSError):
        safe_io.open_file_no_follow(case_root / "linked.bin", containment_root=case_root)


def test_read_and_open_refuse_fifo_without_blocking(tmp_path: Path) -> None:
    require_dir_fd_support()
    if not hasattr(os, "mkfifo"):
        pytest.skip("mkfifo is unavailable on this platform")
    case_root = tmp_path / "case"
    case_root.mkdir()
    fifo_path = case_root / "pipe.bin"
    os.mkfifo(fifo_path)

    with pytest.raises(OSError):
        safe_io.read_bytes_no_follow(fifo_path, containment_root=case_root, max_bytes=16)
    with pytest.raises(OSError):
        safe_io.open_file_no_follow(fifo_path, containment_root=case_root)


def test_write_refuses_fifo_without_blocking(tmp_path: Path) -> None:
    require_dir_fd_support()
    if not hasattr(os, "mkfifo"):
        pytest.skip("mkfifo is unavailable on this platform")
    case_root = tmp_path / "case"
    case_root.mkdir()
    fifo_path = case_root / "pipe.bin"
    os.mkfifo(fifo_path)

    with pytest.raises(OSError):
        safe_io.write_bytes_no_follow(fifo_path, b"data", containment_root=case_root)


def test_write_refuses_broken_leaf_symlink(tmp_path: Path) -> None:
    require_dir_fd_support()
    case_root = tmp_path / "case"
    outside = tmp_path / "missing.bin"
    case_root.mkdir()
    (case_root / "linked.bin").symlink_to(outside)

    with pytest.raises(OSError):
        safe_io.write_bytes_no_follow(
            case_root / "linked.bin",
            b"escape",
            containment_root=case_root,
        )
    assert not outside.exists()


def test_no_dir_fd_mode_fails_closed(tmp_path: Path) -> None:
    safe_io._DIR_FD_SUPPORTED = False
    case_root = tmp_path / "case"
    case_root.mkdir()
    target_dir = case_root / "reports"
    target_dir.mkdir()
    output_path = target_dir / "export.xlsx"
    output_path.write_bytes(b"xlsx")

    with pytest.raises(OSError):
        safe_io.ensure_dir_no_follow(target_dir, containment_root=case_root)
    with pytest.raises(OSError):
        safe_io.write_bytes_no_follow(output_path, b"next", containment_root=case_root)
    with pytest.raises(OSError):
        safe_io.read_bytes_no_follow(output_path, containment_root=case_root)
    with pytest.raises(OSError):
        safe_io.open_file_no_follow(output_path, containment_root=case_root)
    with pytest.raises(OSError):
        safe_io.unlink_no_follow(output_path, containment_root=case_root)