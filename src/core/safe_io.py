"""Contained no-follow file I/O helpers for case-workspace artifacts."""
from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import Optional


_NOFOLLOW_FLAG = getattr(os, "O_NOFOLLOW", None)
_NONBLOCK_FLAG = getattr(os, "O_NONBLOCK", 0)
_DIR_FD_SUPPORTED = (
    os.open in getattr(os, "supports_dir_fd", set())
    and os.mkdir in getattr(os, "supports_dir_fd", set())
    and _NOFOLLOW_FLAG is not None
)


def write_bytes_no_follow(
    path: Path,
    data: bytes,
    *,
    containment_root: Path,
    exclusive: bool = False,
) -> None:
    """Write bytes without following symlinks outside ``containment_root``."""
    if not _DIR_FD_SUPPORTED:
        raise OSError("no-follow dir-fd file I/O is not supported on this platform")

    parent_fd, leaf_name = _open_parent_dir_no_follow(path, containment_root)
    try:
        flags = os.O_WRONLY | os.O_CREAT | _NONBLOCK_FLAG
        flags |= os.O_EXCL if exclusive else 0
        flags |= _NOFOLLOW_FLAG
        fd = os.open(leaf_name, flags, 0o600, dir_fd=parent_fd)
        with os.fdopen(fd, "wb") as fh:
            info = os.fstat(fh.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise OSError("refusing non-regular file")
            if not exclusive:
                os.ftruncate(fh.fileno(), 0)
            fh.write(data)
    finally:
        os.close(parent_fd)


def read_bytes_no_follow(
    path: Path,
    *,
    containment_root: Path,
    max_bytes: Optional[int] = None,
) -> bytes:
    """Read bytes without following symlinks outside ``containment_root``."""
    if not _DIR_FD_SUPPORTED:
        raise OSError("no-follow dir-fd file I/O is not supported on this platform")

    parent_fd, leaf_name = _open_parent_dir_no_follow(path, containment_root)
    try:
        flags = os.O_RDONLY | _NONBLOCK_FLAG
        flags |= _NOFOLLOW_FLAG
        fd = os.open(leaf_name, flags, dir_fd=parent_fd)
        with os.fdopen(fd, "rb") as fh:
            info = os.fstat(fh.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise OSError("refusing non-regular file")
            if max_bytes is not None and info.st_size > max_bytes:
                raise OSError("refusing oversized cached file")
            if max_bytes is not None:
                data = fh.read(max_bytes + 1)
                if len(data) > max_bytes:
                    raise OSError("refusing oversized cached file")
                return data
            return fh.read()
    finally:
        os.close(parent_fd)


def unlink_no_follow(path: Path, *, containment_root: Path, missing_ok: bool = False) -> None:
    """Unlink a contained path without following symlinked parent directories."""
    if not _DIR_FD_SUPPORTED or os.unlink not in getattr(os, "supports_dir_fd", set()):
        raise OSError("no-follow dir-fd unlink is not supported on this platform")

    parent_fd, leaf_name = _open_parent_dir_no_follow(path, containment_root)
    try:
        try:
            os.unlink(leaf_name, dir_fd=parent_fd)
        except FileNotFoundError:
            if not missing_ok:
                raise
    finally:
        os.close(parent_fd)


def open_file_no_follow(path: Path, *, containment_root: Path):
    """Open a contained regular file for reading without following symlinks."""
    if not _DIR_FD_SUPPORTED:
        raise OSError("no-follow dir-fd file I/O is not supported on this platform")

    parent_fd, leaf_name = _open_parent_dir_no_follow(path, containment_root)
    try:
        flags = os.O_RDONLY | _NONBLOCK_FLAG
        flags |= _NOFOLLOW_FLAG
        fd = os.open(leaf_name, flags, dir_fd=parent_fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise OSError("refusing non-regular file")
            return os.fdopen(fd, "rb")
        except Exception:
            os.close(fd)
            raise
    finally:
        os.close(parent_fd)


def is_contained_regular_file(path: Path, *, containment_root: Path) -> bool:
    """Return True for regular, non-symlink files under ``containment_root``."""
    try:
        root = Path(containment_root).resolve()
        candidate = Path(path)
        if candidate.is_symlink() or not candidate.is_file():
            return False
        resolved = candidate.resolve()
        resolved.relative_to(root)
    except (OSError, ValueError):
        return False
    return True


def ensure_dir_no_follow(path: Path, *, containment_root: Path) -> Path:
    """Create/open a directory path under ``containment_root`` without symlinks."""
    root = Path(containment_root).resolve()
    target = Path(path)
    if not target.is_absolute():
        target = root / target
    try:
        rel_target = target.relative_to(root)
    except ValueError as exc:
        raise OSError("path outside containment root") from exc

    if not _DIR_FD_SUPPORTED:
        raise OSError("no-follow dir-fd directory I/O is not supported on this platform")

    dir_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        dir_flags |= os.O_DIRECTORY
    dir_flags |= _NOFOLLOW_FLAG

    current_fd = os.open(root, dir_flags)
    try:
        for part in rel_target.parts:
            if part in {"", ".", ".."}:
                raise ValueError("invalid path component")
            try:
                os.mkdir(part, 0o700, dir_fd=current_fd)
            except FileExistsError:
                pass
            next_fd = os.open(part, dir_flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
    except Exception:
        os.close(current_fd)
        raise
    os.close(current_fd)
    return target


def _open_parent_dir_no_follow(path: Path, containment_root: Path) -> tuple[int, str]:
    """Open a contained parent directory by walking components no-follow."""
    root = Path(containment_root).resolve()
    target = Path(path)
    if not target.is_absolute():
        target = root / target
    rel_target = target.relative_to(root)
    rel_parent = rel_target.parent
    leaf_name = rel_target.name
    if not leaf_name or leaf_name in {".", ".."} or "/" in leaf_name:
        raise ValueError("invalid leaf filename")

    dir_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        dir_flags |= os.O_DIRECTORY
    dir_flags |= _NOFOLLOW_FLAG

    current_fd = os.open(root, dir_flags)
    try:
        for part in rel_parent.parts:
            if part in {"", ".", ".."}:
                raise ValueError("invalid path component")
            next_fd = os.open(part, dir_flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
    except Exception:
        os.close(current_fd)
        raise
    return current_fd, leaf_name