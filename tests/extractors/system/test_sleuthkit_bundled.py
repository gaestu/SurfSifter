from __future__ import annotations

from pathlib import Path

import pytest

from extractors.system.file_list.sleuthkit_utils import get_sleuthkit_bin


def test_get_sleuthkit_bin_prefers_vendor_linux(tmp_path: Path, monkeypatch) -> None:
    vendor_dir = tmp_path / "vendor" / "sleuthkit" / "linux-x86_64"
    vendor_dir.mkdir(parents=True)
    fls_path = vendor_dir / "fls"
    fls_path.write_text("#!/bin/sh\necho test\n")

    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("platform.machine", lambda: "x86_64")

    resolved = get_sleuthkit_bin("fls", base_dir=tmp_path)
    assert resolved == str(fls_path)


def test_get_sleuthkit_bin_falls_back_to_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("platform.machine", lambda: "x86_64")
    monkeypatch.setattr(
        "extractors.system.file_list.sleuthkit_utils.shutil.which",
        lambda _: "/usr/bin/fls",
    )

    resolved = get_sleuthkit_bin("fls", base_dir=tmp_path)
    assert resolved == "/usr/bin/fls"


def test_get_sleuthkit_bin_windows_exe(tmp_path: Path, monkeypatch) -> None:
    vendor_dir = tmp_path / "vendor" / "sleuthkit" / "win64"
    vendor_dir.mkdir(parents=True)
    fls_path = vendor_dir / "fls.exe"
    fls_path.write_text("binary")

    monkeypatch.setattr("platform.system", lambda: "Windows")
    monkeypatch.setattr("platform.machine", lambda: "AMD64")

    resolved = get_sleuthkit_bin("fls", base_dir=tmp_path)
    assert resolved == str(fls_path)


def test_bundled_takes_precedence_over_path(tmp_path: Path, monkeypatch) -> None:
    """Bundled binary is preferred even when PATH has the tool."""
    vendor_dir = tmp_path / "vendor" / "sleuthkit" / "linux-x86_64"
    vendor_dir.mkdir(parents=True)
    fls_path = vendor_dir / "fls"
    fls_path.write_text("#!/bin/sh\necho bundled\n")

    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("platform.machine", lambda: "x86_64")
    # shutil.which would return a system path, but bundled should win
    monkeypatch.setattr(
        "extractors.system.file_list.sleuthkit_utils.shutil.which",
        lambda _: "/usr/bin/fls",
    )

    resolved = get_sleuthkit_bin("fls", base_dir=tmp_path)
    assert resolved == str(fls_path), "Bundled binary should take precedence over PATH"


def test_returns_none_when_not_found(tmp_path: Path, monkeypatch) -> None:
    """Returns None when binary not found bundled or in PATH."""
    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("platform.machine", lambda: "x86_64")
    monkeypatch.setattr(
        "extractors.system.file_list.sleuthkit_utils.shutil.which",
        lambda _: None,
    )

    resolved = get_sleuthkit_bin("fls", base_dir=tmp_path)
    assert resolved is None
