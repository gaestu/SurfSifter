# -*- mode: python ; coding: utf-8 -*-

import sys
import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, collect_submodules

block_cipher = None

# SPECPATH is defined by PyInstaller and points to the directory containing the spec file
project_root = Path(SPECPATH).resolve().parent
source_dir = project_root / "src"

qt_platforms = collect_data_files("PySide6", subdir="Qt/plugins/platforms")
qt_imageformats = collect_data_files("PySide6", subdir="Qt/plugins/imageformats")
qt_styles = collect_data_files("PySide6", subdir="Qt/plugins/styles")
def _add_if_exists(entries, source_path, target_path):
    if source_path.exists():
        entries.append((str(source_path), target_path))


additional_dirs = []
_add_if_exists(additional_dirs, project_root / "config", "config")
_add_if_exists(additional_dirs, project_root / "rules", "rules")
_add_if_exists(additional_dirs, project_root / "docs", "docs")
_add_if_exists(additional_dirs, project_root / "src" / "reports", "src/reports")
_add_if_exists(additional_dirs, project_root / "reference_lists", "reference_lists")
_add_if_exists(additional_dirs, project_root / "pyproject.toml", ".")

# Database migration SQL files — required for case/evidence DB initialization
_add_if_exists(
    additional_dirs,
    source_dir / "core" / "database" / "migrations",
    "core/database/migrations",
)
_add_if_exists(
    additional_dirs,
    source_dir / "core" / "database" / "migrations_evidence",
    "core/database/migrations_evidence",
)
# Non-Python data files inside extractor packages — not collected by
# collect_submodules() which only gathers .py modules.
_add_if_exists(
    additional_dirs,
    source_dir / "extractors" / "_shared" / "appids.json",
    "extractors/_shared",
)
_add_if_exists(
    additional_dirs,
    source_dir / "extractors" / "media" / "scalpel" / "default.conf",
    "extractors/media/scalpel",
)
_add_if_exists(
    additional_dirs,
    source_dir / "extractors" / "carvers" / "browser_carver" / "browser_artifacts.conf",
    "extractors/carvers/browser_carver",
)
_add_if_exists(
    additional_dirs,
    project_root / "vendor" / "sleuthkit" / "win64",
    "vendor/sleuthkit/win64",
)
_add_if_exists(
    additional_dirs,
    project_root / "vendor" / "sleuthkit" / "LICENSE-CPL-1.0.txt",
    "vendor/sleuthkit",
)
_add_if_exists(
    additional_dirs,
    project_root / "vendor" / "sleuthkit" / "NOTICE.txt",
    "vendor/sleuthkit",
)

qt_datas = qt_platforms + qt_imageformats + qt_styles

# Deduplicate data entries while preserving order.
_seen = set()
datas = []
resources_entry = (str(project_root / "resources"), "resources")

for entry in qt_datas + additional_dirs:
    if entry not in _seen:
        datas.append(entry)
        _seen.add(entry)

if (project_root / "resources").exists() and resources_entry not in _seen:
    datas.append(resources_entry)
    _seen.add(resources_entry)

binaries = collect_dynamic_libs("PySide6")
hiddenimports = (
    collect_submodules("PySide6")
    + collect_submodules("extractors")
    + collect_submodules("reports")
)

# Include optional C-extension dependencies when installed.
# These are conditionally imported at runtime (try/except) so PyInstaller
# cannot trace them automatically.
for _opt_mod in [
    "brotli", "zstandard", "olefile", "LnkParse3", "binarycookies",
    "ccl_chromium_reader", "regipy", "tldextract",
]:
    try:
        __import__(_opt_mod)
        hiddenimports += collect_submodules(_opt_mod)
    except ImportError:
        pass

# tldextract ships a bundled TLD suffix snapshot as package data.
try:
    __import__("tldextract")
    datas += collect_data_files("tldextract")
except ImportError:
    pass

excludes = [
    "tests",
    "pytest",
]

analysis = Analysis(
    [str(source_dir / "run.py")],
    pathex=[str(source_dir)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)
pyz = PYZ(analysis.pure, analysis.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    analysis.scripts,
    analysis.binaries,
    analysis.zipfiles,
    analysis.datas,
    [],
    name="SurfSifter",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(project_root / "config" / "branding" / "surfsifter.ico"),
)
