# Installation

This page summarizes installation options and dependencies. It mirrors the project README but is organized for the wiki.

## Option 1: Linux Installer Script (Recommended)
No repository clone is required. Run the installer directly as a one-liner:
```bash
wget -qO- https://raw.githubusercontent.com/gaestu/surfsifter/main/scripts/install.sh | bash
```

### Upgrade
Canonical update command:
```bash
wget -qO- https://raw.githubusercontent.com/gaestu/surfsifter/main/scripts/install.sh | \
  bash -s -- --from-release --release-version latest --non-interactive
```

Expected summary snippet:
```text
Install summary:
  source: release <resolved-tag> (<asset-name>)
  previous version: <old-version>
  installed version: <new-version>
```

Important flags:
- `--dry-run` prints commands without changing the system
- `--prefix /path` installs to a custom target prefix
- `--bin-source /path/to/binary` uses local artifact instead of GitHub release
- `--skip-tools` skips recommended forensic tool installation

## Option 2: Pre-built Releases (Manual)
Download from the Releases page:
- **Linux:** Extract and run `./surfsifter`
- **Windows:** Run `surfsifter.exe`

## Option 3: Install from Source

### Clone
```bash
git clone https://github.com/gaestu/surfsifter.git
cd surfsifter
```

### Install with Poetry (recommended)
```bash
poetry install --extras all
poetry run surfsifter
```

### Install with pip
```bash
pip install -e .[all]
python -m app.main
```

## System Requirements

### Required system packages (Linux: Debian/Ubuntu)
```bash
sudo apt-get install libewf-dev libtsk-dev build-essential python3-dev \
  libpango-1.0-0 libcairo2 libgdk-pixbuf2.0-0 shared-mime-info
```

### Linux distro notes
- `bulk_extractor` is not available in default Ubuntu 24.04 and Fedora 42 repositories.
- On Ubuntu/Debian, `ewfmount` is provided by `ewf-tools`; on Fedora it is `ewftools`.
- If a packaged binary fails to start on a minimal host, validate Qt/XCB runtime libraries (`libxcb-*`, `libxkbcommon`, EGL/GL equivalents).

### Python requirements
- Python **>= 3.10, < 3.14**
- Key packages include PySide6, pytsk3, libewf-python, Pillow, imagehash, WeasyPrint, regipy

## Optional Dependencies (Extras)
Install optional features with Poetry or pip extras:
- **`cache-decompression`** - brotli, zstandard (Chromium cache)
- **`jump-lists`** - olefile, LnkParse3 (Windows Jump Lists)
- **`macos`** - binarycookies (Safari support)
- **`leveldb`** - ccl-chromium-reader (browser storage databases)
- **`ie`** - libesedb-python (IE/Edge ESE parsing; fixes "No ESE library available")
- **`all`** - install all optional features

Examples:
```bash
poetry install --extras jump-lists
pip install -e .[cache-decompression]
```

## External Tools (Optional)
Some features rely on external tools available on your system `PATH`.
See [[general/external-tools|External Tools]] for details and installation hints.
