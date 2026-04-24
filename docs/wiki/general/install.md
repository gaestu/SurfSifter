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
poetry install
poetry run surfsifter
```

### Install with pip
```bash
pip install -e .
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

### Optional runtime tools
- A Chromium-family browser `131+` (`Chromium`, `Google Chrome`, `Microsoft Edge`, or `Brave`) is the default appendix PDF renderer when available and is especially recommended for large image appendices.
- If Chromium `131+` is missing, unsupported, or the Chromium appendix render fails at runtime, appendix export falls back to WeasyPrint when installed; otherwise appendix export is unavailable.

## Bundled Python Dependencies
All artifact-parsing libraries are installed automatically with `poetry install`:
- brotli, zstandard (Chromium cache decompression)
- olefile, LnkParse3 (Windows Jump Lists)
- binarycookies (Safari support)
- ccl-chromium-reader (browser storage databases)
- libesedb-python (IE/Edge ESE parsing)

## External Tools (Optional)
Some features rely on external tools available on your system `PATH`.
See the **External Tools** page for details and installation hints.

For tool path overrides and all application settings, see the **Settings and Preferences** page.

## Installing bulk_extractor Manually

`bulk_extractor` is **not available** in default Ubuntu 24.04 and Fedora 42 repositories. To use bulk extraction features (URL, email, domain, phone number, and cryptocurrency address discovery), you need to build it from source.

### Build from source (Ubuntu/Debian)
```bash
# Install build dependencies
sudo apt-get install -y build-essential autoconf automake libtool \
  flex libssl-dev zlib1g-dev libexpat1-dev pkg-config

# Clone and build
git clone --recursive https://github.com/simsong/bulk_extractor.git
cd bulk_extractor
./bootstrap.sh      # may require running: autoreconf --install
./configure
make -j$(nproc)
sudo make install
```

### Build from source (Fedora/RHEL)
```bash
# Install build dependencies
sudo dnf install -y gcc-c++ autoconf automake libtool flex openssl-devel \
  zlib-devel expat-devel

# Clone and build
git clone --recursive https://github.com/simsong/bulk_extractor.git
cd bulk_extractor
./bootstrap.sh
./configure
make -j$(nproc)
sudo make install
```

### Custom path
If you install bulk_extractor to a non-standard location, you can configure the path in **Preferences → Tools** (see the **Settings and Preferences** page).

### Verification
After installation, restart SurfSifter and check the **Tools** tab to verify bulk_extractor is detected and the version meets the minimum requirement (≥ 1.6.0).
