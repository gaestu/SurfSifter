# External Tools

Some features rely on optional external tools that are discovered on your system `PATH`. If a tool is missing, the related feature is disabled and the **Tools** tab will show it as unavailable.

## Tools and purposes

| Tool | Purpose | Where to get it |
| --- | --- | --- |
| **foremost** | File carving for common formats (images, docs) | Install via OS package manager or the project's website |
| **scalpel** | Advanced file carving (configurable signatures) | Install via OS package manager or the project's website |
| **bulk_extractor** | Bulk extraction of URLs, emails, domains, phone numbers, crypto addresses | Install via OS package manager or the project's website |
| **exiftool** | EXIF/metadata extraction for media files | Install via OS package manager or the project's website |
| **firejail** | Sandboxed browser preview for safer URL inspection | Install via OS package manager or the project's website |
| **ewfmount** | E01 mount fallback for carving workflows | `ewf-tools` (Ubuntu/Debian) or `ewftools` (Fedora) |

## Notes
- These tools are **optional**. The app runs without them.
- After installation, restart the app so it can detect the tools on `PATH`.
- `bulk_extractor` is not available in default Ubuntu 24.04 and Fedora 42 repositories. Install manually from upstream/source if needed.
- SleuthKit (`fls`, `mmls`, `icat`) is used by file-list and related workflows, but it is resolved via bundled binaries or `PATH` and is not currently listed in the Tools tab.

## Installing bulk_extractor

`bulk_extractor` is the most commonly needed external tool and the one most likely to require manual installation.

### Why it matters
bulk_extractor scans raw disk images at the byte level to discover:
- **URLs** — browsing history fragments in unallocated space
- **Email addresses** — contact information across the disk
- **Domain names** — network activity indicators
- **Phone numbers** — communication metadata
- **Cryptocurrency addresses** — Bitcoin, Ethereum, and other wallets
- **Credit card numbers** — financial data indicators

Without it, the **Bulk Extractor** extractor in SurfSifter is disabled.

### Installation
`bulk_extractor` is not in the default repositories for Ubuntu 24.04+ or Fedora 42+. Build from source:

```bash
# Ubuntu/Debian
sudo apt-get install -y build-essential autoconf automake libtool \
  flex libssl-dev zlib1g-dev libexpat1-dev pkg-config
git clone --recursive https://github.com/simsong/bulk_extractor.git
cd bulk_extractor
./bootstrap.sh && ./configure && make -j$(nproc) && sudo make install

# Fedora/RHEL
sudo dnf install -y gcc-c++ autoconf automake libtool flex openssl-devel \
  zlib-devel expat-devel
git clone --recursive https://github.com/simsong/bulk_extractor.git
cd bulk_extractor
./bootstrap.sh && ./configure && make -j$(nproc) && sudo make install
```

### Custom tool paths
If a tool is installed to a non-standard location, open **Preferences → Tools** and set a custom path. The path is saved in `~/.config/surfsifter/tool_paths.json` and persists across sessions. See the **Settings and Preferences** page for details.
