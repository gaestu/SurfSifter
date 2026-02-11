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
