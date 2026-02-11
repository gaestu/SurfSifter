# Downloads (Tab)

## Purpose

- Central place where files from found URLs can be downloaded
- Lets you select URLs to fetch and review the files/images that were retrieved.

## When to use
- When you need to pull down files referenced in extracted URLs.
- When you want to review or export downloaded artifacts.

## Data sources
- Case database tables for URLs, tags, and download metadata.
- Case downloads folder (stored files and thumbnails).

## Key controls
- Settings toggle (max concurrent downloads, size limit, timeout, thumbnail generation).
- Subtabs with counts: Available, Images, Other Files.
- Per-subtab filters and actions (see subtab pages).

## Outputs
- Downloaded files saved under the case downloads folder.
- Download metadata (hashes, timestamps, status) written to the case database.
- Download audit events (one final row per requested URL) written to evidence `download_audit`.

## Subtabs
- [[./available-downloads|Available Downloads]] - Available Downloads subtab - browse and select URLs for download.
- [[./downloaded-files|Downloaded Files]] - Downloaded Files subtab - view downloaded non-image files.
- [[./downloaded-images|Downloaded Images]] - Downloaded Images subtab - view downloaded image files with thumbnails.

## Notes
- Download attempts are auditable from the Audit tab under **Download Audit**.
