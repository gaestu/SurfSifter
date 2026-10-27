# Download List (Appendix Module)

Grid view of downloaded images with thumbnails, URLs, and hashes.

## Purpose
- Provide a visual appendix of all downloaded images.
- Filter by domain or tag to focus on relevant downloads.

## Inputs
- Download records from the case database (filtered for images).
- Tags applied to download entries.

## Filters and controls
- Domain: Filter by source domain (list is data-driven).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Include URL: Show the source URL under each thumbnail.
- Include Hash: Show file hash under each thumbnail.
- Sort By: Order by date, filename, or URL.

## Output
- Grid of downloaded image thumbnails (200x200px) with URL and hash metadata.

## Notes
- Thumbnails require Pillow; optional `pillow-heif` support is used when installed.
- Source files must resolve under the selected evidence workspace. Missing files, files outside that workspace, unreadable files, and images over the thumbnail pixel safety cap are shown without previews.
- Thumbnail bytes are embedded inline in the appendix output. Cache files under `report_thumbs/downloads/` are only a case-local write-through optimization and are not trusted as report-visible evidence.
- Only downloads classified as images are included.
