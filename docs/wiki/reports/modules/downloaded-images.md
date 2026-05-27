# Downloaded Images (Report Module)

Shows completed image downloads in a visual grid with metadata.

## Purpose
- Review images that were downloaded by the browser.
- Filter by domain or tag to focus on specific sources.

## Inputs
- Download records flagged as completed image downloads.
- Tags applied to download entries.

## Filters and controls
- Domain: Filter by source domain (list is data-driven).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Sort By: Order by date, filename, or URL.
- Show Filter Info: Display selected filters below the grid.

## Output
- Image grid with thumbnail, URL, hashes, and download timestamp.

## Notes
- Thumbnails require Pillow; optional `pillow-heif` support is used when installed.
- Source files must resolve under the selected evidence workspace. Missing files, files outside that workspace, unreadable files, and images over the thumbnail pixel safety cap are shown without previews.
- Thumbnail bytes are embedded inline in report output; evidence-derived text fields are HTML-escaped during rendering.
- Only downloads classified as images are included.
