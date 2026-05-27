# Image List (Appendix Module)

Shows a grid of images as appendix content with tag and hash match filters.

## Purpose
- Provide an appendix of images relevant to the case.
- Filter by tags or hash match lists.

## Inputs
- Image artifacts from extractors.
- Image tags and hash match lists.

## Filters and controls
- Tags: Multi-select tag filter.
- Hash Matches: Multi-select match list filter.
- Filter Mode: OR (any tag or match) or AND (must have both).
- Include File Path: Show the image path under each thumbnail.
- Include URL: Show source URLs under each thumbnail.
- Include Cache Key: Show browser cache keys under each thumbnail.
- Sort By: Order by date or filename.

## Output
- Image grid with thumbnails and metadata.

## Notes
- Thumbnails require Pillow; optional `pillow-heif` support is used when installed.
- Source files must resolve under the selected evidence workspace. Missing files, files outside that workspace, unreadable files, and images over the thumbnail pixel safety cap are shown without previews.
- Thumbnail bytes are embedded inline in the appendix output. Cache files under `report_thumbs/` are only a case-local write-through optimization and are not trusted as report-visible evidence.
