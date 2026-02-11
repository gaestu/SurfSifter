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
- Sort By: Order by date or filename.

## Output
- Image grid with thumbnails and metadata.

## Notes
- Thumbnails require Pillow; if not installed, images may show without previews.
