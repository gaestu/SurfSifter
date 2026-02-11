# Images (Report Module)

Displays extracted images with hashes, timestamps, and optional file paths.

## Purpose
- Present extracted or carved images in a visual grid.
- Filter by tags or hash matches to focus on relevant images.

## Inputs
- Image artifacts from extractors (cache, carving, filesystem, etc.).
- Hash match lists and image tags.

## Filters and controls
- Title: Optional heading text.
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Hash Match: Filter by hash match list (All, Any Match, or a specific list).
- Include File Path: Show the image path under each thumbnail.
- Sort By: Order by date or filename.
- Show Filter Info: Display selected filters below the grid.
- Max Images: Limit the number of images displayed.

## Output
- Image grid with thumbnail, hash values, and discovery timestamp.

## Notes
- Thumbnails require Pillow; if not installed, images may show without previews.
- Match list options appear only when hash matches are present.
