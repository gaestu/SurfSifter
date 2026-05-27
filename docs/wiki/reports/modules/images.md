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
- Tags: Filter by tags (multi-select).
- Hash Match: Filter by hash match list (All, Any Match, or a specific list).
- Include File Path: Show the image path under each thumbnail.
- Include URL: Show source URLs under each thumbnail.
- Include Cache Key: Show browser cache keys under each thumbnail.
- Sort By: Order by date or filename.
- Show Filter Info: Display selected filters below the grid.
- Max Images: Limit the number of images displayed.

## Output
- Image grid with thumbnail, hash values, and discovery timestamp.
- Optional URL and cache key annotations per image.

## Notes
- Thumbnails require Pillow; optional `pillow-heif` support is used when installed.
- Source files must resolve under the selected evidence workspace. Missing files, files outside that workspace, unreadable files, and images over the thumbnail pixel safety cap are shown without previews.
- Thumbnail bytes are embedded inline in report output; evidence-derived text fields are HTML-escaped during rendering.
- Match list options appear only when hash matches are present.
