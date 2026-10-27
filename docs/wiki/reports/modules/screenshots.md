# Screenshots (Report Module)

Displays investigator-captured screenshots with optional grouping by sequence.

## Purpose
- Include documentation screenshots captured in the Screenshots feature.
- Group related captures under a sequence name when needed.

## Inputs
- Screenshots captured within the application.

## Filters and controls
- Sequence: Filter by screenshot sequence (list is data-driven).
- Include Notes: Include internal notes (normally hidden).
- Include URLs: Show captured URLs under each screenshot.
- Show Total Count: Display the total screenshot count at the end.

## Output
- Screenshot grid with titles, captions, and timestamps.

## Notes
- This module only includes screenshots captured in the tool (not OS or browser caches).
- Thumbnails require Pillow; optional `pillow-heif` support is used when installed.
- Source screenshot files must resolve under the selected evidence workspace. Missing files, files outside that workspace, unreadable files, and images over the thumbnail pixel safety cap are shown without previews.
- Evidence-derived screenshot titles, captions, URLs, and notes are HTML-escaped during rendering.
