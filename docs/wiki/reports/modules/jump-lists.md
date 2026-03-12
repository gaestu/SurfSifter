# Jump Lists (Report Module)

Displays Windows Jump List entries with application execution context.

## Purpose
- Include recent and pinned Jump List items in a report.
- Show application usage patterns and recently accessed files/URLs.

## Inputs
- Jump List entries extracted by the SystemJumpListsExtractor.
- Tags applied to jump list entries.

## Filters and controls
- Section Title and Description: Custom heading and explanatory text.
- Show Default Description: Toggle descriptive text for non-technical readers.
- Limit: Maximum number of entries (10/25/50/100/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Sort By: Order by access time, creation time, or application.
- Column visibility: application, title, URL, target path, access time, creation time, pin status, App ID.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of Jump List entries with expandable detail rows showing file path and App ID metadata.

## Notes
- Windows-specific; requires Jump List extraction to have run.
