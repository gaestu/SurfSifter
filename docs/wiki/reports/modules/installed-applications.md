# Installed Applications (Report Module)

Displays installed applications discovered from OS artifacts.

## Purpose
- Provide a report-ready list of software installed on the evidence system.
- Filter by tags to highlight forensically relevant applications.

## Inputs
- OS indicator records from registry extraction (installed software entries).
- Tags applied to installed application entries.

## Filters and controls
- Section Title: Custom heading text.
- Limit: Maximum number of entries (25/50/100/250/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Show Publisher: Toggle publisher column visibility.
- Show Version: Toggle version column.
- Show Install Date: Toggle install date column.
- Show Size: Toggle size column.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of installed applications with name, publisher, version, install date, and size.

## Notes
- Requires registry extraction to have run on the evidence.
- Windows-only; if registry artifacts are not present, the module will be empty.
