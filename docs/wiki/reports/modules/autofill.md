# Autofill (Report Module)

Displays saved autofill fields with optional browser and tag filters.

## Purpose
- Review stored autofill fields such as names, addresses, or emails.
- Narrow results by browser, tags, or field name.

## Inputs
- Autofill artifacts extracted from browser profiles.
- Tags applied to autofill entries.

## Filters and controls
- Browser: Filter by browser (list is data-driven).
- Tags: Multi-select tag filter.
- Field Name Contains: Text match on the field name.
- Show Profile Column: Toggle profile column visibility.
- Show Date Columns: Toggle first/last used dates.
- Show Use Count Column: Toggle usage count.
- Sort By: Choose ordering (last used, created, count, name, browser).
- Show Filter Info: Display selected filters below the table.

## Output
- Table of autofill entries with field name, value, usage counts, and dates (depending on toggles).

## Notes
- Tag and browser lists only populate when matching data exists.
