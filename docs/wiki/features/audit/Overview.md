# Audit (Tab)

Audit Tab - forensic audit data with subtabs.

## Purpose
- Audit Tab - forensic audit data with subtabs.
- This tab is added as a per-evidence subtab showing forensic audit data.

## When to use
- When you need to review extractor outputs and run health.
- When auditing what was extracted or diagnosing failures.
- When you need a trace of investigator-initiated download requests and outcomes.

## Data sources
- Evidence database tables:
  - `extracted_files`
  - `extraction_warnings`
  - `download_audit`
  - extractor statistics/process logs

## Key controls
- Subtab-specific filters and pagination (Extraction).
- Outcome filter/search/pagination (Download Audit).
- Summary cards of extractor runs (Statistics).

## Outputs
- Extraction: View all files extracted by any extractor (extracted_files table)
- Warnings: View extraction warnings collected during extraction.
- Download Audit: View final download outcomes (success/failed/blocked/cancelled/error).
- Statistics: View extractor run statistics (summary cards)
- Logs: View persisted and current evidence logs.

## Subtabs
- [[./extraction|Extraction]] - View all files extracted by any extractor (extracted_files table)
- Warnings - View extraction warnings.
- Download Audit - View investigator download audit rows.
- [[./statistics|Statistics]] - View extractor run statistics (summary cards)
- Logs - View per-evidence logs.

## Notes
- v1.13.0: Initial implementation with Extraction subtab.
- v1.13.0: Added Statistics subtab (moved from standalone tab).
- v2.5.0: Added Warnings subtab.
- v2.7.0: Added Logs subtab.
- v2.8.0: Added Download Audit subtab.
- This tab is added as a per-evidence subtab showing forensic audit data.
