# OS Artifacts (Tab)

Review operating system artifacts extracted from the evidence.

## Purpose
- Surface OS-level indicators relevant to the investigation.
- Provide quick access to registry findings, jump lists, and installed apps.

## When to use
- When you need context beyond browser data (system usage and provenance).
- When validating installed software or recent file/URL activity.

## Data sources
- Evidence database tables for registry indicators, jump lists, and installed software.

## Key controls
- Subtab-specific filters (indicator type, browser, pin status, search).
- Export CSV buttons on each subtab.
- Double-click rows to view detailed records.

## Outputs
- CSV exports for registry findings, jump lists, and installed applications.
- Detail dialogs for jump list entries and software records.

## Subtabs
- [[./registry-findings|Registry Findings]] - extracted registry indicators.
- [[./jump-lists|Jump Lists]] - recent and pinned jump list entries.
- [[./installed-applications|Installed Applications]] - detected installed software.

## Notes
- Data availability depends on extracted OS artifacts for the evidence.
