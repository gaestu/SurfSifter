# Workflow and Behavior

This page describes the app's general behavior and forensic workflow assumptions.

## Read-only evidence access
- Evidence is opened in **read-only** mode through libewf + pytsk3.
- Source images are never modified.

## Case-based storage
- Each case has its own directory, chosen when you create/open a case.
- All outputs (databases, extracted artifacts, reports, logs) are stored inside the case directory.

## Offline extraction
- Extraction and parsing are performed locally.
- Optional browser preview (when enabled) is sandboxed for safer URL inspection.

## Audit logging
- The application maintains an **append-only** audit log of processing steps.

## Deterministic outputs
- Extractors are designed to be repeatable on the same inputs.
- Outputs are intended to be stable and defensible for forensic review.

## Cases with several evidences
- Opening a case creates a tab for every evidence, but an evidence's data is only read when you show it. Hidden evidences run no counts or data queries, so opening a case with many evidences stays fast. Only the evidence database is opened and the saved log is read, so extraction and logging work for every evidence.
- Within an evidence, each sub-tab (File List, Browser Inventory, Download, Tags, Reports, Screenshots, Audit, ...) loads its data the first time it is shown. After extraction, ingestion or other changes, sub-tabs that are not visible reload when you next show them.
- The **Overview** counts (URLs, images, indicators, last extraction, tags) are loaded in the background while the Overview is shown. The labels read `Loading…` until the counts arrive, and `—` if they could not be read. Counts are remembered per evidence while the case is open, so switching back to an evidence shows them immediately. They are reloaded after changes made in this window that affect them, for example extraction, ingestion, purge, case-wide runs, tagging, list matching, hash checks, deduplication, downloads, file exports and report previews or exports (these record a process log entry, which can change "Last Extraction"). Counts computed while an extraction is still running are shown but not remembered. Reopening the case reloads them. If a second SurfSifter window or another program changes the same case, reopen the case to see current counts.
- Opening another case or closing an evidence tab stops the background work of all its sub-tabs.
