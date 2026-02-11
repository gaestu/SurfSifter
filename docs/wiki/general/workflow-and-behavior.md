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
