# Download Audit Log (Appendix Module)

Forensic audit trail of every download attempt with outcome and HTTP status.

## Purpose
- Document all download attempts and their outcomes for forensic accountability.
- Include HTTP status codes, durations, and failure reasons.

## Inputs
- Download audit records from the evidence database (written by the network downloader).

## Filters and controls
- Outcome: Filter by All, Success, Failed, Blocked, Cancelled, or Error.
- Include Reason: Show the reason/error message for each entry.
- Include Caller Info: Show which component initiated the download.
- Sort By: Order by date, outcome, or URL.

## Output
- Table with color-coded outcomes and columns for URL, outcome, date, HTTP status, duration, and reason.

## Notes
- This is a forensic audit trail — every download attempt is logged regardless of outcome.
- Use this appendix to demonstrate investigative thoroughness and download provenance.
