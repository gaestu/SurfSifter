# Reports (Tab)

Reports tab shim - integrates self-contained reports module into main app.

## Purpose
- Build investigation reports from case data with custom sections and appendices.
- Preview and export the final report as a PDF.
- Export a working **Tag Summary** workbook (XLSX) for prosecutor review.

## When to use
- When you are ready to compile findings into a report.
- When you need a shareable PDF with standardized formatting.
- When you need a printable per-tag/per-group checklist of artifacts the prosecutor can tick before the final PDF is generated.

## Data sources
- Case metadata and evidence data from the case database.
- Report settings and custom sections stored in the reports module database tables.

## Key controls
- Report title, language, date format, author, and branding fields.
- Add/Edit custom sections and appendices.
- Reuse global text blocks from the section editor and manage them from the Reports header.
- Actions: Preview, Create PDF, and 📝 **Export Tag Summary** (XLSX).

## Outputs
- Report preview (HTML) and exported PDF saved to disk.
- Report settings and custom sections saved in the database.
- **Tag Summary XLSX**: a single-sheet workbook grouped by tag → artifact group (e.g. *Browser search terms*, *Stored sites*, *Bookmarks*) plus a *Reference-list matches* section. Each group heading carries a tickable ☐ checkbox so the prosecutor can mark which groups should appear in the final PDF; up to 10 sample entries are shown per group with a "…and N more" indicator. The export is restricted to the case workspace and the action is recorded in `process_log` before the file is written.

## Subtabs
- None

## Notes
- This tab embeds the self-contained reports module from `src/reports`.
