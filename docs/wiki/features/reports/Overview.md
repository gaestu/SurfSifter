# Reports (Tab)

Reports tab shim - integrates self-contained reports module into main app.

## Purpose
- Build investigation reports from case data with custom sections and appendices.
- Preview and export the final report as a PDF.

## When to use
- When you are ready to compile findings into a report.
- When you need a shareable PDF with standardized formatting.

## Data sources
- Case metadata and evidence data from the case database.
- Report settings and custom sections stored in the reports module database tables.

## Key controls
- Report title, language, date format, author, and branding fields.
- Add/Edit custom sections and appendices.
- Reuse global text blocks from the section editor and manage them from the Reports header.
- Actions: Preview and Create PDF.

## Outputs
- Report preview (HTML) and exported PDF saved to disk.
- Report settings and custom sections saved in the database.

## Subtabs
- None

## Notes
- This tab embeds the self-contained reports module from `src/reports`.
