# Reports

Reports let you assemble a final, shareable document from case data. You can mix narrative text with data-driven modules and add an appendix for long lists.

## General behavior
- Reports are per-evidence: each evidence item stores its own report settings, sections, and appendix items.
- Sections are ordered and can include formatted text plus one or more modules.
- Modules pull read-only data from the evidence database and respect the filters you set.
- Appendix items are separate from main sections and always render after them.
- Settings auto-save as you work.

## Report structure
- Title page with report title, case metadata, and optional branding.
- Table of contents.
- Custom sections (ordered).
- Appendix (optional), with its own ordered items.

## Workflow
1) Add sections and write narrative text.
2) Add modules inside sections to embed data tables or grids.
3) Add appendix modules for supporting lists.
4) Preview in HTML or export to PDF.

## Preview and export
- Preview opens the HTML in your default browser.
- Main report PDF export requires WeasyPrint.
- Tag Summary XLSX export (📝 button in the Reports tab toolbar) produces a printable working document grouped by tag and artifact group with a ☐ checkbox per group; saved by default under `<case>/reports/tag_summary_<evidence>_<utc>.xlsx` and restricted to the case workspace.
- Appendix PDF export uses Chromium-family browsers with supported headless print rendering (`Chromium/Chrome/Edge/Brave 131+`) by default when available and falls back to WeasyPrint when Chromium is missing, unsupported, or the Chromium render fails and WeasyPrint is installed.
- Chromium appendix rendering is disabled while SurfSifter runs as `root`; use a non-root account if you need the Chromium appendix path.
- Under Chromium, appendix TOC entries are listed by title only, without page numbers.
- Appendix PDF determinism is defined as stable investigator-visible content and ordering for a fixed renderer/version environment, not byte-identical PDF bytes.
- Each appendix export performs a fresh Chromium probe pass and records probe attempts plus the render command in `process_log` when Chromium rendering is attempted, so the renderer/version used for an appendix export remains auditable.

## Branding and portability
- Logo files selected in the Reports tab are copied into the case workspace (reports/assets) so the report remains portable.

## Localization and dates
- Report language and date format are configurable per evidence.
- Supported locales: English and German.

## Related pages
- [[reports/modules|Report Modules]]
- [[reports/appendix|Appendix]]
- [[features/reports/text-blocks|Text Blocks]]
