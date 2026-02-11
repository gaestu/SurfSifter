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
- PDF export requires WeasyPrint. If it is not installed, PDF generation is disabled.

## Branding and portability
- Logo files selected in the Reports tab are copied into the case workspace (reports/assets) so the report remains portable.

## Localization and dates
- Report language and date format are configurable per evidence.
- Supported locales: English and German.

## Related pages
- [[reports/modules|Report Modules]]
- [[reports/appendix|Appendix]]
- [[features/reports/text-blocks|Text Blocks]]
