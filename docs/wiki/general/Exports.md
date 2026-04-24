# Exports

Many tabs in SurfSifter support exporting data to external formats. This page summarizes the shared export behavior and case-level packaging functions.

---

## Case Export & Import

SurfSifter can package an entire case into a ZIP archive for archival, sharing, or migration between workstations. Both functions are available from the **Tools** menu.

### Export Case (`Ctrl+Shift+E`)

Opens the **Export Case** dialog. A case must be open first. The export always includes the case and evidence databases; additional content is controlled via checkboxes:

| Option | Description |
|--------|-------------|
| **Source evidence files** | Original image files (E01 segments, DD, etc.). May be very large. |
| **Cached artifacts** | Extracted files from `carved/`, `cache/`, and `thumbnails/` folders. |
| **Reports** | Generated PDF reports from `reports/`. Included by default. |
| **Audit logs** | Case audit log and evidence processing logs. |

A live **size estimate** updates as options change. Export runs in the background with a progress indicator and can be cancelled at any time.

### Import Case (`Ctrl+Shift+I`)

Opens the **Import Case** dialog. Select a previously exported `.zip` file — SurfSifter validates the archive before importing. After import you are offered to open the case immediately.

---

## CSV Export
Most table-based views offer **Export CSV** via the toolbar or context menu. The export includes:
- All visible columns in the current table view.
- Applied filters are respected — only filtered/visible rows are exported.
- Column headers match the displayed names.
- UTF-8 encoding with BOM for Excel compatibility.

Tabs with CSV export:
- **URLs** — full URL list with domain, source, tags, and match status.
- **File List** — file system entries with path, size, timestamps, and tags.
- **Timeline** — fused event records with timestamps and sources.
- **Browser Inventory** subtabs — history, cookies, bookmarks, downloads, and other artifact tables.
- **OS Artifacts** subtabs — registry findings, jump lists, installed applications.
- **Images → Table** — image metadata in tabular format.
- **Images → Clusters** — cluster membership and similarity data.

## PDF Reports
The **Reports** tab generates PDF documents from Jinja2 templates.
- Main report PDFs use WeasyPrint.
- Appendix PDFs use Chromium-family browsers with supported headless print rendering (`Chromium/Chrome/Edge/Brave 131+`) by default when available, fall back to WeasyPrint when Chromium is missing, unsupported, or the Chromium render fails and WeasyPrint is installed, and are otherwise unavailable.

Reports include:
- Title page with case metadata and branding.
- Table of contents (auto-generated).
- Configurable report sections and modules.
- Appendix items (URL lists, file lists, image grids).
- Localization support (English and German).

See the **Reports Overview** page for details.

## JSON Export
- **Text Blocks** — export and import reusable text snippets as JSON via **Preferences → Text Blocks**.
- **Reference Lists** — URL, hash, and file pattern lists stored as text files in `reference_lists/`.

## Tag-based filtering
All export operations respect the current tag filter. To export only tagged items, apply a tag filter before exporting.
