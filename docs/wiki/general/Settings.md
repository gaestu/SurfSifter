# Settings and Preferences

SurfSifter stores user preferences in a JSON file that persists across sessions. Open the Preferences dialog from the menu bar (**Edit → Preferences** or via the gear icon).

## Settings file location

| Context | Path |
| --- | --- |
| Installed / frozen binary | `~/.config/surfsifter/settings.json` |
| Development (from source) | `config/settings.json` (seeded from `config/settings.defaults.json`) |

Settings are auto-saved when you click **Save** in the Preferences dialog. Unknown or legacy fields are ignored on load and omitted the next time settings are saved.

## Preferences tabs

### General
- **Thumbnail size** — pixel size for image thumbnails throughout the UI (default: 180 px).
- **Open config directory** — opens the configuration folder in your file manager.

### Tools
Configure paths for optional external tools. Each tool has:
- **Browse** — select the executable path manually.
- **Test** — verify the tool is functional and meets the minimum version.
- **Reset** — clear the custom path and revert to PATH-based discovery.

Supported tools:
| Tool | Minimum version | Purpose |
| --- | --- | --- |
| bulk_extractor | 1.6.0 | URL/email/crypto/domain bulk extraction |
| foremost | 1.5.0 | File carving (images, documents) |
| exiftool | 12.0.0 | EXIF/metadata extraction |
| ewfmount | 20140608 | E01 image mounting (carving fallback) |

Custom paths are saved in `~/.config/surfsifter/tool_paths.json` and persist across sessions. See the **External Tools** page for installation guides.

### Network
Controls for the built-in download manager (used by the Downloads tab):
- **Concurrency** — maximum parallel download workers (1–4, default: 2).
- **Timeout** — per-request timeout in seconds (5–60, default: 10).
- **Retries** — retry count on failure (0–5, default: 1).
- **Max download size** — per-file size limit in bytes (default: 200 MB).
- **Allowed content types** — MIME type patterns permitted for download (default includes `image/*`, `video/*`, `audio/*`, `application/pdf`, Office document types, archive formats, `text/plain`, `text/html`).

### Reports
Default values for PDF report generation. These serve as pre-filled defaults when creating new reports:
- **Author name** and **Function** (e.g., "Forensic Analyst")
- **Organization** and **Department**
- **Footer text** (default: "SurfSifter Report")
- **Logo** — browse for a logo image; it is copied to `config/branding/` for portability.
- **Locale** — report language (`en` for English, `de` for German).
- **Date format** — `eu` (DD.MM.YYYY) or `us` (MM/DD/YYYY).

### Text Blocks
Reusable plain-text snippets for report sections:
- **Create / Edit / Delete** text blocks with title, tags, and content.
- **Search** and **filter by tag** to find saved blocks.
- **Import / Export** as JSON for sharing across installations (with duplicate handling: skip, rename, or overwrite).
- Storage: `~/.config/surfsifter/text_blocks.json` (global, not per-case).

### Reference Lists
Manage matching lists used by the URL, File List, and Hash matching features:

- **File Lists** — known file path patterns (e.g., browser artifacts, temp locations, system cleaners).
- **Hash Lists** — known file hashes for identification.
- **URL Lists** — URL patterns for matching against extracted URLs.

Each list type supports:
- **Add** — import from file.
- **View** — inspect list contents.
- **Delete** — remove a list.
- **Install Predefined Lists** — load built-in reference lists from the `reference_lists/` directory.

Hash Lists and URL Lists also support **Import Folder**:
- The selected folder is scanned for `.txt` files directly inside that folder only; nested folders are not imported.
- Each `.txt` file is imported as a separate list using the file stem as the list name.
- Existing-name conflicts can be handled with **Skip** (default), **Overwrite**, or **Rename (_1, _2)**.
- Hash Lists are matched directly from the stored text lists by the Images tab **Check Known Hashes** action.
- URL List folder import asks once for shared category, description, and wildcard/regex pattern mode. These values are written as metadata for plain text URL lists.
- URL List files that already contain SurfSifter URL-list metadata keep their existing metadata where practical.

## Sandbox settings
Controls for the secure URL preview feature (right-click → "Open in Sandbox Browser"):
- **Prefer external browser** — use Firejail-sandboxed external browser instead of built-in WebEngine (Linux only; requires Firejail).
- **JavaScript enabled** — enable JavaScript in the built-in preview (disabled by default for security).
- **External browser** — path to the browser executable for sandboxed preview (auto-detected if empty).
- **Log opens** — write an audit log entry each time a URL is opened in the sandbox (default: enabled).

## Restoring defaults
Click **Restore Defaults** in the Preferences dialog to reset all settings to their default values. The defaults are defined in `config/settings.defaults.json`.
