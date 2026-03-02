# SurfSifter — Claude Code Briefing

This document is the **single source of truth** for Claude Code working on this codebase. Read it fully before making any changes.

---

## Claude Code Behavior

- Read this file fully before starting any task
- Run tests after every change — no exceptions (see Testing section)
- Before creating a new migration: check the migrations directory for the current highest number first
- Prefer small, focused changes over large refactors
- When unsure about architecture placement: **ask, don't guess**
- Never modify evidence data — this is forensic software used in legal proceedings
- Check GitHub Issues before starting new work to avoid conflicts with active tasks

---

## Forensic Integrity (Non-Negotiable)

This software is used in legal proceedings. Integrity violations can invalidate evidence.

- Evidence sources are **always read-only** — never write to source images
- All writes target the case workspace only
- Every tool invocation **must** be logged via `core.audit_logging` and `process_log` helpers
- Deterministic outputs required — no timestamps, random IDs, or non-deterministic logic in core
- Append-only audit logs — never delete or modify audit entries

---

## Mission

Build and maintain a forensic workstation that:
- Reads EWF images **without mutating source evidence**
- Extracts and correlates browser artifacts, cached media, and OS indicators
- Provides an investigator-friendly GUI for review, tagging, and reporting
- Preserves forensic defensibility via deterministic outputs and append-only audit logs

---

## Current State

- Python ≥3.10, <3.14 — PySide6 GUI
- Single consolidated baseline schema: `0001_*.sql` in both migration directories
- Existing dev databases require fresh cases — no upgrade path from pre-consolidation schemas
- Known issue: PyEWF/PyTSK3 C libraries have threading bugs → E01 tests excluded from default runs
- Check GitHub Issues for active work items before starting

---

## Critical Rules

**DO:**
- Run tests before committing: `poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q`
- Use existing database helpers (`core.database.helpers.*`) — never write raw SQL in UI code
- Use `core.evidence_fs` abstractions (PyEwfTskFS/MountedFS) for evidence access
- Log tool invocations via `core.audit_logging` and `process_log` helpers
- Place new UI code in the correct feature folder under `src/app/features/`
- Keep extractors self-contained in `src/extractors/` — no dependencies on `src/app/`

**DON'T:**
- Modify existing migration files (breaks upgrades)
- Import between feature modules (features → common/services/data/core only)
- Hard-code version numbers or migration counts — check the migrations directory
- Run GUI tests unless explicitly needed (`-m gui_offscreen` requires display setup)
- Create tests that require E01 images without the `@pytest.mark.e01` marker

---

## Common Task Recipes

**New extractor:**
→ migration → helper in `core/database/helpers/` → export in `__init__.py` → `extractor.py` → tests

**New UI feature tab:**
→ `src/app/features/<name>/` → `__init__.py` + `tab.py` + `models/` → register in `main.py`

**New database artifact:**
→ migration in `migrations_evidence/` → helper → export → tests for migration AND helper

**Bug in extraction:**
→ check `extractors/_shared/` first → then family extractor → run specific test with `-v`

**Add network download:**
→ route through `app.services.net_download.py` → sanitize URL → audit log via `DownloadTask`

---

## Repository Layout

```
src/
  app/                        # PySide6 GUI application
    main.py                   # MainWindow entry point
    features/                 # Feature-sliced UI modules (one per tab)
      audit/                  # Audit log viewer
      browser_inventory/      # Browser artifact subtabs (14 artifact types)
      downloads/              # Download management (available/downloaded)
      extraction/             # Extractor configuration and execution
      file_list/              # File enumeration viewer
      images/                 # Carved/cached image grid and clustering
      os_artifacts/           # Registry, platform indicators, jump lists
      reports/                # Report generation UI
      screenshots/            # Forensic screenshot capture
      settings/               # Preferences dialog
      tags/                   # Tag management
      timeline/               # Fused event timeline
      urls/                   # URL browser and matching
    common/                   # Shared UI components
      dialogs/                # Reusable dialogs (15 modules)
      widgets/                # Shared widgets (case_info, tag_selector, etc.)
      qt_models/              # Base Qt model classes
    services/                 # Background workers and IO helpers
      workers.py              # ValidationWorker, BaseTask, ExecutorTask
      matching_workers.py     # FileListMatchWorker, UrlMatchWorker
      net_download.py         # Audit-logged network downloads
      thumbnailer.py          # Image thumbnail generation
    data/                     # Data access layer
      case_data.py            # CaseDataAccess (aggregates query mixins)
    config/                   # App configuration
      settings.py             # User preferences

  core/                       # Domain logic (no Qt dependencies)
    database/                 # SQLite schema and access
      migrations/             # Case DB migrations
      migrations_evidence/    # Evidence DB migrations
      helpers/                # Per-artifact CRUD modules (~42 modules)
      manager.py              # DatabaseManager, connection pooling
    evidence_fs.py            # EvidenceFS abstraction (PyEwfTskFS, MountedFS)
    audit_logging.py          # Append-only audit trail
    tool_registry.py          # External tool definitions
    tool_discovery.py         # PATH-based tool discovery
    matching/                 # URL/hash/file list matching
    validation.py             # Input validation helpers

  extractors/                 # Modular extraction units
    base.py                   # BaseExtractor interface
    extractor_registry.py     # Auto-discovery and lifecycle
    _shared/                  # Shared extractor utilities (core imports via TYPE_CHECKING only)
    browser/                  # Browser extractors (3-level: family/artifact)
      chromium/               # Chrome, Edge, Brave, Opera, Vivaldi
      firefox/                # Firefox, Tor Browser
      ie_legacy/              # Internet Explorer, Legacy Edge (ESE)
      safari/                 # Safari (experimental)
      tor/                    # Tor Browser specific
    carvers/                  # File carving tools
      bulk_extractor/         # URL/email/crypto discovery
      browser_carver/         # Browser cache carving
    media/                    # Media extraction
      filesystem_images/      # Parallel image extraction
      foremost_carver/        # Foremost-based media carving
      scalpel/                # Scalpel-based media carving
    system/                   # System artifacts
      registry/               # Windows registry
      jump_lists/             # Windows Jump Lists
      file_list/              # SleuthKit enumeration, CSV import

  reports/                    # PDF report generation
    generator.py              # Report orchestration
    modules/                  # Report section modules
    templates/                # Jinja2 + WeasyPrint templates
    appendix/                 # Appendix generators

reference_lists/              # YAML-based matching lists
  urllists/                   # URL pattern lists
  hashlists/                  # Known file hashes
  filelists/                  # Known file patterns

tests/                        # pytest test suites (mirrors src/)
  app/                        # App-level tests
  core/                       # Core utility tests
  extractors/                 # Extractor tests
  gui/                        # Qt/PySide GUI tests (isolated)
  integration/                # Cross-module tests
  compat/                     # Backward compatibility tests
```

---

## Architecture Rules

### Dependency Direction (Strictly Enforced)
```
features/* → common/, services/, data/, core/
common/*   → services/, data/, core/
services/* → data/, core/
data/*     → core/
core/      → (no app dependencies)
extractors/→ core/ (never app/)
```

**Never import between features.** If two features need shared logic, move it to `common/`, `services/`, or `core/`.

### Data Access Pattern
- **UI code:** Use `app.data.CaseDataAccess` or feature-local query mixins
- **Core logic:** Use `core.database.helpers.*` modules
- **Never:** Write raw SQL in `src/app/features/` — use or create a helper

### Evidence Filesystem Access
- **Always:** Use `core.evidence_fs.EvidenceFS` subclasses
- **Never:** Direct filesystem access to evidence images
- Classes: `PyEwfTskFS` (E01 images), `MountedFS` (mounted paths)

### Network Safety
- All downloads go through `app.services.net_download.py`
- URLs must be sanitized before fetch
- Download audit logging: `DownloadTask` writes final outcomes to per-evidence `download_audit` table

---

## Core Dependencies

**Required (pyproject.toml):**
- Python ≥3.10, <3.14
- PySide6, pytsk3, libewf-python (provides pyewf)
- Pillow, imagehash, WeasyPrint, regipy, tldextract
- PyYAML, jsonschema, SQLAlchemy, Jinja2, requests

**Bundled (all installed by default):**
- brotli, zstandard (Chromium cache decompression)
- olefile, LnkParse3 (Windows Jump Lists)
- binarycookies (Safari support)
- ccl-chromium-reader (LevelDB parsing)
- libesedb-python (ESE database parsing)

**External Tools (discovered via PATH):**
- bulk_extractor, foremost, scalpel, exiftool, firejail
- ewfmount (via `core.tool_discovery` only, not in UI Tools tab)

Tool status visible in `core.tool_registry.ToolRegistry` and UI Tools tab.

---

## Data Model

### Databases
- **Case DB:** `{case_number}_surfsifter.sqlite` — case metadata, network audit, report sections
- **Evidence DB:** `evidence_<slug>.sqlite` — per-evidence artifact storage

### Schema Source of Truth
- **Case schema:** `src/core/database/migrations/0001_case_schema.sql`
- **Evidence schema:** `src/core/database/migrations_evidence/0001_evidence_schema.sql`
- **Helpers:** `src/core/database/helpers/` (~42 modules for CRUD operations)

### Case DB Tables
`cases`, `evidences`, `report_sections`, `case_audit_log`

### Evidence DB Tables
Check `0001_evidence_schema.sql` directly — 72 tables total.

Key table groups: `urls`, `browser_*`, `cookies`, `bookmarks`, `session_*`, `images`, `file_list`, `tags`, `timeline`, `process_log`

---

## Migrations

**Location:**
- Case: `src/core/database/migrations/`
- Evidence: `src/core/database/migrations_evidence/`

**To add a migration:**
1. Check current highest number in migrations directory (never hard-code)
2. Create `NNNN_descriptive_name.sql` with next sequential number
3. Use idempotent SQL: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`
4. For `ALTER TABLE ADD COLUMN`: use marker migration + helper function pattern (see `manager.py`)
5. Test fresh creation AND upgrade scenarios

**Never:** Modify existing migrations, skip numbers, or assume migration counts.

---

## Extractors

### Structure
Each extractor folder contains:
- `__init__.py` — exports `{Name}Extractor`
- `extractor.py` — implements `BaseExtractor.run_extraction()` and `run_ingestion()`
- Optional: `ui.py` (config widget), `worker.py` (background processing)

### Discovery
`ExtractorRegistry` in `extractor_registry.py` auto-discovers extractors:
- 2-level groups: `extractors/{group}/{extractor}/` (e.g., `carvers/bulk_extractor/`, `system/registry/`)
- 3-level browser: `extractors/browser/{family}/{artifact}/` (e.g., `browser/chromium/history/`)

Group directories: `browser`, `system`, `media`, `carvers` (see `GROUP_DIRECTORIES` in registry)

### Adding an Extractor with DB Storage
1. Create migration file (see Migrations section)
2. Add helper module: `src/core/database/helpers/<feature>.py`
3. Export in `src/core/database/helpers/__init__.py`
4. Implement: `insert_<thing>()`, `get_<things>()`, `delete_<things>_by_run()`
5. Write tests for migration and helpers

---

## Testing

### Default Command (use this always)
```bash
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q
```

### All Commands
```bash
# GUI tests (requires display or Xvfb)
poetry run pytest tests/gui -m gui_offscreen -q

# Integration tests
poetry run pytest -m integration

# E2E tests
poetry run pytest -m e2e

# Specific file
poetry run pytest tests/path/to/test.py -v
```

### Markers
- `gui_offscreen`, `gui_live` — Qt tests (isolated in `tests/gui/`)
- `integration` — cross-module tests
- `e2e` — full pipeline tests
- `slow` — long-running tests
- `compat` — backward compatibility
- `e01`, `pyewf` — tests requiring E01 images or pyewf

### Known Issues
PyEWF/PyTSK3 C libraries have threading bugs causing segfaults. E01 tests excluded from default runs.

---

## Development Checklist

Before submitting any change:
1. [ ] Tests pass: `poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q`
2. [ ] Dependency direction rules followed
3. [ ] Database changes have migrations AND helpers
4. [ ] UI code uses `CaseDataAccess` or helpers (no raw SQL)
5. [ ] Tool invocations logged via `process_log` helpers
6. [ ] No evidence mutation — writes target case workspace only
7. [ ] Audit log entries written for all relevant actions

---

## Key Documentation

| Document | Purpose |
|----------|---------|
| `tests/README.md` | Test organization and markers |
| `src/core/database/migrations*/0001_*.sql` | Schema source of truth |
| GitHub Issues | Active work items — check before starting |

---

## Run the App

```bash
poetry run python -m app.main
# or
poetry run surfsifter
```