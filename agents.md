# SurfSifter — Agent Briefing

This document is the **single source of truth** for AI agents working on this codebase. Read it fully before making changes.

## Mission

Build and maintain a forensic workstation that:
- Reads EWF images **without mutating source evidence**
- Extracts and correlates browser artifacts, cached media, and OS indicators
- Provides an investigator-friendly GUI for review, tagging, and reporting
- Preserves forensic defensibility via deterministic outputs and append-only audit logs

---

## Critical Rules (Read First)

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
      statistics/             # Extraction statistics
      tags/                   # Tag management
      timeline/               # Fused event timeline
      tools/                  # External tool status
      urls/                   # URL browser and matching
    common/                   # Shared UI components
      dialogs/                # Reusable dialogs (13 modules)
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
      helpers/                # Per-artifact CRUD modules (30+ modules)
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
    media/                    # Media extraction
      filesystem_images/      # Parallel image extraction
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

planning/
  wip/                        # Work-in-progress specs
  features/                   # Feature specifications
  done/                       # Completed milestone docs
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

**Optional Extras:**
- `cache-decompression`: brotli, zstandard
- `jump-lists`: olefile, LnkParse3
- `macos`: binarycookies
- `leveldb`: ccl-chromium-reader (LevelDB parsing)
- `ie`: libesedb-python (ESE database parsing)

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
- **Helpers:** `src/core/database/helpers/` (30+ modules for CRUD operations)

### Case DB Tables (0001_case_schema.sql)
`cases`, `evidences`, `report_sections`, `case_audit_log`

### Evidence DB Tables
**Don't memorize this list** — always check the schema files:
- All 72 tables in single consolidated `0001_evidence_schema.sql`

Key table groups: `urls`, `browser_*`, `cookies`, `bookmarks`, `session_*`, `images`, `file_list`, `tags`, `timeline`, `process_log`

---

## Migrations

**Location:**
- Case: `src/core/database/migrations/`
- Evidence: `src/core/database/migrations_evidence/`

**Important:** Both case and evidence directories contain a **single consolidated `0001_*.sql` baseline**. Existing dev databases require fresh cases — no upgrade path from pre-consolidation schemas.

**To add a migration:**
1. Check current highest number in migrations directory (don't hard-code)
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

### Commands
```bash
# Fast CI run (default, excludes GUI/slow/compat)
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q

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
PyEWF/PyTSK3 C libraries have threading bugs causing segfaults. E01 tests are excluded from default runs.

---

## Development Checklist

Before submitting changes:
1. [ ] Tests pass: `poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q`
2. [ ] New code follows dependency direction rules
3. [ ] Database changes have migrations AND helpers
4. [ ] UI code uses `CaseDataAccess` or helpers (no raw SQL)
5. [ ] Tool invocations logged via `process_log` helpers
6. [ ] No evidence mutation — writes target case workspace only

---

## Key Documentation

| Document | Purpose |
|----------|---------|
| `tests/README.md` | Test organization and markers |
| `planning/wip/` | Active work items |
| `src/core/database/migrations*/0001_*.sql` | Schema source of truth |

---

## Quick Reference

**Add a new feature tab:**
1. Create `src/app/features/<name>/` with `__init__.py`, `tab.py`, `models/`
2. Import only from `common/`, `services/`, `data/`, `core/`
3. Register tab in `src/app/main.py`

**Add database storage for new artifact:**
1. Migration in `migrations_evidence/`
2. Helper in `core/database/helpers/`
3. Export in `helpers/__init__.py`
4. Tests for both

**Run the app:**
```bash
poetry run python -m app.main
# or
poetry run surfsifter
```
