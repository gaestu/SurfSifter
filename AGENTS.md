# SurfSifter — Agent Briefing

Single source of truth for AI agents (Claude Code, Copilot, Codex, Cursor, etc.).
Read fully before any change. Task-specific workflows live in [`prompts/`](prompts/README.md).

---

## Mission

Forensic workstation that reads EWF images **without mutating evidence**, extracts browser/OS artifacts, and produces investigator-ready reports with deterministic outputs and append-only audit logs.

Used in legal proceedings — integrity violations can invalidate evidence.

---

## Forensic Integrity (Non-Negotiable)

- Evidence sources are **always read-only** — never write to source images
- All writes target the **case workspace only**
- Every external tool invocation **must** be logged via `core.audit_logging` and `process_log` helpers
- **Deterministic outputs**: no timestamps, random IDs, or non-deterministic logic in `core/`
- **Report PDF determinism**: generated PDFs in `reports/` must preserve stable investigator-visible content, ordering, and labeling for a fixed renderer/version environment; renderer metadata does not need to be byte-identical across runs, and the selected external renderer/version must be auditable
- **Append-only audit logs** — never delete or modify audit entries
- Evidence access **only** via `core.evidence_fs.EvidenceFS` subclasses (`PyEwfTskFS`, `MountedFS`)

---

## Critical Rules

**DO:**
- Run tests before committing: `poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q`
- Use `core.database.helpers.*` — never raw SQL in UI code
- Use `core.evidence_fs` for evidence access
- Place new UI code in `src/app/features/<feature>/`
- Keep extractors self-contained in `src/extractors/` (no `src/app/` imports)
- Check the migrations directory for the current highest number before adding migrations

**DON'T:**
- Modify existing migration files
- Import between feature modules (`features/*` may not import other `features/*`)
- Hard-code version numbers or migration counts
- Mutate evidence — only the case workspace is writable
- Add timestamps or randomness to `core/`

---

## Architecture

### Dependency Direction (Strictly Enforced)
```
features/* → common/, services/, data/, core/
common/*   → services/, data/, core/
services/* → data/, core/
data/*     → core/
core/      → (no app dependencies)
extractors/→ core/ (never app/)
```

If two features need shared logic → move it to `common/`, `services/`, or `core/`.

### Data Access
- **UI:** `app.data.CaseDataAccess` or feature-local query mixins
- **Core/extractors:** `core.database.helpers.*` modules
- **Never:** raw SQL in `src/app/features/`

### Network
All downloads route through `app.services.net_download.py`. URLs sanitized before fetch. `DownloadTask` writes outcomes to per-evidence `download_audit`.

---

## Repository Layout

```
src/
  app/                     # PySide6 GUI
    main.py                # MainWindow entry
    features/<name>/       # One folder per UI tab (audit, browser_inventory,
                           #   downloads, extraction, file_list, images,
                           #   os_artifacts, reports, screenshots, settings,
                           #   tags, timeline, urls, …)
    common/                # Shared dialogs, widgets, qt_models
    services/              # Workers, net_download, thumbnailer
    data/                  # CaseDataAccess
    config/                # User settings
  core/                    # Domain logic — no Qt
    database/
      migrations/          # Case DB migrations (baseline 0001_*.sql)
      migrations_evidence/ # Evidence DB migrations (baseline 0001_*.sql)
      helpers/             # ~42 per-artifact CRUD modules
      manager.py           # DatabaseManager
    evidence_fs.py         # PyEwfTskFS, MountedFS
    audit_logging.py       # Append-only audit
    tool_registry.py       # External tool definitions
    matching/              # URL/hash/file list matching
  extractors/              # Modular, self-contained
    base.py                # BaseExtractor interface
    extractor_registry.py  # Auto-discovery
    _shared/               # Shared utilities (TYPE_CHECKING-only core imports)
    browser/{family}/{artifact}/   # 3-level: chromium, firefox, ie_legacy, safari, tor
    system/                # registry, jump_lists, file_list
    media/                 # filesystem_images, foremost_carver, scalpel
    carvers/               # bulk_extractor, browser_carver
  reports/                 # PDF generation (Jinja2 + WeasyPrint)
reference_lists/           # YAML matching lists (urllists, hashlists, filelists)
tests/                     # Mirrors src/ — app, core, extractors, gui,
                           #   integration, compat
planning/wip/              # Active work specs
docs/wiki/                 # User-facing wiki
```

---

## Data Model

- **Case DB** `{case_number}_surfsifter.sqlite` — case metadata, network audit, report sections
- **Evidence DB** `evidence_<slug>.sqlite` — per-evidence artifacts (~72 tables)
- **Schema source of truth:** `src/core/database/migrations*/0001_*.sql` — always check directly, don't memorize

Key evidence table groups: `urls`, `browser_*`, `cookies`, `bookmarks`, `session_*`, `images`, `file_list`, `tags`, `timeline`, `process_log`.

---

## Migrations (Adding One)

1. Check current highest number in `migrations/` or `migrations_evidence/`
2. Create `NNNN_descriptive_name.sql` with next sequential number
3. Idempotent SQL only: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`
4. For `ALTER TABLE ADD COLUMN`: marker migration + helper function pattern (see `manager.py`)
5. Test fresh creation AND upgrade

**Never:** modify existing migrations, skip numbers, hard-code counts.

---

## Extractors (Adding One)

1. Folder: `extractors/{group}/{name}/` (groups: `browser`, `system`, `media`, `carvers`)
2. `__init__.py` exports `{Name}Extractor`; `extractor.py` implements `BaseExtractor.run_extraction()` and `run_ingestion()`
3. If new DB storage: migration → helper in `core/database/helpers/` → export in `helpers/__init__.py`
4. Optional: `ui.py` (config widget), `worker.py`
5. Tests for migration + helper + extractor

---

## Testing

```bash
# Default (CI)
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q

# GUI (needs display or Xvfb)
poetry run pytest tests/gui -m gui_offscreen -q

# Specific
poetry run pytest tests/path/to/test.py -v
```

**Markers:** `gui_offscreen`, `gui_live`, `integration`, `e2e`, `slow`, `compat`, `e01`, `pyewf`.

**Known issue:** PyEWF/PyTSK3 C libs have threading bugs → E01 tests excluded by default. Mark new E01-dependent tests with `@pytest.mark.e01`.

---

## Workflow / Prompts

Don't duplicate global rules in task prompts — reference this file.

| Task | Prompt |
|------|--------|
| Implement a GitHub issue | [`prompts/implement_issue.md`](prompts/implement_issue.md) |
| Implement a non-issue task | [`prompts/implement_task.md`](prompts/implement_task.md) |
| Review uncommitted changes (full) | [`prompts/review_uncommitted.md`](prompts/review_uncommitted.md) |
| Specialist reviews | [`prompts/review_correctness.md`](prompts/review_correctness.md), [`review_forensic_safety.md`](prompts/review_forensic_safety.md), [`review_security.md`](prompts/review_security.md), [`review_architecture.md`](prompts/review_architecture.md), [`review_documentation.md`](prompts/review_documentation.md) |
| Issue completeness check | [`prompts/review_issue_completeness.md`](prompts/review_issue_completeness.md) |

---

## Pre-Commit Checklist

1. Tests pass (default command above)
2. Dependency direction respected
3. DB changes have migration **and** helper
4. UI uses `CaseDataAccess` / helpers (no raw SQL)
5. Tool invocations logged via `process_log`
6. No evidence mutation
7. Audit log entries written for relevant actions

---

## Run the App

```bash
poetry run python -m app.main
# or
poetry run surfsifter
```

## Key Documentation

| Document | Purpose |
|----------|---------|
| `tests/README.md` | Test organization and markers |
| `src/core/database/migrations*/0001_*.sql` | Schema source of truth |
| `src/reports/modules/MODULE_GUIDE.md` | Report module dev guide |
| `docs/wiki/` | User-facing wiki |
| `planning/wip/` | Active work items |
