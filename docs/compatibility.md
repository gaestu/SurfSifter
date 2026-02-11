# Compatibility Policy

Last updated: 2026-02-08

## Purpose

This document defines which backward-compatibility shims are intentional, where
they live, and how they are sunset as the project moves toward a stable public API.

## Public API Rule

- Public imports should use canonical feature/module entry points.
- New code must not introduce new legacy aliases or re-export wrappers.
- Compatibility shims are transitional and must have a documented removal target.

## Compatibility Boundary

The primary compatibility boundary is:

- `core.database.compat`

This module centralizes legacy database alias names that historically came from
`db.py`-style APIs and maps them to canonical helper functions.

## Current Transitional Shims

### Database aliases (centralized)

- `core.database.compat` exports legacy names such as:
  - `insert_autofill`, `get_autofill`
  - `insert_site_permissions`, `get_site_permissions`
  - `insert_browser_extensions`, `get_browser_extensions`
  - `insert_local_storage_row`, `delete_indexeddb_by_run`
  - and related alias families.

Canonical equivalents are defined in `core.database.helpers.*`.

### Helper-level legacy aliases (still present)

Some helper modules still expose legacy alias names for compatibility with older
internal/external code:

- `core.database.helpers.autofill`
- `core.database.helpers.extensions`
- `core.database.helpers.artifacts`
- `core.database.helpers.permissions`
- `core.database.helpers.jump_lists`
- `core.database.helpers.images` (legacy parameters)

These aliases are marked deprecated in-code and are scheduled for removal.

### Configuration migration compatibility

- `core.tool_registry.ToolRegistry` still supports migration from legacy path:
  `~/.config/surfsifter/tool_paths.json`

This is runtime migration compatibility, not API compatibility.

## Deprecation Window

- Current transition window: `0.2.x`
- Target removal for deprecated compatibility aliases: `v1.0.0`

Before removal:

1. Remove internal usage of alias names.
2. Keep changelog notes for removed aliases.
3. Update tests/docs to canonical names.

## Canonical Import Examples

- Prefer `from app.features.images import ImagesTab`
- Prefer `from app.features.extraction import ExtractionTab`
- Prefer `from app.features.timeline.config import load_timeline_config`
- Prefer `from core.database import insert_permissions, get_permissions`

Avoid importing deprecated aliases when canonical names are available.

