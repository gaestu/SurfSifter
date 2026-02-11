# Test Suite Guide

## Running Tests

### Default (non-GUI, non-slow, non-compat)
```bash
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat"
```

### CI / Fast
```bash
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" --tb=short -q
```

### GUI Tests (offscreen)
```bash
poetry run pytest tests/gui -m "gui_offscreen" -q
```

### GUI Tests (live display)
```bash
poetry run pytest tests/gui -m "gui_live" -q
```

### Integration Tests
```bash
poetry run pytest -m integration
```

### End-to-End Tests
```bash
poetry run pytest -m e2e
```

### Compatibility Tests
```bash
poetry run pytest -m compat
```

## Test Organization (mirrors src/)

- `tests/app/` — app-level logic and models
- `tests/core/` — core utilities, DB, validation, registry
- `tests/extractors/` — extractor logic and shared extractor helpers
- `tests/reports/` — report generation and modules
- `tests/gui/` — Qt/PySide GUI tests (with GUI-specific fixtures)
- `tests/integration/` — cross-module integration tests
- `tests/compat/` — legacy/backward-compatibility checks
- `tests/fixtures/` — shared fixtures/helpers

## Markers

- `gui_offscreen` / `gui_live` — GUI tests
- `integration` — cross-module or system-level tests
- `e2e` — full pipeline tests
- `slow` — long-running tests
- `compat` — backward compatibility checks
- `e01`, `pyewf` — tests requiring E01 / pyewf

## Notes

- GUI tests are isolated under `tests/gui/` and use a dedicated conftest.
- Default runs exclude GUI + slow + compat to keep the baseline fast and stable.
