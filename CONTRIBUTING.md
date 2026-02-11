# Contributing

## Development Setup

1. Fork and clone the repository.
2. Install dependencies:
   ```bash
   poetry install --extras all
   ```
3. Run the app:
   ```bash
   poetry run surfsifter
   ```

## Running Tests

- Fast default suite:
  ```bash
  poetry run pytest -k "not gui and not e01" -q
  ```
- Compatibility tests:
  ```bash
  poetry run pytest tests/compat -m compat --override-ini addopts= -q
  ```
- GUI smoke tests (one file at a time):
  ```bash
  QT_QPA_PLATFORM=offscreen poetry run pytest tests/gui/app/test_text_blocks_feature.py -m gui_offscreen --override-ini addopts= -q
  ```
- Scoped type checks:
  ```bash
  poetry run mypy src/core/database/connection.py src/core/database/helpers/firefox_cache_index.py src/core/app_version.py --ignore-missing-imports --no-error-summary
  ```

## Pull Request Expectations

- Keep changes focused and include tests for behavior changes.
- Update docs when user-facing behavior or workflows change.
- Ensure CI checks pass before requesting review.
- Use descriptive commit messages and link related issues.

## Release and Compliance Notes

Release builds must include required artifacts, checksums, and SBOM/license outputs. See:
- `.github/workflows/release.yml`
- `scripts/gen-sbom.sh`
