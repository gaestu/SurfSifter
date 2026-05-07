# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Added batch folder import for URL reference lists with shared metadata and conflict handling.
- Removed the unused SQLite Hash DB preferences and rebuild workflow while keeping text hash-list matching.
- Removed deprecated YAML Rules preferences and internal rules_dir plumbing.
- Fixed `NameError: name 'normalized_logical_path' is not defined` in `chromium_sessions` extractor's `_collect_url_data()`, which caused all session files to be marked `error` in `browser_cache_inventory` and silently skipped URL cross-posting to the `urls` table (#55).
