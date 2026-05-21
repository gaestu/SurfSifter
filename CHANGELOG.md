# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Fixed Chromium Media History playback parsing when SQLite rows are read via `sqlite3.Row`, including databases with `playback.origin_id` joined to the `origin` table (#31).
- Add app_execution, browser_history and installed_software to tagged summary export

## v0.3.8-beta - 2026-05-07

- Added batch folder import for URL reference lists with shared metadata and conflict handling.
- Removed the unused SQLite Hash DB preferences and rebuild workflow while keeping text hash-list matching.
- Removed deprecated YAML Rules preferences and internal rules_dir plumbing.
- Fixed `NameError: name 'normalized_logical_path' is not defined` in `chromium_sessions` extractor's `_collect_url_data()`, which caused all session files to be marked `error` in `browser_cache_inventory` and silently skipped URL cross-posting to the `urls` table (#55).
