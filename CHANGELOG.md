# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Add a first-class Chromium Browser Config extractor for curated `Preferences` and `Local State` settings (#57).
	- Note: older case workspaces ingested with the previous Chromium Extensions side-effect may still contain those historical `browser_config` rows until the case is intentionally cleaned or reprocessed.
- Add a read-only Browser Config subtab to Browser Inventory for filtering and inspecting stored `browser_config` evidence rows (#56).
- Fixed Chromium Media History playback parsing when SQLite rows are read via `sqlite3.Row`, including databases with `playback.origin_id` joined to the `origin` table (#31).
- Add app_execution, browser_history and installed_software to tagged summary export.
- Route Tag Summary `download`/`downloads` tags to the investigator downloads table as **Downloaded files**, distinct from browser download artifacts.
- Add embedded thumbnails for tagged images and image reference-list/hash matches in Tag Summary XLSX exports.
- Harden report and appendix image thumbnails: sources must resolve under the selected evidence workspace, generated thumbnails are embedded inline in report output, stale cache files are not trusted as report-visible evidence, and Pillow/optional pillow-heif rendering uses the shared thumbnail safety limits.
- Harden image/download/screenshot report HTML rendering by enabling Jinja autoescaping for evidence-derived fields.

## v0.3.8-beta - 2026-05-07

- Added batch folder import for URL reference lists with shared metadata and conflict handling.
- Removed the unused SQLite Hash DB preferences and rebuild workflow while keeping text hash-list matching.
- Removed deprecated YAML Rules preferences and internal rules_dir plumbing.
- Fixed `NameError: name 'normalized_logical_path' is not defined` in `chromium_sessions` extractor's `_collect_url_data()`, which caused all session files to be marked `error` in `browser_cache_inventory` and silently skipped URL cross-posting to the `urls` table (#55).
