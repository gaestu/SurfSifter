# Browser Config (Subtab)

Browser Config subtab - parsed browser configuration key/value records.

## Purpose
- Review configuration records stored in the evidence database `browser_config` table.
- Inspect browser settings, Tor configuration directives, and other parsed configuration values with provenance.

## Inputs
- Existing `browser_config` rows written by browser configuration extractors.
- Current sources include Tor Browser configuration/state ingestion and Chromium Preferences parsing where available.

## Filters and controls
- Filters: browser and config type.
- Search fields: config key and profile.
- Apply button to run the current filters.
- Double-click or context menu to open the complete record details.
- Context menu actions copy the config key, config value, and source path when present.

## Outputs
- Read-only table view of stored configuration records.
- Detail dialog with record id, evidence id, browser, profile, config type/key, value count, run id, partition index, filesystem type, created time, config value, source path, logical path, forensic path, and notes when present.

## Notes
- This subtab displays existing parsed records only; it does not add extraction coverage or modify evidence.
- Use source and provenance fields to tie configuration values back to the originating artifact.