# System

Source: `src/extractors/system/`

## Overview
- Scope: Windows system artifacts (registry hives, Jump Lists, Recent Items).
- Extraction: Copy registry hives and Jump List/Recent Item files into case output with manifests.
- Ingestion: Analyze hives with rules into `os_indicators`; parse Jump Lists/LNKs into `jump_list_entries` and `urls`.

## Extractors
### SystemRegistryExtractor
- Purpose: Extract Windows registry indicators for forensic analysis (e.g., system info, startup, network profiles).
- Extraction (source): Offline registry hives discovered via glob patterns in evidence (SYSTEM, SOFTWARE, SAM, SECURITY, NTUSER.DAT, UsrClass.dat).
- Ingestion (transform + store): Applies registry rule targets to exported hives; normalizes findings and inserts into `os_indicators`.
- Outputs: `manifest.json` plus copied hive files under `hives/`; ingestion summary `ingestion_registry.json`.
- Dependencies: `regipy` required for ingestion; extraction does not require it.
- Notes: Uses rule set loaded from `rules.py` via `load_registry_rules`; supports purge-by-evidence and run_id cleanup.

### SystemJumpListsExtractor
- Purpose: Extract Jump List and Recent Item artifacts, including browser URLs and file targets.
- Extraction (source): Jump List and Recent Item files from user profile paths (AutomaticDestinations, CustomDestinations, standalone `.lnk`).
- Ingestion (transform + store): Parses OLE/Custom Jump Lists and LNKs, derives titles/URLs, inserts into `jump_list_entries` and `urls`.
- Outputs: `manifest.json` plus copied Jump List/LNK files in the extractor output directory.
- Dependencies: Requires `olefile` for extraction and ingestion; uses AppID registry to classify browser Jump Lists.
- Notes: Clears prior Jump List entries and related URLs for the evidence before ingestion to avoid duplicates.

## Patterns
- File/path patterns: Registry hive globs under `Windows/System32/config/*`, `Users/*/NTUSER.DAT`, `Users/*/.../UsrClass.dat`, and legacy `Documents and Settings` paths; Jump Lists under `Users/*/AppData/Roaming/Microsoft/Windows/Recent/{AutomaticDestinations,CustomDestinations}` and `Recent/*.lnk`.
- Registry/OS patterns: Rule targets include key paths such as `Microsoft\\Windows NT\\CurrentVersion`, `...\\Run/RunOnce`, `...\\NetworkList\\Profiles`, and `ControlSet001\\...` in SYSTEM/SOFTWARE hives.
- Notes: Patterns are case-insensitive via explicit glob variants in registry hive scanning.
