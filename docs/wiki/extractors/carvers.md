# Carvers

Source: `src/extractors/carvers/`

## Overview
- Scope: External carving utilities (bulk_extractor; browser carver via foremost/scalpel).
- Extraction: Run carving tools on evidence sources or disk images to produce tool output files and manifests.
- Ingestion: Parse tool outputs into evidence DB records (URLs/emails/domains/IPs/phones/crypto; carved browser history/cookies; raw URL hits).

## Extractors
### BulkExtractorExtractor
- Purpose: Run bulk_extractor and ingest extracted network/PII indicators plus optional carved images.
- Extraction (source): Evidence source path passed directly to bulk_extractor; output under `{case_root}/evidences/{evidence_label}/bulk_extractor/`.
- Extraction (process): Executes bulk_extractor with selected scanners; writes `*.txt` artifact files; optional JPEG carving enabled by default.
- Ingestion (transform + store): Parses tab-separated output lines; normalizes URLs/emails/domains; inserts into `urls`, `emails`, `domains`, `ip_addresses`, `bitcoin_addresses`, `ethereum_addresses`, `telephone_numbers` (CCN detected but not stored).
- Outputs: Tool output files; `bulk_extractor_stderr.log` on failure; carved image manifest `bulk_extractor_images_manifest.json` and carved image audit records.
- Notes: Requires `bulk_extractor` tool; artifact types selectable at ingestion; image ingestion uses ParallelImageProcessor + `ingest_with_enrichment`.

### SwiftBeaverExtractor
- Purpose: Carve images and extract URLs using SwiftBeaver (Phase 1 toward replacing bulk_extractor). Reads E01 natively via libewf.
- Extraction (source): Evidence source path (E01/EWF) passed directly to SwiftBeaver; output under `{case_root}/evidences/{evidence_label}/swiftbeaver/`.
- Extraction (process): Executes SwiftBeaver with configurable image types (jpeg/png/gif/webp/bmp/tiff/heic/ico), URL scanning, deduplication (`--dedupe --skip-duplicates`), and validation (`--validate-carved --remove-invalid`). Pre-computes md5+sha256 hashes. Outputs structured JSONL metadata.
- Ingestion (transform + store): Parses `carved_files.jsonl` for images (filters by min size, default 4KB); uses `ParallelImageProcessor` for phash/EXIF/thumbnails; ingests via `ingest_with_enrichment()` with pre-computed hashes (skips re-hashing). Parses `string_artefacts.jsonl` for URLs (filters by `artefact_kind == "url"`); batch-inserts via `insert_urls()`.
- Outputs: Carved images in `carved/` subdirectory; `metadata/carved_files.jsonl`, `metadata/string_artefacts.jsonl`, `metadata/run_summary.jsonl`; `swiftbeaver_images_manifest.json`; `extraction_info.json` (subprocess audit).
- Notes: Requires `swiftbeaver` tool (installed via `install.sh --with-recommended-tools`); CPU-only Phase 1 (GPU toggle planned for future phase); forensic offsets preserved via `global_start` field.

### BrowserCarverExtractor
- Purpose: Deep-scan unallocated space for carved browser artifacts using foremost/scalpel.
- Extraction (source): Evidence filesystem image path (`source_path`/`image_path`); output under `{case_root}/evidences/{evidence_label}/browser_carver/`.
- Extraction (process): Carves SQLite files via foremost/scalpel + browser_artifacts.conf (if present), validates SQLite headers, identifies browser DB types, optionally scans carved files for raw URLs, and prunes non-ingested file types under safety caps.
- Ingestion (transform + store): Inserts carved DB inventory rows, parses history/cookies/places via best-effort SQLite parsing, and inserts raw URLs into `urls` with `discovered_by` run identifiers.
- Outputs: `manifest.json`; optional `raw_urls.txt`; carved output directory contents.
- Notes: Requires foremost or scalpel; future expansion not implemented for Chrome cache block files, LevelDB, OLE compound files, and compressed artifacts.

## Patterns
- File/path patterns: bulk_extractor emits named `*.txt` outputs (url.txt, email.txt, domain.txt, ip.txt, telephone.txt, ccn.txt, bitcoin.txt, ether.txt) and image dirs (`jpeg_carved/`, `jpeg/`, `images/`); browser_carver writes `manifest.json` and optional `raw_urls.txt`; SwiftBeaver writes JSONL metadata under `metadata/` and carved files under `carved/`.
