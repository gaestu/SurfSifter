# Media

Source: `src/extractors/media/`

## Overview
- Scope: Filesystem image extraction plus image carving (Foremost, Scalpel).
- Extraction: FilesystemImages copies images discovered via `file_list` (or SleuthKit `fls` for EWF); carvers run external tools against mounted/EWF sources.
- Ingestion: Computes perceptual hashes/EXIF (and thumbnails) and stores images with discovery records in the evidence DB; writes extracted_files audit records when available.

## Extractors
### FilesystemImagesExtractor
- Purpose: Extract image files from the evidence filesystem with path/timestamp context.
- Extraction (source): Evidence filesystem; discovery requires `file_list` table and may auto-generate via SleuthKit `fls` for EWF images; fails if `file_list` is missing.
- Extraction (selection): Filters by extension list (config or `SUPPORTED_IMAGE_EXTENSIONS`), include/exclude glob patterns, and min/max size; normalizes Windows backslashes for pattern matching.
- Extraction (processing): Copies files to `output_dir/extracted`, computes MD5/SHA256, records MACB timestamps/inode, optional signature verification, and skips sparse/OneDrive placeholders.
- Outputs: `manifest.json`, `files_to_extract.csv`, `discovery_summary.json`, extracted files under `extracted/`, plus `extracted_files` audit rows.
- Ingestion (transform + store): Loads manifest, computes pHash/EXIF/thumbnail via `ParallelImageProcessor`, inserts/enriches via `insert_image_with_discovery` (images + image_discoveries), updates manifest ingestion stats.
- Notes: Parallel extraction is supported for `PyEwfTskFS` with multiple workers; run IDs are generated per extraction run.

### ForemostCarverExtractor (slow)
- Purpose: Carve deleted images from unallocated space using Foremost.
- Extraction (source): Runs `foremost` on mounted paths or EWF images (via ewfmount when available) with a generated config from `DEFAULT_FILE_TYPES` signatures.
- Extraction (processing): Writes to `carved/`, collects carved files by image extension, verifies with Pillow, computes MD5/SHA256, and builds a validated manifest.
- Outputs: `manifest.json`, `carved/` tree, `carver.conf`, and `carved/audit.txt` when produced by Foremost.
- Ingestion (transform + store): `run_image_ingestion` computes pHash/EXIF/thumbnails and inserts/enriches images via `insert_image_with_discovery`; manifest updated with ingestion stats.
- Notes: Records `extracted_files` audit rows with byte offsets derived from Foremost audit data; requires the `foremost` tool.

Pay attention this extractor is working single threathed an is slow for that reason.

### ScalpelExtractor (experimental)
- Purpose: Carve deleted images from unallocated space using Scalpel.
- Extraction (source): Runs `scalpel` on mounted paths or EWF images (via ewfmount when available) using a config file (default `default.conf` or configured).
- Extraction (processing): Writes to `carved/`, collects carved files by image extension, verifies with Pillow, computes MD5/SHA256, and builds a validated manifest.
- Outputs: `manifest.json`, `carved/` tree, and `carver.conf` (copy of the config used).
- Ingestion (transform + store): `run_image_ingestion` computes pHash/EXIF/thumbnails and inserts/enriches images via `insert_image_with_discovery`; manifest updated with ingestion stats.
- Notes: Records `extracted_files` audit rows with offsets stored in manifest entries; requires the `scalpel` tool.

Pay attention:
- this extractor is working single threathed an is slow for that reason.
- this extractor is configured loosely and may produce a lot of junk data

## Patterns
- File/path patterns: FilesystemImages supports case-insensitive `fnmatch` include/exclude globs; carving uses signature patterns from Foremost `DEFAULT_FILE_TYPES` and Scalpel `default.conf` (magic headers/footers + max sizes).
