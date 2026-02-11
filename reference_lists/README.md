# Reference Lists

This directory contains predefined reference lists for SurfSifter.

Reference lists let you match extracted browser artifacts (URLs, files, hashes) against known patterns during forensic investigations.

## Directory Structure

- `filelists/` — Filename pattern lists for matching files by name
- `hashlists/` — Hash lists for matching files by cryptographic hash
- `urllists/` — URL/domain pattern lists for matching discovered URLs

## Predefined Lists

### File Lists
- `deepfreeze.txt` — DeepFreeze system restore software artifacts
- `system_cleaners.txt` — CCleaner, BleachBit, and similar cleanup tools
- `browser_artifacts.txt` — Common browser database and cache files
- `temp_locations.txt` — Files in temporary directories

### Hash Lists
- `sample.txt` — Sample hash list for testing purposes

### URL Lists
- `sample_demo.txt` — Demo/template URL list using RFC 2606 reserved domains

## Usage

These reference lists are used with the matching features to:
1. Match imported file lists against known patterns
2. Match discovered URLs against reference domain lists
3. Identify files of interest for forensic investigation
4. Tag relevant files for inclusion in reports

## Adding Custom Lists

Create `.txt` files in the appropriate subdirectory. See `urllists/sample_demo.txt` for format examples, or consult the user manual.