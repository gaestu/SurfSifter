# Compliance & Licensing Notes

The analyzer is distributed under Apache 2.0. Third-party components fall into two buckets:

1. **Bundled artifacts** – the Python dependencies and assets embedded in wheels/AppImages, plus SleuthKit binaries (CPL-1.0) in `vendor/sleuthkit/` for supported platforms. These are enumerated in `dist/THIRD_PARTY_LICENSES.md` after running `bash scripts/gen-sbom.sh`.
2. **Runtime-discovered tools** – optional incident-response utilities (`bulk_extractor`, `foremost`, `ewfmount`, `exiftool`, etc.) located on the investigator's workstation. These are not redistributed with the application; instead we record their invocation path/version in `process_log` for traceability.

Bundling GPL utilities would impose reciprocal obligations, so keep them out of the packaged artifacts. If investigators choose to install GPL tools locally, the application will detect and invoke them in-place, preserving the read-only stance on evidence.

## Generating the SBOM & License Manifest

Run the helper script whenever dependencies change:

```bash
bash scripts/gen-sbom.sh
```

This produces:

- `dist/sbom.json` – CycloneDX software bill of materials for the resolved Python environment.
- `dist/THIRD_PARTY_LICENSES.md` – Markdown summary (with authors and URLs) for included dependencies.

Store these alongside release artifacts to make downstream reviews easier.
