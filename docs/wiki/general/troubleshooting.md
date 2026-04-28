# Troubleshooting

## Install and dependency issues
**Symptom:** App fails to start or crashes during import.
- Ensure required system packages are installed (libewf, libtsk, cairo/pango stack).
- Confirm your Python version is **>= 3.10, < 3.14**.

**Symptom:** PDF reports fail or render incorrectly.
- Verify `libpango`, `libcairo`, and related GTK dependencies are installed (Linux).

**Symptom:** Appendix PDFs are slow or fall back to WeasyPrint.
- Install a recent Chromium-family browser (Chromium, Chrome, Edge, or Brave).
- Appendix PDFs use Chromium `131+` by default when available; if Chromium is missing, unsupported, or the Chromium render fails at runtime, SurfSifter falls back to the slower WeasyPrint path when WeasyPrint is installed.
- Chromium appendix rendering is intentionally disabled when SurfSifter runs as `root`, because Chromium sandboxing would be unavailable in that mode. Run SurfSifter as a non-root user to use Chromium appendix rendering.
- If appendix generation still fails, review the evidence `process_log` entries for `chromium_probe` and/or `chromium_appendix_pdf`, depending on how far the export progressed.

## Optional features not available
**Symptom:** A feature is missing (e.g., Jump Lists, cache decompression, Safari).
- All artifact-parsing libraries are now installed by default. Re-run `poetry install` to ensure they are present.

**Symptom:** `No ESE library available. Install libesedb-python or dissect.esedb ...`
- Source install: re-run `poetry install` (libesedb-python is now a standard dependency).
- Prebuilt release binary: update to the latest release/installer build.

## External tools not detected
**Symptom:** Carving or metadata features are disabled.
- Ensure the tool is installed and available on your system `PATH`.
- Use the **Tools** tab to verify detection status.

## Evidence access problems
**Symptom:** E01 image fails to open or partitions are missing.
- Re-check that the image is not corrupted and all segments are present.
- Confirm libewf/libtsk are installed correctly on Linux.

If you still get errors, capture the logs from your case directory and open an issue with the steps to reproduce.
