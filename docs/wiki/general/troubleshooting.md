# Troubleshooting

## Install and dependency issues
**Symptom:** App fails to start or crashes during import.
- Ensure required system packages are installed (libewf, libtsk, cairo/pango stack).
- Confirm your Python version is **>= 3.10, < 3.14**.

**Symptom:** PDF reports fail or render incorrectly.
- Verify `libpango`, `libcairo`, and related GTK dependencies are installed (Linux).

## Optional features not available
**Symptom:** A feature is missing (e.g., Jump Lists, cache decompression, Safari).
- All artifact-parsing libraries are now installed by default. Re-run `poetry install` to ensure they are present.

**Symptom:** `No ESE library available. Install libesedb-python or dissect.esedb ...`
- Source install: re-run `poetry install` (libesedb-python is now a standard dependency).
- Prebuilt release binary: update to the latest release/installer build.

**Symptom:** `No ESE library available. Install libesedb-python or dissect.esedb ...`
- Source install: run `poetry install --extras ie` (or `pip install -e .[ie]`).
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
