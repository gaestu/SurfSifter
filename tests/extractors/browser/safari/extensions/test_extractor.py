"""Tests for Safari extensions extractor."""

from __future__ import annotations

import fnmatch
import json
import plistlib
from pathlib import Path

from extractors.browser.safari.extensions import SafariExtensionsExtractor
from extractors.extractor_registry import ExtractorRegistry


class _Callbacks:
    def on_step(self, step_name):
        return None

    def on_log(self, message, level="info"):
        return None

    def on_error(self, error, details=""):
        return None

    def on_progress(self, current, total, message=""):
        return None

    def is_cancelled(self):
        return False


class _FakeEvidenceFS:
    def __init__(self, file_map):
        self.file_map = file_map
        self.fs_type = "APFS"
        self.source_path = "/tmp/evidence.E01"
        self.partition_index = 0

    def iter_paths(self, pattern):
        for path in self.file_map:
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(f"/{path}", pattern):
                yield path

    def read_file(self, path):
        if path in self.file_map:
            return self.file_map[path]
        alt = path.lstrip("/")
        if alt in self.file_map:
            return self.file_map[alt]
        raise FileNotFoundError(path)


def _plist_bytes(payload):
    return plistlib.dumps(payload, fmt=plistlib.FMT_BINARY)


# =========================================================================
# Metadata & Registry
# =========================================================================


def test_metadata_and_registry_discovery() -> None:
    extractor = SafariExtensionsExtractor()
    assert extractor.metadata.name == "safari_extensions"
    assert extractor.metadata.can_extract is True
    assert extractor.metadata.can_ingest is True

    registry = ExtractorRegistry()
    assert "safari_extensions" in registry.list_names()
    assert isinstance(registry.get("safari_extensions"), SafariExtensionsExtractor)


# =========================================================================
# Extraction Phase
# =========================================================================


def test_extraction_copies_extension_files(tmp_path: Path) -> None:
    """FakeEvidenceFS with Extensions.plist → manifest written with correct artifact_type."""
    extensions_plist = _plist_bytes({
        "Installed Extensions": [
            {
                "Bundle Identifier": "com.test.ext",
                "Archive File Name": "TestExt.safariextz",
                "Enabled": True,
            },
        ]
    })

    file_map = {
        "Users/testuser/Library/Safari/Extensions/Extensions.plist": extensions_plist,
    }

    evidence_fs = _FakeEvidenceFS(file_map)
    extractor = SafariExtensionsExtractor()
    output_dir = tmp_path / "safari_extensions"
    config = {"evidence_id": 1, "evidence_label": "test"}
    callbacks = _Callbacks()

    result = extractor.run_extraction(evidence_fs, output_dir, config, callbacks)
    assert result is True

    # Find manifest
    manifests = list(output_dir.glob("*/manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text())

    assert manifest["status"] == "ok"
    assert len(manifest["files"]) >= 1

    # Verify the extensions plist was copied with correct artifact_type
    plist_files = [f for f in manifest["files"] if f["artifact_type"] == "extensions_plist"]
    assert len(plist_files) == 1
    assert plist_files[0]["browser"] == "safari"
    assert plist_files[0]["md5"]
    assert plist_files[0]["sha256"]


def test_extraction_no_files(tmp_path: Path) -> None:
    """Empty evidence → manifest with status skipped."""
    evidence_fs = _FakeEvidenceFS({})
    extractor = SafariExtensionsExtractor()
    output_dir = tmp_path / "safari_extensions"
    config = {"evidence_id": 1, "evidence_label": "test"}
    callbacks = _Callbacks()

    result = extractor.run_extraction(evidence_fs, output_dir, config, callbacks)
    assert result is True  # skipped is not error/cancelled

    manifests = list(output_dir.glob("*/manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text())
    assert manifest["status"] == "skipped"


def test_can_run_extraction_no_fs() -> None:
    extractor = SafariExtensionsExtractor()
    ok, msg = extractor.can_run_extraction(None)
    assert ok is False
    assert "No evidence" in msg


def test_can_run_ingestion_no_manifest(tmp_path: Path) -> None:
    extractor = SafariExtensionsExtractor()
    ok, msg = extractor.can_run_ingestion(tmp_path)
    assert ok is False
    assert "manifest" in msg.lower()
