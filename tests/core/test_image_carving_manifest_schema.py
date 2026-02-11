"""Tests for image carving manifest schema validation."""

from datetime import datetime, timezone

import pytest

from core.manifest import ManifestValidationError, validate_image_carving_manifest


def _base_manifest():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": "1.0.0",
        "run_id": "test_run",
        "extractor": "image_carving",
        "tool": {
            "name": "foremost",
            "version": "1.5.7",
            "path": "/usr/bin/foremost",
            "arguments": ["foremost"],
        },
        "started_at": now,
        "completed_at": now,
        "input": {
            "source": "/fake/evidence.E01",
            "source_type": "ewf",
            "evidence_id": 1,
            "context": {},
        },
        "output": {
            "root": "/tmp/out",
            "carved_dir": "/tmp/out/carved",
            "manifest_path": "/tmp/out/manifest.json",
        },
        "file_types": {},
        "stats": {
            "carved_total": 1,
            "zero_byte": 0,
            "failed_validation": 0,
            "by_type": {"jpg": 1},
        },
        "warnings": [],
        "notes": [],
        "carved_files": [
            {
                "rel_path": "carved/jpg/image1.jpg",
                "size": 12,
                "md5": "0" * 32,
                "sha256": "0" * 64,
                "file_type": "jpg",
                "offset": 0,
                "warnings": [],
                "errors": [],
                "validated": {"pillow_ok": True},
            }
        ],
    }


def test_validate_image_carving_manifest_missing_required():
    manifest = _base_manifest()
    manifest.pop("run_id")
    with pytest.raises(ManifestValidationError):
        validate_image_carving_manifest(manifest)
