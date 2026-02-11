"""JSON manifest helpers (schema loading + validation)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from jsonschema import Draft202012Validator

from .logging import get_logger

LOGGER = get_logger("core.manifest")


@dataclass(frozen=True)
class ManifestValidationError(Exception):
    """Raised when manifest validation fails."""

    errors: List[str]

    def __str__(self) -> str:
        return " | ".join(self.errors)


def _project_root() -> Path:
    """Return project root (two levels up from this file)."""
    return Path(__file__).resolve().parents[2]


def load_schema(schema_path: Path) -> Draft202012Validator:
    """Load a JSON schema file and return a compiled validator."""
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def iter_validation_errors(validator: Draft202012Validator, document: Dict[str, Any]) -> Iterable[str]:
    """Yield human-readable error strings for a document."""
    for error in validator.iter_errors(document):
        path = "/".join(str(p) for p in error.path)
        pointer = f"{path}: " if path else ""
        yield f"{pointer}{error.message}"


def validate_image_carving_manifest(
    manifest_data: Dict[str, Any],
    schema_path: Optional[Path] = None,
) -> None:
    """
    Validate an image carving manifest against the canonical schema.

    Raises ManifestValidationError with formatted errors if validation fails.
    """
    schema_file = schema_path or _project_root() / "docs" / "schemas" / "image_carving_manifest.schema.json"
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema not found: {schema_file}")

    validator = load_schema(schema_file)
    errors = list(iter_validation_errors(validator, manifest_data))
    if errors:
        LOGGER.error("Manifest validation failed: %s", " | ".join(errors))
        raise ManifestValidationError(errors)
