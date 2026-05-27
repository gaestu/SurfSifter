"""Case-workspace path helpers for extracted image artifacts."""
from __future__ import annotations

from pathlib import Path
from typing import Mapping, Optional

from core.database.manager import slugify_label


# Maps image provenance values stored in ``images.first_discovered_by`` /
# ``image_discoveries.discovered_by`` to their case-workspace output folders.
IMAGE_SOURCE_SUBDIRS: Mapping[str, Optional[str]] = {
    "bulk_extractor": "bulk_extractor",
    "bulk_extractor:images": "bulk_extractor",
    "bulk_extractor_images": "bulk_extractor",
    "foremost_carver": "foremost_carver",
    "scalpel": "scalpel",
    "swiftbeaver": "swiftbeaver",
    "image_carving": None,
    "filesystem_images": "filesystem_images/extracted",
    "cache_simple": "cache_simple",
    "cache_blockfile": "cache_simple",
    "cache_blockfile_orphan": "cache_simple",
    "cache_appcache": "cache_simple",
    "cache_firefox": "cache_firefox",
    "browser_storage_indexeddb": "browser_storage",
    "chromium_browser_storage": "chromium_browser_storage",
    "media_history": "media_history",
    "safari": "safari",
    "safari_cache": "safari_cache",
    "safari_favicons": "safari_favicons",
    "firefox_favicons": "firefox_favicons",
    "chromium_favicons": "chromium_favicons",
}


def _source_subdir(discovered_by: Optional[str]) -> Optional[str]:
    source = discovered_by or ""
    if source in IMAGE_SOURCE_SUBDIRS:
        return IMAGE_SOURCE_SUBDIRS[source]
    source_prefix = source.split(":", 1)[0]
    return IMAGE_SOURCE_SUBDIRS.get(source_prefix)


def safe_relative_artifact_path(rel_path: str) -> Optional[Path]:
    """Return a relative artifact path, rejecting absolute/traversal input."""
    if not rel_path:
        return None
    path = Path(rel_path)
    if path.is_absolute() or ".." in path.parts:
        return None
    return path


def _contained_path(
    candidate: Path,
    case_root: Path,
    *,
    require_exists: bool,
    scope_root: Optional[Path] = None,
) -> Optional[Path]:
    """Resolve ``candidate`` only when it stays under the allowed roots."""
    try:
        resolved = candidate.resolve()
        resolved.relative_to(case_root)
        if scope_root is not None:
            resolved.relative_to(scope_root)
    except (OSError, ValueError):
        return None
    if require_exists and not resolved.exists():
        return None
    return resolved


def resolve_case_image_path(
    *,
    rel_path: str,
    discovered_by: Optional[str],
    case_folder: Path,
    evidence_id: int,
    evidence_label: Optional[str],
    require_exists: bool = True,
    allow_case_root_fallback: bool = False,
) -> Optional[Path]:
    """Resolve an extracted image path inside one evidence workspace.

    Resolution is constrained to ``case_folder`` so evidence-derived path
    strings cannot escape the case workspace or point at another filesystem
    location.  By default the helper tries the selected evidence folder only;
    callers may opt into a final ``case_folder / rel_path`` fallback for legacy
    report modules that already used that behavior.
    """
    rel = safe_relative_artifact_path(rel_path)
    if rel is None:
        return None

    evidence_root = evidence_workspace_root(
        case_folder=case_folder,
        evidence_id=evidence_id,
        evidence_label=evidence_label,
    )
    if evidence_root is None:
        return None
    case_root = Path(case_folder).resolve()
    evidence_dir = evidence_root

    candidates: list[tuple[Path, Optional[Path]]] = []
    subdir = _source_subdir(discovered_by)
    if subdir:
        candidates.append((evidence_dir / subdir / rel, evidence_root))
    candidates.append((evidence_dir / rel, evidence_root))
    if allow_case_root_fallback:
        candidates.append((case_root / rel, None))

    for candidate, scope_root in candidates:
        resolved = _contained_path(
            candidate,
            case_root,
            require_exists=require_exists,
            scope_root=scope_root,
        )
        if resolved is not None:
            return resolved
    return None


def evidence_workspace_root(
    *,
    case_folder: Path,
    evidence_id: int,
    evidence_label: Optional[str],
) -> Optional[Path]:
    """Return the selected evidence workspace root when it is case-contained."""
    try:
        case_root = Path(case_folder).resolve()
    except OSError:
        return None
    evidence_slug = (
        slugify_label(evidence_label, evidence_id)
        if evidence_label
        else f"evidence_{evidence_id}"
    )
    evidence_dir = case_root / "evidences" / evidence_slug
    try:
        if evidence_dir.is_symlink():
            return None
        evidence_root = evidence_dir.resolve()
        if evidence_root != evidence_dir:
            return None
        evidence_root.relative_to(case_root)
    except (OSError, ValueError):
        return None
    return evidence_root