from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable as IterableABC
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from uuid import uuid4

TEXT_BLOCKS_FILENAME = "text_blocks.json"
TEXT_BLOCKS_SCHEMA_VERSION = 1
TEXT_BLOCKS_EXPORT_FORMAT = "surfsifter-text-blocks"
TEXT_BLOCKS_APP_DIRNAME = "surfsifter"
_LEGACY_APP_DIRNAME = "web-and-browser-analyzer"


@dataclass
class TextBlock:
    """Reusable text snippet for report sections."""

    id: str
    title: str
    content: str
    tags: List[str]
    created_at: str
    updated_at: str


@dataclass
class TextBlockImportResult:
    """Summary of an import operation."""

    total_processed: int = 0
    imported: int = 0
    overwritten: int = 0
    renamed: int = 0
    skipped_duplicates: int = 0
    skipped_invalid: int = 0


class TextBlockStore:
    """File-backed storage for global text blocks."""

    def __init__(self, config_dir: Optional[Path] = None, filename: str = TEXT_BLOCKS_FILENAME) -> None:
        self.config_dir = Path(config_dir) if config_dir is not None else default_text_blocks_dir()
        self.path = self.config_dir / filename

    def load_blocks(self) -> List[TextBlock]:
        """Load all text blocks from disk."""
        payload = self._read_payload()
        raw_blocks = payload.get("text_blocks", [])
        blocks: List[TextBlock] = []

        for raw in raw_blocks:
            block = self._parse_stored_block(raw)
            if block is not None:
                blocks.append(block)

        return sorted(blocks, key=lambda block: block.title.casefold())

    def save_blocks(self, blocks: Sequence[TextBlock]) -> None:
        """Persist the given blocks to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": TEXT_BLOCKS_SCHEMA_VERSION,
            "text_blocks": [asdict(block) for block in blocks],
        }
        self.path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def list_blocks(self, search: str = "", tag: Optional[str] = None) -> List[TextBlock]:
        """Load and filter blocks by search query and optional tag."""
        return self.filter_blocks(self.load_blocks(), search=search, tag=tag)

    @staticmethod
    def filter_blocks(
        blocks: Sequence[TextBlock],
        search: str = "",
        tag: Optional[str] = None,
    ) -> List[TextBlock]:
        """Filter blocks in-memory by text query and optional tag."""
        query = search.strip().casefold()
        selected_tag = (tag or "").strip().casefold()

        filtered: List[TextBlock] = []
        for block in blocks:
            if selected_tag and selected_tag != "all" and selected_tag not in {
                tag_value.casefold() for tag_value in block.tags
            }:
                continue

            if query:
                haystack = "\n".join([block.title, block.content, " ".join(block.tags)]).casefold()
                if query not in haystack:
                    continue

            filtered.append(block)

        return sorted(filtered, key=lambda block: block.title.casefold())

    @staticmethod
    def all_tags(blocks: Sequence[TextBlock]) -> List[str]:
        """Collect all unique tags used across blocks."""
        unique_tags = {tag.strip().lower() for block in blocks for tag in block.tags if tag.strip()}
        return sorted(unique_tags)

    def get_block(self, block_id: str) -> Optional[TextBlock]:
        """Return a block by ID."""
        for block in self.load_blocks():
            if block.id == block_id:
                return block
        return None

    def add_block(self, title: str, content: str, tags: Iterable[str]) -> TextBlock:
        """Create and persist a new text block."""
        blocks = self.load_blocks()
        now = _utc_now()

        block = TextBlock(
            id=str(uuid4()),
            title=title.strip(),
            content=content.strip(),
            tags=_normalize_tags(tags),
            created_at=now,
            updated_at=now,
        )
        blocks.append(block)
        self.save_blocks(blocks)
        return block

    def update_block(self, block_id: str, title: str, content: str, tags: Iterable[str]) -> Optional[TextBlock]:
        """Update an existing text block."""
        blocks = self.load_blocks()
        updated: Optional[TextBlock] = None

        for index, block in enumerate(blocks):
            if block.id != block_id:
                continue

            updated = TextBlock(
                id=block.id,
                title=title.strip(),
                content=content.strip(),
                tags=_normalize_tags(tags),
                created_at=block.created_at,
                updated_at=_utc_now(),
            )
            blocks[index] = updated
            break

        if updated is not None:
            self.save_blocks(blocks)
        return updated

    def delete_block(self, block_id: str) -> bool:
        """Delete an existing block by ID."""
        blocks = self.load_blocks()
        remaining = [block for block in blocks if block.id != block_id]
        if len(remaining) == len(blocks):
            return False

        self.save_blocks(remaining)
        return True

    def export_blocks(self, export_path: Path, block_ids: Optional[Sequence[str]] = None) -> int:
        """Export blocks into portable JSON format.

        Returns:
            Number of exported blocks.
        """
        blocks = self.load_blocks()
        if block_ids:
            selected = set(block_ids)
            blocks = [block for block in blocks if block.id in selected]

        payload = {
            "format": TEXT_BLOCKS_EXPORT_FORMAT,
            "version": TEXT_BLOCKS_SCHEMA_VERSION,
            "exported_at": _utc_now(),
            "blocks": [
                {
                    "title": block.title,
                    "content": block.content,
                    "tags": list(block.tags),
                }
                for block in blocks
            ],
        }

        export_path = Path(export_path)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return len(blocks)

    def import_blocks(self, import_path: Path, duplicate_strategy: str = "skip") -> TextBlockImportResult:
        """Import blocks from exported JSON.

        Args:
            import_path: JSON file in export format.
            duplicate_strategy: One of "skip", "overwrite", "rename".
        """
        duplicate_mode = duplicate_strategy.strip().lower()
        if duplicate_mode not in {"skip", "overwrite", "rename"}:
            raise ValueError("duplicate_strategy must be one of: skip, overwrite, rename")

        payload = json.loads(Path(import_path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Import payload must be a JSON object")

        raw_blocks = payload.get("blocks")
        if raw_blocks is not None:
            payload_format = str(payload.get("format", "")).strip()
            if payload_format != TEXT_BLOCKS_EXPORT_FORMAT:
                raise ValueError(
                    "Invalid import format. Expected "
                    f"'{TEXT_BLOCKS_EXPORT_FORMAT}'."
                )
            payload_version = payload.get("version")
            if payload_version != TEXT_BLOCKS_SCHEMA_VERSION:
                raise ValueError(
                    "Unsupported import version. "
                    f"Expected {TEXT_BLOCKS_SCHEMA_VERSION}, got {payload_version!r}."
                )
        else:
            raw_blocks = payload.get("text_blocks")
            payload_version = payload.get("version")
            if payload_version not in (None, TEXT_BLOCKS_SCHEMA_VERSION):
                raise ValueError(
                    "Unsupported storage version. "
                    f"Expected {TEXT_BLOCKS_SCHEMA_VERSION}, got {payload_version!r}."
                )
        if not isinstance(raw_blocks, list):
            raise ValueError("Import file does not contain a valid blocks list")

        result = TextBlockImportResult(total_processed=len(raw_blocks))
        existing = self.load_blocks()
        existing_by_title = {block.title.casefold(): index for index, block in enumerate(existing)}

        changed = False
        for raw in raw_blocks:
            prepared = self._parse_import_entry(raw)
            if prepared is None:
                result.skipped_invalid += 1
                continue

            title, content, tags = prepared
            key = title.casefold()
            now = _utc_now()

            if key in existing_by_title:
                if duplicate_mode == "skip":
                    result.skipped_duplicates += 1
                    continue

                if duplicate_mode == "overwrite":
                    existing_index = existing_by_title[key]
                    current = existing[existing_index]
                    existing[existing_index] = TextBlock(
                        id=current.id,
                        title=current.title,
                        content=content,
                        tags=tags,
                        created_at=current.created_at,
                        updated_at=now,
                    )
                    result.overwritten += 1
                    changed = True
                    continue

                # duplicate_mode == "rename"
                title = self._generate_unique_title(title, [block.title for block in existing])
                key = title.casefold()
                result.renamed += 1

            block = TextBlock(
                id=str(uuid4()),
                title=title,
                content=content,
                tags=tags,
                created_at=now,
                updated_at=now,
            )
            existing.append(block)
            existing_by_title[key] = len(existing) - 1
            result.imported += 1
            changed = True

        if changed:
            self.save_blocks(existing)

        return result

    def _read_payload(self) -> dict:
        if not self.path.exists():
            return {
                "version": TEXT_BLOCKS_SCHEMA_VERSION,
                "text_blocks": [],
            }

        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid text blocks JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise ValueError("Text blocks file must contain a JSON object")

        blocks = payload.get("text_blocks")
        if blocks is None:
            payload["text_blocks"] = []
        elif not isinstance(blocks, list):
            raise ValueError("Invalid text blocks payload: text_blocks must be a list")

        return payload

    def _parse_stored_block(self, raw: object) -> Optional[TextBlock]:
        if not isinstance(raw, dict):
            return None

        title = str(raw.get("title", "")).strip()
        content = str(raw.get("content", "")).strip()
        if not title or not content:
            return None

        now = _utc_now()
        block_id = str(raw.get("id", "")).strip() or str(uuid4())
        created_at = str(raw.get("created_at", "")).strip() or now
        updated_at = str(raw.get("updated_at", "")).strip() or created_at
        tags = _normalize_tags(raw.get("tags", []))

        return TextBlock(
            id=block_id,
            title=title,
            content=content,
            tags=tags,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _parse_import_entry(self, raw: object) -> Optional[tuple[str, str, List[str]]]:
        if not isinstance(raw, dict):
            return None

        title = str(raw.get("title", "")).strip()
        content = str(raw.get("content", "")).strip()
        if not title or not content:
            return None

        tags = _normalize_tags(raw.get("tags", []))
        return (title, content, tags)

    def _generate_unique_title(self, base_title: str, existing_titles: Sequence[str]) -> str:
        existing = {title.casefold() for title in existing_titles}
        if base_title.casefold() not in existing:
            return base_title

        suffix = 2
        while True:
            candidate = f"{base_title} ({suffix})"
            if candidate.casefold() not in existing:
                return candidate
            suffix += 1



def _normalize_tags(raw_tags: object) -> List[str]:
    """Normalize tags to trimmed, lowercase, deduplicated values."""
    values: Iterable[object]
    if isinstance(raw_tags, str):
        values = raw_tags.split(",")
    elif isinstance(raw_tags, IterableABC):
        values = raw_tags
    else:
        values = []

    normalized: List[str] = []
    seen = set()
    for value in values:
        tag = str(value).strip().lower()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)

    return normalized



def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_text_blocks_dir() -> Path:
    """Return default OS-specific app config directory for text blocks."""
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))

    primary = base / TEXT_BLOCKS_APP_DIRNAME
    legacy = base / _LEGACY_APP_DIRNAME
    # Use legacy path if it exists and primary doesn't, for backward compat
    if not primary.exists() and legacy.exists():
        return legacy
    return primary
