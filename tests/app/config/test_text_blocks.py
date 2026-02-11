"""Unit tests for global text blocks storage."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from app.config import text_blocks as text_blocks_module
from app.config.text_blocks import (
    TEXT_BLOCKS_EXPORT_FORMAT,
    TEXT_BLOCKS_SCHEMA_VERSION,
    TextBlockStore,
    default_text_blocks_dir,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_missing_file_returns_empty(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    assert store.load_blocks() == []


def test_add_update_delete_roundtrip(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)

    created = store.add_block(
        title="Methodology",
        content="Initial content",
        tags=[" Methodology ", "BROWSER", "methodology"],
    )

    loaded = store.load_blocks()
    assert len(loaded) == 1
    assert loaded[0].id == created.id
    assert loaded[0].tags == ["methodology", "browser"]

    updated = store.update_block(
        created.id,
        title="Methodology Updated",
        content="Updated content",
        tags=["findings", "Findings"],
    )
    assert updated is not None

    refreshed = store.get_block(created.id)
    assert refreshed is not None
    assert refreshed.title == "Methodology Updated"
    assert refreshed.content == "Updated content"
    assert refreshed.tags == ["findings"]

    assert store.delete_block(created.id) is True
    assert store.load_blocks() == []


def test_filter_by_text_and_tag(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    store.add_block("Methodology", "Browser extraction steps", ["methodology", "browser"])
    store.add_block("Disclaimer", "Legal and compliance notice", ["disclaimer"])

    blocks = store.load_blocks()

    by_text = TextBlockStore.filter_blocks(blocks, search="compliance")
    assert [block.title for block in by_text] == ["Disclaimer"]

    by_tag = TextBlockStore.filter_blocks(blocks, tag="methodology")
    assert [block.title for block in by_tag] == ["Methodology"]

    assert TextBlockStore.all_tags(blocks) == ["browser", "disclaimer", "methodology"]


def test_export_uses_portable_format(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    block = store.add_block("Methodology", "Reusable section body", ["methodology"])

    export_path = tmp_path / "export.json"
    count = store.export_blocks(export_path, block_ids=[block.id])

    assert count == 1
    payload = json.loads(export_path.read_text(encoding="utf-8"))
    assert payload["format"] == TEXT_BLOCKS_EXPORT_FORMAT
    assert payload["version"] == TEXT_BLOCKS_SCHEMA_VERSION
    assert len(payload["blocks"]) == 1
    assert set(payload["blocks"][0].keys()) == {"title", "content", "tags"}


def test_import_skip_duplicates(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    store.add_block("Methodology", "Original", ["methodology"])

    import_payload = {
        "format": TEXT_BLOCKS_EXPORT_FORMAT,
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "blocks": [
            {"title": "Methodology", "content": "New", "tags": ["new"]},
            {"title": "Disclaimer", "content": "Legal text", "tags": ["disclaimer"]},
        ],
    }
    import_path = _write_json(tmp_path / "import_skip.json", import_payload)

    result = store.import_blocks(import_path, duplicate_strategy="skip")
    assert result.total_processed == 2
    assert result.imported == 1
    assert result.skipped_duplicates == 1

    titles = [block.title for block in store.load_blocks()]
    assert titles == ["Disclaimer", "Methodology"]


def test_import_overwrite_duplicates(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    store.add_block("Methodology", "Original", ["methodology"])

    import_payload = {
        "format": TEXT_BLOCKS_EXPORT_FORMAT,
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "blocks": [
            {"title": "Methodology", "content": "Overwritten", "tags": ["new-tag"]},
        ]
    }
    import_path = _write_json(tmp_path / "import_overwrite.json", import_payload)

    result = store.import_blocks(import_path, duplicate_strategy="overwrite")
    assert result.overwritten == 1
    assert result.imported == 0

    loaded = store.load_blocks()
    assert len(loaded) == 1
    assert loaded[0].content == "Overwritten"
    assert loaded[0].tags == ["new-tag"]


def test_import_rename_duplicates(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    store.add_block("Methodology", "Original", ["methodology"])

    import_payload = {
        "format": TEXT_BLOCKS_EXPORT_FORMAT,
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "blocks": [
            {"title": "Methodology", "content": "Copy", "tags": ["duplicate"]},
        ]
    }
    import_path = _write_json(tmp_path / "import_rename.json", import_payload)

    result = store.import_blocks(import_path, duplicate_strategy="rename")
    assert result.imported == 1
    assert result.renamed == 1

    titles = [block.title for block in store.load_blocks()]
    assert titles == ["Methodology", "Methodology (2)"]


def test_import_skips_invalid_entries(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)

    import_payload = {
        "format": TEXT_BLOCKS_EXPORT_FORMAT,
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "blocks": [
            "not-a-dict",
            {"title": "", "content": "missing title"},
            {"title": "No content", "content": ""},
            {"title": "Valid", "content": "Entry", "tags": ["ok"]},
        ]
    }
    import_path = _write_json(tmp_path / "import_invalid.json", import_payload)

    result = store.import_blocks(import_path, duplicate_strategy="skip")
    assert result.imported == 1
    assert result.skipped_invalid == 3

    loaded = store.load_blocks()
    assert len(loaded) == 1
    assert loaded[0].title == "Valid"


def test_import_rejects_wrong_format(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    import_payload = {
        "format": "wrong-format",
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "blocks": [{"title": "T", "content": "C", "tags": []}],
    }
    import_path = _write_json(tmp_path / "bad_format.json", import_payload)
    with pytest.raises(ValueError, match="Invalid import format"):
        store.import_blocks(import_path, duplicate_strategy="skip")


def test_import_rejects_unsupported_version(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    import_payload = {
        "format": TEXT_BLOCKS_EXPORT_FORMAT,
        "version": 999,
        "blocks": [{"title": "T", "content": "C", "tags": []}],
    }
    import_path = _write_json(tmp_path / "bad_version.json", import_payload)
    with pytest.raises(ValueError, match="Unsupported import version"):
        store.import_blocks(import_path, duplicate_strategy="skip")


def test_import_accepts_storage_format_payload(tmp_path: Path) -> None:
    store = TextBlockStore(tmp_path)
    import_payload = {
        "version": TEXT_BLOCKS_SCHEMA_VERSION,
        "text_blocks": [{"title": "Stored", "content": "Entry", "tags": []}],
    }
    import_path = _write_json(tmp_path / "storage_payload.json", import_payload)
    result = store.import_blocks(import_path, duplicate_strategy="skip")
    assert result.imported == 1


def test_default_text_blocks_dir_linux(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(text_blocks_module.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CONFIG_HOME", "/tmp/xdg-config")
    assert default_text_blocks_dir() == Path("/tmp/xdg-config/surfsifter")


def test_default_text_blocks_dir_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(text_blocks_module.sys, "platform", "win32")
    monkeypatch.setenv("APPDATA", "/tmp/appdata")
    assert default_text_blocks_dir() == Path("/tmp/appdata/surfsifter")
