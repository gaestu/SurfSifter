"""Tests for case-workspace image artifact path resolution."""
from __future__ import annotations

from pathlib import Path

from core.image_paths import resolve_case_image_path


def test_resolves_image_under_selected_evidence(tmp_path: Path) -> None:
    image_path = (
        tmp_path
        / "evidences"
        / "evidence_1"
        / "filesystem_images"
        / "extracted"
        / "img"
        / "one.jpg"
    )
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")

    resolved = resolve_case_image_path(
        rel_path="img/one.jpg",
        discovered_by="filesystem_images",
        case_folder=tmp_path,
        evidence_id=1,
        evidence_label=None,
    )

    assert resolved == image_path.resolve()


def test_resolves_safari_cache_images_under_source_subdir(tmp_path: Path) -> None:
    image_path = (
        tmp_path
        / "evidences"
        / "evidence_1"
        / "safari_cache"
        / "run_1"
        / "carved_images"
        / "one.jpg"
    )
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")

    resolved = resolve_case_image_path(
        rel_path="run_1/carved_images/one.jpg",
        discovered_by="safari_cache",
        case_folder=tmp_path,
        evidence_id=1,
        evidence_label=None,
    )

    assert resolved == image_path.resolve()


def test_resolves_dynamic_cache_source_prefix(tmp_path: Path) -> None:
    image_path = (
        tmp_path
        / "evidences"
        / "evidence_1"
        / "cache_simple"
        / "run_1"
        / "carved_images"
        / "one.jpg"
    )
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")

    resolved = resolve_case_image_path(
        rel_path="run_1/carved_images/one.jpg",
        discovered_by="cache_blockfile:1.2.3:run_1",
        case_folder=tmp_path,
        evidence_id=1,
        evidence_label=None,
    )

    assert resolved == image_path.resolve()


def test_resolves_media_history_album_art_under_source_subdir(tmp_path: Path) -> None:
    image_path = (
        tmp_path
        / "evidences"
        / "evidence_1"
        / "media_history"
        / "album_art"
        / "one.jpg"
    )
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")

    resolved = resolve_case_image_path(
        rel_path="album_art/one.jpg",
        discovered_by="media_history",
        case_folder=tmp_path,
        evidence_id=1,
        evidence_label=None,
    )

    assert resolved == image_path.resolve()


def test_rejects_symlink_to_other_evidence(tmp_path: Path) -> None:
    other_image = (
        tmp_path
        / "evidences"
        / "evidence_2"
        / "filesystem_images"
        / "extracted"
        / "img"
        / "two.jpg"
    )
    other_image.parent.mkdir(parents=True)
    other_image.write_bytes(b"image")

    source_parent = tmp_path / "evidences" / "evidence_1" / "filesystem_images"
    source_parent.mkdir(parents=True)
    (source_parent / "extracted").symlink_to(other_image.parent, target_is_directory=True)

    resolved = resolve_case_image_path(
        rel_path="img/two.jpg",
        discovered_by="filesystem_images",
        case_folder=tmp_path,
        evidence_id=1,
        evidence_label=None,
    )

    assert resolved is None


def test_rejects_absolute_and_traversal_paths(tmp_path: Path) -> None:
    for rel_path in ("../other.jpg", str((tmp_path / "outside.jpg").resolve())):
        assert resolve_case_image_path(
            rel_path=rel_path,
            discovered_by="filesystem_images",
            case_folder=tmp_path,
            evidence_id=1,
            evidence_label=None,
        ) is None