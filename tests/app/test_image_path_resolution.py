from __future__ import annotations

from pathlib import Path

from app.data._images import ImageQueryMixin


class StubImageDataAccess(ImageQueryMixin):
    def __init__(self, case_folder: Path, labels: dict[int, str] | None = None) -> None:
        self.case_folder = case_folder
        self.labels = labels or {}

    def _get_evidence_label(self, evidence_id: int) -> str | None:
        return self.labels.get(evidence_id)


def test_resolve_image_path_without_context_returns_contained_file_path(tmp_path: Path) -> None:
    case_folder = tmp_path / "case"
    case_folder.mkdir()
    data_access = StubImageDataAccess(case_folder)

    resolved = data_access.resolve_image_path("images/example.jpg")

    assert resolved == case_folder / "images" / "example.jpg"
    assert resolved != case_folder


def test_resolve_image_path_rejects_escape_without_context(tmp_path: Path) -> None:
    case_folder = tmp_path / "case"
    case_folder.mkdir()
    data_access = StubImageDataAccess(case_folder)

    resolved = data_access.resolve_image_path("../outside.jpg")

    assert resolved is None


def test_resolve_image_path_with_evidence_context_stays_in_evidence_workspace(tmp_path: Path) -> None:
    case_folder = tmp_path / "case"
    evidence_one = case_folder / "evidences" / "ev"
    evidence_two = case_folder / "evidences" / "ev2"
    (evidence_one / "filesystem_images" / "extracted").mkdir(parents=True)
    (evidence_two / "filesystem_images" / "extracted" / "images").mkdir(parents=True)
    cross_evidence_path = "evidences/ev2/filesystem_images/extracted/images/example.jpg"
    (case_folder / cross_evidence_path).write_bytes(b"image")
    data_access = StubImageDataAccess(case_folder, labels={1: "EV"})

    resolved = data_access.resolve_image_path(
        cross_evidence_path,
        evidence_id=1,
        discovered_by="filesystem_images",
    )

    assert resolved is None


def test_resolve_image_path_with_missing_discovery_uses_evidence_workspace(tmp_path: Path) -> None:
    case_folder = tmp_path / "case"
    evidence_root = case_folder / "evidences" / "ev"
    (evidence_root / "images").mkdir(parents=True)
    data_access = StubImageDataAccess(case_folder, labels={1: "EV"})

    resolved = data_access.resolve_image_path(
        "images/example.jpg",
        evidence_id=1,
        discovered_by=None,
    )

    assert resolved == evidence_root / "images" / "example.jpg"