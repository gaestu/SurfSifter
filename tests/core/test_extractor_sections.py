"""Tests for shared extractor section definitions."""

from __future__ import annotations

from types import SimpleNamespace

from core.extractor_sections import EXTRACTOR_SECTIONS, group_extractors_by_section


def _fake_extractor(name: str):
    return SimpleNamespace(metadata=SimpleNamespace(name=name))


def test_extractor_section_names_are_unique():
    """Section names are used as dictionary keys by the UI grouping helper."""
    section_names = [section["name"] for section in EXTRACTOR_SECTIONS]

    assert len(section_names) == len(set(section_names))


def test_explicit_extractor_mappings_are_unique():
    """An extractor should not be explicitly assigned to multiple sections."""
    extractor_names = [
        extractor_name
        for section in EXTRACTOR_SECTIONS
        if not section.get("auto_populate", False)
        for extractor_name in section["extractors"]
    ]

    duplicates = {
        extractor_name
        for extractor_name in extractor_names
        if extractor_names.count(extractor_name) > 1
    }
    assert duplicates == set()


def test_carvers_grouped_once_with_all_carver_extractors():
    """Regression test for duplicated Carvers UI sections."""
    carvers = [
        _fake_extractor("swiftbeaver"),
        _fake_extractor("bulk_extractor"),
        _fake_extractor("foremost_carver"),
        _fake_extractor("scalpel"),
    ]

    grouped = group_extractors_by_section(carvers)

    assert list(grouped).count("Carvers") == 1
    assert [extractor.metadata.name for extractor in grouped["Carvers"]] == [
        "swiftbeaver",
        "bulk_extractor",
        "foremost_carver",
        "scalpel",
    ]
