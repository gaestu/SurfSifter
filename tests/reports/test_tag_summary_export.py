"""
Tests for the standalone XLSX writer used by Reports → Export Tag Summary.
"""
from __future__ import annotations

import zipfile
from xml.etree import ElementTree as ET

from reports.tag_summary_export import (
    CHECKBOX_GLYPH,
    TagSummaryExportData,
    write_tag_summary_xlsx,
)


XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _read_sheet_text_cells(xlsx_path) -> list[str]:
    """Return the inline string contents of every cell in sheet1, in order."""
    with zipfile.ZipFile(xlsx_path) as zf:
        sheet_xml = zf.read("xl/worksheets/sheet1.xml")
    root = ET.fromstring(sheet_xml)
    out: list[str] = []
    for row in root.findall(".//s:sheetData/s:row", XLSX_NS):
        for cell in row.findall("s:c", XLSX_NS):
            text = cell.findtext("s:is/s:t", default="", namespaces=XLSX_NS)
            out.append(text)
    return out


def _make_data(**overrides) -> TagSummaryExportData:
    base = dict(
        evidence_label="EV-001",
        exported_at_iso="2025-01-01T12:00:00+00:00",
        top_n=3,
        tags=[],
        reference_list_matches=[],
    )
    base.update(overrides)
    return TagSummaryExportData(**base)


def test_writer_produces_valid_zip_with_required_parts(tmp_path):
    out = tmp_path / "tag_summary.xlsx"
    write_tag_summary_xlsx(_make_data(), out)
    assert out.exists()
    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
    assert {
        "[Content_Types].xml",
        "_rels/.rels",
        "xl/workbook.xml",
        "xl/_rels/workbook.xml.rels",
        "xl/styles.xml",
        "xl/worksheets/sheet1.xml",
    } <= names


def test_header_rows_present(tmp_path):
    out = tmp_path / "h.xlsx"
    write_tag_summary_xlsx(_make_data(), out)
    cells = _read_sheet_text_cells(out)
    assert "Tag Summary — EV-001" in cells
    assert "Exported: 2025-01-01T12:00:00+00:00" in cells


def test_no_tags_message(tmp_path):
    out = tmp_path / "n.xlsx"
    write_tag_summary_xlsx(_make_data(), out)
    cells = _read_sheet_text_cells(out)
    assert "No tagged artifacts." in cells
    assert "Reference-list matches" in cells
    assert "No reference-list matches." in cells


def test_tag_section_renders_rows_and_truncation(tmp_path):
    data = _make_data(
        tags=[
            {
                "tag_name": "Suspicious",
                "sections": [
                    {
                        "label": "URLs",
                        "headers": ("URL", "Visit timestamp", "Visit count", "Source"),
                        "rows": [
                            ["https://a", "2024-01-03T00:00:00Z", 5, "history"],
                            ["https://b", "2024-01-02T00:00:00Z", 1, "history"],
                            ["https://c", "2024-01-01T00:00:00Z", 1, "cache"],
                        ],
                        "total": 7,
                    }
                ],
            }
        ]
    )
    out = tmp_path / "t.xlsx"
    write_tag_summary_xlsx(data, out)
    cells = _read_sheet_text_cells(out)

    assert "Tag: Suspicious" in cells
    assert "URLs" in cells
    assert "https://a" in cells and "https://b" in cells and "https://c" in cells
    # Checkbox now sits on the group heading row, not on each entry row,
    # so a single-section tag yields exactly one checkbox glyph.
    assert cells.count(CHECKBOX_GLYPH) == 1
    # Truncation row.
    assert "…and 4 more" in cells


def test_reference_list_section_rendered(tmp_path):
    data = _make_data(
        reference_list_matches=[
            {
                "list_name": "blocklist",
                "kind": "url",
                "headers": ("URL", "Visit timestamp", "Source"),
                "rows": [
                    ["https://bad", "2024-01-01T00:00:00Z", "history"],
                ],
                "total": 1,
            }
        ]
    )
    out = tmp_path / "r.xlsx"
    write_tag_summary_xlsx(data, out)
    cells = _read_sheet_text_cells(out)
    assert "Reference-list matches" in cells
    assert "blocklist (url)" in cells
    assert "https://bad" in cells


def test_special_chars_are_escaped(tmp_path):
    data = _make_data(
        tags=[
            {
                "tag_name": "T<&>",
                "sections": [
                    {
                        "label": "URLs",
                        "headers": ("URL",),
                        "rows": [["https://x?a=1&b=2"]],
                        "total": 1,
                    }
                ],
            }
        ]
    )
    out = tmp_path / "x.xlsx"
    write_tag_summary_xlsx(data, out)
    # Should round-trip through the XML parser without raising and yield
    # the original text values.
    cells = _read_sheet_text_cells(out)
    assert "Tag: T<&>" in cells
    assert "https://x?a=1&b=2" in cells


def test_xml_invalid_control_chars_are_stripped(tmp_path):
    """Raw bytes (0x00-0x08 etc.) recovered from carved data must not
    corrupt the workbook XML."""
    nasty = "before\x00\x01\x07\x0b\x0c\x1fafter"
    data = _make_data(
        tags=[
            {
                "tag_name": f"Tag\x00X",
                "sections": [
                    {
                        "label": "URLs",
                        "headers": ("URL",),
                        "rows": [[nasty]],
                        "total": 1,
                    }
                ],
            }
        ]
    )
    out = tmp_path / "ctrl.xlsx"
    write_tag_summary_xlsx(data, out)
    # Must parse without raising; cells must contain the cleaned values.
    cells = _read_sheet_text_cells(out)
    assert "Tag: TagX" in cells
    assert "beforeafter" in cells


def test_writer_is_deterministic_for_identical_input(tmp_path):
    """Two writes with identical inputs produce byte-identical bytes."""
    data = _make_data(
        tags=[
            {
                "tag_name": "Det",
                "sections": [
                    {
                        "label": "URLs",
                        "headers": ("URL",),
                        "rows": [["https://a"], ["https://b"]],
                        "total": 2,
                    }
                ],
            }
        ]
    )
    a = tmp_path / "a.xlsx"
    b = tmp_path / "b.xlsx"
    write_tag_summary_xlsx(data, a)
    write_tag_summary_xlsx(data, b)
    assert a.read_bytes() == b.read_bytes()
