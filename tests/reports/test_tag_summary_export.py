"""
Tests for the standalone XLSX writer used by Reports → Export Tag Summary.
"""
from __future__ import annotations

import base64
import zipfile
from xml.etree import ElementTree as ET

from reports.tag_summary_export import (
    CHECKBOX_GLYPH,
    TagSummaryExportData,
    write_tag_summary_xlsx,
)


XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
JPEG_1X1 = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////"
    "////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDASIAAhEBAxEB/"
    "8QAFQABAQAAAAAAAAAAAAAAAAAAAAX/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAH/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAEFAqf/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/ASP/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/ASP/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAY/Aqf/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAE/IV//2gAMAwEAAgADAAAAEP/EFBQRAQAAAAAAAAAAAAAAAAAAABD/2gAIAQMBAT8QH//EFBQRAQAAAAAAAAAAAAAAAAAAABD/2gAIAQIBAT8QH//EFBABAQAAAAAAAAAAAAAAAAAAABD/2gAIAQEAAT8QH//Z"
)


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


def _read_sheet_cell_map(xlsx_path) -> dict[str, str]:
    """Return sheet1 inline string cells keyed by Excel cell reference."""
    with zipfile.ZipFile(xlsx_path) as zf:
        sheet_xml = zf.read("xl/worksheets/sheet1.xml")
    root = ET.fromstring(sheet_xml)
    out: dict[str, str] = {}
    for cell in root.findall(".//s:sheetData/s:row/s:c", XLSX_NS):
        ref = cell.attrib["r"]
        text = cell.findtext("s:is/s:t", default="", namespaces=XLSX_NS)
        out[ref] = text
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


def _write_xlsx(data: TagSummaryExportData, out) -> None:
    write_tag_summary_xlsx(data, out, containment_root=out.parent)


def test_writer_produces_valid_zip_with_required_parts(tmp_path):
    out = tmp_path / "tag_summary.xlsx"
    _write_xlsx(_make_data(), out)
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
    _write_xlsx(_make_data(), out)
    cells = _read_sheet_text_cells(out)
    assert "Tag Summary — EV-001" in cells
    assert "Exported: 2025-01-01T12:00:00+00:00" in cells


def test_no_tags_message(tmp_path):
    out = tmp_path / "n.xlsx"
    _write_xlsx(_make_data(), out)
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
    _write_xlsx(data, out)
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
    _write_xlsx(data, out)
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
    _write_xlsx(data, out)
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
    _write_xlsx(data, out)
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
    _write_xlsx(data, a)
    _write_xlsx(data, b)
    assert a.read_bytes() == b.read_bytes()


def test_writer_embeds_tag_and_reference_thumbnails(tmp_path):
    data = _make_data(
        tags=[
            {
                "tag_name": "Pictures",
                "sections": [
                    {
                        "label": "Images",
                        "headers": ("Filename", "Path", "Timestamp"),
                        "rows": [["a.jpg", "img/a.jpg", "2025-01-01T00:00:00Z"]],
                        "thumbnail_bytes": [JPEG_1X1],
                        "total": 1,
                    }
                ],
            }
        ],
        reference_list_matches=[
            {
                "list_name": "known",
                "kind": "image",
                "headers": ("Filename", "Path", "MD5"),
                "rows": [["b.jpg", "img/b.jpg", "abc"]],
                "thumbnail_bytes": [JPEG_1X1],
                "total": 1,
            }
        ],
    )
    out = tmp_path / "thumbs.xlsx"
    _write_xlsx(data, out)

    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
        assert "xl/worksheets/_rels/sheet1.xml.rels" in names
        assert "xl/drawings/drawing1.xml" in names
        assert "xl/drawings/_rels/drawing1.xml.rels" in names
        assert "xl/media/image1.jpg" in names
        assert "xl/media/image2.jpg" in names
        assert zf.read("xl/media/image1.jpg") == JPEG_1X1
        ET.fromstring(zf.read("xl/drawings/drawing1.xml"))

    cells = _read_sheet_text_cells(out)
    assert cells.count("Thumbnail") == 2


def test_writer_handles_mixed_thumbnail_rows(tmp_path):
    data = _make_data(
        tags=[
            {
                "tag_name": "Pictures",
                "sections": [
                    {
                        "label": "Images",
                        "headers": ("Filename",),
                        "rows": [["a.jpg"], ["missing.jpg"], ["c.jpg"]],
                        "thumbnail_bytes": [JPEG_1X1, None, JPEG_1X1],
                        "total": 5,
                    }
                ],
            }
        ]
    )
    out = tmp_path / "mixed-thumbs.xlsx"
    _write_xlsx(data, out)

    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
        assert "xl/media/image1.jpg" in names
        assert "xl/media/image2.jpg" in names
        assert "xl/media/image3.jpg" not in names

    cells = _read_sheet_cell_map(out)
    assert cells["B6"] == "Thumbnail"
    assert cells["C6"] == "Filename"
    assert cells["C10"] == "…and 2 more"


def test_writer_is_deterministic_with_thumbnails(tmp_path):
    data = _make_data(
        tags=[
            {
                "tag_name": "Pictures",
                "sections": [
                    {
                        "label": "Images",
                        "headers": ("Filename",),
                        "rows": [["a.jpg"]],
                        "thumbnail_bytes": [JPEG_1X1],
                        "total": 1,
                    }
                ],
            }
        ]
    )
    a = tmp_path / "thumb-a.xlsx"
    b = tmp_path / "thumb-b.xlsx"
    _write_xlsx(data, a)
    _write_xlsx(data, b)
    assert a.read_bytes() == b.read_bytes()
