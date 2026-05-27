"""
Single-sheet XLSX writer for the "Export Tag Summary" Reports button.

Produces a printable working document grouped by tag → artifact type, plus
a final reference-list matches section.  Each entry row carries an empty
☐ checkbox character in the first column so an investigator can tick the
items that should end up in the final PDF report after prosecutor review.

The writer uses only the Python standard library (``zipfile`` + minimal
hand-written XML) so SurfSifter does not need to take a new dependency.
The XLSX produced is openable in Excel, LibreOffice Calc, and Numbers and
is intended for printing on A4.
"""
from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

from core.safe_io import write_bytes_no_follow

__all__ = [
    "TagSummaryExportData",
    "write_tag_summary_xlsx",
]


# Codepoints that are *illegal* in XML 1.0 even when escaped.  Embedding
# any of these in the workbook produces a file that Excel/LibreOffice will
# refuse to open.  Strip them defensively so hostile evidence values can
# never corrupt the workbook.
_XML_INVALID_RE = re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F\uFFFE\uFFFF]"
)

# Fixed timestamp embedded in every ZIP local file header so two runs
# with identical inputs produce byte-identical workbook bytes.  Per the
# ZIP spec the minimum legal MS-DOS date is 1980-01-01.
_DETERMINISTIC_MTIME = (1980, 1, 1, 0, 0, 0)


CHECKBOX_GLYPH = "\u2610"  # ☐
_THUMB_ROW_HEIGHT = 78
_THUMB_EMU = 914400  # 96 px at 96 DPI.


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------

@dataclass
class TagSummaryExportData:
    """Pre-computed export payload passed to :func:`write_tag_summary_xlsx`."""

    evidence_label: str
    exported_at_iso: str
    top_n: int
    # Each tag: {"tag_name": str, "sections": [
    #   {"label": str, "headers": tuple[str, ...],
    #    "rows": list[list[Any]], "total": int}, ...]}
    tags: List[dict]
    # Each ref-list bucket: {"list_name": str, "kind": str,
    #   "headers": tuple[str, ...], "rows": list[list[Any]], "total": int}
    reference_list_matches: List[dict]


@dataclass(frozen=True)
class _EmbeddedImage:
    row: int
    col: int
    image_bytes: bytes


# ---------------------------------------------------------------------------
# XLSX low-level helpers
# ---------------------------------------------------------------------------

def _xml_escape(value: Any) -> str:
    """Escape a value for inclusion in XLSX inline-string text content.

    Also strips XML-invalid control characters that would otherwise
    produce a corrupt workbook when present in evidence-derived strings
    (e.g. raw bytes recovered from carved data).
    """
    if value is None:
        return ""
    s = _XML_INVALID_RE.sub("", str(value))
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _col_letter(index: int) -> str:
    """1-based column index → Excel column letter (1 → 'A')."""
    if index < 1:
        raise ValueError("column index must be >= 1")
    out = ""
    n = index
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


# ---------------------------------------------------------------------------
# Cell + row builders
# ---------------------------------------------------------------------------

# Style indices (must match _STYLES_XML below).
_STYLE_DEFAULT = 0
_STYLE_TITLE = 1
_STYLE_SUBTITLE = 2
_STYLE_TAG = 3
_STYLE_TYPE = 4
_STYLE_HEADER = 5
_STYLE_CHECKBOX = 6
_STYLE_MORE = 7


def _cell(col_letter: str, row: int, text: Any, style: int) -> str:
    return (
        f'<c r="{col_letter}{row}" s="{style}" t="inlineStr">'
        f"<is><t xml:space=\"preserve\">{_xml_escape(text)}</t></is>"
        f"</c>"
    )


def _row_xml(
    row_num: int,
    cells: Iterable[str],
    *,
    height: int | None = None,
) -> str:
    attrs = f' r="{row_num}"'
    if height is not None:
        attrs += f' ht="{height}" customHeight="1"'
    return f"<row{attrs}>" + "".join(cells) + "</row>"


def _has_thumbnail_bytes(values: Sequence[Any]) -> bool:
    return any(isinstance(v, (bytes, bytearray)) and len(v) > 0 for v in values)


# ---------------------------------------------------------------------------
# Sheet builder
# ---------------------------------------------------------------------------

def _build_sheet_xml(data: TagSummaryExportData) -> tuple[str, List[_EmbeddedImage]]:
    rows_xml: List[str] = []
    embedded_images: List[_EmbeddedImage] = []
    row = 1
    max_cols = 1

    def emit(cells: Sequence[str], *, height: int | None = None) -> None:
        nonlocal row
        rows_xml.append(_row_xml(row, cells, height=height))
        row += 1

    # --- Header rows ---
    emit([_cell("A", row, f"Tag Summary — {data.evidence_label}", _STYLE_TITLE)])
    emit([_cell("A", row, f"Exported: {data.exported_at_iso}", _STYLE_SUBTITLE)])
    emit([_cell("A", row, "", _STYLE_DEFAULT)])  # blank spacer

    # --- Tag sections ---
    #
    # Layout per group:
    #   row N+0: [   ] [Tag: <name>]                  (tag heading)
    #   row N+1: [ ☐ ] [Browser search terms]         (group heading w/ checkbox)
    #   row N+2: [   ] [Term] [Timestamp] ...         (column headers)
    #   row N+3: [   ] [value] [value]   ...          (entries — no checkbox)
    #   ...
    #   row N+k: [   ] […and N more]
    if not data.tags:
        emit([_cell("A", row, "", _STYLE_DEFAULT),
              _cell("B", row, "No tagged artifacts.", _STYLE_SUBTITLE)])
        emit([_cell("A", row, "", _STYLE_DEFAULT)])
    else:
        for tag in data.tags:
            emit([
                _cell("A", row, "", _STYLE_DEFAULT),
                _cell("B", row, f"Tag: {tag['tag_name']}", _STYLE_TAG),
            ])
            for section in tag["sections"]:
                headers = section["headers"]
                thumbnails = section.get("thumbnail_bytes") or []
                has_thumbnails = _has_thumbnail_bytes(thumbnails)
                # +1 for checkbox; +1 for thumbnail when available.
                cols_total = len(headers) + 1 + (1 if has_thumbnails else 0)
                max_cols = max(max_cols, cols_total)

                # Group heading row carries the tickable checkbox.
                emit([
                    _cell("A", row, CHECKBOX_GLYPH, _STYLE_CHECKBOX),
                    _cell("B", row, section["label"], _STYLE_TYPE),
                ])

                # Unsupported section: render the gap note rather than
                # an empty header strip + a misleading "…and N more".
                note = section.get("unsupported_note")
                if note:
                    emit([
                        _cell("A", row, "", _STYLE_DEFAULT),
                        _cell("B", row, note, _STYLE_MORE),
                    ])
                    emit([_cell("A", row, "", _STYLE_DEFAULT)])
                    continue

                # Column header row: blank under the checkbox column,
                # then the section column headers.
                header_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
                first_data_col = 2
                if has_thumbnails:
                    header_cells.append(_cell("B", row, "Thumbnail", _STYLE_HEADER))
                    first_data_col = 3
                for idx, h in enumerate(headers, start=first_data_col):
                    header_cells.append(_cell(_col_letter(idx), row, h, _STYLE_HEADER))
                emit(header_cells)

                for body_idx, body_row in enumerate(section["rows"]):
                    body_row_num = row
                    body_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
                    if has_thumbnails:
                        body_cells.append(_cell("B", row, "", _STYLE_DEFAULT))
                    for idx, value in enumerate(body_row, start=first_data_col):
                        body_cells.append(
                            _cell(_col_letter(idx), row, value, _STYLE_DEFAULT)
                        )
                    emit(
                        body_cells,
                        height=_THUMB_ROW_HEIGHT if has_thumbnails else None,
                    )
                    if has_thumbnails and body_idx < len(thumbnails):
                        thumbnail = thumbnails[body_idx]
                        if isinstance(thumbnail, (bytes, bytearray)) and thumbnail:
                            embedded_images.append(
                                _EmbeddedImage(
                                    row=body_row_num,
                                    col=2,
                                    image_bytes=bytes(thumbnail),
                                )
                            )

                shown = len(section["rows"])
                if section["total"] > shown:
                    extra = section["total"] - shown
                    emit([
                        _cell("A", row, "", _STYLE_DEFAULT),
                        _cell(
                            _col_letter(first_data_col),
                            row,
                            f"…and {extra} more",
                            _STYLE_MORE,
                        ),
                    ])
                emit([_cell("A", row, "", _STYLE_DEFAULT)])  # spacer between sections
            emit([_cell("A", row, "", _STYLE_DEFAULT)])  # spacer after tag

    # --- Reference-list matches ---
    emit([
        _cell("A", row, "", _STYLE_DEFAULT),
        _cell("B", row, "Reference-list matches", _STYLE_TAG),
    ])
    if not data.reference_list_matches:
        emit([
            _cell("A", row, "", _STYLE_DEFAULT),
            _cell("B", row, "No reference-list matches.", _STYLE_SUBTITLE),
        ])
    else:
        for bucket in data.reference_list_matches:
            headers = bucket["headers"]
            thumbnails = bucket.get("thumbnail_bytes") or []
            has_thumbnails = _has_thumbnail_bytes(thumbnails)
            cols_total = len(headers) + 1 + (1 if has_thumbnails else 0)
            max_cols = max(max_cols, cols_total)

            label = f"{bucket['list_name']} ({bucket['kind']})"
            # Group heading row carries the tickable checkbox.
            emit([
                _cell("A", row, CHECKBOX_GLYPH, _STYLE_CHECKBOX),
                _cell("B", row, label, _STYLE_TYPE),
            ])

            header_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
            first_data_col = 2
            if has_thumbnails:
                header_cells.append(_cell("B", row, "Thumbnail", _STYLE_HEADER))
                first_data_col = 3
            for idx, h in enumerate(headers, start=first_data_col):
                header_cells.append(_cell(_col_letter(idx), row, h, _STYLE_HEADER))
            emit(header_cells)

            for body_idx, body_row in enumerate(bucket["rows"]):
                body_row_num = row
                body_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
                if has_thumbnails:
                    body_cells.append(_cell("B", row, "", _STYLE_DEFAULT))
                for idx, value in enumerate(body_row, start=first_data_col):
                    body_cells.append(
                        _cell(_col_letter(idx), row, value, _STYLE_DEFAULT)
                    )
                emit(
                    body_cells,
                    height=_THUMB_ROW_HEIGHT if has_thumbnails else None,
                )
                if has_thumbnails and body_idx < len(thumbnails):
                    thumbnail = thumbnails[body_idx]
                    if isinstance(thumbnail, (bytes, bytearray)) and thumbnail:
                        embedded_images.append(
                            _EmbeddedImage(
                                row=body_row_num,
                                col=2,
                                image_bytes=bytes(thumbnail),
                            )
                        )

            shown = len(bucket["rows"])
            if bucket["total"] > shown:
                extra = bucket["total"] - shown
                emit([
                    _cell("A", row, "", _STYLE_DEFAULT),
                    _cell(
                        _col_letter(first_data_col),
                        row,
                        f"…and {extra} more",
                        _STYLE_MORE,
                    ),
                ])
            emit([_cell("A", row, "", _STYLE_DEFAULT)])  # spacer

    # Column widths: narrow checkbox column, wider data columns.
    if embedded_images:
        cols_block = (
            '<cols>'
            '<col min="1" max="1" width="6" customWidth="1"/>'
            '<col min="2" max="2" width="16" customWidth="1"/>'
            f'<col min="3" max="{max(3, max_cols)}" width="36" customWidth="1"/>'
            '</cols>'
        )
    else:
        cols_block = (
            '<cols>'
            '<col min="1" max="1" width="6" customWidth="1"/>'
            f'<col min="2" max="{max(2, max_cols)}" width="36" customWidth="1"/>'
            '</cols>'
        )

    worksheet_attrs = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    drawing_ref = ""
    if embedded_images:
        worksheet_attrs += ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"'
        drawing_ref = '<drawing r:id="rId1"/>'

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<worksheet {worksheet_attrs}>'
        '<sheetPr><pageSetUpPr fitToPage="1"/></sheetPr>'
        f'{cols_block}'
        "<sheetData>" + "".join(rows_xml) + "</sheetData>"
        '<printOptions horizontalCentered="1"/>'
        '<pageMargins left="0.5" right="0.5" top="0.6" bottom="0.6" '
        'header="0.3" footer="0.3"/>'
        '<pageSetup paperSize="9" orientation="portrait" '
        'fitToHeight="0" fitToWidth="1"/>'
        f'{drawing_ref}'
        "</worksheet>"
    )
    return sheet_xml, embedded_images


# ---------------------------------------------------------------------------
# Static workbook parts
# ---------------------------------------------------------------------------

def _content_types_xml(has_images: bool) -> str:
    image_default = (
        '<Default Extension="jpg" ContentType="image/jpeg"/>'
        if has_images
        else ""
    )
    drawing_override = (
        '<Override PartName="/xl/drawings/drawing1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.drawing+xml"/>'
        if has_images
        else ""
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        f'{image_default}'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        f'{drawing_override}'
        '</Types>'
    )

_ROOT_RELS_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
    'Target="xl/workbook.xml"/>'
    '</Relationships>'
)

_WORKBOOK_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
    'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
    '<sheets>'
    '<sheet name="Tag Summary" sheetId="1" r:id="rId1"/>'
    '</sheets>'
    '</workbook>'
)

_WORKBOOK_RELS_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
    'Target="worksheets/sheet1.xml"/>'
    '<Relationship Id="rId2" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
    'Target="styles.xml"/>'
    '</Relationships>'
)

_SHEET_RELS_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing" '
    'Target="../drawings/drawing1.xml"/>'
    '</Relationships>'
)


def _drawing_xml(images: Sequence[_EmbeddedImage]) -> str:
    anchors: List[str] = []
    for idx, image in enumerate(images, start=1):
        col = image.col - 1
        row = image.row - 1
        # OOXML drawing anchors are zero-based; worksheet cells above are one-based.
        anchors.append(
            '<xdr:oneCellAnchor>'
            '<xdr:from>'
            f'<xdr:col>{col}</xdr:col><xdr:colOff>95250</xdr:colOff>'
            f'<xdr:row>{row}</xdr:row><xdr:rowOff>95250</xdr:rowOff>'
            '</xdr:from>'
            f'<xdr:ext cx="{_THUMB_EMU}" cy="{_THUMB_EMU}"/>'
            '<xdr:pic>'
            '<xdr:nvPicPr>'
            f'<xdr:cNvPr id="{idx}" name="thumbnail {idx}"/>'
            '<xdr:cNvPicPr><a:picLocks noChangeAspect="1"/></xdr:cNvPicPr>'
            '</xdr:nvPicPr>'
            '<xdr:blipFill>'
            f'<a:blip r:embed="rId{idx}"/>'
            '<a:stretch><a:fillRect/></a:stretch>'
            '</xdr:blipFill>'
            '<xdr:spPr><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></xdr:spPr>'
            '</xdr:pic>'
            '<xdr:clientData/>'
            '</xdr:oneCellAnchor>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<xdr:wsDr '
        'xmlns:xdr="http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing" '
        'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        + "".join(anchors)
        + '</xdr:wsDr>'
    )


def _drawing_rels_xml(images: Sequence[_EmbeddedImage]) -> str:
    rels = []
    for idx, _image in enumerate(images, start=1):
        rels.append(
            f'<Relationship Id="rId{idx}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
            f'Target="../media/image{idx}.jpg"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(rels)
        + '</Relationships>'
    )

# Style table: indices must line up with _STYLE_* constants above.
_STYLES_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    '<fonts count="6">'
    '<font><sz val="11"/><name val="Calibri"/></font>'                       # 0 default
    '<font><sz val="18"/><b/><name val="Calibri"/></font>'                   # 1 title
    '<font><sz val="10"/><i/><color rgb="FF555555"/><name val="Calibri"/></font>'  # 2 subtitle
    '<font><sz val="16"/><b/><name val="Calibri"/></font>'                   # 3 tag heading
    '<font><sz val="14"/><b/><color rgb="FF333333"/><name val="Calibri"/></font>'  # 4 type heading
    '<font><sz val="18"/><name val="Calibri"/></font>'                       # 5 checkbox glyph
    '</fonts>'
    '<fills count="3">'
    '<fill><patternFill patternType="none"/></fill>'
    '<fill><patternFill patternType="gray125"/></fill>'
    '<fill><patternFill patternType="solid"><fgColor rgb="FFEFEFEF"/>'
    '<bgColor indexed="64"/></patternFill></fill>'                           # 2 header band
    '</fills>'
    '<borders count="2">'
    '<border><left/><right/><top/><bottom/><diagonal/></border>'
    '<border><left/><right/><top style="thin"><color rgb="FFAAAAAA"/></top>'
    '<bottom/><diagonal/></border>'
    '</borders>'
    '<cellStyleXfs count="1">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>'
    '</cellStyleXfs>'
    '<cellXfs count="8">'
    # 0 default
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
    '<alignment vertical="top" wrapText="1"/></xf>'
    # 1 title
    '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1">'
    '<alignment vertical="center"/></xf>'
    # 2 subtitle
    '<xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
    # 3 tag
    '<xf numFmtId="0" fontId="3" fillId="2" borderId="1" xfId="0" '
    'applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1">'
    '<alignment vertical="center"/></xf>'
    # 4 type
    '<xf numFmtId="0" fontId="4" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1">'
    '<alignment vertical="center"/></xf>'
    # 5 header (column header row under each section)
    '<xf numFmtId="0" fontId="4" fillId="2" borderId="0" xfId="0" '
    'applyFont="1" applyFill="1" applyAlignment="1">'
    '<alignment vertical="center"/></xf>'
    # 6 checkbox glyph
    '<xf numFmtId="0" fontId="5" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1">'
    '<alignment horizontal="center" vertical="center"/></xf>'
    # 7 "and N more"
    '<xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1">'
    '<alignment vertical="top"/></xf>'
    '</cellXfs>'
    '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
    '</styleSheet>'
)


# ---------------------------------------------------------------------------
# Public writer
# ---------------------------------------------------------------------------

def write_tag_summary_xlsx(
    data: TagSummaryExportData,
    output_path: Path,
    *,
    containment_root: Path,
) -> Path:
    """
    Write the tag-summary working document to ``output_path``.

    Returns the resolved output path.
    """
    output_path = Path(output_path)

    sheet_xml, embedded_images = _build_sheet_xml(data)

    parts = [
        ("[Content_Types].xml", _content_types_xml(bool(embedded_images))),
        ("_rels/.rels", _ROOT_RELS_XML),
        ("xl/workbook.xml", _WORKBOOK_XML),
        ("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS_XML),
        ("xl/styles.xml", _STYLES_XML),
        ("xl/worksheets/sheet1.xml", sheet_xml),
    ]
    if embedded_images:
        parts.extend(
            [
                ("xl/worksheets/_rels/sheet1.xml.rels", _SHEET_RELS_XML),
                ("xl/drawings/drawing1.xml", _drawing_xml(embedded_images)),
                (
                    "xl/drawings/_rels/drawing1.xml.rels",
                    _drawing_rels_xml(embedded_images),
                ),
            ]
        )
        for idx, image in enumerate(embedded_images, start=1):
            parts.append((f"xl/media/image{idx}.jpg", image.image_bytes))

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in parts:
            # Pin date_time so ZIP local-file headers do not embed wall
            # clock state — required for deterministic workbook bytes.
            info = zipfile.ZipInfo(filename=name, date_time=_DETERMINISTIC_MTIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, payload)

    write_bytes_no_follow(
        output_path,
        buf.getvalue(),
        containment_root=containment_root,
        exclusive=True,
    )
    return output_path
