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
from typing import Any, Iterable, List, Sequence

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


def _row_xml(row_num: int, cells: Iterable[str]) -> str:
    return f'<row r="{row_num}">' + "".join(cells) + "</row>"


# ---------------------------------------------------------------------------
# Sheet builder
# ---------------------------------------------------------------------------

def _build_sheet_xml(data: TagSummaryExportData) -> str:
    rows_xml: List[str] = []
    row = 1
    max_cols = 1

    def emit(cells: Sequence[str]) -> None:
        nonlocal row
        rows_xml.append(_row_xml(row, cells))
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
                cols_total = len(headers) + 1  # +1 for the checkbox column
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
                for idx, h in enumerate(headers, start=2):
                    header_cells.append(_cell(_col_letter(idx), row, h, _STYLE_HEADER))
                emit(header_cells)

                for body_row in section["rows"]:
                    body_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
                    for idx, value in enumerate(body_row, start=2):
                        body_cells.append(
                            _cell(_col_letter(idx), row, value, _STYLE_DEFAULT)
                        )
                    emit(body_cells)

                shown = len(section["rows"])
                if section["total"] > shown:
                    extra = section["total"] - shown
                    emit([
                        _cell("A", row, "", _STYLE_DEFAULT),
                        _cell("B", row, f"…and {extra} more", _STYLE_MORE),
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
            cols_total = len(headers) + 1
            max_cols = max(max_cols, cols_total)

            label = f"{bucket['list_name']} ({bucket['kind']})"
            # Group heading row carries the tickable checkbox.
            emit([
                _cell("A", row, CHECKBOX_GLYPH, _STYLE_CHECKBOX),
                _cell("B", row, label, _STYLE_TYPE),
            ])

            header_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
            for idx, h in enumerate(headers, start=2):
                header_cells.append(_cell(_col_letter(idx), row, h, _STYLE_HEADER))
            emit(header_cells)

            for body_row in bucket["rows"]:
                body_cells = [_cell("A", row, "", _STYLE_DEFAULT)]
                for idx, value in enumerate(body_row, start=2):
                    body_cells.append(
                        _cell(_col_letter(idx), row, value, _STYLE_DEFAULT)
                    )
                emit(body_cells)

            shown = len(bucket["rows"])
            if bucket["total"] > shown:
                extra = bucket["total"] - shown
                emit([
                    _cell("A", row, "", _STYLE_DEFAULT),
                    _cell("B", row, f"…and {extra} more", _STYLE_MORE),
                ])
            emit([_cell("A", row, "", _STYLE_DEFAULT)])  # spacer

    # Column widths: narrow checkbox column, wider data columns.
    cols_block = (
        '<cols>'
        '<col min="1" max="1" width="6" customWidth="1"/>'
        f'<col min="2" max="{max(2, max_cols)}" width="36" customWidth="1"/>'
        '</cols>'
    )

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetPr><pageSetUpPr fitToPage="1"/></sheetPr>'
        f'{cols_block}'
        "<sheetData>" + "".join(rows_xml) + "</sheetData>"
        '<printOptions horizontalCentered="1"/>'
        '<pageMargins left="0.5" right="0.5" top="0.6" bottom="0.6" '
        'header="0.3" footer="0.3"/>'
        '<pageSetup paperSize="9" orientation="portrait" '
        'fitToHeight="0" fitToWidth="1"/>'
        "</worksheet>"
    )
    return sheet_xml


# ---------------------------------------------------------------------------
# Static workbook parts
# ---------------------------------------------------------------------------

_CONTENT_TYPES_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
    '<Default Extension="xml" ContentType="application/xml"/>'
    '<Override PartName="/xl/workbook.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
    '<Override PartName="/xl/worksheets/sheet1.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
    '<Override PartName="/xl/styles.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
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
) -> Path:
    """
    Write the tag-summary working document to ``output_path``.

    Returns the resolved output path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheet_xml = _build_sheet_xml(data)

    parts = [
        ("[Content_Types].xml", _CONTENT_TYPES_XML),
        ("_rels/.rels", _ROOT_RELS_XML),
        ("xl/workbook.xml", _WORKBOOK_XML),
        ("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS_XML),
        ("xl/styles.xml", _STYLES_XML),
        ("xl/worksheets/sheet1.xml", sheet_xml),
    ]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in parts:
            # Pin date_time so ZIP local-file headers do not embed wall
            # clock state — required for deterministic workbook bytes.
            info = zipfile.ZipInfo(filename=name, date_time=_DETERMINISTIC_MTIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, payload)

    output_path.write_bytes(buf.getvalue())
    return output_path
