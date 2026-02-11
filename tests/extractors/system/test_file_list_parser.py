"""
Test file list parsers (FTK, EnCase, Generic).
"""
import json
import tempfile
from pathlib import Path

import pytest

from extractors.system.file_list.parser import (
    BaseFileListParser,
    FTKParser,
    EnCaseParser,
    GenericParser,
    detect_parser,
)


def test_ftk_parser_detect_encoding():
    """Test encoding detection for UTF-16 LE file."""
    parser = FTKParser()
    ftk_file = Path(__file__).resolve().parent.parent.parent.parent / "images" / "hackcase" / "direcotry_listing.csv"

    if ftk_file.exists():
        encoding = parser.detect_encoding(ftk_file)
        assert encoding in ("utf-16-le", "utf-16"), f"Expected UTF-16, got {encoding}"

def test_ftk_parser_timestamp_parsing():
    """Test FTK timestamp format parsing."""
    parser = FTKParser()

    # FTK format: 2004-Aug-19 16:57:43.694987 UTC
    timestamp = parser._parse_timestamp("2004-Aug-19 16:57:43.694987 UTC")
    assert timestamp == "2004-08-19T16:57:43Z"

    # FTK format without microseconds
    timestamp = parser._parse_timestamp("2004-Aug-19 16:57:43 UTC")
    assert timestamp == "2004-08-19T16:57:43Z"

    # Invalid timestamp
    timestamp = parser._parse_timestamp("invalid")
    assert timestamp is None

    # Empty timestamp
    timestamp = parser._parse_timestamp("")
    assert timestamp is None


def test_ftk_parser_extension_extraction():
    """Test extension extraction from filenames."""
    parser = FTKParser()

    assert parser._extract_extension("test.exe") == ".exe"
    assert parser._extract_extension("document.PDF") == ".pdf"  # lowercase
    assert parser._extract_extension("file.tar.gz") == ".gz"
    assert parser._extract_extension("noextension") == ""
    assert parser._extract_extension("") == ""


def test_ftk_parser_filename_extraction():
    """Test filename extraction from full paths."""
    parser = FTKParser()

    # Windows paths
    assert parser._extract_filename("C:\\Windows\\System32\\cmd.exe") == "cmd.exe"
    assert parser._extract_filename("Partition 1\\NTFS\\[root]\\boot.ini") == "boot.ini"

    # Unix paths
    assert parser._extract_filename("/usr/bin/python") == "python"

    # Edge cases
    assert parser._extract_filename("justfilename") == "justfilename"
    assert parser._extract_filename("") == ""


def test_encase_parser_with_synthetic_data(tmp_path):
    """Test EnCase parser with synthetic CSV data."""
    # Create synthetic EnCase CSV
    encase_csv = tmp_path / "encase_test.csv"
    encase_csv.write_text(
        "File Name,Full Path,File Size,Created Date,Modified Date,Accessed Date,Deleted,MD5 Hash\n"
        "test.exe,C:\\Temp\\test.exe,1024,2025-01-01 10:00:00,2025-01-02 11:00:00,2025-01-03 12:00:00,No,d41d8cd98f00b204e9800998ecf8427e\n"
        "document.pdf,C:\\Users\\test\\document.pdf,50000,2025-01-01 10:00:00,2025-01-02 11:00:00,2025-01-03 12:00:00,Yes,\n"
    )

    parser = EnCaseParser()
    entries = parser.parse(encase_csv)

    assert len(entries) == 2

    # Check first entry
    entry1 = entries[0]
    assert entry1["file_name"] == "test.exe"
    assert entry1["file_path"] == "C:\\Temp\\test.exe"
    assert entry1["size_bytes"] == 1024
    assert entry1["deleted"] is False
    assert entry1["md5_hash"] == "d41d8cd98f00b204e9800998ecf8427e"
    assert entry1["extension"] == ".exe"

    # Check second entry (deleted, no hash)
    entry2 = entries[1]
    assert entry2["file_name"] == "document.pdf"
    assert entry2["deleted"] is True
    assert entry2["md5_hash"] is None


def test_generic_parser_with_custom_mapping(tmp_path):
    """Test generic parser with user-defined column mapping."""
    # Create custom CSV
    custom_csv = tmp_path / "custom_test.csv"
    custom_csv.write_text(
        "Name,Path,Size,Date Modified\n"
        "app.exe,C:\\Apps\\app.exe,2048,2025-01-01 10:00:00\n"
        "data.db,C:\\Data\\data.db,100000,2025-01-02 11:00:00\n"
    )

    # Define column mapping
    column_mapping = {
        "file_name": "Name",
        "file_path": "Path",
        "size_bytes": "Size",
        "modified_ts": "Date Modified",
    }

    parser = GenericParser(column_mapping)
    entries = parser.parse(custom_csv)

    assert len(entries) == 2

    entry = entries[0]
    assert entry["file_name"] == "app.exe"
    assert entry["file_path"] == "C:\\Apps\\app.exe"
    assert entry["size_bytes"] == 2048
    assert entry["modified_ts"] == "2025-01-01T10:00:00Z"
    assert entry["extension"] == ".exe"


def test_detect_parser_ftk(tmp_path):
    """Test auto-detection of FTK format."""
    # Create FTK-style CSV (tab-delimited)
    ftk_csv = tmp_path / "ftk_test.csv"
    ftk_csv.write_text(
        "Filename\tFull Path\tSize (bytes)\tCreated\tModified\tAccessed\tIs Deleted\n"
        "test.exe\tC:\\test.exe\t1024\t\t\t\tno\n",
        encoding="utf-8"
    )

    parser = detect_parser(ftk_csv)
    assert isinstance(parser, FTKParser)


def test_detect_parser_encase(tmp_path):
    """Test auto-detection of EnCase format."""
    # Create EnCase-style CSV (comma-delimited)
    encase_csv = tmp_path / "encase_test.csv"
    encase_csv.write_text(
        "File Name,Full Path,File Size,Created Date,Modified Date,Accessed Date,Deleted\n"
        "test.exe,C:\\test.exe,1024,,,, No\n"
    )

    parser = detect_parser(encase_csv)
    assert isinstance(parser, EnCaseParser)


def test_detect_parser_generic(tmp_path):
    """Test fallback to generic parser for unknown format."""
    # Create custom format CSV
    custom_csv = tmp_path / "custom_test.csv"
    custom_csv.write_text(
        "Name,Location,Bytes\n"
        "file.txt,/tmp/file.txt,100\n"
    )

    parser = detect_parser(custom_csv)
    assert isinstance(parser, GenericParser)


def test_parser_handles_malformed_rows(tmp_path):
    """Test that parser gracefully handles malformed rows."""
    # Create CSV with malformed data
    malformed_csv = tmp_path / "malformed_test.csv"
    malformed_csv.write_text(
        "Filename\tFull Path\tSize (bytes)\tCreated\tModified\tAccessed\tIs Deleted\n"
        "good.exe\tC:\\good.exe\t1024\t\t\t\tno\n"
        "\t\t\t\t\t\t\n"  # Empty row
        "bad_size.exe\tC:\\bad.exe\tNOT_A_NUMBER\t\t\t\tno\n"  # Invalid size
        "another.exe\tC:\\another.exe\t2048\t\t\t\tno\n"
    )

    parser = FTKParser()
    entries = parser.parse(malformed_csv)

    # Should parse good rows and skip bad ones
    assert len(entries) >= 2  # At least the two good rows

    # Verify good rows parsed correctly
    good_file = next((e for e in entries if e["file_name"] == "good.exe"), None)
    assert good_file is not None
    assert good_file["size_bytes"] == 1024

    # Bad size should result in None, not crash
    bad_file = next((e for e in entries if e["file_name"] == "bad_size.exe"), None)
    if bad_file:  # May be skipped entirely or parsed with None size
        assert bad_file["size_bytes"] is None


def test_parser_handles_utf8_with_bom(tmp_path):
    """Test parsing UTF-8 file with BOM."""
    # Create UTF-8 file with BOM
    utf8_bom_csv = tmp_path / "utf8_bom_test.csv"
    content = "Filename\tFull Path\tSize (bytes)\tCreated\tModified\tAccessed\tIs Deleted\n"
    content += "test.exe\tC:\\test.exe\t1024\t\t\t\tno\n"
    utf8_bom_csv.write_bytes(b"\xef\xbb\xbf" + content.encode("utf-8"))

    parser = FTKParser()
    encoding = parser.detect_encoding(utf8_bom_csv)
    assert encoding in ("utf-8", "UTF-8-SIG", "utf-8-sig")

    entries = parser.parse(utf8_bom_csv)
    assert len(entries) >= 1


def test_parser_metadata_preservation(tmp_path):
    """Test that extra columns are preserved in metadata field."""
    # Create CSV with extra columns
    csv_with_extra = tmp_path / "extra_columns_test.csv"
    csv_with_extra.write_text(
        "Filename\tFull Path\tSize (bytes)\tCreated\tModified\tAccessed\tIs Deleted\tCustom1\tCustom2\n"
        "test.exe\tC:\\test.exe\t1024\t\t\t\tno\textra_data_1\textra_data_2\n"
    )

    parser = FTKParser()
    entries = parser.parse(csv_with_extra)

    assert len(entries) == 1
    entry = entries[0]

    # Check metadata field contains extra columns
    assert entry["metadata"] is not None
    metadata = json.loads(entry["metadata"])
    assert "Custom1" in metadata
    assert metadata["Custom1"] == "extra_data_1"
    assert "Custom2" in metadata
    assert metadata["Custom2"] == "extra_data_2"


def test_bool_parsing():
    """Test boolean value parsing."""
    parser = FTKParser()

    # True variants
    assert parser._parse_bool("yes") is True
    assert parser._parse_bool("Yes") is True
    assert parser._parse_bool("YES") is True
    assert parser._parse_bool("true") is True
    assert parser._parse_bool("True") is True
    assert parser._parse_bool("1") is True
    assert parser._parse_bool("y") is True
    assert parser._parse_bool("t") is True

    # False variants
    assert parser._parse_bool("no") is False
    assert parser._parse_bool("No") is False
    assert parser._parse_bool("false") is False
    assert parser._parse_bool("0") is False
    assert parser._parse_bool("") is False
    assert parser._parse_bool("random") is False
