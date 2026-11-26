"""File list paths on directory evidence (#99).

Directory evidence (``MountedFS``) gets its file list from a CSV import. Its
paths must be readable in the forms the file list holds: with a leading slash
(the form ``fls`` writes) and after converting the Windows paths of FTK and
EnCase exports.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from core.evidence_fs import MountedFS
from extractors.browser.firefox.cookies import FirefoxCookiesExtractor
from extractors.browser.firefox.history import FirefoxHistoryExtractor
from extractors.browser.firefox.transport_security import FirefoxTransportSecurityExtractor
from extractors.system.file_list.parser import (
    EnCaseParser,
    FTKParser,
    GenericParser,
    NormalizedPath,
    normalize_import_path,
)
from extractors.system.file_list.worker import FileListExtractor
from tests.fixtures.file_list_discovery import add_to_file_list, mock_callbacks, write_file

PROFILE = "Users/alice/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default-release"
FTK_LISTING = Path(__file__).resolve().parents[2] / "files" / "direcotry_listing.csv"

FTK_HEADER = "Filename\tFull Path\tSize (bytes)\tCreated\tModified\tAccessed\tIs Deleted\n"
ENCASE_HEADER = "File Name,Full Path,File Size,Created Date,Modified Date,Accessed Date,Deleted\n"


def windows(path: str) -> str:
    return path.replace("/", "\\")


# ---------------------------------------------------------------------------
# normalize_import_path()
# ---------------------------------------------------------------------------


class TestNormalizeImportPath:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            # FTK Imager directory listing (form of tests/files/direcotry_listing.csv)
            ("Partition 1\\NONAME [NTFS]\\[root]\\boot.ini", NormalizedPath("/boot.ini", 1)),
            ("Partition 1\\NONAME [NTFS]\\[root]\\Temp\\", NormalizedPath("/Temp/", 1)),
            ("Partition 1\\NONAME [NTFS]\\[root]\\", NormalizedPath("/", 1)),
            ("Partition 1\\NONAME [NTFS]\\[root]", NormalizedPath("/", 1)),
            (
                "image.E01\\Partition 3\\OS [NTFS]\\[root]\\Users\\alice\\a.txt",
                NormalizedPath("/Users/alice/a.txt", 3),
            ),
            # FTK entries outside the file system root: separators only
            (
                "Partition 1\\NONAME [NTFS]\\[orphan]\\x.txt",
                NormalizedPath("Partition 1/NONAME [NTFS]/[orphan]/x.txt"),
            ),
            (
                "Partition 1\\NONAME [NTFS]\\[unallocated space]\\",
                NormalizedPath("Partition 1/NONAME [NTFS]/[unallocated space]/"),
            ),
            # Drive letters
            ("C:\\Users\\alice\\a.txt", NormalizedPath("/Users/alice/a.txt")),
            ("d:\\Data\\a.txt", NormalizedPath("/Data/a.txt")),
            ("C:/Users/alice/a.txt", NormalizedPath("/Users/alice/a.txt")),
            ("C:\\", NormalizedPath("/")),
            # Relative Windows path
            ("Users\\alice\\a.txt", NormalizedPath("Users/alice/a.txt")),
            # A folder named [root] without an FTK volume label is not a prefix
            ("Users\\[root]\\a.txt", NormalizedPath("Users/[root]/a.txt")),
            # Unchanged
            ("/Users/alice/a.txt", NormalizedPath("/Users/alice/a.txt")),
            ("Users/alice/a.txt", NormalizedPath("Users/alice/a.txt")),
            ("/home/alice/back\\slash.txt", NormalizedPath("/home/alice/back\\slash.txt")),
            ("C:\\Users/mixed.txt", NormalizedPath("C:\\Users/mixed.txt")),
            ("", NormalizedPath("")),
            # Folders named like the FTK prefix further down are not a prefix:
            # anyone can create them on the evidence (security review of #99)
            (
                "C:\\Users\\bob\\Partition 1\\x [NTFS]\\[root]\\Users\\alice\\places.sqlite",
                NormalizedPath("/Users/bob/Partition 1/x [NTFS]/[root]/Users/alice/places.sqlite"),
            ),
            (
                "Users\\bob\\Partition 1\\x [NTFS]\\[root]\\a.txt",
                NormalizedPath("Users/bob/Partition 1/x [NTFS]/[root]/a.txt"),
            ),
            (
                "/home/bob/Partition 1/x [ext4]/[root]/etc/shadow",
                NormalizedPath("/home/bob/Partition 1/x [ext4]/[root]/etc/shadow"),
            ),
            (
                "C:\\Partition 1\\x [NTFS]\\[root]\\a.txt",
                NormalizedPath("/Partition 1/x [NTFS]/[root]/a.txt"),
            ),
        ],
    )
    def test_forms(self, raw, expected):
        assert normalize_import_path(raw) == expected

    def test_long_bracket_run_is_fast(self):
        """The FTK pattern must not backtrack on a segment full of brackets."""
        import time

        start = time.perf_counter()
        result = normalize_import_path("Partition 1/" + "[" * 100_000)
        assert time.perf_counter() - start < 1.0
        assert result.ftk_partition is None


class TestParsersNormalize:
    def test_ftk_row_keeps_original_path_and_partition_in_metadata(self, tmp_path):
        csv_path = tmp_path / "ftk.csv"
        csv_path.write_text(
            FTK_HEADER + "boot.ini\tPartition 1\\NONAME [NTFS]\\[root]\\boot.ini\t194\t\t\t\tno\n",
            encoding="utf-8",
        )
        parser = FTKParser()
        [entry] = parser.parse(csv_path)

        assert entry["file_path"] == "/boot.ini"
        assert entry["file_name"] == "boot.ini"
        # FTK's partition number is not used as the partition index
        assert entry.get("partition_index") is None
        metadata = json.loads(entry["metadata"])
        assert metadata["path_as_imported"] == "Partition 1\\NONAME [NTFS]\\[root]\\boot.ini"
        assert metadata["ftk_partition"] == 1
        assert parser.normalized_path_count == 1

    def test_csv_column_cannot_pose_as_import_metadata(self, tmp_path):
        """A CSV column named like an import key is kept under another name."""
        csv_path = tmp_path / "ftk.csv"
        csv_path.write_text(
            FTK_HEADER.rstrip("\n") + "\tpath_as_imported\tftk_partition\n"
            "a.txt\tC:\\a.txt\t1\t\t\t\tno\tforged\t9\n"
            "b.txt\t/b.txt\t1\t\t\t\tno\tforged\t9\n",
            encoding="utf-8",
        )
        changed, unchanged = FTKParser().parse(csv_path)

        changed_meta = json.loads(changed["metadata"])
        assert changed_meta["path_as_imported"] == "C:\\a.txt"
        assert "ftk_partition" not in changed_meta
        assert changed_meta["csv:path_as_imported"] == "forged"
        assert changed_meta["csv:ftk_partition"] == "9"

        unchanged_meta = json.loads(unchanged["metadata"])
        assert "path_as_imported" not in unchanged_meta
        assert "ftk_partition" not in unchanged_meta
        assert unchanged_meta["csv:path_as_imported"] == "forged"

    def test_unchanged_path_adds_no_metadata(self, tmp_path):
        csv_path = tmp_path / "encase.csv"
        csv_path.write_text(ENCASE_HEADER + "a.txt,/Users/alice/a.txt,1,,,,No\n", encoding="utf-8")
        parser = EnCaseParser()
        [entry] = parser.parse(csv_path)

        assert entry["file_path"] == "/Users/alice/a.txt"
        assert entry["metadata"] is None
        assert parser.normalized_path_count == 0

    def test_generic_parser_keeps_its_partition_column(self, tmp_path):
        csv_path = tmp_path / "generic.csv"
        csv_path.write_text("Name,Path,Partition\na.txt,C:\\Users\\a.txt,2\n", encoding="utf-8")
        parser = GenericParser({"file_name": "Name", "file_path": "Path", "partition_index": "Partition"})
        [entry] = parser.parse(csv_path)

        assert entry["file_path"] == "/Users/a.txt"
        assert entry["partition_index"] == 2
        assert json.loads(entry["metadata"])["path_as_imported"] == "C:\\Users\\a.txt"

    def test_colliding_paths_stay_unconverted(self, tmp_path):
        """Rows that would get the same path keep their CSV path (review of #99).

        The file list keeps one row per partition and path; converting both
        would drop one of them.
        """
        csv_path = tmp_path / "ftk.csv"
        csv_path.write_text(
            FTK_HEADER
            + "$MFT\tPartition 1\\System Reserved [NTFS]\\[root]\\$MFT\t111\t\t\t\tno\n"
            + "$MFT\tPartition 2\\OS [NTFS]\\[root]\\$MFT\t222\t\t\t\tno\n"
            + "evil.exe\tC:\\Tools\\evil.exe\t1\t\t\t\tno\n"
            + "evil.exe\tD:\\Tools\\evil.exe\t999\t\t\t\tno\n"
            + "places.sqlite\tPartition 2\\OS [NTFS]\\[root]\\Users\\a\\places.sqlite\t5\t\t\t\tno\n"
            + "dup.txt\tC:\\dup.txt\t1\t\t\t\tno\n"
            + "dup.txt\tC:\\dup.txt\t1\t\t\t\tno\n",
            encoding="utf-8",
        )
        parser = FTKParser()
        entries = parser.parse(csv_path)
        paths = [entry["file_path"] for entry in entries]

        assert paths == [
            "Partition 1\\System Reserved [NTFS]\\[root]\\$MFT",
            "Partition 2\\OS [NTFS]\\[root]\\$MFT",
            "C:\\Tools\\evil.exe",
            "D:\\Tools\\evil.exe",
            # No collision: converted
            "/Users/a/places.sqlite",
            # The same CSV path twice is not a collision
            "/dup.txt",
            "/dup.txt",
        ]
        for entry in entries[:4]:
            assert entry["metadata"] is None
        assert json.loads(entries[4]["metadata"])["ftk_partition"] == 2
        assert parser.collision_row_count == 4
        assert parser.collision_examples == paths[:4]
        assert parser.normalized_path_count == 3

    def test_converted_path_colliding_with_an_unconverted_row(self, tmp_path):
        csv_path = tmp_path / "encase.csv"
        csv_path.write_text(
            ENCASE_HEADER + "a.txt,/Users/a.txt,1,,,,No\n" + "a.txt,C:\\Users\\a.txt,2,,,,No\n",
            encoding="utf-8",
        )
        parser = EnCaseParser()
        entries = parser.parse(csv_path)

        assert [entry["file_path"] for entry in entries] == ["/Users/a.txt", "C:\\Users\\a.txt"]
        assert parser.collision_row_count == 1

    def test_count_is_reset_per_parse(self, tmp_path):
        csv_path = tmp_path / "ftk.csv"
        csv_path.write_text(FTK_HEADER + "a.txt\tC:\\a.txt\t1\t\t\t\tno\n", encoding="utf-8")
        parser = FTKParser()
        parser.parse(csv_path)
        parser.parse(csv_path)
        assert parser.normalized_path_count == 1

    def test_real_ftk_imager_listing(self):
        """The FTK Imager export in tests/files: paths below [root] become file list paths."""
        if not FTK_LISTING.exists():
            pytest.skip("FTK listing not available")
        entries = FTKParser().parse(FTK_LISTING)
        by_path = {entry["file_path"]: entry for entry in entries}

        assert "/boot.ini" in by_path
        assert "/WINDOWS/" in by_path
        boot = json.loads(by_path["/boot.ini"]["metadata"])
        assert boot["ftk_partition"] == 1
        assert boot["path_as_imported"] == "Partition 1\\NONAME [NTFS]\\[root]\\boot.ini"
        # Nothing below [root] keeps a backslash or the FTK prefix
        below_root = [e for e in entries if "[root]" in json.loads(e["metadata"] or "{}").get("path_as_imported", "")]
        assert below_root
        assert all(e["file_path"].startswith("/") and "\\" not in e["file_path"] for e in below_root)


# ---------------------------------------------------------------------------
# Directory evidence: extractors read file list paths in both forms
# ---------------------------------------------------------------------------


def run_extraction(extractor, evidence_fs, output_dir, evidence_conn, evidence_id):
    ok = extractor.run_extraction(
        evidence_fs,
        output_dir,
        {"evidence_conn": evidence_conn, "evidence_id": evidence_id, "browsers": ["firefox"]},
        mock_callbacks(),
    )
    manifest = json.loads((output_dir / "manifest.json").read_text())
    return ok, manifest


@pytest.mark.parametrize(
    ("extractor_class", "file_name"),
    [
        (FirefoxCookiesExtractor, "cookies.sqlite"),
        (FirefoxHistoryExtractor, "places.sqlite"),
    ],
    ids=["cookies", "history"],
)
@pytest.mark.parametrize("leading_slash", [False, True], ids=["relative", "absolute"])
def test_directory_evidence_reads_both_path_forms(
    tmp_path, evidence_db, case_context, extractor_class, file_name, leading_slash
):
    """Reproduction from #99: a leading slash was rejected as path traversal."""
    mount = tmp_path / "evidence"
    content = b"SQLite format 3\x00" + file_name.encode()
    write_file(mount, f"{PROFILE}/{file_name}", content)
    listed = ("/" if leading_slash else "") + f"{PROFILE}/{file_name}"
    add_to_file_list(evidence_db, case_context.evidence_id, listed, 0)

    ok, manifest = run_extraction(
        extractor_class(), MountedFS(mount), tmp_path / "out", evidence_db, case_context.evidence_id
    )

    assert ok is True
    assert manifest["status"] == "ok", manifest.get("notes")
    assert not any("traversal" in note.lower() for note in manifest.get("notes", []))
    assert len(manifest["files"]) == 1
    copied = manifest["files"][0]
    assert copied["copy_status"] == "ok"
    assert copied["sha256"] == hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------------
# Imported CSV with Windows paths on directory evidence
# ---------------------------------------------------------------------------


HSTS_CONTENT = b"example.com:HSTS\t0\t19000\t1893456000000,1,0\n"


@pytest.mark.parametrize(
    "csv_path_column",
    [
        windows(f"C:/{PROFILE}/SiteSecurityServiceState.txt"),
        windows(f"Partition 2/OS [NTFS]/[root]/{PROFILE}/SiteSecurityServiceState.txt"),
    ],
    ids=["drive-letter", "ftk-imager"],
)
def test_imported_windows_paths_are_found_and_read(tmp_path, evidence_db, case_context, csv_path_column):
    """A CSV with Windows paths is imported, then Firefox HSTS finds and reads the file."""
    mount = tmp_path / "evidence"
    write_file(mount, f"{PROFILE}/SiteSecurityServiceState.txt", HSTS_CONTENT)

    csv_file = tmp_path / "listing.csv"
    csv_file.write_text(
        FTK_HEADER + f"SiteSecurityServiceState.txt\t{csv_path_column}\t{len(HSTS_CONTENT)}\t\t\t\tno\n",
        encoding="utf-8",
    )
    stats = FileListExtractor(evidence_db, case_context.evidence_id, csv_file).run()
    assert stats["inserted_rows"] == 1
    assert stats["normalized_paths"] == 1

    stored_path, partition_index, metadata = evidence_db.execute(
        "SELECT file_path, partition_index, metadata FROM file_list WHERE evidence_id = ?",
        (case_context.evidence_id,),
    ).fetchone()
    assert stored_path == f"/{PROFILE}/SiteSecurityServiceState.txt"
    assert partition_index is None
    assert json.loads(metadata)["path_as_imported"] == csv_path_column

    ok, manifest = run_extraction(
        FirefoxTransportSecurityExtractor(),
        MountedFS(mount),
        tmp_path / "out",
        evidence_db,
        case_context.evidence_id,
    )

    assert ok is True
    assert manifest["status"] == "ok", manifest.get("notes")
    assert len(manifest["files"]) == 1
    copied = manifest["files"][0]
    assert copied["copy_status"] == "ok"
    assert copied["logical_path"] == stored_path
    assert copied["md5"] == hashlib.md5(HSTS_CONTENT).hexdigest()


def test_import_keeps_every_row_of_colliding_paths(tmp_path, evidence_db, case_context):
    """Two FTK partitions with the same path: both rows are stored (review of #99)."""
    csv_file = tmp_path / "listing.csv"
    csv_file.write_text(
        FTK_HEADER
        + "$MFT\tPartition 1\\SYS [NTFS]\\[root]\\$MFT\t111\t\t\t\tno\n"
        + "$MFT\tPartition 2\\OS [NTFS]\\[root]\\$MFT\t222\t\t\t\tno\n"
        + "a.txt\tPartition 2\\OS [NTFS]\\[root]\\Users\\a.txt\t3\t\t\t\tno\n",
        encoding="utf-8",
    )
    stats = FileListExtractor(evidence_db, case_context.evidence_id, csv_file).run()

    assert stats["inserted_rows"] == 3
    assert stats["skipped_rows"] == 0
    assert stats["normalized_paths"] == 1
    assert stats["unconverted_collisions"] == 2
    rows = evidence_db.execute(
        "SELECT file_path, size_bytes FROM file_list WHERE evidence_id = ? ORDER BY id",
        (case_context.evidence_id,),
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("Partition 1\\SYS [NTFS]\\[root]\\$MFT", 111),
        ("Partition 2\\OS [NTFS]\\[root]\\$MFT", 222),
        ("/Users/a.txt", 3),
    ]
