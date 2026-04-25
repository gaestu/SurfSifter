"""
Tests for get_tagged_artifact_export() and get_reference_list_match_export().
"""
from __future__ import annotations

import sqlite3

import pytest

from core.database import DatabaseManager
from core.database.helpers.tag_export import (
    get_reference_list_match_export,
    get_tagged_artifact_export,
)


EVIDENCE_ID = 1


# ---------------------------------------------------------------------------
# Fixtures (mirror tests/core/database/helpers/test_tag_match_summary.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def evidence_db(tmp_path):
    case_path = tmp_path / "test_case"
    case_path.mkdir()

    case_db_path = case_path / "CASE-2025-001_surfsifter.sqlite"
    case_conn = sqlite3.connect(case_db_path)
    case_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS evidences (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            source_path TEXT NOT NULL,
            evidence_slug TEXT NOT NULL
        )
        """
    )
    case_conn.execute(
        "INSERT INTO evidences (id, label, source_path, evidence_slug) "
        "VALUES (1, 'EV-001', '/test.E01', 'ev-001')"
    )
    case_conn.commit()
    case_conn.close()

    db_manager = DatabaseManager(case_path, case_db_path=case_db_path)
    return db_manager.get_evidence_conn(1, label="EV-001")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_tag(conn, name: str) -> int:
    cursor = conn.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized, created_by) "
        "VALUES (?, ?, ?, 'test')",
        (EVIDENCE_ID, name, name.lower()),
    )
    conn.commit()
    return cursor.lastrowid


def _tag_artifact(conn, tag_id: int, artifact_type: str, artifact_id: int) -> None:
    conn.execute(
        "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, "
        "artifact_id, tagged_by) VALUES (?, ?, ?, ?, 'test')",
        (tag_id, EVIDENCE_ID, artifact_type, artifact_id),
    )
    conn.commit()


def _insert_url(conn, url_id: int, url: str, last_seen: str = "2024-01-01T00:00:00Z"):
    conn.execute(
        "INSERT INTO urls (id, evidence_id, url, domain, discovered_by, "
        "last_seen_utc, occurrence_count) "
        "VALUES (?, ?, ?, 'example.com', 'test', ?, 1)",
        (url_id, EVIDENCE_ID, url, last_seen),
    )
    conn.commit()


def _insert_bookmark(conn, bid: int, url: str, title: str, ts: str):
    conn.execute(
        "INSERT INTO bookmarks (id, evidence_id, browser, profile, url, title, "
        "date_added_utc) VALUES (?, ?, 'chrome', 'Default', ?, ?, ?)",
        (bid, EVIDENCE_ID, url, title, ts),
    )
    conn.commit()


def _insert_image(conn, image_id: int, filename: str = "img.jpg", md5: str = "abc"):
    conn.execute(
        "INSERT INTO images (id, evidence_id, rel_path, filename, md5, "
        "first_discovered_by) VALUES (?, ?, ?, ?, ?, 'test')",
        (image_id, EVIDENCE_ID, f"img/{filename}", filename, md5),
    )
    conn.commit()


def _insert_file(conn, fid: int, name: str = "f.txt"):
    conn.execute(
        "INSERT INTO file_list (id, evidence_id, file_path, file_name, extension, "
        "size_bytes, modified_ts, import_source, import_timestamp) "
        "VALUES (?, ?, ?, ?, 'txt', 100, '2024-01-01T00:00:00Z', "
        "'test', '2024-01-01T00:00:00Z')",
        (fid, EVIDENCE_ID, f"/p/{name}", name),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# get_tagged_artifact_export
# ---------------------------------------------------------------------------

class TestGetTaggedArtifactExport:

    def test_empty(self, evidence_db):
        assert get_tagged_artifact_export(evidence_db, EVIDENCE_ID) == []

    def test_single_tag_single_type(self, evidence_db):
        _insert_url(evidence_db, 1, "https://a.com")
        tag = _create_tag(evidence_db, "Suspicious")
        _tag_artifact(evidence_db, tag, "url", 1)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["tag_name"] == "Suspicious"
        assert len(result[0]["sections"]) == 1
        section = result[0]["sections"][0]
        assert section["artifact_type"] == "url"
        assert section["raw_artifact_types"] == ["url"]
        assert section["column_count"] == 4
        assert section["total"] == 1
        assert len(section["rows"]) == 1
        assert section["rows"][0][0] == "https://a.com"

    def test_top_n_truncation_and_count(self, evidence_db):
        # 5 URLs tagged with same tag; top_n=3 should yield 3 rows + total 5.
        for i in range(1, 6):
            _insert_url(
                evidence_db,
                i,
                f"https://x{i}.com",
                last_seen=f"2024-01-{i:02d}T00:00:00Z",
            )
        tag = _create_tag(evidence_db, "Bulk")
        for i in range(1, 6):
            _tag_artifact(evidence_db, tag, "url", i)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID, top_n=3)
        section = result[0]["sections"][0]
        assert section["total"] == 5
        assert len(section["rows"]) == 3
        # Most recent first.
        urls = [r[0] for r in section["rows"]]
        assert urls == ["https://x5.com", "https://x4.com", "https://x3.com"]

    def test_multi_tag_artifact_appears_under_each_tag(self, evidence_db):
        _insert_url(evidence_db, 1, "https://shared.example")
        a = _create_tag(evidence_db, "Alpha")
        b = _create_tag(evidence_db, "Beta")
        _tag_artifact(evidence_db, a, "url", 1)
        _tag_artifact(evidence_db, b, "url", 1)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        names = [t["tag_name"] for t in result]
        assert names == ["Alpha", "Beta"]
        for tag_entry in result:
            assert tag_entry["sections"][0]["rows"][0][0] == "https://shared.example"

    def test_alphabetical_tag_ordering_case_insensitive(self, evidence_db):
        _insert_url(evidence_db, 1, "https://a.com")
        _insert_url(evidence_db, 2, "https://b.com")
        _insert_url(evidence_db, 3, "https://c.com")
        for name, uid in (("Zeta", 1), ("alpha", 2), ("Mu", 3)):
            tag = _create_tag(evidence_db, name)
            _tag_artifact(evidence_db, tag, "url", uid)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        assert [t["tag_name"] for t in result] == ["alpha", "Mu", "Zeta"]

    def test_unknown_artifact_type_surfaced_as_unsupported(self, evidence_db):
        # Unknown artifact_type values must NOT be silently dropped — the
        # export surfaces them as a placeholder section so the
        # investigator notices the gap.
        _insert_url(evidence_db, 1, "https://a.com")
        tag = _create_tag(evidence_db, "MixedTag")
        _tag_artifact(evidence_db, tag, "url", 1)
        _tag_artifact(evidence_db, tag, "totally_unknown_type", 999)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        sections = result[0]["sections"]
        types = {s["artifact_type"]: s for s in sections}
        assert set(types) == {"url", "totally_unknown_type"}
        unsupported = types["totally_unknown_type"]
        assert unsupported["supported"] is False
        assert unsupported["total"] == 1
        assert unsupported["rows"] == []
        assert unsupported["raw_artifact_types"] == ["totally_unknown_type"]
        # Supported sections keep supported=True.
        assert types["url"]["supported"] is True

    def test_artifact_type_aliases(self, evidence_db):
        # 'downloads' should be treated as 'browser_download'.
        evidence_db.execute(
            "INSERT INTO browser_downloads (id, evidence_id, browser, url, "
            "filename, target_path, start_time_utc) "
            "VALUES (10, ?, 'chrome', 'https://e/f', 'f.bin', '/t/f.bin', "
            "'2024-02-02T00:00:00Z')",
            (EVIDENCE_ID,),
        )
        evidence_db.commit()
        tag = _create_tag(evidence_db, "Downloaded")
        _tag_artifact(evidence_db, tag, "downloads", 10)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        assert len(result[0]["sections"]) == 1
        assert result[0]["sections"][0]["artifact_type"] == "browser_download"
        assert result[0]["sections"][0]["raw_artifact_types"] == ["downloads"]

    def test_bookmarks_section_columns(self, evidence_db):
        _insert_bookmark(
            evidence_db, 7, "https://b.example", "Title", "2024-03-01T00:00:00Z"
        )
        tag = _create_tag(evidence_db, "Bookmarked")
        _tag_artifact(evidence_db, tag, "bookmark", 7)

        section = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)[0]["sections"][0]
        assert section["artifact_type"] == "bookmark"
        assert section["column_count"] == 3
        row = section["rows"][0]
        assert row[0] == "Title"
        assert row[1] == "https://b.example"
        assert "chrome" in str(row[2])

    def test_evidence_isolation(self, evidence_db):
        _insert_url(evidence_db, 1, "https://a.com")
        tag = _create_tag(evidence_db, "Alpha")
        _tag_artifact(evidence_db, tag, "url", 1)
        assert get_tagged_artifact_export(evidence_db, 9999) == []


# ---------------------------------------------------------------------------
# get_reference_list_match_export
# ---------------------------------------------------------------------------

class TestGetReferenceListMatchExport:

    def test_empty(self, evidence_db):
        assert get_reference_list_match_export(evidence_db, EVIDENCE_ID) == []

    def test_url_match_grouped_by_list(self, evidence_db):
        _insert_url(evidence_db, 1, "https://bad.example")
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type) "
            "VALUES (?, 1, 'blocklist', 'exact')",
            (EVIDENCE_ID,),
        )
        evidence_db.commit()

        result = get_reference_list_match_export(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        bucket = result[0]
        assert bucket["list_name"] == "blocklist"
        assert bucket["kind"] == "url"
        assert bucket["total"] == 1
        assert bucket["rows"][0][0] == "https://bad.example"

    def test_top_n_truncation(self, evidence_db):
        for i in range(1, 6):
            _insert_url(
                evidence_db,
                i,
                f"https://b{i}.example",
                last_seen=f"2024-04-{i:02d}T00:00:00Z",
            )
            evidence_db.execute(
                "INSERT INTO url_matches (evidence_id, url_id, list_name, "
                "match_type) VALUES (?, ?, 'blocklist', 'exact')",
                (EVIDENCE_ID, i),
            )
        evidence_db.commit()

        result = get_reference_list_match_export(evidence_db, EVIDENCE_ID, top_n=3)
        bucket = result[0]
        assert bucket["total"] == 5
        assert len(bucket["rows"]) == 3
        # Reverse-chronological order
        urls = [r[0] for r in bucket["rows"]]
        assert urls == [
            "https://b5.example",
            "https://b4.example",
            "https://b3.example",
        ]

    def test_multiple_kinds_separate_buckets(self, evidence_db):
        _insert_image(evidence_db, 1, filename="x.jpg", md5="aaa")
        evidence_db.execute(
            "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, "
            "list_name) VALUES (?, 1, 'projectvic', 'aaa', 'projectvic')",
            (EVIDENCE_ID,),
        )
        _insert_file(evidence_db, 1, name="malware.exe")
        evidence_db.execute(
            "INSERT INTO file_list_matches (evidence_id, file_list_id, "
            "reference_list_name, match_type, matched_value, matched_at) "
            "VALUES (?, 1, 'malware_filenames', 'exact', 'malware.exe', "
            "'2024-01-01T00:00:00Z')",
            (EVIDENCE_ID,),
        )
        evidence_db.commit()

        result = get_reference_list_match_export(evidence_db, EVIDENCE_ID)
        kinds = {(b["list_name"], b["kind"]) for b in result}
        assert ("projectvic", "image") in kinds
        assert ("malware_filenames", "file") in kinds

    def test_duplicate_match_rows_collapse_to_distinct_artifact(self, evidence_db):
        """Multiple raw match rows for the same artifact must count once."""
        _insert_url(evidence_db, 1, "https://dup.example")
        for _ in range(4):  # same (list_name, url_id) inserted 4 times
            evidence_db.execute(
                "INSERT INTO url_matches (evidence_id, url_id, list_name, "
                "match_type) VALUES (?, 1, 'blocklist', 'exact')",
                (EVIDENCE_ID,),
            )
        evidence_db.commit()

        result = get_reference_list_match_export(evidence_db, EVIDENCE_ID)
        bucket = result[0]
        assert bucket["total"] == 1
        assert len(bucket["rows"]) == 1

    def test_null_list_name_is_handled(self, evidence_db):
        """A NULL list_name (allowed on hash_matches) must not crash export."""
        _insert_image(evidence_db, 1, filename="x.jpg", md5="aaa")
        evidence_db.execute(
            "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, "
            "list_name) VALUES (?, 1, 'projectvic', 'aaa', NULL)",
            (EVIDENCE_ID,),
        )
        evidence_db.commit()

        result = get_reference_list_match_export(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert isinstance(result[0]["list_name"], str)
        assert result[0]["list_name"]  # non-empty sentinel


# ---------------------------------------------------------------------------
# Alias merging
# ---------------------------------------------------------------------------

class TestAliasMerging:

    def test_legacy_and_canonical_artifact_types_merge(self, evidence_db):
        """A tag carrying both 'bookmark' and 'bookmarks' must yield ONE
        merged section, not two duplicate sections with split totals.
        """
        _insert_bookmark(
            evidence_db, 1, "https://a.example", "A", "2024-01-01T00:00:00Z"
        )
        _insert_bookmark(
            evidence_db, 2, "https://b.example", "B", "2024-01-02T00:00:00Z"
        )
        tag = _create_tag(evidence_db, "Mixed")
        _tag_artifact(evidence_db, tag, "bookmark", 1)
        _tag_artifact(evidence_db, tag, "bookmarks", 2)

        result = get_tagged_artifact_export(evidence_db, EVIDENCE_ID)
        sections = result[0]["sections"]
        assert len(sections) == 1
        assert sections[0]["artifact_type"] == "bookmark"
        assert set(sections[0]["raw_artifact_types"]) == {"bookmark", "bookmarks"}
        assert sections[0]["total"] == 2


# ---------------------------------------------------------------------------
# Determinism — collisions on (timestamp, tie) must always pick the same rows
# ---------------------------------------------------------------------------

class TestSampleSelectionIsDeterministic:

    def test_artifact_top_n_breaks_ties_on_pk(self, evidence_db):
        """When ts AND tie are equal across rows, the unique PK breaks
        the tie so two runs against identical evidence pick the SAME
        sample rows.
        """
        # All five timeline rows share the same kind/ref_table/ts so the
        # only stable tiebreaker is the row id.
        for i in range(1, 6):
            evidence_db.execute(
                "INSERT INTO timeline (id, evidence_id, kind, ts_utc, "
                "ref_table, ref_id) VALUES (?, ?, 'visit', "
                "'2024-01-01T00:00:00Z', 'urls', ?)",
                (i, EVIDENCE_ID, i),
            )
        evidence_db.commit()
        tag = _create_tag(evidence_db, "Det")
        for i in range(1, 6):
            _tag_artifact(evidence_db, tag, "timeline", i)

        first = get_tagged_artifact_export(evidence_db, EVIDENCE_ID, top_n=3)
        second = get_tagged_artifact_export(evidence_db, EVIDENCE_ID, top_n=3)
        assert first == second
        # And specifically: pk ASC means we get ids 1,2,3.
        section = first[0]["sections"][0]
        assert section["total"] == 5
        assert len(section["rows"]) == 3

    def test_reference_match_top_n_breaks_ties_on_pk(self, evidence_db):
        """Same guarantee for the reference-list query path."""
        for i in range(1, 6):
            _insert_url(
                evidence_db, i,
                f"https://same.example/{i:02d}",
                last_seen="2024-01-01T00:00:00Z",
            )
            evidence_db.execute(
                "INSERT INTO url_matches (evidence_id, url_id, list_name, "
                "match_type) VALUES (?, ?, 'L', 'exact')",
                (EVIDENCE_ID, i),
            )
        evidence_db.commit()

        first = get_reference_list_match_export(evidence_db, EVIDENCE_ID, top_n=3)
        second = get_reference_list_match_export(evidence_db, EVIDENCE_ID, top_n=3)
        assert first == second
        assert first[0]["total"] == 5
        assert len(first[0]["rows"]) == 3
