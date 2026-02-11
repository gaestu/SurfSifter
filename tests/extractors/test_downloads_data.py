"""
Unit tests for downloads data layer (CaseDataAccess download methods).

Tests for the new downloads table and related CRUD operations.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict

import pytest

from app.data.case_data import CaseDataAccess
from core.database import DatabaseManager


@pytest.fixture
def temp_case_folder(tmp_path: Path) -> Path:
    """Create a temporary case folder."""
    case_folder = tmp_path / "test_case_browser_analyzing"
    case_folder.mkdir()
    return case_folder


@pytest.fixture
def db_manager(temp_case_folder: Path) -> DatabaseManager:
    """Create a DatabaseManager with initialized databases."""
    case_db_path = temp_case_folder / "test_surfsifter.sqlite"
    manager = DatabaseManager(temp_case_folder, case_db_path=case_db_path)

    # Initialize case record
    case_conn = manager.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
        )

    return manager


@pytest.fixture
def case_data(temp_case_folder: Path, db_manager: DatabaseManager) -> CaseDataAccess:
    """Create CaseDataAccess instance."""
    return CaseDataAccess(temp_case_folder, db_manager=db_manager)


@pytest.fixture
def evidence_id(db_manager: DatabaseManager) -> int:
    """Create a test evidence record and return its ID."""
    case_conn = db_manager.get_case_conn()
    with case_conn:
        cur = case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "TestEvidence", "/test/image.E01", "2024-01-01T00:00:00"),
        )
        evidence_id = cur.lastrowid

    # Initialize evidence database
    _ = db_manager.get_evidence_conn(evidence_id, "TestEvidence")

    return evidence_id


class TestDownloadsTableMigration:
    """Tests for downloads table schema."""

    def test_downloads_table_exists(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Verify downloads table is created."""
        with case_data._use_evidence_conn(evidence_id):
            with case_data._connect() as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='downloads'"
                )
                result = cursor.fetchone()
                assert result is not None

    def test_downloads_table_core_columns(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Verify downloads table has core columns."""
        expected_columns = {
            "id", "evidence_id", "url_id", "url", "domain", "filename",
            "file_type", "file_extension", "status", "dest_path", "size_bytes",
            "md5", "sha256", "content_type", "phash", "exif_json", "width",
            "height", "queued_at_utc", "completed_at_utc",
        }

        with case_data._use_evidence_conn(evidence_id):
            with case_data._connect() as conn:
                cursor = conn.execute("PRAGMA table_info(downloads)")
                actual_columns = {row[1] for row in cursor.fetchall()}

        # Check that all expected columns exist (may have more)
        missing = expected_columns - actual_columns
        assert not missing, f"Missing columns: {missing}"


class TestInsertDownload:
    """Tests for insert_download method."""

    def test_insert_download_basic(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test basic download insertion."""
        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/image.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="image.jpg",
        )

        assert download_id > 0

    def test_insert_download_returns_id(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test that insert returns a unique ID."""
        id1 = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/a.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="a.jpg",
        )
        id2 = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/b.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="b.jpg",
        )

        assert id1 != id2
        assert id2 == id1 + 1

    def test_insert_download_with_url_id(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test insertion with url_id reference."""
        # First insert a URL to reference
        with case_data._use_evidence_conn(evidence_id):
            with case_data._connect() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO urls (evidence_id, url, domain, discovered_by)
                    VALUES (?, ?, ?, ?)
                    """,
                    (evidence_id, "https://example.com/video.mp4", "example.com", "test"),
                )
                conn.commit()
                url_id = cursor.lastrowid

        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/video.mp4",
            domain="example.com",
            file_type="video",
            file_extension=".mp4",
            url_id=url_id,
            filename="video.mp4",
        )

        # Verify by fetching
        download = case_data.get_download(evidence_id, download_id)
        assert download is not None
        assert download["url"] == "https://example.com/video.mp4"
        assert download["file_type"] == "video"
        assert download["url_id"] == url_id


class TestUpdateDownloadStatus:
    """Tests for update_download_status method."""

    def test_update_status_to_completed(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test updating download status to completed."""
        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/file.zip",
            domain="example.com",
            file_type="archive",
            file_extension=".zip",
            filename="file.zip",
        )

        case_data.update_download_status(
            evidence_id=evidence_id,
            download_id=download_id,
            status="completed",
            dest_path="evidences/ev-testevidence/_downloads/example.com/file.zip",
            size_bytes=5000,
            md5="testmd5",
            sha256="testsha256",
        )

        download = case_data.get_download(evidence_id, download_id)
        assert download["status"] == "completed"
        assert download["size_bytes"] == 5000
        assert download["md5"] == "testmd5"

    def test_update_status_to_failed(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test updating download status to failed with error message."""
        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/missing.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="missing.jpg",
        )

        case_data.update_download_status(
            evidence_id=evidence_id,
            download_id=download_id,
            status="failed",
            error_message="404 Not Found",
        )

        download = case_data.get_download(evidence_id, download_id)
        assert download["status"] == "failed"
        assert download["error_message"] == "404 Not Found"


class TestUpdateDownloadImageMetadata:
    """Tests for update_download_image_metadata method."""

    def test_update_image_metadata(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test updating pHash, EXIF, and dimensions."""
        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/photo.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="photo.jpg",
        )

        # Update to completed first
        case_data.update_download_status(
            evidence_id, download_id, "completed",
            dest_path="photo.jpg", size_bytes=1000,
        )

        exif_data = {"DateTimeOriginal": "2024:01:15 10:30:00", "Camera": "TestCam"}

        case_data.update_download_image_metadata(
            evidence_id=evidence_id,
            download_id=download_id,
            phash="abcd1234efgh5678",
            exif_json=json.dumps(exif_data),
            width=4000,
            height=3000,
        )

        download = case_data.get_download(evidence_id, download_id)
        assert download["phash"] == "abcd1234efgh5678"
        assert download["width"] == 4000
        assert download["height"] == 3000

        parsed_exif = json.loads(download["exif_json"])
        assert parsed_exif["Camera"] == "TestCam"


class TestListDownloads:
    """Tests for list_downloads and filtering."""

    @pytest.fixture
    def populated_downloads(
        self, case_data: CaseDataAccess, evidence_id: int
    ) -> list:
        """Create several downloads for testing filters."""
        downloads = []

        # Image downloads
        for i in range(3):
            did = case_data.insert_download(
                evidence_id=evidence_id,
                url=f"https://example.com/img{i}.jpg",
                domain="example.com",
                file_type="image",
                file_extension=".jpg",
                filename=f"img{i}.jpg",
            )
            case_data.update_download_status(evidence_id, did, "completed")
            downloads.append(did)

        # Video download
        did = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://other.com/video.mp4",
            domain="other.com",
            file_type="video",
            file_extension=".mp4",
            filename="video.mp4",
        )
        case_data.update_download_status(evidence_id, did, "completed")
        downloads.append(did)

        # Failed download
        did = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/missing.pdf",
            domain="example.com",
            file_type="document",
            file_extension=".pdf",
            filename="missing.pdf",
        )
        case_data.update_download_status(
            evidence_id, did, "failed", error_message="Not found"
        )
        downloads.append(did)

        return downloads

    def test_list_all_downloads(
        self, case_data: CaseDataAccess, evidence_id: int, populated_downloads: list
    ):
        """Test listing all downloads."""
        downloads = case_data.list_downloads(evidence_id)
        assert len(downloads) == 5

    def test_list_by_file_type(
        self, case_data: CaseDataAccess, evidence_id: int, populated_downloads: list
    ):
        """Test filtering by file type."""
        images = case_data.list_downloads(evidence_id, file_type="image")
        assert len(images) == 3

        videos = case_data.list_downloads(evidence_id, file_type="video")
        assert len(videos) == 1

    def test_list_by_status(
        self, case_data: CaseDataAccess, evidence_id: int, populated_downloads: list
    ):
        """Test filtering by status."""
        completed = case_data.list_downloads(evidence_id, status_filter="completed")
        assert len(completed) == 4

        failed = case_data.list_downloads(evidence_id, status_filter="failed")
        assert len(failed) == 1

    def test_list_by_domain(
        self, case_data: CaseDataAccess, evidence_id: int, populated_downloads: list
    ):
        """Test filtering by domain."""
        example_downloads = case_data.list_downloads(
            evidence_id, domain_filter="example.com"
        )
        assert len(example_downloads) == 4

        other_downloads = case_data.list_downloads(
            evidence_id, domain_filter="other.com"
        )
        assert len(other_downloads) == 1


class TestCountDownloads:
    """Tests for count_downloads method."""

    def test_count_downloads_all(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test counting all downloads."""
        # Add some downloads
        for i in range(3):
            case_data.insert_download(
                evidence_id=evidence_id,
                url=f"https://example.com/img{i}.jpg",
                domain="example.com",
                file_type="image",
                file_extension=".jpg",
                filename=f"img{i}.jpg",
            )

        case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/doc.pdf",
            domain="example.com",
            file_type="document",
            file_extension=".pdf",
            filename="doc.pdf",
        )

        # count_downloads returns an int, not a dict
        count = case_data.count_downloads(evidence_id)
        assert count == 4

    def test_count_downloads_by_type(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test counting downloads by file type."""
        # Add some downloads
        for i in range(3):
            case_data.insert_download(
                evidence_id=evidence_id,
                url=f"https://example.com/img{i}.jpg",
                domain="example.com",
                file_type="image",
                file_extension=".jpg",
                filename=f"img{i}.jpg",
            )

        case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/doc.pdf",
            domain="example.com",
            file_type="document",
            file_extension=".pdf",
            filename="doc.pdf",
        )

        image_count = case_data.count_downloads(evidence_id, file_type="image")
        assert image_count == 3

        doc_count = case_data.count_downloads(evidence_id, file_type="document")
        assert doc_count == 1

        video_count = case_data.count_downloads(evidence_id, file_type="video")
        assert video_count == 0


class TestGetDownloadByPath:
    """Tests for get_download_by_path method."""

    def test_get_by_path_found(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test finding download by path."""
        dest_path = "evidences/ev-testevidence/_downloads/example.com/test.jpg"

        download_id = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/test.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="test.jpg",
        )
        case_data.update_download_status(
            evidence_id, download_id, "completed", dest_path=dest_path
        )

        found = case_data.get_download_by_path(evidence_id, dest_path)
        assert found is not None
        assert found["id"] == download_id

    def test_get_by_path_not_found(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test that missing path returns None."""
        found = case_data.get_download_by_path(evidence_id, "nonexistent/path.jpg")
        assert found is None


class TestDownloadStats:
    """Tests for get_download_stats method."""

    def test_download_stats(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test aggregate download statistics."""
        # Add completed downloads
        for i in range(3):
            did = case_data.insert_download(
                evidence_id=evidence_id,
                url=f"https://example.com/file{i}.jpg",
                domain="example.com",
                file_type="image",
                file_extension=".jpg",
                filename=f"file{i}.jpg",
            )
            case_data.update_download_status(
                evidence_id, did, "completed",
                size_bytes=1000 * (i + 1)
            )

        # Add a failed download
        did = case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/bad.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="bad.jpg",
        )
        case_data.update_download_status(
            evidence_id, did, "failed", error_message="Error"
        )

        # get_download_stats returns status counts like {"completed": 3, "failed": 1}
        stats = case_data.get_download_stats(evidence_id)
        assert stats.get("completed", 0) == 3
        assert stats.get("failed", 0) == 1


class TestListDownloadDomains:
    """Tests for list_download_domains method."""

    def test_list_domains(
        self, case_data: CaseDataAccess, evidence_id: int
    ):
        """Test listing unique domains."""
        domains = ["example.com", "other.com", "third.com"]

        for domain in domains:
            case_data.insert_download(
                evidence_id=evidence_id,
                url=f"https://{domain}/file.jpg",
                domain=domain,
                file_type="image",
                file_extension=".jpg",
                filename="file.jpg",
            )

        # Add duplicate domain
        case_data.insert_download(
            evidence_id=evidence_id,
            url="https://example.com/other.jpg",
            domain="example.com",
            file_type="image",
            file_extension=".jpg",
            filename="other.jpg",
        )

        result = case_data.list_download_domains(evidence_id)
        assert set(result) == set(domains)
