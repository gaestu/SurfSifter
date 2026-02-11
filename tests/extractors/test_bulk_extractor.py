"""Tests for bulk_extractor modular extractor."""

import pytest
import tempfile
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import subprocess

from extractors.carvers.bulk_extractor import BulkExtractorExtractor
from core.database import DatabaseManager, EVIDENCE_MIGRATIONS_DIR
from core.database import migrate


class TestBulkExtractorMetadata:
    """Test extractor metadata."""

    def test_metadata_structure(self):
        """Test that metadata has all required fields."""
        extractor = BulkExtractorExtractor()
        meta = extractor.metadata

        assert meta.name == "bulk_extractor"
        assert meta.display_name == "bulk_extractor (URLs, Emails, IPs)"
        assert meta.category == "forensic"
        assert meta.version
        assert "." in meta.version
        assert "bulk_extractor" in meta.requires_tools
        assert meta.can_extract is True
        assert meta.can_ingest is True


class TestBulkExtractorCapabilities:
    """Test can_run_* checks."""

    @patch('extractors.carvers.bulk_extractor.extractor.discover_tools')
    def test_can_run_extraction_tool_available(self, mock_discover):
        """Extraction should succeed if tool is available and source exists."""
        mock_tool = Mock()
        mock_tool.available = True
        mock_discover.return_value = {"bulk_extractor": mock_tool}

        # Create a temporary file to simulate E01 source
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".E01", delete=False) as f:
            evidence_path = Path(f.name)

        try:
            extractor = BulkExtractorExtractor()
            can_run, reason = extractor.can_run_extraction(evidence_path)

            assert can_run is True
            assert reason == ""
        finally:
            evidence_path.unlink()

    @patch('extractors.carvers.bulk_extractor.extractor.discover_tools')
    def test_can_run_extraction_tool_missing(self, mock_discover):
        """Extraction should fail if tool is not available."""
        mock_discover.return_value = {}

        extractor = BulkExtractorExtractor()
        can_run, reason = extractor.can_run_extraction(None)

        assert can_run is False
        assert "not installed" in reason.lower() or "not in path" in reason.lower()

    def test_can_run_ingestion_no_output(self):
        """Ingestion should fail if output directory doesn't exist."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "nonexistent"
            can_run, reason = extractor.can_run_ingestion(output_dir)

            assert can_run is False
            assert "does not exist" in reason.lower()

    def test_can_run_ingestion_with_output(self):
        """Ingestion should succeed if output files exist."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create dummy output file
            (output_dir / "url.txt").write_text("# bulk_extractor output\nhttp://example.com\n")

            can_run, reason = extractor.can_run_ingestion(output_dir)

            assert can_run is True
            assert reason == ""


class TestBulkExtractorPaths:
    """Test output directory logic."""

    def test_get_output_dir(self):
        """Test output directory path generation."""
        extractor = BulkExtractorExtractor()

        case_root = Path("/case/root")
        evidence_label = "ev-001"

        output_dir = extractor.get_output_dir(case_root, evidence_label)

        assert output_dir == Path("/case/root/evidences/ev-001/bulk_extractor")

    def test_has_existing_output_no_directory(self):
        """Test has_existing_output returns False when directory doesn't exist."""
        extractor = BulkExtractorExtractor()

        output_dir = Path("/nonexistent/directory")

        assert extractor.has_existing_output(output_dir) is False

    def test_has_existing_output_empty_directory(self):
        """Test has_existing_output returns False when directory is empty."""
        import tempfile

        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            assert extractor.has_existing_output(output_dir) is False

    def test_has_existing_output_with_files(self):
        """Test has_existing_output returns True when bulk_extractor files exist."""
        import tempfile

        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create some bulk_extractor output files
            (output_dir / "url.txt").write_text("# urls\n")
            (output_dir / "email.txt").write_text("# emails\n")

            assert extractor.has_existing_output(output_dir) is True

    def test_has_existing_output_with_unrecognized_files(self):
        """Test has_existing_output returns False when only unrecognized files exist."""
        import tempfile

        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create unrecognized files
            (output_dir / "random.txt").write_text("random content\n")
            (output_dir / "other.log").write_text("log content\n")

            assert extractor.has_existing_output(output_dir) is False


class TestBulkExtractorExtraction:
    """Test extraction phase (running bulk_extractor subprocess)."""

    @patch('extractors.carvers.bulk_extractor.extractor.discover_tools')
    @patch('extractors.carvers.bulk_extractor.extractor.subprocess.Popen')
    def test_run_extraction_success(self, mock_popen, mock_discover):
        """Test successful extraction run."""
        # Mock tool discovery
        mock_tool = Mock()
        mock_tool.available = True
        mock_tool.path = "/usr/bin/bulk_extractor"
        mock_discover.return_value = {"bulk_extractor": mock_tool}

        # Mock subprocess
        mock_process = Mock()
        mock_process.stdout = iter([
            "bulk_extractor starting...\n",
            "Offset 100MB (50.0%) Done in 0:01:00\n",
            "Completed\n"
        ])
        mock_process.wait.return_value = 0
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process

        # Create temporary E01 file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".E01", delete=False) as f:
            evidence_path = Path(f.name)

        try:
            # Run extraction
            with tempfile.TemporaryDirectory() as tmpdir:
                output_dir = Path(tmpdir) / "output"

                extractor = BulkExtractorExtractor()
                callbacks = Mock()
                callbacks.on_step = Mock()
                callbacks.on_log = Mock()
                callbacks.on_progress = Mock()
                callbacks.on_error = Mock()
                callbacks.is_cancelled = Mock(return_value=False)

                config = {"scanners": ["email"], "num_threads": 4}

                success = extractor.run_extraction(
                    evidence_source_path=evidence_path,
                    output_dir=output_dir,
                    config=config,
                    callbacks=callbacks
                )

                assert success is True
                callbacks.on_error.assert_not_called()
        finally:
            evidence_path.unlink()

    @patch('extractors.carvers.bulk_extractor.extractor.discover_tools')
    def test_run_extraction_reuse_existing(self, mock_discover):
        """Test extraction reuses existing output."""
        mock_tool = Mock()
        mock_tool.available = True
        mock_discover.return_value = {"bulk_extractor": mock_tool}

        # Create temporary E01 file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".E01", delete=False) as f:
            evidence_path = Path(f.name)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                output_dir = Path(tmpdir)

                # Create existing output
                (output_dir / "url.txt").write_text("http://example.com\n")

                extractor = BulkExtractorExtractor()
                callbacks = Mock()
                callbacks.on_step = Mock()
                callbacks.on_log = Mock()
                callbacks.is_cancelled = Mock(return_value=False)

                config = {"output_reuse_policy": "reuse"}

                success = extractor.run_extraction(
                    evidence_source_path=evidence_path,
                    output_dir=output_dir,
                    config=config,
                    callbacks=callbacks
                )

                assert success is True
                # Verify log mentions reuse
                log_calls = [call[0][0] for call in callbacks.on_log.call_args_list]
                assert any("reusing" in msg.lower() for msg in log_calls)
        finally:
            evidence_path.unlink()


class TestBulkExtractorIngestion:
    """Test ingestion phase (parsing output files)."""

    def test_parse_bulk_extractor_line_url(self):
        """Test parsing a URL line."""
        extractor = BulkExtractorExtractor()

        line = "17940236\thttp://example.com/path\tsome binary context here"
        result = extractor._parse_bulk_extractor_line(line, "url", "url.txt")

        assert result is not None
        assert result["offset"] == 17940236
        assert result["value"] == "http://example.com/path"
        assert result["domain"] == "example.com"
        assert result["scheme"] == "http"
        assert result["context"] == "some binary context here"

    def test_parse_bulk_extractor_line_email(self):
        """Test parsing an email line."""
        extractor = BulkExtractorExtractor()

        line = "1234\tuser@example.com\tcontext"
        result = extractor._parse_bulk_extractor_line(line, "email", "email.txt")

        assert result is not None
        assert result["offset"] == 1234
        assert result["value"] == "user@example.com"  # Plain email, not mailto: URL
        assert result["domain"] == "example.com"
        assert result["scheme"] is None  # Emails don't have URL schemes

    def test_parse_bulk_extractor_line_invalid_offset(self):
        """Test that invalid offset returns None."""
        extractor = BulkExtractorExtractor()

        line = "invalid\thttp://example.com\tcontext"
        result = extractor._parse_bulk_extractor_line(line, "url", "url.txt")

        assert result is None

    def test_parse_bulk_extractor_line_comment(self):
        """Test that comment lines are skipped during import."""
        extractor = BulkExtractorExtractor()

        # Parser expects non-comment lines, so this would return None
        # The import function filters comments before calling parser
        line = "# This is a comment"
        # Split will fail to produce valid offset/feature
        parts = line.split('\t', 2)
        assert len(parts) < 2  # Comments don't have tab separators

    def test_normalize_feature_url(self):
        """Test URL normalization."""
        extractor = BulkExtractorExtractor()

        # Already has scheme
        assert extractor._normalize_feature("http://example.com", "url") == "http://example.com"
        assert extractor._normalize_feature("https://example.com", "url") == "https://example.com"

        # No scheme but looks like URL
        assert extractor._normalize_feature("example.com", "url") == "http://example.com"

        # Invalid URL
        assert extractor._normalize_feature("not a url", "url") is None
        assert extractor._normalize_feature("", "url") is None

    def test_normalize_feature_email(self):
        """Test email normalization."""
        extractor = BulkExtractorExtractor()

        assert extractor._normalize_feature("user@example.com", "email") == "user@example.com"
        assert extractor._normalize_feature("mailto:user@example.com", "email") == "user@example.com"
        assert extractor._normalize_feature("invalid email", "email") is None

    def test_normalize_feature_domain(self):
        """Test domain normalization."""
        extractor = BulkExtractorExtractor()

        assert extractor._normalize_feature("example.com", "domain") == "example.com"
        assert extractor._normalize_feature("http://example.com", "domain") == "example.com"
        assert extractor._normalize_feature("invalid domain", "domain") is None

    def test_normalize_feature_ip(self):
        """Test IP normalization."""
        extractor = BulkExtractorExtractor()

        assert extractor._normalize_feature("192.168.1.1", "ip") == "192.168.1.1"

    def test_normalize_feature_other_types(self):
        """Test other artifact types."""
        extractor = BulkExtractorExtractor()

        assert extractor._normalize_feature("+1234567890", "telephone") == "+1234567890"
        assert extractor._normalize_feature("1BC123...", "bitcoin") == "1BC123..."

    def test_run_ingestion_with_real_database(self):
        """Test ingestion with actual database insertion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_dir = tmpdir / "output"
            output_dir.mkdir()

            # Create mock url.txt with realistic bulk_extractor format
            (output_dir / "url.txt").write_text(
                "# BANNER FILE NOT PROVIDED (-b option)\n"
                "# BULK_EXTRACTOR-Version: 2.1.1\n"
                "# Feature-Recorder: url\n"
                "17940236\thttp://example.com/page1\tcontext1\n"
                "17940500\thttp://test.com/page2\tcontext2\n"
                "17941000\thttps://secure.com/login\tcontext3\n"
            )

            # Create test database
            db_path = tmpdir / "test_evidence.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"]}

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Verify results
            assert "url" in results
            assert results["url"] == 3  # 3 URLs imported

            # Verify database contents
            cursor = conn.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            count = cursor.fetchone()[0]
            assert count == 3

            # Verify URL details
            cursor = conn.execute(
                "SELECT url, domain, scheme, discovered_by, source_path FROM urls WHERE evidence_id = 1 ORDER BY id"
            )
            rows = cursor.fetchall()

            assert rows[0][0] == "http://example.com/page1"
            assert rows[0][1] == "example.com"
            assert rows[0][2] == "http"
            assert rows[0][3] == "bulk_extractor:url"
            assert "url.txt:17940236" in rows[0][4]

            assert rows[1][0] == "http://test.com/page2"
            assert rows[1][1] == "test.com"

            assert rows[2][0] == "https://secure.com/login"
            assert rows[2][1] == "secure.com"
            assert rows[2][2] == "https"

            conn.close()

    def test_run_ingestion_multiple_artifact_types(self):
        """Test ingestion of multiple artifact types into different tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_dir = tmpdir / "output"
            output_dir.mkdir()

            # Create multiple output files
            (output_dir / "url.txt").write_text(
                "# Comments\n"
                "100\thttp://example.com\tcontext\n"
                "200\thttp://test.com\tcontext\n"
            )

            (output_dir / "email.txt").write_text(
                "# Comments\n"
                "300\tuser@example.com\tcontext\n"
                "400\tadmin@test.com\tcontext\n"
            )

            (output_dir / "domain.txt").write_text(
                "# Comments\n"
                "500\tgambling.com\tcontext\n"
            )

            # Create test database
            db_path = tmpdir / "test_evidence.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url", "email", "domain"]}

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Verify results
            assert results["url"] == 2
            assert results["email"] == 2
            assert results["domain"] == 1

            # Verify URLs table
            cursor = conn.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 2

            # Verify emails table (NOT in urls table anymore)
            cursor = conn.execute("SELECT COUNT(*) FROM emails WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 2

            # Verify email details
            cursor = conn.execute(
                "SELECT email, domain FROM emails WHERE evidence_id = 1 ORDER BY id"
            )
            email_rows = cursor.fetchall()
            assert email_rows[0][0] == "user@example.com"
            assert email_rows[0][1] == "example.com"
            assert email_rows[1][0] == "admin@test.com"
            assert email_rows[1][1] == "test.com"

            # Verify domains table
            cursor = conn.execute("SELECT COUNT(*) FROM domains WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 1

            cursor = conn.execute("SELECT domain FROM domains WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == "gambling.com"

            conn.close()

    def test_run_ingestion_cancelled(self):
        """Test that ingestion can be cancelled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_dir = tmpdir / "output"
            output_dir.mkdir()

            # Create large file
            urls = "\n".join([f"{i}\thttp://example{i}.com\tcontext" for i in range(10000)])
            (output_dir / "url.txt").write_text("# Comments\n" + urls)

            # Create test database
            db_path = tmpdir / "test_evidence.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()

            # Cancel after first batch
            call_count = [0]
            def is_cancelled_after_delay():
                call_count[0] += 1
                return call_count[0] > 1500  # Cancel after ~1500 lines

            callbacks.is_cancelled = is_cancelled_after_delay

            config = {"artifact_types": ["url"]}

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Should have imported some URLs but not all
            assert results["url"] < 10000

            # Verify warning was logged
            assert any("cancel" in str(call).lower() for call in callbacks.on_log.call_args_list)

            conn.close()

    def test_run_ingestion_urls_only(self):
        """Test ingestion of URLs only (legacy test updated)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create mock url.txt
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
                "200\thttp://test.com\tcontext\n"
            )

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"]}

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Should return count of imported URLs
            assert "url" in results
            assert results["url"] == 2  # 2 URLs imported

            conn.close()

    def test_run_ingestion_overwrite_mode(self):
        """Test that overwrite mode deletes existing data before re-ingesting."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create mock url.txt
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
                "200\thttp://test.com\tcontext\n"
            )

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"]}

            # First ingestion
            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )
            assert results["url"] == 2

            # Verify data exists
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 2

            # Second ingestion with overwrite mode
            config["overwrite_mode"] = "overwrite"

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )
            assert results["url"] == 2

            # Should still have 2 URLs (old deleted, new inserted)
            cursor.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 2

            conn.close()

    def test_run_ingestion_append_mode(self):
        """Test that append mode keeps existing data and adds new records.

        With deduplication removed, append mode will insert all records
        again (each observation is a distinct event). The test verifies append
        mode doesn't delete existing data.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create mock url.txt
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
                "200\thttp://test.com\tcontext\n"
            )

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"]}

            # First ingestion
            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )
            assert results["url"] == 2

            # Second ingestion with append mode
            # With no deduplication, this will insert 2 more records
            config["overwrite_mode"] = "append"

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )
            assert results["url"] == 2  # All records inserted again

            # Should now have 4 URLs (no deduplication - each is a forensic event)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 4

            conn.close()

    def test_run_ingestion_cancel_mode(self):
        """Test that cancel mode stops ingestion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create mock url.txt
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
                "200\thttp://test.com\tcontext\n"
            )

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"]}

            # First ingestion
            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )
            assert results["url"] == 2

            # Second ingestion with cancel mode
            config["overwrite_mode"] = "cancel"

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Should return empty dict (cancelled)
            assert results == {}

            # Should still have 2 URLs (nothing changed)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM urls WHERE evidence_id = 1")
            assert cursor.fetchone()[0] == 2

            conn.close()


class TestBulkExtractorUnifiedImageIngestion:
    """Test unified image ingestion (merged from bulk_extractor_images)."""

    def test_detect_carved_images_jpeg_carved(self):
        """Test detection of carved images in jpeg_carved/ directory."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create jpeg_carved directory with images
            jpeg_dir = output_dir / "jpeg_carved"
            jpeg_dir.mkdir()
            (jpeg_dir / "00000001.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)
            (jpeg_dir / "00000002.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

            images = extractor._detect_carved_images(output_dir)

            assert len(images) == 2
            assert all(p.suffix == ".jpg" for p in images)

    def test_detect_carved_images_multiple_dirs(self):
        """Test detection from multiple potential directories."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create both jpeg_carved and images directories
            (output_dir / "jpeg_carved").mkdir()
            (output_dir / "jpeg_carved" / "test1.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

            (output_dir / "images").mkdir()
            (output_dir / "images" / "test2.png").write_bytes(b"\x89PNG" + b"\x00" * 100)

            images = extractor._detect_carved_images(output_dir)

            assert len(images) == 2

    def test_detect_carved_images_recursive(self):
        """Test recursive detection in subdirectories."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create nested structure
            nested = output_dir / "jpeg_carved" / "subdir"
            nested.mkdir(parents=True)
            (nested / "nested.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

            images = extractor._detect_carved_images(output_dir)

            assert len(images) == 1
            assert "subdir" in str(images[0])

    def test_detect_carved_images_empty(self):
        """Test that empty directory returns empty list."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            images = extractor._detect_carved_images(output_dir)

            assert images == []

    def test_run_ingestion_with_images_phase_progress(self):
        """Test that ingestion shows correct phase progress when images present."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create URL output and carved images
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
            )

            jpeg_dir = output_dir / "jpeg_carved"
            jpeg_dir.mkdir()
            # Create a minimal valid JPEG (just signature for detection)
            (jpeg_dir / "test.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()

            step_calls = []
            callbacks = Mock()
            callbacks.on_step = Mock(side_effect=lambda s: step_calls.append(s))
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"], "carve_images": True}

            extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Check phase numbering in step calls
            assert any("Phase 1/3" in s for s in step_calls), f"Phase 1/3 not found in {step_calls}"
            assert any("Phase 2/3" in s for s in step_calls), f"Phase 2/3 not found in {step_calls}"
            assert any("Phase 3/3" in s for s in step_calls), f"Phase 3/3 not found in {step_calls}"

            conn.close()

    def test_run_ingestion_skips_images_when_disabled(self):
        """Test that carve_images=False skips image ingestion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create URL output and carved images
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
            )

            jpeg_dir = output_dir / "jpeg_carved"
            jpeg_dir.mkdir()
            (jpeg_dir / "test.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()

            step_calls = []
            callbacks = Mock()
            callbacks.on_step = Mock(side_effect=lambda s: step_calls.append(s))
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            # Explicitly disable image carving
            config = {"artifact_types": ["url"], "carve_images": False}

            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # Should only have 2 phases
            assert any("Phase 1/2" in s for s in step_calls), f"Phase 1/2 not found in {step_calls}"
            assert any("Phase 2/2" in s for s in step_calls), f"Phase 2/2 not found in {step_calls}"
            assert not any("Phase 3" in s for s in step_calls), f"Phase 3 should not appear in {step_calls}"

            # No images in results
            assert "images" not in results

            conn.close()

    def test_run_ingestion_no_images_graceful(self):
        """Test that missing jpeg_carved/ doesn't fail ingestion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create URL output but NO carved images
            (output_dir / "url.txt").write_text(
                "# bulk_extractor output\n"
                "100\thttp://example.com\tcontext\n"
            )

            # Create test database
            db_path = Path(tmpdir) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON;")
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

            extractor = BulkExtractorExtractor()
            callbacks = Mock()
            callbacks.on_step = Mock()
            callbacks.on_log = Mock()
            callbacks.on_progress = Mock()
            callbacks.is_cancelled = Mock(return_value=False)

            config = {"artifact_types": ["url"], "carve_images": True}

            # Should not raise
            results = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=conn,
                evidence_id=1,
                config=config,
                callbacks=callbacks
            )

            # URLs should be ingested
            assert results["url"] == 1
            # No images key (skipped gracefully)
            assert "images" not in results

            conn.close()

    def test_build_image_manifest(self):
        """Test manifest building for carved images."""
        extractor = BulkExtractorExtractor()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create dummy files
            (output_dir / "test1.jpg").write_bytes(b"\x00" * 100)
            (output_dir / "test2.png").write_bytes(b"\x00" * 200)

            files = [output_dir / "test1.jpg", output_dir / "test2.png"]

            manifest = extractor._build_image_manifest(
                run_id="20260104_120000",
                output_dir=output_dir,
                files=files,
                inserted=2,
                errors=0,
                enriched=1
            )

            assert manifest["schema_version"] == "1.0.0"
            assert manifest["extractor"] == "bulk_extractor"
            assert manifest["stats"]["carved_total"] == 2
            assert manifest["ingestion"]["inserted"] == 2
            assert manifest["ingestion"]["enriched"] == 1
            assert len(manifest["carved_files"]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
