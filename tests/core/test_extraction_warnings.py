"""
Test suite for Extraction Warnings feature.

Tests database helpers, ExtractionWarningCollector, and discovery utilities
for tracking unknown schemas, parse errors, and other findings during extraction.

Initial implementation.
"""

from datetime import datetime, timezone
import json
import sqlite3
import pytest

from core.database import (
    EVIDENCE_MIGRATIONS_DIR,
    migrate,
)
from core.database.helpers.extraction_warnings import (
    insert_extraction_warning,
    insert_extraction_warnings,
    get_extraction_warnings,
    get_extraction_warnings_count,
    get_extraction_warnings_summary,
    get_extraction_warnings_by_run,
    delete_extraction_warnings_by_run,
    delete_extraction_warnings_by_extractor,
    get_distinct_warning_extractors,
    get_warning_count_for_extractor,
    # Constants
    WARNING_TYPE_UNKNOWN_TABLE,
    WARNING_TYPE_UNKNOWN_COLUMN,
    WARNING_TYPE_UNKNOWN_TOKEN_TYPE,
    WARNING_TYPE_JSON_PARSE_ERROR,
    CATEGORY_DATABASE,
    CATEGORY_JSON,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    SEVERITY_ERROR,
)


@pytest.fixture
def evidence_db(tmp_path):
    """Create a temporary evidence database with schema."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    # Apply all evidence migrations
    migrate(conn, EVIDENCE_MIGRATIONS_DIR)

    yield conn
    conn.close()


class TestInsertExtractionWarning:
    """Tests for single warning insertion."""

    def test_insert_basic_warning(self, evidence_db):
        """Insert minimal required fields."""
        row_id = insert_extraction_warning(
            evidence_db,
            evidence_id=1,
            run_id="test_run_001",
            extractor_name="chromium_autofill",
            warning_type=WARNING_TYPE_UNKNOWN_TABLE,
            item_name="new_autofill_table",
        )

        assert row_id > 0

        # Verify record
        row = evidence_db.execute(
            "SELECT * FROM extraction_warnings WHERE id = ?", (row_id,)
        ).fetchone()

        assert row["evidence_id"] == 1
        assert row["extractor_name"] == "chromium_autofill"
        assert row["run_id"] == "test_run_001"
        assert row["warning_type"] == WARNING_TYPE_UNKNOWN_TABLE
        assert row["item_name"] == "new_autofill_table"
        assert row["severity"] == SEVERITY_WARNING  # default
        assert row["created_at_utc"] is not None

    def test_insert_with_all_fields(self, evidence_db):
        """Insert with all optional fields."""
        context = {"columns": ["id", "value", "timestamp"]}

        row_id = insert_extraction_warning(
            evidence_db,
            evidence_id=1,
            run_id="test_run_002",
            extractor_name="chromium_autofill",
            warning_type=WARNING_TYPE_UNKNOWN_TABLE,
            item_name="addresses_v2",
            severity=SEVERITY_WARNING,
            category=CATEGORY_DATABASE,
            artifact_type="autofill",
            source_file="/Users/test/AppData/Local/Google/Chrome/User Data/Default/Web Data",
            item_value="3 columns",
            context_json=context,
        )

        assert row_id > 0

        row = evidence_db.execute(
            "SELECT * FROM extraction_warnings WHERE id = ?", (row_id,)
        ).fetchone()

        assert row["severity"] == SEVERITY_WARNING
        assert row["category"] == CATEGORY_DATABASE
        assert row["artifact_type"] == "autofill"
        assert row["source_file"] is not None
        assert row["item_value"] == "3 columns"
        assert json.loads(row["context_json"]) == context


class TestInsertExtractionWarnings:
    """Tests for batch warning insertion."""

    def test_insert_batch_warnings(self, evidence_db):
        """Insert multiple warnings in a batch."""
        warnings = [
            {
                "run_id": "batch_run_001",
                "extractor_name": "chromium_autofill",
                "warning_type": WARNING_TYPE_UNKNOWN_TABLE,
                "item_name": "table1",
                "severity": SEVERITY_WARNING,
                "category": CATEGORY_DATABASE,
            },
            {
                "run_id": "batch_run_001",
                "extractor_name": "chromium_autofill",
                "warning_type": WARNING_TYPE_UNKNOWN_COLUMN,
                "item_name": "new_column",
                "severity": SEVERITY_INFO,
                "category": CATEGORY_DATABASE,
            },
            {
                "run_id": "batch_run_001",
                "extractor_name": "chromium_autofill",
                "warning_type": WARNING_TYPE_JSON_PARSE_ERROR,
                "item_name": "Preferences",
                "item_value": "Unexpected token at line 42",
                "severity": SEVERITY_ERROR,
                "category": CATEGORY_JSON,
            },
        ]

        count = insert_extraction_warnings(evidence_db, evidence_id=1, warnings=warnings)

        assert count == 3

        # Verify all were inserted
        rows = evidence_db.execute(
            "SELECT * FROM extraction_warnings WHERE evidence_id = 1 ORDER BY id"
        ).fetchall()

        assert len(rows) == 3
        assert rows[0]["warning_type"] == WARNING_TYPE_UNKNOWN_TABLE
        assert rows[1]["warning_type"] == WARNING_TYPE_UNKNOWN_COLUMN
        assert rows[2]["warning_type"] == WARNING_TYPE_JSON_PARSE_ERROR

    def test_insert_empty_batch(self, evidence_db):
        """Inserting empty batch returns 0."""
        count = insert_extraction_warnings(evidence_db, evidence_id=1, warnings=[])
        assert count == 0


class TestGetExtractionWarnings:
    """Tests for querying warnings."""

    @pytest.fixture
    def seeded_db(self, evidence_db):
        """Seed database with test warnings."""
        warnings = [
            # Extractor 1, run 1
            {"run_id": "run1", "extractor_name": "chromium_autofill", "warning_type": WARNING_TYPE_UNKNOWN_TABLE, "item_name": "t1", "severity": SEVERITY_WARNING, "category": CATEGORY_DATABASE},
            {"run_id": "run1", "extractor_name": "chromium_autofill", "warning_type": WARNING_TYPE_UNKNOWN_COLUMN, "item_name": "c1", "severity": SEVERITY_INFO, "category": CATEGORY_DATABASE},
            {"run_id": "run1", "extractor_name": "chromium_autofill", "warning_type": WARNING_TYPE_JSON_PARSE_ERROR, "item_name": "Prefs", "severity": SEVERITY_ERROR, "category": CATEGORY_JSON},
            # Extractor 2, run 2
            {"run_id": "run2", "extractor_name": "firefox_history", "warning_type": WARNING_TYPE_UNKNOWN_TABLE, "item_name": "t2", "severity": SEVERITY_WARNING, "category": CATEGORY_DATABASE},
        ]
        insert_extraction_warnings(evidence_db, evidence_id=1, warnings=warnings)
        return evidence_db

    def test_get_all_warnings(self, seeded_db):
        """Get all warnings without filters."""
        results = get_extraction_warnings(seeded_db, evidence_id=1)
        assert len(results) == 4

    def test_filter_by_extractor(self, seeded_db):
        """Filter by extractor name."""
        results = get_extraction_warnings(seeded_db, evidence_id=1, extractor_name="chromium_autofill")
        assert len(results) == 3

    def test_filter_by_category(self, seeded_db):
        """Filter by category."""
        results = get_extraction_warnings(seeded_db, evidence_id=1, category=CATEGORY_JSON)
        assert len(results) == 1
        assert results[0]["warning_type"] == WARNING_TYPE_JSON_PARSE_ERROR

    def test_filter_by_severity(self, seeded_db):
        """Filter by severity."""
        results = get_extraction_warnings(seeded_db, evidence_id=1, severity=SEVERITY_ERROR)
        assert len(results) == 1
        assert results[0]["item_name"] == "Prefs"

    def test_filter_by_run_id(self, seeded_db):
        """Filter by run ID."""
        results = get_extraction_warnings(seeded_db, evidence_id=1, run_id="run2")
        assert len(results) == 1
        assert results[0]["extractor_name"] == "firefox_history"

    def test_pagination(self, seeded_db):
        """Test limit and offset pagination."""
        results = get_extraction_warnings(seeded_db, evidence_id=1, limit=2, offset=0)
        assert len(results) == 2

        results2 = get_extraction_warnings(seeded_db, evidence_id=1, limit=2, offset=2)
        assert len(results2) == 2

    def test_context_json_parsed(self, evidence_db):
        """Context JSON is parsed when retrieved."""
        context = {"columns": ["a", "b", "c"]}
        insert_extraction_warning(
            evidence_db,
            evidence_id=1,
            run_id="ctx_test",
            extractor_name="test",
            warning_type=WARNING_TYPE_UNKNOWN_TABLE,
            item_name="test_table",
            context_json=context,
        )

        results = get_extraction_warnings(evidence_db, evidence_id=1)
        assert results[0]["context_json"] == context


class TestGetExtractionWarningsSummary:
    """Tests for summary statistics."""

    @pytest.fixture
    def seeded_db(self, evidence_db):
        """Seed database with test warnings."""
        warnings = [
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_INFO, "category": CATEGORY_DATABASE},
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_INFO, "category": CATEGORY_DATABASE},
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_WARNING, "category": CATEGORY_JSON},
            {"run_id": "r1", "extractor_name": "ex2", "warning_type": "t", "item_name": "i", "severity": SEVERITY_ERROR, "category": CATEGORY_JSON},
        ]
        insert_extraction_warnings(evidence_db, evidence_id=1, warnings=warnings)
        return evidence_db

    def test_summary_totals(self, seeded_db):
        """Summary has correct totals."""
        summary = get_extraction_warnings_summary(seeded_db, evidence_id=1)

        assert summary["total"] == 4
        assert summary["by_severity"]["info"] == 2
        assert summary["by_severity"]["warning"] == 1
        assert summary["by_severity"]["error"] == 1

    def test_summary_by_category(self, seeded_db):
        """Summary groups by category."""
        summary = get_extraction_warnings_summary(seeded_db, evidence_id=1)

        assert summary["by_category"][CATEGORY_DATABASE] == 2
        assert summary["by_category"][CATEGORY_JSON] == 2

    def test_summary_by_extractor(self, seeded_db):
        """Summary groups by extractor."""
        summary = get_extraction_warnings_summary(seeded_db, evidence_id=1)

        assert summary["by_extractor"]["ex1"] == 3
        assert summary["by_extractor"]["ex2"] == 1


class TestDeleteExtractionWarnings:
    """Tests for deleting warnings."""

    @pytest.fixture
    def seeded_db(self, evidence_db):
        """Seed database with test warnings."""
        warnings = [
            {"run_id": "run1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i"},
            {"run_id": "run1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i"},
            {"run_id": "run2", "extractor_name": "ex1", "warning_type": "t", "item_name": "i"},
            {"run_id": "run1", "extractor_name": "ex2", "warning_type": "t", "item_name": "i"},
        ]
        insert_extraction_warnings(evidence_db, evidence_id=1, warnings=warnings)
        return evidence_db

    def test_delete_by_run(self, seeded_db):
        """Delete warnings by run ID."""
        deleted = delete_extraction_warnings_by_run(seeded_db, evidence_id=1, extractor_name="ex1", run_id="run1")

        assert deleted == 2
        assert get_extraction_warnings_count(seeded_db, evidence_id=1) == 2

    def test_delete_by_extractor(self, seeded_db):
        """Delete all warnings for an extractor."""
        deleted = delete_extraction_warnings_by_extractor(seeded_db, evidence_id=1, extractor_name="ex1")

        assert deleted == 3
        assert get_extraction_warnings_count(seeded_db, evidence_id=1) == 1


class TestGetWarningCountForExtractor:
    """Tests for extractor warning counts."""

    def test_count_by_severity(self, evidence_db):
        """Get warning counts by severity for an extractor."""
        warnings = [
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_INFO},
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_INFO},
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_WARNING},
            {"run_id": "r1", "extractor_name": "ex1", "warning_type": "t", "item_name": "i", "severity": SEVERITY_ERROR},
        ]
        insert_extraction_warnings(evidence_db, evidence_id=1, warnings=warnings)

        counts = get_warning_count_for_extractor(evidence_db, evidence_id=1, extractor_name="ex1")

        assert counts["total"] == 4
        assert counts["info"] == 2
        assert counts["warning"] == 1
        assert counts["error"] == 1
