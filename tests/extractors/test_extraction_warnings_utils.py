"""
Test suite for Extraction Warnings shared utilities.

Tests ExtractionWarningCollector and discovery functions used by extractors
to track and report unknown schemas and parse errors.

Initial implementation.
"""

import sqlite3
import pytest

from core.database import EVIDENCE_MIGRATIONS_DIR, migrate
from core.database.helpers.extraction_warnings import (
    get_extraction_warnings,
    get_extraction_warnings_count,
)
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_columns,
    track_unknown_values,
    discover_unknown_json_keys,
    # Constants from shared module
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
    migrate(conn, EVIDENCE_MIGRATIONS_DIR)
    yield conn
    conn.close()


class TestExtractionWarningCollector:
    """Tests for ExtractionWarningCollector dataclass."""

    def test_collector_initialization(self):
        """Collector initializes with empty warnings list."""
        collector = ExtractionWarningCollector(
            extractor_name="test_extractor",
            run_id="run_001",
            evidence_id=1,
        )

        assert collector.extractor_name == "test_extractor"
        assert collector.run_id == "run_001"
        assert collector.evidence_id == 1
        assert collector._warnings == []

    def test_add_unknown_table(self):
        """Add unknown table warning."""
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id="run_001",
            evidence_id=1,
        )

        collector.add_unknown_table(
            table_name="new_autofill_v2",
            columns=["id", "value"],
            source_file="/path/to/Web Data",
        )

        assert len(collector._warnings) == 1
        w = collector._warnings[0]
        assert w.warning_type == WARNING_TYPE_UNKNOWN_TABLE
        assert w.item_name == "new_autofill_v2"
        assert w.severity == SEVERITY_WARNING
        assert w.category == CATEGORY_DATABASE
        assert w.context_json["columns"] == ["id", "value"]

    def test_add_unknown_column(self):
        """Add unknown column warning."""
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id="run_001",
            evidence_id=1,
        )

        collector.add_unknown_column(
            table_name="autofill",
            column_name="new_field",
            column_type="TEXT",
            source_file="/path/to/Web Data",
        )

        assert len(collector._warnings) == 1
        w = collector._warnings[0]
        assert w.warning_type == WARNING_TYPE_UNKNOWN_COLUMN
        assert w.item_name == "new_field"
        assert w.severity == SEVERITY_INFO

    def test_add_unknown_token_type(self):
        """Add unknown token type warning."""
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id="run_001",
            evidence_id=1,
        )

        collector.add_unknown_token_type(
            token_type=999,
            source_file="/path/to/Web Data",
        )

        assert len(collector._warnings) == 1
        w = collector._warnings[0]
        assert w.warning_type == WARNING_TYPE_UNKNOWN_TOKEN_TYPE
        assert w.item_name == "TOKEN_TYPE"
        assert w.item_value == "999"
        assert w.severity == SEVERITY_INFO

    def test_add_json_parse_error(self):
        """Add JSON parse error warning."""
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id="run_001",
            evidence_id=1,
        )

        collector.add_json_parse_error(
            filename="Preferences",
            error="Unexpected token at position 42",
        )

        assert len(collector._warnings) == 1
        w = collector._warnings[0]
        assert w.warning_type == WARNING_TYPE_JSON_PARSE_ERROR
        assert w.item_name == "Preferences"
        assert w.item_value == "Unexpected token at position 42"
        assert w.severity == SEVERITY_ERROR
        assert w.category == CATEGORY_JSON

    def test_add_warning_generic(self):
        """Add generic warning with custom type."""
        collector = ExtractionWarningCollector(
            extractor_name="custom_extractor",
            run_id="run_001",
            evidence_id=1,
        )

        collector.add_warning(
            warning_type="custom_issue",
            item_name="some_item",
            severity=SEVERITY_WARNING,
            category="custom_category",
            item_value="some_value",
            artifact_type="bookmark",
            source_file="/path/to/file",
            context_json={"key": "value"},
        )

        assert len(collector._warnings) == 1
        w = collector._warnings[0]
        assert w.warning_type == "custom_issue"
        assert w.item_name == "some_item"
        assert w.severity == SEVERITY_WARNING
        assert w.category == "custom_category"
        assert w.artifact_type == "bookmark"

    def test_flush_to_database(self, evidence_db):
        """Flush collected warnings to database."""
        collector = ExtractionWarningCollector(
            extractor_name="test_extractor",
            run_id="flush_test",
            evidence_id=1,
        )

        collector.add_unknown_table("table1", ["col1"], "/path/1")
        collector.add_unknown_table("table2", ["col2"], "/path/2")
        collector.add_unknown_column("table3", "col1", "TEXT", "/path/3")

        # Flush to database
        count = collector.flush_to_database(evidence_db)

        assert count == 3
        assert len(collector._warnings) == 0  # Cleared after flush

        # Verify in database
        db_count = get_extraction_warnings_count(evidence_db, evidence_id=1)
        assert db_count == 3

    def test_flush_empty_warnings(self, evidence_db):
        """Flushing empty warnings list returns 0."""
        collector = ExtractionWarningCollector(
            extractor_name="test_extractor",
            run_id="empty_test",
            evidence_id=1,
        )

        count = collector.flush_to_database(evidence_db)

        assert count == 0
        assert get_extraction_warnings_count(evidence_db, evidence_id=1) == 0


class TestDiscoverUnknownTables:
    """Tests for discover_unknown_tables function."""

    @pytest.fixture
    def sample_conn(self, tmp_path):
        """Create a sample SQLite database for discovery tests."""
        db_path = tmp_path / "sample.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE known_table (id INTEGER, name TEXT)")
        conn.execute("CREATE TABLE unknown_table (id INTEGER, value BLOB)")
        conn.execute("CREATE TABLE another_unknown (x INTEGER, y INTEGER)")
        conn.commit()
        return conn

    def test_finds_unknown_tables(self, sample_conn):
        """Discovers tables not in known set."""
        known = {"known_table", "sqlite_sequence"}

        unknown = discover_unknown_tables(sample_conn, known)

        table_names = {t["name"] for t in unknown}
        assert "unknown_table" in table_names
        assert "another_unknown" in table_names
        assert "known_table" not in table_names

    def test_all_tables_known(self, sample_conn):
        """Returns empty list when all tables are known."""
        known = {"known_table", "unknown_table", "another_unknown"}

        unknown = discover_unknown_tables(sample_conn, known)

        assert unknown == []

    def test_filters_with_patterns(self, sample_conn):
        """Filters to tables matching patterns."""
        known = {"known_table"}
        patterns = ["unknown"]  # Only match tables with "unknown" in name

        unknown = discover_unknown_tables(sample_conn, known, patterns)

        table_names = {t["name"] for t in unknown}
        assert "unknown_table" in table_names
        # another_unknown also has "unknown" in it based on pattern matching
        assert "another_unknown" in table_names

    def test_returns_columns(self, sample_conn):
        """Returns column info for each unknown table."""
        known = {"known_table"}

        unknown = discover_unknown_tables(sample_conn, known)

        unknown_table_info = next((t for t in unknown if t["name"] == "unknown_table"), None)
        assert unknown_table_info is not None
        assert "id" in unknown_table_info["columns"]
        assert "value" in unknown_table_info["columns"]


class TestDiscoverUnknownColumns:
    """Tests for discover_unknown_columns function."""

    @pytest.fixture
    def db_with_columns(self, tmp_path):
        """Create a database with specific columns."""
        db_path = tmp_path / "columns.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE test_table (id INTEGER, name TEXT, value BLOB, unknown_col TEXT)"
        )
        conn.commit()
        return conn

    def test_finds_unknown_columns(self, db_with_columns):
        """Discovers columns not in known set."""
        known = {"id", "name", "value"}

        unknown = discover_unknown_columns(db_with_columns, "test_table", known)

        col_names = {c["name"] for c in unknown}
        assert "unknown_col" in col_names
        assert "id" not in col_names

    def test_all_columns_known(self, db_with_columns):
        """Returns empty list when all columns are known."""
        known = {"id", "name", "value", "unknown_col"}

        unknown = discover_unknown_columns(db_with_columns, "test_table", known)

        assert unknown == []


class TestTrackUnknownValues:
    """Tests for track_unknown_values function."""

    def test_tracks_unknown_values(self):
        """Identifies values not in known mapping."""
        known = {1: "name", 2: "email", 3: "phone", 4: "address", 5: "city"}
        found = {1, 2, 6, 7, 3}

        unknown = track_unknown_values(known, found)

        assert unknown == {6, 7}

    def test_all_values_known(self):
        """Returns empty set when all values are known."""
        known = {1: "a", 2: "b", 3: "c"}
        found = {1, 2, 3}

        unknown = track_unknown_values(known, found)

        assert unknown == set()

    def test_empty_found_set(self):
        """Returns empty set when found set is empty."""
        known = {1: "a", 2: "b", 3: "c"}
        found = set()

        unknown = track_unknown_values(known, found)

        assert unknown == set()


class TestDiscoverUnknownJsonKeys:
    """Tests for discover_unknown_json_keys function."""

    def test_finds_unknown_keys(self):
        """Discovers keys not in known set."""
        json_data = {"known_key": 1, "another_known": 2, "unknown_key": 3}
        known = {"known_key", "another_known"}

        unknown = discover_unknown_json_keys(json_data, known)

        unknown_paths = {u["path"] for u in unknown}
        assert "unknown_key" in unknown_paths

    def test_nested_keys(self):
        """Discovers keys in nested objects with path prefix."""
        json_data = {
            "known": 1,
            "nested": {"known_nested": 2, "unknown_nested": 3},
        }
        known = {"known", "nested", "nested.known_nested"}

        unknown = discover_unknown_json_keys(json_data, known)

        unknown_paths = {u["path"] for u in unknown}
        assert "nested.unknown_nested" in unknown_paths

    def test_all_keys_known(self):
        """Returns empty list when all keys are known."""
        json_data = {"a": 1, "b": 2}
        known = {"a", "b"}

        unknown = discover_unknown_json_keys(json_data, known)

        assert unknown == []

    def test_handles_non_dict(self):
        """Handles non-dict input gracefully."""
        unknown = discover_unknown_json_keys("not a dict", set())

        assert unknown == []


class TestCollectorIntegration:
    """Integration tests for collector with discovery functions."""

    @pytest.fixture
    def sample_conn(self, tmp_path):
        """Create a sample SQLite database for discovery tests."""
        db_path = tmp_path / "sample.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE known_table (id INTEGER, name TEXT)")
        conn.execute("CREATE TABLE unknown_table (id INTEGER, value BLOB)")
        conn.execute("CREATE TABLE another_unknown (x INTEGER, y INTEGER)")
        conn.commit()
        return conn

    def test_collector_with_table_discovery(self, evidence_db, sample_conn):
        """Collector integrates with table discovery."""
        collector = ExtractionWarningCollector(
            extractor_name="test_extractor",
            run_id="integration_test",
            evidence_id=1,
        )

        known_tables = {"known_table"}
        unknown = discover_unknown_tables(sample_conn, known_tables)

        for table_info in unknown:
            collector.add_unknown_table(
                table_info["name"],
                table_info["columns"],
                "sample.sqlite"
            )

        count = collector.flush_to_database(evidence_db)

        assert count == 2  # unknown_table and another_unknown

        warnings = get_extraction_warnings(evidence_db, evidence_id=1)
        table_names = {w["item_name"] for w in warnings}
        assert "unknown_table" in table_names
        assert "another_unknown" in table_names
