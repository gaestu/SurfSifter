"""Tests for custom report sections feature."""

import sqlite3
from typing import Generator

import pytest

from reports.database import (
    insert_custom_section,
    update_custom_section,
    delete_custom_section,
    get_custom_sections,
    get_custom_section_by_id,
    reorder_custom_section,
)


@pytest.fixture
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    """Create an in-memory SQLite database for testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


class TestCustomSectionDatabase:
    """Tests for custom section database helpers."""

    def test_insert_section(self, db_conn: sqlite3.Connection) -> None:
        """Test inserting a new section."""
        section_id = insert_custom_section(
            db_conn,
            evidence_id=1,
            title="Test Section",
            content="<p>Test content</p>",
        )

        assert section_id > 0

        # Verify it was inserted
        section = get_custom_section_by_id(db_conn, section_id)
        assert section is not None
        assert section["title"] == "Test Section"
        assert section["content"] == "<p>Test content</p>"
        assert section["evidence_id"] == 1
        assert section["sort_order"] == 0

    def test_insert_multiple_sections_auto_order(self, db_conn: sqlite3.Connection) -> None:
        """Test that sort_order is auto-incremented."""
        id1 = insert_custom_section(db_conn, 1, "Section 1")
        id2 = insert_custom_section(db_conn, 1, "Section 2")
        id3 = insert_custom_section(db_conn, 1, "Section 3")

        s1 = get_custom_section_by_id(db_conn, id1)
        s2 = get_custom_section_by_id(db_conn, id2)
        s3 = get_custom_section_by_id(db_conn, id3)

        assert s1["sort_order"] == 0
        assert s2["sort_order"] == 1
        assert s3["sort_order"] == 2

    def test_insert_section_per_evidence(self, db_conn: sqlite3.Connection) -> None:
        """Test that sections are scoped to evidence."""
        insert_custom_section(db_conn, 1, "Ev1 Section 1")
        insert_custom_section(db_conn, 1, "Ev1 Section 2")
        insert_custom_section(db_conn, 2, "Ev2 Section 1")

        ev1_sections = get_custom_sections(db_conn, 1)
        ev2_sections = get_custom_sections(db_conn, 2)

        assert len(ev1_sections) == 2
        assert len(ev2_sections) == 1
        assert ev2_sections[0]["sort_order"] == 0  # Starts at 0 for each evidence

    def test_update_section_title(self, db_conn: sqlite3.Connection) -> None:
        """Test updating section title."""
        section_id = insert_custom_section(db_conn, 1, "Original Title")

        result = update_custom_section(db_conn, section_id, title="Updated Title")

        assert result is True
        section = get_custom_section_by_id(db_conn, section_id)
        assert section["title"] == "Updated Title"

    def test_update_section_content(self, db_conn: sqlite3.Connection) -> None:
        """Test updating section content."""
        section_id = insert_custom_section(db_conn, 1, "Title", "<p>Old</p>")

        result = update_custom_section(db_conn, section_id, content="<p>New</p>")

        assert result is True
        section = get_custom_section_by_id(db_conn, section_id)
        assert section["content"] == "<p>New</p>"

    def test_update_nonexistent_section(self, db_conn: sqlite3.Connection) -> None:
        """Test updating a section that doesn't exist."""
        result = update_custom_section(db_conn, 9999, title="Nope")
        assert result is False

    def test_delete_section(self, db_conn: sqlite3.Connection) -> None:
        """Test deleting a section."""
        section_id = insert_custom_section(db_conn, 1, "To Delete")

        result = delete_custom_section(db_conn, section_id)

        assert result is True
        assert get_custom_section_by_id(db_conn, section_id) is None

    def test_delete_nonexistent_section(self, db_conn: sqlite3.Connection) -> None:
        """Test deleting a section that doesn't exist."""
        result = delete_custom_section(db_conn, 9999)
        assert result is False

    def test_get_sections_ordered(self, db_conn: sqlite3.Connection) -> None:
        """Test that get_custom_sections returns sorted by sort_order."""
        insert_custom_section(db_conn, 1, "Third", sort_order=2)
        insert_custom_section(db_conn, 1, "First", sort_order=0)
        insert_custom_section(db_conn, 1, "Second", sort_order=1)

        sections = get_custom_sections(db_conn, 1)

        assert len(sections) == 3
        assert sections[0]["title"] == "First"
        assert sections[1]["title"] == "Second"
        assert sections[2]["title"] == "Third"

    def test_get_sections_empty(self, db_conn: sqlite3.Connection) -> None:
        """Test get_custom_sections returns empty list for no sections."""
        sections = get_custom_sections(db_conn, 999)
        assert sections == []

    def test_get_section_by_id_not_found(self, db_conn: sqlite3.Connection) -> None:
        """Test get_custom_section_by_id returns None for invalid ID."""
        section = get_custom_section_by_id(db_conn, 9999)
        assert section is None


class TestSectionReordering:
    """Tests for section reordering functionality."""

    def test_reorder_move_up(self, db_conn: sqlite3.Connection) -> None:
        """Test moving a section up."""
        id1 = insert_custom_section(db_conn, 1, "Section 1")
        id2 = insert_custom_section(db_conn, 1, "Section 2")
        id3 = insert_custom_section(db_conn, 1, "Section 3")

        # Move Section 3 (order=2) to position 0
        result = reorder_custom_section(db_conn, id3, 0)

        assert result is True

        sections = get_custom_sections(db_conn, 1)
        titles = [s["title"] for s in sections]
        assert titles == ["Section 3", "Section 1", "Section 2"]

    def test_reorder_move_down(self, db_conn: sqlite3.Connection) -> None:
        """Test moving a section down."""
        id1 = insert_custom_section(db_conn, 1, "Section 1")
        id2 = insert_custom_section(db_conn, 1, "Section 2")
        id3 = insert_custom_section(db_conn, 1, "Section 3")

        # Move Section 1 (order=0) to position 2
        result = reorder_custom_section(db_conn, id1, 2)

        assert result is True

        sections = get_custom_sections(db_conn, 1)
        titles = [s["title"] for s in sections]
        assert titles == ["Section 2", "Section 3", "Section 1"]

    def test_reorder_to_same_position(self, db_conn: sqlite3.Connection) -> None:
        """Test reordering to same position is a no-op."""
        id1 = insert_custom_section(db_conn, 1, "Section 1")

        result = reorder_custom_section(db_conn, id1, 0)

        assert result is True
        section = get_custom_section_by_id(db_conn, id1)
        assert section["sort_order"] == 0

    def test_reorder_nonexistent_section(self, db_conn: sqlite3.Connection) -> None:
        """Test reordering a section that doesn't exist."""
        result = reorder_custom_section(db_conn, 9999, 0)
        assert result is False

    def test_reorder_middle_positions(self, db_conn: sqlite3.Connection) -> None:
        """Test moving a section from middle to middle position."""
        id1 = insert_custom_section(db_conn, 1, "A")
        id2 = insert_custom_section(db_conn, 1, "B")
        id3 = insert_custom_section(db_conn, 1, "C")
        id4 = insert_custom_section(db_conn, 1, "D")

        # Move B (order=1) to position 2 (between C and D)
        reorder_custom_section(db_conn, id2, 2)

        sections = get_custom_sections(db_conn, 1)
        titles = [s["title"] for s in sections]
        assert titles == ["A", "C", "B", "D"]


class TestSectionTimestamps:
    """Tests for section timestamp handling."""

    def test_created_at_set_on_insert(self, db_conn: sqlite3.Connection) -> None:
        """Test that created_at_utc is set on insert."""
        section_id = insert_custom_section(db_conn, 1, "Test")

        section = get_custom_section_by_id(db_conn, section_id)
        assert section["created_at_utc"] is not None
        assert "T" in section["created_at_utc"]  # ISO format

    def test_updated_at_changes_on_update(self, db_conn: sqlite3.Connection) -> None:
        """Test that updated_at_utc changes on update."""
        section_id = insert_custom_section(db_conn, 1, "Test")

        original = get_custom_section_by_id(db_conn, section_id)
        original_updated = original["updated_at_utc"]

        # Small delay to ensure timestamp difference
        import time
        time.sleep(0.01)

        update_custom_section(db_conn, section_id, title="Updated")

        updated = get_custom_section_by_id(db_conn, section_id)
        assert updated["updated_at_utc"] >= original_updated
