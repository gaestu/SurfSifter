"""
Unit tests for Screenshots feature

Tests for:
- Database helpers (CRUD operations)
- Screenshot storage utilities
- Screenshots table model
- Report module
"""

import sqlite3
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch


# =============================================================================
# Database Helpers Tests
# =============================================================================

class TestScreenshotsDatabaseHelpers:
    """Tests for screenshots database CRUD operations."""

    @pytest.fixture
    def db_conn(self, tmp_path):
        """Create an in-memory database with screenshots table."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        # Create the screenshots table
        conn.execute("""
            CREATE TABLE screenshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                captured_url TEXT,
                dest_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                size_bytes INTEGER,
                width INTEGER,
                height INTEGER,
                md5 TEXT,
                sha256 TEXT,
                title TEXT,
                caption TEXT,
                notes TEXT,
                sequence_name TEXT,
                sequence_order INTEGER DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'sandbox',
                captured_at_utc TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT
            )
        """)
        conn.commit()
        return conn

    def test_insert_screenshot_basic(self, db_conn):
        """Test inserting a basic screenshot record."""
        from core.database.helpers import insert_screenshot

        screenshot_id = insert_screenshot(
            db_conn,
            evidence_id=1,
            dest_path="screenshots/test.png",
            filename="test.png",
            title="Test Screenshot",
            caption="A test caption",
        )

        assert screenshot_id is not None
        assert screenshot_id > 0

        # Verify insertion
        row = db_conn.execute(
            "SELECT * FROM screenshots WHERE id = ?", (screenshot_id,)
        ).fetchone()

        assert row["evidence_id"] == 1
        assert row["dest_path"] == "screenshots/test.png"
        assert row["filename"] == "test.png"
        assert row["title"] == "Test Screenshot"
        assert row["caption"] == "A test caption"
        assert row["source"] == "sandbox"

    def test_insert_screenshot_with_all_fields(self, db_conn):
        """Test inserting a screenshot with all fields populated."""
        from core.database.helpers import insert_screenshot

        screenshot_id = insert_screenshot(
            db_conn,
            evidence_id=1,
            dest_path="screenshots/full.png",
            filename="full.png",
            captured_url="https://example.com/page",
            size_bytes=12345,
            width=1920,
            height=1080,
            md5="abc123",
            sha256="def456",
            title="Full Screenshot",
            caption="Full caption",
            notes="Internal notes",
            sequence_name="test_sequence",
            sequence_order=5,
            source="upload",
        )

        row = db_conn.execute(
            "SELECT * FROM screenshots WHERE id = ?", (screenshot_id,)
        ).fetchone()

        assert row["captured_url"] == "https://example.com/page"
        assert row["size_bytes"] == 12345
        assert row["width"] == 1920
        assert row["height"] == 1080
        assert row["sequence_name"] == "test_sequence"
        assert row["sequence_order"] == 5
        assert row["source"] == "upload"

    def test_get_screenshot(self, db_conn):
        """Test retrieving a single screenshot."""
        from core.database.helpers import insert_screenshot, get_screenshot

        screenshot_id = insert_screenshot(
            db_conn,
            evidence_id=1,
            dest_path="screenshots/get_test.png",
            filename="get_test.png",
            title="Get Test",
            caption="Caption",
        )

        screenshot = get_screenshot(db_conn, 1, screenshot_id)

        assert screenshot is not None
        assert screenshot["id"] == screenshot_id
        assert screenshot["title"] == "Get Test"

    def test_get_screenshot_not_found(self, db_conn):
        """Test retrieving non-existent screenshot returns None."""
        from core.database.helpers import get_screenshot

        screenshot = get_screenshot(db_conn, 1, 99999)
        assert screenshot is None

    def test_get_screenshots_list(self, db_conn):
        """Test retrieving multiple screenshots."""
        from core.database.helpers import insert_screenshot, get_screenshots

        # Insert multiple screenshots
        insert_screenshot(db_conn, 1, "screenshots/a.png", "a.png", title="A", caption="A")
        insert_screenshot(db_conn, 1, "screenshots/b.png", "b.png", title="B", caption="B")
        insert_screenshot(db_conn, 2, "screenshots/c.png", "c.png", title="C", caption="C")  # Different evidence

        screenshots = get_screenshots(db_conn, 1)

        assert len(screenshots) == 2
        titles = {s["title"] for s in screenshots}
        assert titles == {"A", "B"}

    def test_get_screenshots_filter_by_sequence(self, db_conn):
        """Test filtering screenshots by sequence."""
        from core.database.helpers import insert_screenshot, get_screenshots

        insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A", sequence_name="flow1")
        insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B", sequence_name="flow1")
        insert_screenshot(db_conn, 1, "s/c.png", "c.png", title="C", caption="C", sequence_name="flow2")

        flow1 = get_screenshots(db_conn, 1, sequence_name="flow1")
        assert len(flow1) == 2

        flow2 = get_screenshots(db_conn, 1, sequence_name="flow2")
        assert len(flow2) == 1

    def test_get_screenshots_filter_by_source(self, db_conn):
        """Test filtering screenshots by source."""
        from core.database.helpers import insert_screenshot, get_screenshots

        insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A", source="sandbox")
        insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B", source="upload")

        sandbox = get_screenshots(db_conn, 1, source="sandbox")
        assert len(sandbox) == 1
        assert sandbox[0]["title"] == "A"

        upload = get_screenshots(db_conn, 1, source="upload")
        assert len(upload) == 1
        assert upload[0]["title"] == "B"

    def test_update_screenshot(self, db_conn):
        """Test updating a screenshot."""
        from core.database.helpers import insert_screenshot, update_screenshot, get_screenshot

        screenshot_id = insert_screenshot(
            db_conn, 1, "s/update.png", "update.png",
            title="Original", caption="Original Caption"
        )

        success = update_screenshot(
            db_conn, 1, screenshot_id,
            title="Updated Title",
            caption="Updated Caption",
            notes="New notes"
        )

        assert success is True

        updated = get_screenshot(db_conn, 1, screenshot_id)
        assert updated["title"] == "Updated Title"
        assert updated["caption"] == "Updated Caption"
        assert updated["notes"] == "New notes"
        assert updated["updated_at_utc"] is not None

    def test_update_screenshot_not_found(self, db_conn):
        """Test updating non-existent screenshot returns False."""
        from core.database.helpers import update_screenshot

        success = update_screenshot(db_conn, 1, 99999, title="New")
        assert success is False

    def test_delete_screenshot(self, db_conn):
        """Test deleting a screenshot."""
        from core.database.helpers import insert_screenshot, delete_screenshot, get_screenshot

        screenshot_id = insert_screenshot(
            db_conn, 1, "s/delete.png", "delete.png",
            title="To Delete", caption="Caption"
        )

        success = delete_screenshot(db_conn, 1, screenshot_id)
        assert success is True

        deleted = get_screenshot(db_conn, 1, screenshot_id)
        assert deleted is None

    def test_delete_screenshot_not_found(self, db_conn):
        """Test deleting non-existent screenshot returns False."""
        from core.database.helpers import delete_screenshot

        success = delete_screenshot(db_conn, 1, 99999)
        assert success is False

    def test_get_screenshot_count(self, db_conn):
        """Test counting screenshots."""
        from core.database.helpers import insert_screenshot, get_screenshot_count

        assert get_screenshot_count(db_conn, 1) == 0

        insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A")
        insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B")

        assert get_screenshot_count(db_conn, 1) == 2

    def test_get_sequences(self, db_conn):
        """Test getting unique sequence names."""
        from core.database.helpers import insert_screenshot, get_sequences

        insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A", sequence_name="login")
        insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B", sequence_name="payment")
        insert_screenshot(db_conn, 1, "s/c.png", "c.png", title="C", caption="C", sequence_name="login")  # Duplicate
        insert_screenshot(db_conn, 1, "s/d.png", "d.png", title="D", caption="D")  # No sequence

        sequences = get_sequences(db_conn, 1)

        assert len(sequences) == 2
        assert "login" in sequences
        assert "payment" in sequences

    def test_reorder_sequence(self, db_conn):
        """Test reordering screenshots in a sequence."""
        from core.database.helpers import insert_screenshot, reorder_sequence, get_screenshots

        id1 = insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A",
                                sequence_name="flow", sequence_order=0)
        id2 = insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B",
                                sequence_name="flow", sequence_order=1)
        id3 = insert_screenshot(db_conn, 1, "s/c.png", "c.png", title="C", caption="C",
                                sequence_name="flow", sequence_order=2)

        # Reorder: C, A, B
        reorder_sequence(db_conn, 1, "flow", [id3, id1, id2])

        screenshots = get_screenshots(db_conn, 1, sequence_name="flow")

        # Find by ID and check new order
        orders = {s["id"]: s["sequence_order"] for s in screenshots}
        assert orders[id3] == 0
        assert orders[id1] == 1
        assert orders[id2] == 2

    def test_get_screenshot_stats(self, db_conn):
        """Test getting screenshot statistics."""
        from core.database.helpers import insert_screenshot, get_screenshot_stats

        insert_screenshot(db_conn, 1, "s/a.png", "a.png", title="A", caption="A",
                          source="sandbox", sequence_name="flow1", size_bytes=1000)
        insert_screenshot(db_conn, 1, "s/b.png", "b.png", title="B", caption="B",
                          source="upload", sequence_name="flow1", size_bytes=2000)
        insert_screenshot(db_conn, 1, "s/c.png", "c.png", title="C", caption="C",
                          source="sandbox", size_bytes=500)  # No sequence

        stats = get_screenshot_stats(db_conn, 1)

        assert stats["total_count"] == 3
        assert stats["by_source"]["sandbox"] == 2
        assert stats["by_source"]["upload"] == 1
        assert stats["total_size_bytes"] == 3500


# =============================================================================
# Screenshot Storage Tests
# =============================================================================

class TestScreenshotStorage:
    """Tests for screenshot storage utilities."""

    def test_get_screenshots_dir(self, tmp_path):
        """Test creating screenshots directory."""
        from app.features.screenshots.storage import get_screenshots_dir

        screenshots_dir = get_screenshots_dir(tmp_path, "Test Evidence", 1)

        assert screenshots_dir.exists()
        assert screenshots_dir.is_dir()
        assert "screenshots" in str(screenshots_dir)

    def test_screenshot_metadata_dataclass(self):
        """Test ScreenshotMetadata dataclass."""
        from app.features.screenshots.storage import ScreenshotMetadata

        meta = ScreenshotMetadata(
            dest_path="screenshots/test.png",
            filename="test.png",
            width=1920,
            height=1080,
            size_bytes=12345,
            md5="abc123",
            sha256="def456",
        )

        assert meta.dest_path == "screenshots/test.png"
        assert meta.filename == "test.png"
        assert meta.width == 1920
        assert meta.height == 1080
        assert meta.size_bytes == 12345


@pytest.mark.gui_offscreen
class TestScreenshotStorageQt:
    """Tests for Qt-dependent screenshot storage utilities."""

    @pytest.fixture
    def qapp(self):
        """Ensure Qt application is available."""
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        if app is None:
            app = QApplication([])
        return app

    @patch('app.features.screenshots.storage.slugify_label')
    def test_save_screenshot_creates_file(self, mock_slugify, qapp, tmp_path):
        """Test that save_screenshot creates a PNG file."""
        from app.features.screenshots.storage import save_screenshot
        from PySide6.QtGui import QPixmap, QColor

        mock_slugify.return_value = "ev-test"

        # Create a test pixmap
        pixmap = QPixmap(100, 100)
        pixmap.fill(QColor(255, 0, 0))  # Red

        metadata = save_screenshot(
            pixmap,
            tmp_path,
            "Test Evidence",
            1,
            prefix="test",
        )

        assert metadata.filename.startswith("test_")
        assert metadata.filename.endswith(".png")
        assert metadata.width == 100
        assert metadata.height == 100
        assert metadata.size_bytes > 0
        assert len(metadata.md5) == 32
        assert len(metadata.sha256) == 64

        # Verify file exists
        full_path = tmp_path / "evidences" / "ev-test" / metadata.dest_path
        assert full_path.exists()


# =============================================================================
# Table Model Tests
# =============================================================================

@pytest.mark.gui_offscreen
class TestScreenshotsTableModel:
    """Tests for ScreenshotsTableModel."""

    @pytest.fixture
    def qapp(self):
        """Ensure Qt application is available."""
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        if app is None:
            app = QApplication([])
        return app

    def test_model_empty_state(self, qapp):
        """Test model with no data."""
        from app.features.screenshots.models import ScreenshotsTableModel

        model = ScreenshotsTableModel()

        assert model.rowCount() == 0
        assert model.columnCount() == 7  # checkbox, thumbnail, title, caption, sequence, source, date

    def test_model_load_data(self, qapp):
        """Test loading data into model."""
        from app.features.screenshots.models import ScreenshotsTableModel

        model = ScreenshotsTableModel()

        data = [
            {"id": 1, "title": "Test 1", "caption": "Caption 1"},
            {"id": 2, "title": "Test 2", "caption": "Caption 2"},
        ]

        model.load_data(data)

        assert model.rowCount() == 2

    def test_model_checkbox_toggle(self, qapp):
        """Test checkbox selection in model."""
        from app.features.screenshots.models import ScreenshotsTableModel
        from PySide6.QtCore import Qt

        model = ScreenshotsTableModel()
        model.load_data([{"id": 1, "title": "Test", "caption": "Cap"}])

        # Initially unchecked
        index = model.index(0, 0)
        assert model.data(index, Qt.CheckStateRole) == Qt.Unchecked

        # Toggle to checked
        model.setData(index, Qt.Checked, Qt.CheckStateRole)
        assert model.data(index, Qt.CheckStateRole) == Qt.Checked

        # Get checked items
        checked = model.get_checked_screenshots()
        assert len(checked) == 1
        assert checked[0]["id"] == 1

    def test_model_select_all(self, qapp):
        """Test select all functionality."""
        from app.features.screenshots.models import ScreenshotsTableModel

        model = ScreenshotsTableModel()
        model.load_data([
            {"id": 1, "title": "Test 1", "caption": "Cap"},
            {"id": 2, "title": "Test 2", "caption": "Cap"},
        ])

        model.select_all()

        assert model.get_checked_count() == 2

        model.deselect_all()

        assert model.get_checked_count() == 0

    def test_model_header_data(self, qapp):
        """Test column headers."""
        from app.features.screenshots.models import ScreenshotsTableModel
        from PySide6.QtCore import Qt

        model = ScreenshotsTableModel()

        # First column is checkbox (empty header)
        assert model.headerData(0, Qt.Horizontal, Qt.DisplayRole) == ""
        assert model.headerData(1, Qt.Horizontal, Qt.DisplayRole) == "Preview"
        assert model.headerData(2, Qt.Horizontal, Qt.DisplayRole) == "Title"


# =============================================================================
# Report Module Tests
# =============================================================================

class TestScreenshotsReportModule:
    """Tests for ScreenshotsModule report module."""

    @pytest.fixture
    def db_conn(self):
        """Create test database."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE screenshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                captured_url TEXT,
                dest_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                size_bytes INTEGER,
                width INTEGER,
                height INTEGER,
                md5 TEXT,
                sha256 TEXT,
                title TEXT,
                caption TEXT,
                notes TEXT,
                sequence_name TEXT,
                sequence_order INTEGER DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'sandbox',
                captured_at_utc TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT
            )
        """)
        conn.commit()
        return conn

    def test_module_metadata(self):
        """Test module metadata."""
        from reports.modules.screenshots import ScreenshotsModule

        module = ScreenshotsModule()
        meta = module.metadata

        assert meta.module_id == "screenshots"
        assert meta.name == "Screenshots"
        assert meta.category == "Documentation"
        assert meta.icon == "📷"

    def test_module_filter_fields(self):
        """Test filter field definitions."""
        from reports.modules.screenshots import ScreenshotsModule

        module = ScreenshotsModule()
        fields = module.get_filter_fields()

        assert len(fields) == 4

        keys = {f.key for f in fields}
        assert "sequence_filter" in keys
        assert "include_notes" in keys
        assert "include_url" in keys
        assert "show_total" in keys

    def test_module_render_empty(self, db_conn):
        """Test rendering with no screenshots."""
        from reports.modules.screenshots import ScreenshotsModule

        module = ScreenshotsModule()
        html = module.render(db_conn, 1, {})

        assert "No screenshots found" in html or "no_screenshots_found" in html

    def test_module_render_with_data(self, db_conn):
        """Test rendering with screenshot data."""
        from reports.modules.screenshots import ScreenshotsModule

        # Insert test data
        db_conn.execute("""
            INSERT INTO screenshots (evidence_id, dest_path, filename, title, caption,
                                     sequence_name, source, captured_at_utc, created_at_utc)
            VALUES (1, 'screenshots/test.png', 'test.png', 'Login Page',
                    'Shows the login form', 'login_flow', 'sandbox',
                    '2026-01-26T14:30:00Z', '2026-01-26T14:30:00Z')
        """)
        db_conn.commit()

        module = ScreenshotsModule()
        html = module.render(db_conn, 1, {})

        assert "Login Page" in html
        assert "Shows the login form" in html
        assert "login_flow" in html


# =============================================================================
# ForensicContext Tests
# =============================================================================

class TestForensicContext:
    """Tests for ForensicContext dataclass."""

    def test_forensic_context_creation(self, tmp_path):
        """Test creating ForensicContext."""
        from app.common import ForensicContext

        mock_conn = MagicMock()

        context = ForensicContext(
            evidence_id=1,
            evidence_label="Test Evidence",
            workspace_path=tmp_path,
            db_conn=mock_conn,
        )

        assert context.evidence_id == 1
        assert context.evidence_label == "Test Evidence"
        assert context.workspace_path == tmp_path
        assert context.db_conn is mock_conn


# =============================================================================
# Integration Tests
# =============================================================================

@pytest.mark.gui_offscreen
class TestScreenshotsIntegration:
    """Integration tests for screenshots workflow."""

    @pytest.fixture
    def qapp(self):
        """Ensure Qt application is available."""
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        if app is None:
            app = QApplication([])
        return app

    @pytest.fixture
    def setup_evidence(self, tmp_path):
        """Set up test evidence folder structure."""
        # Create case structure
        evidences_dir = tmp_path / "evidences" / "ev-test"
        evidences_dir.mkdir(parents=True)

        # Create evidence database
        db_path = evidences_dir.parent / "evidence_test.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE screenshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                captured_url TEXT,
                dest_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                size_bytes INTEGER,
                width INTEGER,
                height INTEGER,
                md5 TEXT,
                sha256 TEXT,
                title TEXT,
                caption TEXT,
                notes TEXT,
                sequence_name TEXT,
                sequence_order INTEGER DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'sandbox',
                captured_at_utc TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT
            )
        """)
        conn.commit()

        return {
            "workspace_path": tmp_path,
            "evidence_label": "test",
            "evidence_id": 1,
            "db_conn": conn,
        }

    @patch('app.features.screenshots.storage.slugify_label')
    def test_full_screenshot_workflow(self, mock_slugify, qapp, setup_evidence):
        """Test complete screenshot save and retrieve workflow."""
        from app.features.screenshots.storage import save_screenshot
        from core.database.helpers import insert_screenshot, get_screenshots
        from PySide6.QtGui import QPixmap, QColor

        mock_slugify.return_value = "ev-test"

        ctx = setup_evidence

        # 1. Create a screenshot
        pixmap = QPixmap(100, 50)
        pixmap.fill(QColor(0, 128, 255))

        # 2. Save to disk
        metadata = save_screenshot(
            pixmap,
            ctx["workspace_path"],
            ctx["evidence_label"],
            ctx["evidence_id"],
            prefix="workflow_test",
        )

        # 3. Insert database record
        screenshot_id = insert_screenshot(
            ctx["db_conn"],
            ctx["evidence_id"],
            metadata.dest_path,
            metadata.filename,
            captured_url="https://test.example.com",
            size_bytes=metadata.size_bytes,
            width=metadata.width,
            height=metadata.height,
            md5=metadata.md5,
            sha256=metadata.sha256,
            title="Workflow Test",
            caption="Testing the full workflow",
        )

        # 4. Retrieve and verify
        screenshots = get_screenshots(ctx["db_conn"], ctx["evidence_id"])

        assert len(screenshots) == 1
        assert screenshots[0]["id"] == screenshot_id
        assert screenshots[0]["title"] == "Workflow Test"
        assert screenshots[0]["width"] == 100
        assert screenshots[0]["height"] == 50
        assert screenshots[0]["captured_url"] == "https://test.example.com"
