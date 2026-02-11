import sqlite3
import tempfile
from pathlib import Path
import pytest
from app.data.case_data import CaseDataAccess
from core.database import init_db

def test_unified_tagging_methods():
    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir)
        db_path = case_folder / "test_surfsifter.sqlite"

        # 1. Initialize Case DB
        conn = init_db(case_folder, db_path)
        conn.execute("INSERT INTO cases (id, case_id, title, created_at_utc) VALUES (1, 'TEST', 'Test Case', datetime('now'))")
        conn.execute("INSERT INTO evidences (id, case_id, label, source_path, added_at_utc) VALUES (1, 1, 'test_ev', '/tmp/test', datetime('now'))")
        conn.commit()
        conn.close()

        # 2. Initialize DAO
        dao = CaseDataAccess(case_folder, db_path=db_path)

        # 3. Get Evidence Connection (creates evidence DB and applies migrations)
        evidence_id = 1

        # We need to ensure the evidence DB is created and populated
        # Accessing it via _use_evidence_conn will trigger creation via DatabaseManager
        with dao._use_evidence_conn(evidence_id):
            with dao._connect() as ev_conn:
                # Insert test data - use first_discovered_by for images
                ev_conn.execute("INSERT INTO urls (id, evidence_id, url, discovered_by) VALUES (1, ?, 'http://example.com', 'test')", (evidence_id,))
                ev_conn.execute("INSERT INTO images (id, evidence_id, rel_path, filename, first_discovered_by) VALUES (1, ?, 'img1.jpg', 'img1.jpg', 'test')", (evidence_id,))
                ev_conn.commit()

        # 4. Run Tests

        # 1. Create Tag
        tag_id = dao.create_tag(evidence_id, "Gambling")
        assert tag_id is not None

        # 2. Get Tag
        tag = dao.get_tag(evidence_id, "gambling") # Case insensitive
        assert tag is not None
        assert tag['name'] == "Gambling"
        assert tag['usage_count'] == 0

        # 3. Tag Artifact (URL)
        dao.tag_artifact(evidence_id, "Gambling", "url", 1)

        # Check usage count
        tag = dao.get_tag(evidence_id, "Gambling")
        assert tag['usage_count'] == 1

        # 4. Tag Artifact (Image)
        dao.tag_artifact(evidence_id, "Gambling", "image", 1)

        # Check usage count
        tag = dao.get_tag(evidence_id, "Gambling")
        assert tag['usage_count'] == 2

        # 5. Get Artifact Tags
        tags = dao.get_artifact_tags(evidence_id, "url", 1)
        assert len(tags) == 1
        assert tags[0]['name'] == "Gambling"

        # 6. Get Artifacts by Tag
        artifacts = dao.get_artifacts_by_tag(evidence_id, "Gambling")
        assert 'url' in artifacts
        assert 1 in artifacts['url']
        assert 'image' in artifacts
        assert 1 in artifacts['image']

        # 7. Get URLs by Tag (legacy method updated)
        urls = dao.get_urls_by_tag(evidence_id, "Gambling")
        assert len(urls) == 1
        assert urls[0]['url'] == "http://example.com"

        # 8. Get Images by Tag (legacy method updated)
        images = dao.get_images_by_tag(evidence_id, "Gambling")
        assert len(images) == 1
        assert images[0]['filename'] == "img1.jpg"

        # 9. Rename Tag
        dao.rename_tag(evidence_id, tag_id, "High Risk")
        tag = dao.get_tag(evidence_id, "high risk")
        assert tag is not None
        assert tag['name'] == "High Risk"

        # Check artifacts still tagged
        tags = dao.get_artifact_tags(evidence_id, "url", 1)
        assert tags[0]['name'] == "High Risk"

        # 10. Untag Artifact
        dao.untag_artifact(evidence_id, "High Risk", "url", 1)
        tags = dao.get_artifact_tags(evidence_id, "url", 1)
        assert len(tags) == 0

        tag = dao.get_tag(evidence_id, "High Risk")
        assert tag['usage_count'] == 1 # Still on image

        # 11. Merge Tags
        # Create another tag "Risk" and tag URL with it
        risk_id = dao.create_tag(evidence_id, "Risk")
        dao.tag_artifact(evidence_id, "Risk", "url", 1)

        # Merge "Risk" into "High Risk"
        dao.merge_tags(evidence_id, [risk_id], tag_id)

        # "Risk" should be gone
        assert dao.get_tag(evidence_id, "Risk") is None

        # URL should now be tagged "High Risk"
        tags = dao.get_artifact_tags(evidence_id, "url", 1)
        assert len(tags) == 1
        assert tags[0]['name'] == "High Risk"

        # Usage count of High Risk should be 2 (Image + URL)
        tag = dao.get_tag(evidence_id, "High Risk")
        assert tag['usage_count'] == 2

        # 12. Delete Tag
        dao.delete_tag(evidence_id, tag_id)
        assert dao.get_tag(evidence_id, "High Risk") is None

        # Artifacts should be untagged
        tags = dao.get_artifact_tags(evidence_id, "url", 1)
        assert len(tags) == 0
