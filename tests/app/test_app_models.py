from pathlib import Path

from app.data.case_data import CaseDataAccess
from tests.fixtures.helpers import prepare_case_with_data


def test_case_data_access_counts(tmp_path: Path) -> None:
    case_data, evidence_id = prepare_case_with_data(tmp_path)
    counts = case_data.get_evidence_counts(evidence_id)
    assert counts.urls == 1
    assert counts.images == 1
    assert counts.indicators == 1
    assert counts.last_run_utc is None

    urls = case_data.iter_urls(evidence_id, limit=10)
    assert urls and urls[0]["url"] == "https://example.com"

    images = case_data.iter_images(evidence_id, limit=10)
    assert images and images[0]["filename"] == "file.jpg"

    indicators = case_data.iter_indicators(evidence_id)
    assert indicators and indicators[0]["name"] == "ComputerName"
