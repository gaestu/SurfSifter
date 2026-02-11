
import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from extractors.carvers.bulk_extractor.extractor import BulkExtractorExtractor

@pytest.fixture
def extractor():
    return BulkExtractorExtractor()

@pytest.fixture
def mock_tools():
    with patch("extractors.carvers.bulk_extractor.extractor.discover_tools") as mock_discover:
        mock_tool = Mock()
        mock_tool.available = True
        mock_tool.path = Path("/usr/bin/bulk_extractor")
        mock_discover.return_value = {"bulk_extractor": mock_tool}
        yield mock_discover

def test_command_construction_multiple_scanners(extractor, mock_tools, tmp_path):
    """Test that multiple scanners are enabled correctly with -e flag."""
    output_dir = tmp_path / "output"
    evidence_path = tmp_path / "evidence.E01"
    evidence_path.touch()

    config = {
        "scanners": ["email", "accts"],
        "carve_images": False,
        "num_threads": 4,
        "output_reuse_policy": "overwrite"
    }

    callbacks = Mock()

    with patch("extractors.carvers.bulk_extractor.extractor.subprocess.Popen") as mock_popen:
        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.stderr = Mock()
        mock_process.stderr.read.return_value = ""
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        extractor.run_extraction(evidence_path, output_dir, config, callbacks)

        # Verify command arguments
        args, _ = mock_popen.call_args
        cmd = args[0]

        # Should contain -x all
        assert "-x" in cmd
        assert "all" in cmd

        # Should contain -e email AND -e accts
        # Note: order matters in list, but we just check presence
        email_idx = cmd.index("email")
        assert cmd[email_idx-1] == "-e"

        accts_idx = cmd.index("accts")
        assert cmd[accts_idx-1] == "-e"

        # Should NOT contain -E (exclusive)
        assert "-E" not in cmd

def test_command_construction_carve_images(extractor, mock_tools, tmp_path):
    """Test that carve_images adds exif scanner and jpeg_carve_mode."""
    output_dir = tmp_path / "output"
    evidence_path = tmp_path / "evidence.E01"
    evidence_path.touch()

    config = {
        "scanners": ["email"],
        "carve_images": True,
        "num_threads": 4,
        "output_reuse_policy": "overwrite"
    }

    callbacks = Mock()

    with patch("extractors.carvers.bulk_extractor.extractor.subprocess.Popen") as mock_popen:
        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.stderr = Mock()
        mock_process.stderr.read.return_value = ""
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        extractor.run_extraction(evidence_path, output_dir, config, callbacks)

        args, _ = mock_popen.call_args
        cmd = args[0]

        # Should contain -e email
        email_idx = cmd.index("email")
        assert cmd[email_idx-1] == "-e"

        # Should contain -e exif (added by carve_images)
        exif_idx = cmd.index("exif")
        assert cmd[exif_idx-1] == "-e"

        # Should contain -S jpeg_carve_mode=2
        assert "-S" in cmd
        assert "jpeg_carve_mode=2" in cmd

