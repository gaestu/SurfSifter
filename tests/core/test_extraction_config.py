"""
Tests for ExtractionConfig in core.config.

Tests auto_generate_file_list configuration loading.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


class TestExtractionConfig:
    """Tests for ExtractionConfig loading."""

    def test_default_auto_generate_file_list_enabled(self, tmp_path: Path):
        """Default config should have auto_generate_file_list=True."""
        from core.config import load_app_config

        # Create minimal config directory structure
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.yml").write_text("tool_paths: {}\n")

        config = load_app_config(tmp_path)

        assert config.extraction.auto_generate_file_list is True

    def test_explicit_auto_generate_file_list_false(self, tmp_path: Path):
        """Config can disable auto_generate_file_list."""
        from core.config import load_app_config

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.yml").write_text("""
tool_paths: {}
extraction:
  auto_generate_file_list: false
""")

        config = load_app_config(tmp_path)

        assert config.extraction.auto_generate_file_list is False

    def test_explicit_auto_generate_file_list_true(self, tmp_path: Path):
        """Config can explicitly enable auto_generate_file_list."""
        from core.config import load_app_config

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.yml").write_text("""
tool_paths: {}
extraction:
  auto_generate_file_list: true
""")

        config = load_app_config(tmp_path)

        assert config.extraction.auto_generate_file_list is True

    def test_missing_extraction_section_uses_defaults(self, tmp_path: Path):
        """Missing extraction section should use defaults."""
        from core.config import load_app_config

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.yml").write_text("""
tool_paths: {}
logging:
  level: DEBUG
""")

        config = load_app_config(tmp_path)

        # Should still have extraction config with defaults
        assert hasattr(config, 'extraction')
        assert config.extraction.auto_generate_file_list is True

    def test_extraction_config_dataclass(self):
        """ExtractionConfig dataclass should be importable and have correct defaults."""
        from core.config import ExtractionConfig

        config = ExtractionConfig()

        assert config.auto_generate_file_list is True

    def test_extraction_config_with_explicit_value(self):
        """ExtractionConfig should accept explicit values."""
        from core.config import ExtractionConfig

        config = ExtractionConfig(auto_generate_file_list=False)

        assert config.auto_generate_file_list is False
