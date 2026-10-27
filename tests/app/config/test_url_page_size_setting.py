"""Tests for the persisted URLs tab rows-per-page setting (issue #61)."""

import json


def test_url_page_size_defaults_to_500():
    from app.config.settings import AppSettings

    assert AppSettings().general.url_page_size == 500


def test_url_page_size_persists_through_save_load(tmp_path):
    from app.config.settings import AppSettings

    settings = AppSettings()
    settings.general.url_page_size = 5000
    settings_file = tmp_path / "settings.json"
    settings.save(settings_file)

    assert AppSettings.load(settings_file).general.url_page_size == 5000


def test_legacy_config_without_url_page_size_uses_default(tmp_path):
    from app.config.settings import AppSettings

    settings_file = tmp_path / "settings.json"
    settings_file.write_text(json.dumps({"general": {"thumbnail_size": 200}}), encoding="utf-8")

    loaded = AppSettings.load(settings_file)
    assert loaded.general.thumbnail_size == 200
    assert loaded.general.url_page_size == 500
