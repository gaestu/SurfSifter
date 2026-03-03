"""Tests for window settings persistence."""

import json


class TestWindowSettings:
    """Tests for WindowSettings integration with AppSettings."""

    def test_app_settings_includes_window(self):
        """AppSettings exposes window settings."""
        from app.config.settings import AppSettings, WindowSettings

        settings = AppSettings()

        assert hasattr(settings, "window")
        assert isinstance(settings.window, WindowSettings)

    def test_app_settings_save_load_window(self, tmp_path):
        """Window settings persist through save/load."""
        from app.config.settings import AppSettings

        settings = AppSettings()
        settings.window.x = 120
        settings.window.y = 80
        settings.window.width = 1728
        settings.window.height = 972
        settings.window.maximized = True

        settings_file = tmp_path / "settings.json"
        settings.save(settings_file)

        loaded = AppSettings.load(settings_file)

        assert loaded.window.x == 120
        assert loaded.window.y == 80
        assert loaded.window.width == 1728
        assert loaded.window.height == 972
        assert loaded.window.maximized is True

    def test_app_settings_load_without_window_uses_defaults(self, tmp_path):
        """Loading legacy configs without window settings keeps defaults."""
        from app.config.settings import AppSettings

        old_config = {
            "general": {"thumbnail_size": 180},
            "tools": {},
            "network": {},
            "hash": {},
        }

        settings_file = tmp_path / "settings.json"
        settings_file.write_text(json.dumps(old_config), encoding="utf-8")

        loaded = AppSettings.load(settings_file)

        assert loaded.window.x is None
        assert loaded.window.y is None
        assert loaded.window.width is None
        assert loaded.window.height is None
        assert loaded.window.maximized is False
