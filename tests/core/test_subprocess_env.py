"""Tests for core.subprocess_env — PyInstaller LD_LIBRARY_PATH cleanup."""

from __future__ import annotations

import os
from unittest import mock

import pytest

from core.subprocess_env import clean_subprocess_env


class TestCleanSubprocessEnvNotFrozen:
    """When running from source (not frozen), clean_subprocess_env returns None."""

    def test_returns_none_when_not_frozen(self):
        assert clean_subprocess_env() is None

    def test_returns_none_when_frozen_attr_missing(self):
        # Ensure sys.frozen doesn't exist (default state)
        import sys
        assert not hasattr(sys, "frozen") or not sys.frozen
        assert clean_subprocess_env() is None


class TestCleanSubprocessEnvFrozen:
    """When running inside a PyInstaller bundle, env vars are cleaned."""

    @pytest.fixture(autouse=True)
    def _mock_frozen(self):
        with mock.patch("core.subprocess_env.sys") as mock_sys:
            mock_sys.frozen = True
            yield mock_sys

    def test_restores_ld_library_path_from_orig(self, monkeypatch):
        monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/_MEIfakeXX")
        monkeypatch.setenv("LD_LIBRARY_PATH_ORIG", "/usr/lib:/usr/local/lib")

        env = clean_subprocess_env()
        assert env is not None
        assert env["LD_LIBRARY_PATH"] == "/usr/lib:/usr/local/lib"

    def test_removes_ld_library_path_when_orig_empty(self, monkeypatch):
        monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/_MEIfakeXX")
        monkeypatch.setenv("LD_LIBRARY_PATH_ORIG", "")

        env = clean_subprocess_env()
        assert env is not None
        assert "LD_LIBRARY_PATH" not in env

    def test_removes_ld_library_path_when_no_orig(self, monkeypatch):
        monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/_MEIfakeXX")
        monkeypatch.delenv("LD_LIBRARY_PATH_ORIG", raising=False)

        env = clean_subprocess_env()
        assert env is not None
        assert "LD_LIBRARY_PATH" not in env

    def test_no_ld_library_path_at_all(self, monkeypatch):
        monkeypatch.delenv("LD_LIBRARY_PATH", raising=False)
        monkeypatch.delenv("LD_LIBRARY_PATH_ORIG", raising=False)

        env = clean_subprocess_env()
        assert env is not None
        assert "LD_LIBRARY_PATH" not in env

    def test_preserves_other_env_vars(self, monkeypatch):
        monkeypatch.setenv("MY_CUSTOM_VAR", "hello")
        monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/_MEIfakeXX")

        env = clean_subprocess_env()
        assert env is not None
        assert env["MY_CUSTOM_VAR"] == "hello"

    def test_handles_dyld_library_path(self, monkeypatch):
        monkeypatch.setenv("DYLD_LIBRARY_PATH", "/tmp/_MEIfakeXX/lib")
        monkeypatch.setenv("DYLD_LIBRARY_PATH_ORIG", "/usr/lib")

        env = clean_subprocess_env()
        assert env is not None
        assert env["DYLD_LIBRARY_PATH"] == "/usr/lib"

    def test_handles_dyld_framework_path(self, monkeypatch):
        monkeypatch.setenv("DYLD_FRAMEWORK_PATH", "/tmp/_MEIfakeXX")
        monkeypatch.delenv("DYLD_FRAMEWORK_PATH_ORIG", raising=False)

        env = clean_subprocess_env()
        assert env is not None
        assert "DYLD_FRAMEWORK_PATH" not in env

    def test_returns_copy_not_actual_environ(self, monkeypatch):
        monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/_MEIfakeXX")

        env = clean_subprocess_env()
        assert env is not None
        # Modifying the returned dict should not affect os.environ
        env["NEW_KEY"] = "test"
        assert "NEW_KEY" not in os.environ
