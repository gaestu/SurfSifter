"""Tests for report/appendix package exports and registry coverage."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Set, Type
from unittest.mock import patch

import reports.appendix as appendix_package
import reports.modules as modules_package
from reports.appendix import AppendixRegistry
from reports.appendix.base import BaseAppendixModule
from reports.modules import ModuleRegistry
from reports.modules.base import BaseReportModule


def _collect_exported_module_ids(
    package_name: str,
    package_paths,
    base_class: Type,
) -> Set[str]:
    """Collect module IDs from exported module classes in child packages."""
    module_ids: Set[str] = set()

    for _, subpackage_name, ispkg in pkgutil.iter_modules(package_paths):
        if not ispkg or subpackage_name.startswith("_"):
            continue

        subpackage = importlib.import_module(f"{package_name}.{subpackage_name}")

        exported_classes = [
            getattr(subpackage, attr_name)
            for attr_name in dir(subpackage)
            if (
                isinstance(getattr(subpackage, attr_name), type)
                and issubclass(getattr(subpackage, attr_name), base_class)
                and getattr(subpackage, attr_name) is not base_class
            )
        ]

        assert exported_classes, f"No exported module class found in {package_name}.{subpackage_name}"

        for module_class in exported_classes:
            module_ids.add(module_class().metadata.module_id)

    return module_ids


def test_reports_module_exports_match_registry() -> None:
    """All report module packages export classes that are discoverable by the registry."""
    expected_ids = _collect_exported_module_ids(
        "reports.modules",
        modules_package.__path__,
        BaseReportModule,
    )

    registry = ModuleRegistry()
    registry.reload()

    assert expected_ids == set(registry.get_all_module_ids())


def test_appendix_module_exports_match_registry() -> None:
    """All appendix module packages export classes that are discoverable by the registry."""
    expected_ids = _collect_exported_module_ids(
        "reports.appendix",
        appendix_package.__path__,
        BaseAppendixModule,
    )

    registry = AppendixRegistry()
    registry.reload()

    assert expected_ids == set(registry.get_all_module_ids())


def test_module_registry_fallback_when_dir_missing() -> None:
    """ModuleRegistry discovers modules via __all__ when modules dir is absent."""
    fake_dir = Path("/nonexistent/reports/modules")
    # Reset singleton so _discover_modules runs fresh
    ModuleRegistry._instance = None
    try:
        with patch("reports.modules.registry.get_modules_dir", return_value=fake_dir):
            registry = ModuleRegistry()
        assert len(registry.get_all_module_ids()) > 0, (
            "Fallback should discover modules from package exports"
        )
    finally:
        # Reset singleton for other tests
        ModuleRegistry._instance = None


def test_appendix_registry_fallback_when_dir_missing() -> None:
    """AppendixRegistry discovers modules via __all__ when appendix dir is absent."""
    fake_dir = Path("/nonexistent/reports/appendix")
    AppendixRegistry._instance = None
    try:
        with patch("reports.appendix.registry.get_appendix_dir", return_value=fake_dir):
            registry = AppendixRegistry()
        assert len(registry.get_all_module_ids()) > 0, (
            "Fallback should discover appendix modules from package exports"
        )
    finally:
        AppendixRegistry._instance = None
