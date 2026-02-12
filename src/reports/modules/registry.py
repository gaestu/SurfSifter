"""
Module registry for auto-discovering and managing report modules.

Scans src/reports/modules/ for subfolders containing module.py files
and registers them automatically.  Falls back to package-level exports
when running inside a PyInstaller frozen bundle.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Type

from .base import BaseReportModule, ModuleMetadata
from ..paths import get_modules_dir

logger = logging.getLogger(__name__)


class ModuleRegistry:
    """Registry for discovering and managing report modules.

    Modules are auto-discovered from subdirectories of src/reports/modules/
    that contain a module.py file with a class extending BaseReportModule.

    Usage:
        registry = ModuleRegistry()

        # List all available modules
        for meta in registry.list_modules():
            print(f"{meta.name}: {meta.description}")

        # Get a specific module instance
        module = registry.get_module("tagged_urls")
        html = module.render(db_conn, evidence_id, config)
    """

    _instance: Optional["ModuleRegistry"] = None
    _modules: Dict[str, Type[BaseReportModule]]
    _instances: Dict[str, BaseReportModule]

    def __new__(cls) -> "ModuleRegistry":
        """Singleton pattern - only one registry instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._modules = {}
            cls._instance._instances = {}
            cls._instance._discover_modules()
        return cls._instance

    def _discover_modules(self) -> None:
        """Scan for modules in subdirectories."""
        modules_dir = get_modules_dir()

        if modules_dir.exists():
            for item in modules_dir.iterdir():
                if not item.is_dir():
                    continue

                # Skip __pycache__ and other non-module directories
                if item.name.startswith("_"):
                    continue

                module_file = item / "module.py"
                if not module_file.exists():
                    continue

                try:
                    self._load_module(item.name, module_file)
                except Exception as e:
                    logger.warning(f"Failed to load module from {item.name}: {e}")
        else:
            logger.warning("Report modules directory not found: %s", modules_dir)

        # Frozen-bundle fallback: filesystem scans may find nothing inside
        # a PyInstaller one-file archive.  Use package-level exports instead.
        if not self._modules:
            self._discover_from_package_exports()

    def _discover_from_package_exports(self) -> None:
        """Fallback discovery using package-level ``__all__`` exports.

        The parent ``reports.modules`` package already imports every built-in
        module class.  Iterating over ``__all__`` is reliable regardless of
        whether the filesystem is actually present (PyInstaller one-file mode).
        """
        try:
            pkg = importlib.import_module(__package__ or "reports.modules")
        except ImportError:
            return

        for name in getattr(pkg, "__all__", []):
            cls = getattr(pkg, name, None)
            if (
                isinstance(cls, type)
                and issubclass(cls, BaseReportModule)
                and cls is not BaseReportModule
            ):
                try:
                    instance = cls()
                    module_id = instance.metadata.module_id
                    if module_id not in self._modules:
                        self._modules[module_id] = cls
                        self._instances[module_id] = instance
                        logger.debug("Registered module (export fallback): %s", module_id)
                except Exception as e:
                    logger.warning("Failed to register module %s: %s", name, e)

    def _load_module(self, folder_name: str, module_file: Path) -> None:
        """Load a module from its module.py file.

        Args:
            folder_name: Name of the module folder
            module_file: Path to module.py
        """
        # Create module spec and load
        module_prefix = __package__ or "reports.modules"
        spec = importlib.util.spec_from_file_location(
            f"{module_prefix}.{folder_name}.module",
            module_file,
        )
        if spec is None or spec.loader is None:
            logger.warning(f"Could not create spec for {module_file}")
            return

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find the BaseReportModule subclass
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseReportModule)
                and attr is not BaseReportModule
            ):
                # Instantiate to get metadata
                instance = attr()
                module_id = instance.metadata.module_id

                self._modules[module_id] = attr
                self._instances[module_id] = instance

                logger.debug(f"Registered module: {module_id}")
                break

    def register(self, module_class: Type[BaseReportModule]) -> None:
        """Manually register a module class.

        Args:
            module_class: A class extending BaseReportModule
        """
        instance = module_class()
        module_id = instance.metadata.module_id

        self._modules[module_id] = module_class
        self._instances[module_id] = instance

    def get_module(self, module_id: str) -> Optional[BaseReportModule]:
        """Get a module instance by ID.

        Args:
            module_id: Module identifier

        Returns:
            Module instance, or None if not found
        """
        return self._instances.get(module_id)

    def get_module_class(self, module_id: str) -> Optional[Type[BaseReportModule]]:
        """Get a module class by ID.

        Args:
            module_id: Module identifier

        Returns:
            Module class, or None if not found
        """
        return self._modules.get(module_id)

    def list_modules(self) -> List[ModuleMetadata]:
        """List metadata for all registered modules.

        Returns:
            List of ModuleMetadata for all available modules
        """
        return [inst.metadata for inst in self._instances.values()]

    def list_modules_by_category(self) -> Dict[str, List[ModuleMetadata]]:
        """List modules grouped by category.

        Returns:
            Dictionary mapping category names to lists of module metadata
        """
        result: Dict[str, List[ModuleMetadata]] = {}

        for instance in self._instances.values():
            meta = instance.metadata
            if meta.category not in result:
                result[meta.category] = []
            result[meta.category].append(meta)

        # Sort categories and modules within
        return {
            cat: sorted(modules, key=lambda m: m.name)
            for cat, modules in sorted(result.items())
        }

    def get_all_module_ids(self) -> List[str]:
        """Get list of all registered module IDs.

        Returns:
            List of module ID strings
        """
        return list(self._modules.keys())

    def is_registered(self, module_id: str) -> bool:
        """Check if a module is registered.

        Args:
            module_id: Module identifier

        Returns:
            True if module exists in registry
        """
        return module_id in self._modules

    def reload(self) -> None:
        """Clear and re-discover all modules.

        Useful for development/testing.
        """
        self._modules.clear()
        self._instances.clear()
        self._discover_modules()


# Convenience function
def get_registry() -> ModuleRegistry:
    """Get the singleton module registry instance."""
    return ModuleRegistry()
