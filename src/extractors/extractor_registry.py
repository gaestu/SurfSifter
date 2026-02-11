"""
Extractor registry for auto-discovery and management.
"""

from typing import Dict, List, Optional, Set
from pathlib import Path
import importlib
import pkgutil
import sys

from .base import BaseExtractor


# Directories to skip during extractor discovery
SKIP_DIRECTORIES: Set[str] = {
    # Internal/base modules
    'base',
    'extractor_registry',
    'callbacks',
    'exceptions',
    'workers',
    # Shared utilities (not extractors)
    '_shared',
    # Legacy/renamed modules
    'image_carving',
}

# Group directories that contain nested extractors
GROUP_DIRECTORIES: Set[str] = {
    'browser',  # browser/chromium/, browser/firefox/, browser/safari/
    'system',   # system/registry/, system/jump_lists/, system/file_list/
    'media',    # media/foremost/, media/scalpel/
    'carvers',  # carvers/bulk_extractor/, carvers/browser_carver/
    'cache',    # cache/cache_simple/, cache/cache_firefox/
}

# Implementation modules within groups that are NOT registry-discoverable
# (used as delegates by wrapper extractors in other locations)
SKIP_GROUP_MODULES: Dict[str, Set[str]] = {
    # Currently no modules need to be skipped
}


class ExtractorRegistry:
    """
    Central registry for all extractor modules.

    Auto-discovers modules in extractors/ and manages lifecycle.

    Discovery convention:
        - Each extractor lives in extractors/{name}/
        - Must have __init__.py that exports {Name}Extractor class
        - Class must inherit from BaseExtractor

    Example directory structure:
        src/extractors/
            bulk_extractor/
                __init__.py          # from .extractor import BulkExtractorExtractor
                extractor.py         # class BulkExtractorExtractor(BaseExtractor)
                ui.py
                worker.py

    Usage:
        registry = ExtractorRegistry()

        # Get specific extractor
        bulk = registry.get("bulk_extractor")

        # Get all extractors
        all_extractors = registry.get_all()

        # Get by category
        forensic = registry.get_by_category("forensic_tools")
    """

    def __init__(self):
        self._modules: Dict[str, BaseExtractor] = {}
        self._discover_modules()

    def _discover_modules(self):
        """
        Auto-discover extractor modules in src/extractors/.

        Looks for subpackages that export a class inheriting from BaseExtractor.
        Silently ignores modules that fail to load (allows partial registry).

        Supports two discovery patterns:
        1. Flat: extractors/{module_name}/ (e.g., bulk_extractor)
        2. Nested: extractors/{group}/{family}/{artifact}/ (e.g., browser/chromium/history)
        """
        extractors_package = None
        candidates = []
        if __package__:
            candidates.append(__package__)
        candidates.append("extractors")
        for candidate in dict.fromkeys(candidates):
            try:
                extractors_package = importlib.import_module(candidate)
                self._package_import_prefix = candidate
                break
            except ImportError:
                continue
        if extractors_package is None:
            # Cannot find extractors package
            return

        package_path = Path(extractors_package.__file__).parent

        # Iterate over subdirectories
        for finder, module_name, ispkg in pkgutil.iter_modules([str(package_path)]):
            # Skip private modules and non-packages
            if module_name.startswith('_') or not ispkg:
                continue

            # Skip special directories (base modules, utilities, legacy)
            if module_name in SKIP_DIRECTORIES:
                continue

            # Check if this is a group directory (contains nested extractors)
            if module_name in GROUP_DIRECTORIES:
                self._discover_group_modules(package_path / module_name, module_name)
                continue

            try:
                self._load_module(module_name)
            except Exception as e:
                # Log but don't fail - allow partial registry
                print(f"Warning: Failed to load extractor module '{module_name}': {e}")

    def _discover_group_modules(self, group_path: Path, group_name: str):
        """
        Discover extractors within a group directory.

        Supports two nesting patterns:
        1. 3-level (browser): group/family/artifact/ (e.g., browser/chromium/history)
        2. 2-level (system, media, carvers, cache, importers): group/extractor/ (e.g., system/registry)

        Args:
            group_path: Path to the group directory (e.g., .../extractors/browser)
            group_name: Name of the group (e.g., "browser")
        """
        if not group_path.is_dir():
            return

        # Browser group uses 3-level nesting (family/artifact)
        if group_name == 'browser':
            self._discover_browser_family_modules(group_path, group_name)
        else:
            # Other groups use 2-level nesting (direct extractor folders)
            self._discover_direct_modules(group_path, group_name)

    def _discover_browser_family_modules(self, group_path: Path, group_name: str):
        """
        Discover browser extractors with 3-level nesting: browser/family/artifact/.

        Example: browser/chromium/history/ → ChromiumHistoryExtractor
        """
        # Iterate over family directories (e.g., chromium, firefox, safari)
        for family_dir in group_path.iterdir():
            if not family_dir.is_dir() or family_dir.name.startswith('_'):
                continue

            family_name = family_dir.name

            # Iterate over artifact directories (e.g., history, cookies)
            for artifact_dir in family_dir.iterdir():
                if not artifact_dir.is_dir() or artifact_dir.name.startswith('_'):
                    continue

                artifact_name = artifact_dir.name

                # Check if this directory has an __init__.py (is a package)
                if not (artifact_dir / '__init__.py').exists():
                    continue

                # Build the module path: browser.chromium.history
                nested_module_path = f"{group_name}.{family_name}.{artifact_name}"

                try:
                    self._load_nested_module(nested_module_path, family_name, artifact_name)
                except Exception as e:
                    print(f"Warning: Failed to load extractor '{nested_module_path}': {e}")

    def _discover_direct_modules(self, group_path: Path, group_name: str):
        """
        Discover extractors with 2-level nesting: group/extractor/.

        Example: system/registry/ → SystemRegistryExtractor

        Class naming convention: {Group}{Extractor}Extractor
        - system/registry → SystemRegistryExtractor
        - media/filesystem_images → MediaFilesystemImagesExtractor (but we use legacy names)
        """
        # Get skip list for this group (implementation modules not meant for discovery)
        skip_modules = SKIP_GROUP_MODULES.get(group_name, set())

        for extractor_dir in group_path.iterdir():
            if not extractor_dir.is_dir() or extractor_dir.name.startswith('_'):
                continue

            extractor_name = extractor_dir.name

            # Skip implementation modules (used as delegates, not directly discoverable)
            if extractor_name in skip_modules:
                continue

            # Check if this directory has an __init__.py (is a package)
            if not (extractor_dir / '__init__.py').exists():
                continue

            # Build the module path: system.registry
            module_path = f"{group_name}.{extractor_name}"

            try:
                self._load_group_module(module_path, group_name, extractor_name)
            except Exception as e:
                print(f"Warning: Failed to load extractor '{module_path}': {e}")

    def _load_module(self, module_name: str):
        """
        Load a single extractor module.

        Args:
            module_name: Name of the module (e.g., "bulk_extractor")
        """
        # Import the module package using discovered prefix
        module_path = f'{self._package_import_prefix}.{module_name}'
        module = importlib.import_module(module_path)

        # Look for extractor class using naming convention
        # bulk_extractor → BulkExtractorExtractor
        # file_list_importer → FileListImporterExtractor
        class_name = self._module_name_to_class_name(module_name)

        extractor_class = getattr(module, class_name, None)

        if extractor_class is None:
            raise ValueError(
                f"Module '{module_name}' does not export '{class_name}'"
            )

        # Get BaseExtractor from the SAME import path as the module
        # This ensures isinstance/issubclass checks work correctly
        base_module = importlib.import_module(f'{self._package_import_prefix}.base')
        RegistryBaseExtractor = base_module.BaseExtractor

        if not issubclass(extractor_class, RegistryBaseExtractor):
            raise ValueError(
                f"Class '{class_name}' does not inherit from BaseExtractor"
            )

        # Instantiate and register
        instance = extractor_class()
        self._modules[instance.metadata.name] = instance

    def _load_nested_module(self, nested_path: str, family_name: str, artifact_name: str):
        """
        Load a nested extractor module from a group directory.

        Args:
            nested_path: Dotted path within extractors (e.g., "browser.chromium.history")
            family_name: Name of the family (e.g., "chromium")
            artifact_name: Name of the artifact (e.g., "history")

        The expected class name follows the pattern: {Family}{Artifact}Extractor
        Examples:
            browser.chromium.history → ChromiumHistoryExtractor
            browser.firefox.cookies → FirefoxCookiesExtractor
            system.registry.hives → RegistryHivesExtractor
        """
        # Import the module package
        module_path = f'{self._package_import_prefix}.{nested_path}'
        module = importlib.import_module(module_path)

        # Build class name: chromium_history → ChromiumHistoryExtractor
        combined_name = f"{family_name}_{artifact_name}"
        class_name = self._module_name_to_class_name(combined_name)

        extractor_class = getattr(module, class_name, None)

        if extractor_class is None:
            raise ValueError(
                f"Module '{nested_path}' does not export '{class_name}'"
            )

        # Get BaseExtractor from the SAME import path as the module
        base_module = importlib.import_module(f'{self._package_import_prefix}.base')
        RegistryBaseExtractor = base_module.BaseExtractor

        if not issubclass(extractor_class, RegistryBaseExtractor):
            raise ValueError(
                f"Class '{class_name}' does not inherit from BaseExtractor"
            )

        # Instantiate and register
        instance = extractor_class()
        self._modules[instance.metadata.name] = instance

    def _load_group_module(self, module_path: str, group_name: str, extractor_name: str):
        """
        Load an extractor from a 2-level group directory.

        Args:
            module_path: Dotted path within extractors (e.g., "system.registry")
            group_name: Name of the group (e.g., "system")
            extractor_name: Name of the extractor folder (e.g., "registry")

        The expected class name follows the pattern: {Group}{Extractor}Extractor
        Examples:
            system.registry → SystemRegistryExtractor
            system.jump_lists → SystemJumpListsExtractor
        """
        # Import the module package
        full_path = f'{self._package_import_prefix}.{module_path}'
        module = importlib.import_module(full_path)

        # Build class name: system_registry → SystemRegistryExtractor
        combined_name = f"{group_name}_{extractor_name}"
        class_name = self._module_name_to_class_name(combined_name)

        extractor_class = getattr(module, class_name, None)

        if extractor_class is None:
            raise ValueError(
                f"Module '{module_path}' does not export '{class_name}'"
            )

        # Get BaseExtractor from the SAME import path as the module
        base_module = importlib.import_module(f'{self._package_import_prefix}.base')
        RegistryBaseExtractor = base_module.BaseExtractor

        if not issubclass(extractor_class, RegistryBaseExtractor):
            raise ValueError(
                f"Class '{class_name}' does not inherit from BaseExtractor"
            )

        # Instantiate and register
        instance = extractor_class()
        self._modules[instance.metadata.name] = instance

    def _module_name_to_class_name(self, module_name: str) -> str:
        """
        Convert module name to expected class name.

        Args:
            module_name: Module name (e.g., "bulk_extractor")

        Returns:
            Expected class name (e.g., "BulkExtractorExtractor")

        Examples:
            bulk_extractor → BulkExtractorExtractor
            file_list_importer → FileListImporterExtractor
            browser_history → BrowserHistoryExtractor
        """
        # Split on underscore, capitalize each part
        parts = module_name.split('_')
        capitalized = ''.join(word.capitalize() for word in parts)
        return f"{capitalized}Extractor"

    def get(self, name: str) -> Optional[BaseExtractor]:
        """
        Get extractor by name.

        Args:
            name: Extractor name (from metadata.name)

        Returns:
            Extractor instance or None if not found

        Example:
            bulk = registry.get("bulk_extractor")
        """
        return self._modules.get(name)

    def get_all(self) -> List[BaseExtractor]:
        """
        Get all registered extractors.

        Returns:
            List of all extractor instances

        Example:
            for extractor in registry.get_all():
                print(extractor.metadata.display_name)
        """
        return list(self._modules.values())

    def get_by_category(self, category: str) -> List[BaseExtractor]:
        """
        Get extractors in a specific category.

        Args:
            category: Category name ("forensic_tools" | "browser" | "system")

        Returns:
            List of extractors in that category

        Example:
            forensic = registry.get_by_category("forensic_tools")
        """
        return [
            extractor
            for extractor in self._modules.values()
            if extractor.metadata.category == category
        ]

    def list_names(self) -> List[str]:
        """
        Get list of all registered extractor names.

        Returns:
            List of extractor names

        Example:
            names = registry.list_names()
            # ["bulk_extractor", "browser_history", "file_list_importer"]
        """
        return list(self._modules.keys())

    def count(self) -> int:
        """
        Get count of registered extractors.

        Returns:
            Number of registered extractors
        """
        return len(self._modules)
