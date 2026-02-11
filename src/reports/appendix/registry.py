"""
Appendix module registry for auto-discovering appendix modules.

Scans src/reports/appendix/ for subfolders containing module.py files
and registers them automatically.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Dict, List, Optional, Type

from .base import BaseAppendixModule
from ..paths import get_appendix_dir

logger = logging.getLogger(__name__)


class AppendixRegistry:
    """Registry for discovering and managing appendix modules."""

    _instance: Optional["AppendixRegistry"] = None
    _modules: Dict[str, Type[BaseAppendixModule]]
    _instances: Dict[str, BaseAppendixModule]

    def __new__(cls) -> "AppendixRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._modules = {}
            cls._instance._instances = {}
            cls._instance._discover_modules()
        return cls._instance

    def _discover_modules(self) -> None:
        modules_dir = get_appendix_dir()

        if not modules_dir.exists():
            logger.warning("Appendix modules directory not found: %s", modules_dir)
            return

        for item in modules_dir.iterdir():
            if not item.is_dir():
                continue
            if item.name.startswith("_"):
                continue

            module_file = item / "module.py"
            if not module_file.exists():
                continue

            try:
                self._load_module(item.name, module_file)
            except Exception as exc:
                logger.warning("Failed to load appendix module from %s: %s", item.name, exc)

    def _load_module(self, folder_name: str, module_file: Path) -> None:
        module_prefix = __package__ or "reports.appendix"
        spec = importlib.util.spec_from_file_location(
            f"{module_prefix}.{folder_name}.module",
            module_file,
        )
        if spec is None or spec.loader is None:
            logger.warning("Could not create spec for %s", module_file)
            return

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseAppendixModule)
                and attr is not BaseAppendixModule
            ):
                instance = attr()
                module_id = instance.metadata.module_id
                self._modules[module_id] = attr
                self._instances[module_id] = instance
                logger.debug("Registered appendix module: %s", module_id)
                break

    def register(self, module_class: Type[BaseAppendixModule]) -> None:
        instance = module_class()
        module_id = instance.metadata.module_id
        self._modules[module_id] = module_class
        self._instances[module_id] = instance

    def get_module(self, module_id: str) -> Optional[BaseAppendixModule]:
        return self._instances.get(module_id)

    def get_module_class(self, module_id: str) -> Optional[Type[BaseAppendixModule]]:
        return self._modules.get(module_id)

    def list_modules(self):
        return [inst.metadata for inst in self._instances.values()]

    def list_modules_by_category(self) -> Dict[str, List]:
        result: Dict[str, List] = {}
        for instance in self._instances.values():
            meta = instance.metadata
            result.setdefault(meta.category, []).append(meta)
        return {
            cat: sorted(mods, key=lambda m: m.name)
            for cat, mods in sorted(result.items())
        }

    def get_all_module_ids(self) -> List[str]:
        return list(self._modules.keys())

    def is_registered(self, module_id: str) -> bool:
        return module_id in self._modules

    def reload(self) -> None:
        self._modules.clear()
        self._instances.clear()
        self._discover_modules()


def get_registry() -> AppendixRegistry:
    return AppendixRegistry()
