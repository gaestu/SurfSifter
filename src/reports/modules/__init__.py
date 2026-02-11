"""
Report modules package.

Provides plugin-based report modules that can be added to custom sections.
Each module lives in its own subfolder with code and HTML template.

Usage:
    from reports.modules import ModuleRegistry, BaseReportModule

    # Get all available modules
    registry = ModuleRegistry()
    modules = registry.get_all_modules()

    # Instantiate and render a module
    module = registry.get_module("tagged_urls")
    html = module.render(db_conn, evidence_id, config)
"""

from .base import BaseReportModule, FilterField, FilterType, ModuleMetadata
from .registry import ModuleRegistry

__all__ = [
    "BaseReportModule",
    "FilterField",
    "FilterType",
    "ModuleMetadata",
    "ModuleRegistry",
]
