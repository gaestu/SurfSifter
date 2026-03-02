"""Reports module - self-contained report generation.

This module provides:
- UI components in reports.ui
- Database persistence in reports.database
- Module system in reports.modules (plugin-based report modules)
- Report generation with templates (generator.py)
- HTML templates in reports/templates/
"""

from core.app_version import get_app_version

__version__ = get_app_version()

# UI components
from .ui import ReportTabWidget, SectionEditorDialog, SectionCard, ModulePickerDialog

# Database helpers
from .database import (
    insert_custom_section,
    update_custom_section,
    delete_custom_section,
    get_custom_sections,
    get_custom_section_by_id,
    reorder_custom_section,
    # Module helpers
    insert_section_module,
    update_section_module,
    delete_section_module,
    get_section_modules,
    get_section_module_by_id,
    reorder_section_module,
    delete_modules_by_section,
    get_modules_count_by_section,
    # Appendix helpers
    insert_appendix_module,
    update_appendix_module,
    delete_appendix_module,
    get_appendix_modules,
    get_appendix_module_by_id,
    reorder_appendix_module,
)

# Module system
from .modules import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
    ModuleRegistry,
)
from .appendix import (
    BaseAppendixModule,
    AppendixRegistry,
)

# Report generation
from .generator import (
    ReportBuilder,
    ReportGenerator,
    ReportData,
    ReportMode,
    SectionData,
    build_report,
)

__all__ = [
    # UI
    "ReportTabWidget",
    "SectionEditorDialog",
    "SectionCard",
    "ModulePickerDialog",
    # Section database
    "insert_custom_section",
    "update_custom_section",
    "delete_custom_section",
    "get_custom_sections",
    "get_custom_section_by_id",
    "reorder_custom_section",
    # Module database
    "insert_section_module",
    "update_section_module",
    "delete_section_module",
    "get_section_modules",
    "get_section_module_by_id",
    "reorder_section_module",
    "delete_modules_by_section",
    "get_modules_count_by_section",
    # Appendix database
    "insert_appendix_module",
    "update_appendix_module",
    "delete_appendix_module",
    "get_appendix_modules",
    "get_appendix_module_by_id",
    "reorder_appendix_module",
    # Module system
    "BaseReportModule",
    "FilterField",
    "FilterType",
    "ModuleMetadata",
    "ModuleRegistry",
    # Appendix system
    "BaseAppendixModule",
    "AppendixRegistry",
    # Report generation
    "ReportBuilder",
    "ReportGenerator",
    "ReportData",
    "ReportMode",
    "SectionData",
    "build_report",
]
