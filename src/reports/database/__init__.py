"""Reports database module - persistence for custom report sections and modules."""

from .helpers import (
    insert_custom_section,
    update_custom_section,
    delete_custom_section,
    get_custom_sections,
    get_custom_section_by_id,
    reorder_custom_section,
)

from .module_helpers import (
    insert_section_module,
    update_section_module,
    delete_section_module,
    get_section_modules,
    get_section_module_by_id,
    reorder_section_module,
    delete_modules_by_section,
    get_modules_count_by_section,
)
from .appendix_helpers import (
    insert_appendix_module,
    update_appendix_module,
    delete_appendix_module,
    get_appendix_modules,
    get_appendix_module_by_id,
    reorder_appendix_module,
)
from .settings_helpers import (
    get_report_settings,
    save_report_settings,
    delete_report_settings,
)

__all__ = [
    # Section helpers
    "insert_custom_section",
    "update_custom_section",
    "delete_custom_section",
    "get_custom_sections",
    "get_custom_section_by_id",
    "reorder_custom_section",
    # Module helpers
    "insert_section_module",
    "update_section_module",
    "delete_section_module",
    "get_section_modules",
    "get_section_module_by_id",
    "reorder_section_module",
    "delete_modules_by_section",
    "get_modules_count_by_section",
    # Appendix helpers
    "insert_appendix_module",
    "update_appendix_module",
    "delete_appendix_module",
    "get_appendix_modules",
    "get_appendix_module_by_id",
    "reorder_appendix_module",
    # Settings helpers
    "get_report_settings",
    "save_report_settings",
    "delete_report_settings",
]
