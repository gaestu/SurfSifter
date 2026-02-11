"""Core orchestration layer for the forensic analyzer."""

from .config import AppConfig, load_app_config  # noqa: F401
from .database import (  # noqa: F401
    init_db,
    migrate,
    DatabaseManager,
    ensure_case_structure,
    slugify_label,
)
# NOTE: extraction_orchestrator not exported from package to avoid circular import
# Import directly: from core.extraction_orchestrator import run_extraction_pipeline
# Legacy alias: execute_rule_pipeline is also available for backward compatibility
