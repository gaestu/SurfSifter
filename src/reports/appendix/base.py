"""
Base classes for appendix modules.

Appendix modules render content that is separate from standard report modules.
They share the same filter and rendering interface but live in a distinct registry.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..modules.base import BaseReportModule, FilterField, FilterType, ModuleMetadata


class BaseAppendixModule(BaseReportModule):
    """Base class for all appendix modules."""

    def get_default_title(self) -> str:
        """Return default title for appendix items."""
        return self.metadata.name

    # Re-export type hints for convenience in appendix modules.
    FilterField = FilterField
    FilterType = FilterType
    ModuleMetadata = ModuleMetadata
    ConfigDict = Dict[str, Any]
    FilterList = List[FilterField]
