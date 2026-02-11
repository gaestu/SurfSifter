"""
System extractors - Windows system artifact analysis.

This module provides extractors for Windows system artifacts:
- Registry: Offline Windows registry parsing
- Jump Lists: Windows Jump List URL recovery

Usage:
    from extractors.system.registry import SystemRegistryExtractor
    from extractors.system.jump_lists import SystemJumpListsExtractor
"""

from __future__ import annotations

# New v2.0 extractors with StatisticsCollector integration
from .registry import SystemRegistryExtractor
from .jump_lists import SystemJumpListsExtractor

__all__ = [
    "SystemRegistryExtractor",
    "SystemJumpListsExtractor",
]
