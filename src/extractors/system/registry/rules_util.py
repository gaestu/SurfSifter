"""
Registry rules utility

Provides registry analysis rules for the SystemRegistryExtractor.
Rules are now defined in Python (rules.py) for modular architecture.

Backward-compatible API: load_registry_rules() returns targets in dict format.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from .rules import (
    get_registry_targets,
    get_all_targets_as_dicts,
    RegistryTarget,
)


@dataclass(slots=True)
class RegistryRules:
    """
    Container for registry analysis rules.

    Compatible with the old CompiledRules interface but backed by Python rules.
    """
    targets: List[Dict[str, Any]]

    @property
    def signatures(self) -> List[Dict[str, Any]]:
        """No signatures in registry rules."""
        return []

    @property
    def detectors(self) -> List[Dict[str, Any]]:
        """No detectors in registry rules."""
        return []

    @property
    def timeline_sources(self) -> List[Dict[str, Any]]:
        """No timeline sources in registry rules."""
        return []


def load_registry_rules(custom_path: Optional[str] = None) -> RegistryRules:
    """
    Load registry rules from Python module.

    Args:
        custom_path: Ignored (kept for backward compatibility).
                     Custom rules are no longer supported.

    Returns:
        RegistryRules object with targets in dict format

    Note:
        The custom_path parameter is ignored in.
        Rules are now defined in rules.py for modular architecture.
    """
    # Ignore custom_path - rules are now embedded in Python
    targets = get_all_targets_as_dicts()
    return RegistryRules(targets=targets)


def get_targets() -> List[RegistryTarget]:
    """
    Get registry targets as dataclass objects.

    Returns:
        List of RegistryTarget objects (for new code)
    """
    return get_registry_targets()
