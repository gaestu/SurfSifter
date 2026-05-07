"""
Registry rules utility

Provides registry analysis rules for the SystemRegistryExtractor.
Rules are now defined in Python (rules.py) for modular architecture.

Returns registry analysis targets in legacy dict format for existing consumers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List

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


def load_registry_rules() -> RegistryRules:
    """
    Load registry rules from Python module.

    Returns:
        RegistryRules object with targets in dict format

    Note:
        Rules are now defined in rules.py for modular architecture.
    """
    targets = get_all_targets_as_dicts()
    return RegistryRules(targets=targets)


def get_targets() -> List[RegistryTarget]:
    """
    Get registry targets as dataclass objects.

    Returns:
        List of RegistryTarget objects (for new code)
    """
    return get_registry_targets()
