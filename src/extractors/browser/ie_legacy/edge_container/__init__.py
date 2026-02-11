"""
Legacy Edge Container.dat Extractor.

Handles Edge-specific container.dat ESE databases from UWP package paths.
"""

from .extractor import LegacyEdgeContainerExtractor

# Registry expects IeLegacyEdgeContainerExtractor (family_artifact pattern)
IeLegacyEdgeContainerExtractor = LegacyEdgeContainerExtractor

__all__ = ["LegacyEdgeContainerExtractor", "IeLegacyEdgeContainerExtractor"]
