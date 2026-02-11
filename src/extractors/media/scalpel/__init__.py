"""Scalpel image carving extractor."""

from .extractor import ScalpelExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
MediaScalpelExtractor = ScalpelExtractor

__all__ = ["ScalpelExtractor", "MediaScalpelExtractor"]
