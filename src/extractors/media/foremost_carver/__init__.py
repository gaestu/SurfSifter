"""Foremost carver extractor (image carving)."""

from .extractor import ForemostCarverExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
MediaForemostCarverExtractor = ForemostCarverExtractor

__all__ = ["ForemostCarverExtractor", "MediaForemostCarverExtractor"]
