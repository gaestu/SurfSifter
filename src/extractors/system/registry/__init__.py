"""
System Registry Extractor

Extracts Windows registry indicators for forensic analysis.

Features:
- Offline registry parsing (no Windows required)
- SYSTEM and SOFTWARE hive support
- Rule-based detection (Deep Freeze, kiosk mode, etc.)
- Forensic provenance tracking
- StatisticsCollector integration for run tracking
"""

from .extractor import SystemRegistryExtractor

__all__ = ["SystemRegistryExtractor"]
