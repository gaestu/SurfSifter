"""
File List Extractor - Generate or import file lists.

Dual-path workflow:
- PATH A: Generate from E01 using SleuthKit fls (recommended)
- PATH B: Import external CSV from FTK/EnCase exports

Components:
- SystemFileListExtractor: Main extractor class (registry-discoverable)
- SleuthKitFileListGenerator: Generate file lists from EWF images using fls
- BodyfileParser: Parse SleuthKit bodyfile output
"""

from .extractor import SystemFileListExtractor
from .sleuthkit_generator import GenerationResult, SleuthKitFileListGenerator
from .bodyfile_parser import BodyfileEntry, BodyfileParser

# Legacy alias for backwards compatibility
FileListImporterExtractor = SystemFileListExtractor

__all__ = [
    'SystemFileListExtractor',
    'FileListImporterExtractor',  # Legacy alias
    'SleuthKitFileListGenerator',
    'GenerationResult',
    'BodyfileEntry',
    'BodyfileParser',
]
