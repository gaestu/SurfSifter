"""Appendix module system - separate registry for appendix modules."""

from .base import BaseAppendixModule
from .registry import AppendixRegistry, get_registry
from .extracted_analyzed_data import AppendixExtractedAnalyzedDataModule
from .file_list import AppendixFileListModule
from .image_list import AppendixImageListModule
from .url_list import AppendixUrlListModule

__all__ = [
    "BaseAppendixModule",
    "AppendixRegistry",
    "get_registry",
    # Built-in appendix modules
    "AppendixExtractedAnalyzedDataModule",
    "AppendixFileListModule",
    "AppendixImageListModule",
    "AppendixUrlListModule",
]
