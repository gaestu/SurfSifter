"""Appendix module system - separate registry for appendix modules."""

from .base import BaseAppendixModule
from .registry import AppendixRegistry, get_registry

__all__ = [
    "BaseAppendixModule",
    "AppendixRegistry",
    "get_registry",
]
