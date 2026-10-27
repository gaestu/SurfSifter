"""Firefox browser configuration extractor."""

from .extractor import FirefoxConfigExtractor
from ._parser import parse_firefox_pref_file

__all__ = [
    "FirefoxConfigExtractor",
    "parse_firefox_pref_file",
]
