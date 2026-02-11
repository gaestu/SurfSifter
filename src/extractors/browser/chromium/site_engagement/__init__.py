"""
Chromium Site Engagement Extractor

Extracts site engagement and media engagement data from Chromium Preferences
files. This includes user interaction metrics stored by Chrome/Edge/Brave/Opera.

Initial implementation
"""

from .extractor import ChromiumSiteEngagementExtractor

__all__ = ["ChromiumSiteEngagementExtractor"]
