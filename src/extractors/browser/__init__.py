"""
Browser extractors organized by browser family.

Structure:
    browser/
    ├── chromium/    # Chrome, Edge, Brave, Opera (Blink/V8 engine)
    ├── firefox/     # Firefox, Tor (Gecko engine)
    ├── safari/      # Safari (WebKit engine, macOS only)
    └── ie_legacy/   # Internet Explorer, Legacy Edge (ESE database)

Each family shares similar internals (SQLite schemas, cache formats, paths).

Usage:
    from extractors.browser import chromium, firefox, safari, ie_legacy

    # Or directly:
    from extractors.browser.chromium import ChromiumHistoryExtractor
    from extractors.browser.ie_legacy import IEHistoryExtractor
"""

from . import chromium
from . import firefox
from . import safari
from . import ie_legacy

__all__ = ['chromium', 'firefox', 'safari', 'ie_legacy']
