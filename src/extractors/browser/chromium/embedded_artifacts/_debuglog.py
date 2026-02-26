"""
Chromium debug.log parser for embedded artifact extraction.

Parses Chromium/CEF/CefSharp ``debug.log`` files to extract URLs from
CONSOLE log entries and other HTTP references.

Log Format (Chromium ``base/logging.h``):

    Old (CefSharp ≤ 75):
        [MMDD/HHMMSS:SEVERITY:source(line)] message

    New (Chromium 76+):
        [MMDD/HHMMSS.mmm:PID:TID:SEVERITY:source(line)] message

CONSOLE entries additionally include::

    "js message", source: URL (line)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, TextIO, Tuple

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Match the header of a Chromium log line.
# Captures:  1=MMDD  2=HHMMSS  3=milliseconds(opt)
#            4=PID(opt)  5=TID(opt)  6=SEVERITY  7=source(line)
_LOG_HEADER_RE = re.compile(
    r"^\[(\d{4})/(\d{6})"    # [MMDD/HHMMSS
    r"(?:\.(\d+))?"           # optional .mmm (milliseconds)
    r":"                       # separator
    r"(?:(\d+):(\d+):)?"      # optional PID:TID:
    r"(\w+)"                   # SEVERITY
    r":([^\]]+)"               # source(line)
    r"\]\s?"                   # ] and optional space
)

# "source: URL (line)" at end of CONSOLE entries (with optional leading comma).
_CONSOLE_SOURCE_RE = re.compile(
    r",?\s*source:\s+(https?://\S+)\s+\((\d+)\)\s*$"
)

# HTTP(S) URLs anywhere in text.  Terminates at typical delimiters.
_HTTP_URL_RE = re.compile(r"https?://[^\s<>\"'`,;\]{}]+")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DebugLogEntry:
    """A single parsed Chromium debug.log entry (possibly multi-line)."""

    line_number: int
    date_code: str           # "MMDD"  e.g. "0607"
    time_code: str           # "HHMMSS" e.g. "155418"
    severity: str            # INFO, WARNING, ERROR, FATAL
    source_location: str     # "CONSOLE(3)" or "backend_impl.cc(1037)"
    message: str
    milliseconds: Optional[str] = None
    pid: Optional[int] = None
    tid: Optional[int] = None
    # Populated for CONSOLE entries that include a "source:" directive
    console_source_url: Optional[str] = None
    console_source_line: Optional[int] = None
    # All HTTP(S) URLs found in the message body (excluding console_source_url)
    message_urls: List[str] = field(default_factory=list)

    @property
    def is_console(self) -> bool:
        """True if this entry originated from a JavaScript console message."""
        return self.source_location.upper().startswith("CONSOLE")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _iter_raw_entries(lines: Iterator[str]) -> Iterator[Tuple[int, str]]:
    """
    Yield ``(line_number, full_text)`` for each log entry.

    Chromium log entries start with ``[MMDD/HHMMSS…`` and may span multiple
    lines.  Continuation lines (those that do *not* start with a new header)
    are appended to the current entry.
    """
    current_line_no: Optional[int] = None
    current_text: Optional[str] = None

    for line_no, line in enumerate(lines, start=1):
        if _LOG_HEADER_RE.match(line):
            # New entry — yield whatever we accumulated so far
            if current_text is not None:
                yield current_line_no, current_text  # type: ignore[arg-type]
            current_line_no = line_no
            current_text = line.rstrip("\n\r")
        elif current_text is not None:
            # Continuation of current entry
            current_text += "\n" + line.rstrip("\n\r")
        # Lines before the first log header are silently skipped.

    # Flush the last accumulated entry
    if current_text is not None:
        yield current_line_no, current_text  # type: ignore[arg-type]


def _clean_url(url: str) -> str:
    """Strip trailing punctuation that is not part of the URL."""
    return url.rstrip(".,;:")


# ---------------------------------------------------------------------------
# Entry parser
# ---------------------------------------------------------------------------

def parse_entry(line_number: int, text: str) -> Optional[DebugLogEntry]:
    """Parse a single (possibly multi-line) log entry into a *DebugLogEntry*."""
    m = _LOG_HEADER_RE.match(text)
    if m is None:
        return None

    message = text[m.end():]

    entry = DebugLogEntry(
        line_number=line_number,
        date_code=m.group(1),
        time_code=m.group(2),
        severity=m.group(6),
        source_location=m.group(7),
        message=message,
        milliseconds=m.group(3),
        pid=int(m.group(4)) if m.group(4) else None,
        tid=int(m.group(5)) if m.group(5) else None,
    )

    # For CONSOLE entries, extract the ``source:`` URL directive
    if entry.is_console:
        source_match = _CONSOLE_SOURCE_RE.search(message)
        if source_match:
            entry.console_source_url = _clean_url(source_match.group(1))
            entry.console_source_line = int(source_match.group(2))

    # Extract all HTTP(S) URLs from the message body.
    # Exclude the console_source_url that was already captured above.
    urls = _HTTP_URL_RE.findall(message)
    cleaned: List[str] = []
    for u in urls:
        u = _clean_url(u)
        if u and u != entry.console_source_url:
            cleaned.append(u)
    entry.message_urls = cleaned

    return entry


# ---------------------------------------------------------------------------
# File-level parser
# ---------------------------------------------------------------------------

def _parse_stream(stream: TextIO) -> List[DebugLogEntry]:
    """Parse all log entries from an open text stream."""
    entries: List[DebugLogEntry] = []
    for line_no, text in _iter_raw_entries(iter(stream)):
        entry = parse_entry(line_no, text)
        if entry is not None:
            entries.append(entry)
    return entries


def parse_debuglog(source: "TextIO | Path | str") -> List[DebugLogEntry]:
    """
    Parse a Chromium ``debug.log`` file or stream.

    Args:
        source: File path (str or *Path*), or an already-open text stream.

    Returns:
        List of parsed *DebugLogEntry* objects.
    """
    if isinstance(source, (str, Path)):
        path = Path(source)
        # Try UTF-8 first; fall back to latin-1 for binary-ish content.
        for encoding in ("utf-8", "latin-1"):
            try:
                with open(path, "r", encoding=encoding, errors="replace") as f:
                    return _parse_stream(f)
            except UnicodeDecodeError:
                continue
        return []
    else:
        return _parse_stream(source)


# ---------------------------------------------------------------------------
# URL extraction
# ---------------------------------------------------------------------------

def extract_urls(entries: List[DebugLogEntry]) -> List[Dict[str, object]]:
    """
    Extract unique URLs from parsed debug.log entries.

    Returns a list of dicts suitable for batch insertion into the ``urls``
    table.  Each URL appears **once**; metadata reflects its first and last
    occurrence plus total count.

    Keys in each dict:
        url, source_context, severity, source_location,
        first_seen, last_seen, occurrence_count
    """
    url_info: Dict[str, Dict[str, object]] = {}

    for entry in entries:
        # CONSOLE source URLs (highest confidence)
        if entry.console_source_url:
            _track_url(url_info, entry.console_source_url, entry, "console_source")

        # Message-body URLs
        for url in entry.message_urls:
            _track_url(url_info, url, entry, "message_body")

    return list(url_info.values())


def _track_url(
    url_info: Dict[str, Dict[str, object]],
    url: str,
    entry: DebugLogEntry,
    context_type: str,
) -> None:
    """Track a single URL occurrence, maintaining first/last/count."""
    url = _clean_url(url)
    if not url:
        return

    time_str = f"{entry.date_code}/{entry.time_code}"

    if url not in url_info:
        url_info[url] = {
            "url": url,
            "source_context": context_type,
            "severity": entry.severity,
            "source_location": entry.source_location,
            "first_seen": time_str,
            "last_seen": time_str,
            "occurrence_count": 1,
        }
    else:
        info = url_info[url]
        info["last_seen"] = time_str
        info["occurrence_count"] = int(info["occurrence_count"]) + 1  # type: ignore[arg-type]
