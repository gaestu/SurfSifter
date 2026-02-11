"""
OLE Compound File Parser for Jump Lists

Parses .automaticDestinations-ms files (OLE Compound Files).
These files contain:
- DestList stream: MRU/MFU metadata
- Numbered streams (1, 2, 3...): LNK file data
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict, Any

LOGGER = logging.getLogger(__name__)


def parse_jumplist_ole(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse Jump List OLE file and extract LNK entries.

    Args:
        filepath: Path to .automaticDestinations-ms file

    Returns:
        List of entry dicts with LNK data
    """
    try:
        import olefile
    except ImportError:
        LOGGER.warning("olefile not installed - Jump List parsing unavailable")
        return []

    entries = []

    try:
        ole = olefile.OleFileIO(str(filepath))
    except Exception as e:
        LOGGER.error("Failed to open OLE file %s: %s", filepath, e)
        return []

    try:
        # Parse DestList stream for metadata
        destlist_data = _parse_destlist(ole)

        # Get numbered streams (LNK data)
        for stream_name in ole.listdir():
            name = '/'.join(stream_name) if isinstance(stream_name, list) else stream_name

            # Skip DestList stream
            if name.lower() == 'destlist':
                continue

            # LNK streams are typically numbered (1, 2, 3, etc.)
            try:
                stream_id = int(name.replace('/', ''))
            except ValueError:
                # Try hex interpretation
                try:
                    stream_id = int(name.replace('/', ''), 16)
                except ValueError:
                    continue

            # Read LNK data
            try:
                lnk_data = ole.openstream(stream_name).read()

                # Parse LNK
                from .lnk_parser import parse_lnk_data
                lnk_info = parse_lnk_data(lnk_data)

                if lnk_info:
                    lnk_info["entry_id"] = str(stream_id)

                    # Add DestList metadata if available
                    if stream_id in destlist_data:
                        lnk_info.update(destlist_data[stream_id])

                    entries.append(lnk_info)

            except Exception as e:
                LOGGER.debug("Failed to parse stream %s: %s", name, e)

    finally:
        ole.close()

    return entries


def _parse_destlist(ole) -> Dict[int, Dict[str, Any]]:
    """
    Parse DestList stream for MRU/MFU metadata.

    DestList header format (all versions):
    - Offset 0: Version (4 bytes)
    - Offset 4: Number of entries (4 bytes)
    - Offset 8: Number of pinned entries (4 bytes)
    - Offset 12-31: Various counters and unknowns

    Entry record format for Windows 10/11 (version 4-6):
    Fixed header is 128 bytes:
    - Checksum (8 bytes) at +0
    - DROID GUIDs (64 bytes) at +8 through +71
    - NetBIOS name (16 bytes) at +72 through +87
    - Entry ID (4 bytes) at +88
    - Score/frequency float64 (8 bytes) at +92
    - Access time FILETIME (8 bytes) at +100
    - Pin status (4 bytes) at +108
    - Counter/unknown (4 bytes) at +112
    - Unknown (12 bytes) at +116
    - Path starts at +128 (UTF-16LE null-terminated)

    Entry record format for Windows 7/8 (version 1-2):
    Fixed header is 130 bytes (different layout).

    Returns:
        Dict mapping entry ID to metadata
    """
    import struct
    from datetime import datetime, timezone

    metadata = {}

    try:
        # Find DestList stream
        destlist_stream = None
        for stream_name in ole.listdir():
            name = '/'.join(stream_name) if isinstance(stream_name, list) else stream_name
            if name.lower() == 'destlist':
                destlist_stream = stream_name
                break

        if not destlist_stream:
            return metadata

        data = ole.openstream(destlist_stream).read()

        if len(data) < 32:
            return metadata

        # Parse header
        version = struct.unpack_from('<I', data, 0)[0]
        num_entries = struct.unpack_from('<I', data, 4)[0]
        num_pinned = struct.unpack_from('<I', data, 8)[0]

        LOGGER.debug("DestList: version=%d, entries=%d, pinned=%d", version, num_entries, num_pinned)

        # Parse entries based on version
        offset = 32  # Header size

        for _ in range(num_entries):
            if version >= 3:
                # Windows 10/11 format - 128 byte fixed header
                if offset + 128 > len(data):
                    break

                entry_id = struct.unpack_from('<I', data, offset + 88)[0]
                score = struct.unpack_from('<d', data, offset + 92)[0]
                access_filetime = struct.unpack_from('<Q', data, offset + 100)[0]
                pin_value = struct.unpack_from('<I', data, offset + 108)[0]

                # Find null-terminated path starting at offset + 128
                path_start = offset + 128
                path_end = path_start
                while path_end + 1 < len(data):
                    if data[path_end:path_end+2] == b'\x00\x00':
                        break
                    path_end += 2

                path_len = path_end - path_start

                # Calculate entry size: 128 byte header + path + 2 null bytes + padding to 4-byte boundary
                raw_size = 128 + path_len + 2
                entry_size = (raw_size + 3) & ~3  # Round up to 4-byte boundary

                # Convert score to access count (it's a float representing frequency)
                access_count = int(score) if 0 <= score < 100000 else None
            else:
                # Windows 7/8 format - 130 byte header + path_len field
                if offset + 130 > len(data):
                    break

                entry_id = struct.unpack_from('<I', data, offset + 88)[0]
                access_filetime = struct.unpack_from('<Q', data, offset + 100)[0]
                pin_value = struct.unpack_from('<I', data, offset + 108)[0]
                access_count = struct.unpack_from('<I', data, offset + 116)[0]
                path_len = struct.unpack_from('<H', data, offset + 128)[0]
                entry_size = 130 + path_len

            # Convert FILETIME to ISO8601
            access_time = None
            if access_filetime > 116444736000000000:
                try:
                    timestamp = (access_filetime - 116444736000000000) / 10000000
                    access_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                except (OSError, OverflowError, ValueError):
                    pass

            # Determine pin status
            pin_status = "pinned" if pin_value == 0xFFFFFFFF else "recent"

            metadata[entry_id] = {
                "access_count": access_count,
                "access_time": access_time,
                "pin_status": pin_status,
            }

            offset += entry_size

        LOGGER.debug("Parsed %d DestList entries", len(metadata))

    except Exception as e:
        LOGGER.debug("Failed to parse DestList: %s", e)

    return metadata


def parse_jumplist_custom(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse CustomDestinations-ms file (concatenated LNK format).

    CustomDestinations files are NOT OLE compound files. They contain:
    - 4-byte header (format version or magic)
    - Concatenated LNK entries, each with a footer marker

    The LNK entries are separated by a footer marker: 0xAB FB BF BA

    Args:
        filepath: Path to .customDestinations-ms file

    Returns:
        List of entry dicts with LNK data
    """
    from .lnk_parser import parse_lnk_data

    entries = []

    try:
        data = filepath.read_bytes()
    except Exception as e:
        LOGGER.error("Failed to read CustomDestinations file %s: %s", filepath, e)
        return []

    if len(data) < 24:  # Minimum viable size
        LOGGER.debug("CustomDestinations file too small: %d bytes", len(data))
        return []

    # CustomDestinations footer marker between LNK entries
    FOOTER_MARKER = b'\xab\xfb\xbf\xba'

    # LNK header magic
    LNK_MAGIC = b'\x4c\x00\x00\x00'

    # Skip initial header (typically 4 bytes)
    # Look for first LNK magic
    offset = 0
    while offset < len(data) - 4:
        if data[offset:offset + 4] == LNK_MAGIC:
            break
        offset += 1

    if offset >= len(data) - 4:
        LOGGER.debug("No LNK entries found in CustomDestinations file")
        return []

    entry_id = 0

    while offset < len(data) - 4:
        # Check for LNK magic
        if data[offset:offset + 4] != LNK_MAGIC:
            # Try to find next LNK magic
            next_lnk = data.find(LNK_MAGIC, offset)
            if next_lnk == -1:
                break
            offset = next_lnk
            continue

        # Find the end of this LNK entry (footer marker or next LNK magic)
        footer_pos = data.find(FOOTER_MARKER, offset + 4)
        next_lnk_pos = data.find(LNK_MAGIC, offset + 4)

        # Determine entry end
        if footer_pos != -1 and (next_lnk_pos == -1 or footer_pos < next_lnk_pos):
            entry_end = footer_pos
            next_offset = footer_pos + 4  # Skip footer marker
        elif next_lnk_pos != -1:
            entry_end = next_lnk_pos
            next_offset = next_lnk_pos
        else:
            # Last entry - goes to end of file
            entry_end = len(data)
            next_offset = len(data)

        # Extract and parse LNK data
        lnk_data = data[offset:entry_end]

        if len(lnk_data) >= 76:  # Minimum LNK size
            try:
                lnk_info = parse_lnk_data(lnk_data)

                if lnk_info:
                    entry_id += 1
                    lnk_info["entry_id"] = str(entry_id)
                    lnk_info["pin_status"] = "pinned"  # CustomDestinations are typically pinned
                    entries.append(lnk_info)
                    LOGGER.debug("Parsed CustomDestinations entry %d", entry_id)
            except Exception as e:
                LOGGER.debug("Failed to parse LNK entry at offset %d: %s", offset, e)

        offset = next_offset

    LOGGER.debug("Parsed %d entries from CustomDestinations file", len(entries))
    return entries


def parse_jumplist_file(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse any Jump List file (auto-detect format).

    Automatically detects whether the file is:
    - AutomaticDestinations-ms (OLE format)
    - CustomDestinations-ms (concatenated LNK format)

    Args:
        filepath: Path to Jump List file

    Returns:
        List of entry dicts with LNK data
    """
    filename = filepath.name.lower()

    if filename.endswith('.automaticdestinations-ms'):
        return parse_jumplist_ole(filepath)
    elif filename.endswith('.customdestinations-ms'):
        return parse_jumplist_custom(filepath)
    else:
        # Try OLE first, fall back to custom format
        try:
            import olefile
            if olefile.isOleFile(str(filepath)):
                return parse_jumplist_ole(filepath)
        except Exception:
            pass

        # Try custom format
        return parse_jumplist_custom(filepath)
