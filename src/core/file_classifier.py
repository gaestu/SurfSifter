"""
File classification utilities for the Download Tab.

Provides extension-to-type mapping and file type classification.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

# Extension to file type mapping
DOWNLOADABLE_EXTENSIONS: Dict[str, str] = {
    # Images
    ".jpg": "image",
    ".jpeg": "image",
    ".png": "image",
    ".gif": "image",
    ".webp": "image",
    ".bmp": "image",
    ".tiff": "image",
    ".tif": "image",
    ".svg": "image",
    ".ico": "image",
    # Videos
    ".mp4": "video",
    ".webm": "video",
    ".avi": "video",
    ".mov": "video",
    ".mkv": "video",
    ".flv": "video",
    ".wmv": "video",
    ".m4v": "video",
    # Audio
    ".mp3": "audio",
    ".wav": "audio",
    ".ogg": "audio",
    ".m4a": "audio",
    ".flac": "audio",
    ".aac": "audio",
    ".wma": "audio",
    # Documents
    ".pdf": "document",
    ".doc": "document",
    ".docx": "document",
    ".xls": "document",
    ".xlsx": "document",
    ".ppt": "document",
    ".pptx": "document",
    ".txt": "document",
    ".rtf": "document",
    ".odt": "document",
    ".ods": "document",
    ".odp": "document",
    # Archives
    ".zip": "archive",
    ".rar": "archive",
    ".7z": "archive",
    ".tar": "archive",
    ".gz": "archive",
    ".bz2": "archive",
    ".xz": "archive",
    ".tgz": "archive",
}

# File types for UI display
FILE_TYPES: List[str] = ["image", "video", "audio", "document", "archive", "other"]

# Human-readable labels
FILE_TYPE_LABELS: Dict[str, str] = {
    "image": "Images",
    "video": "Videos",
    "audio": "Audio",
    "document": "Documents",
    "archive": "Archives",
    "other": "Other",
    "all": "All Types",
}


def classify_file_type(filename: str | Path) -> str:
    """
    Classify a file by its extension.

    Args:
        filename: The filename, path, or URL to classify

    Returns:
        File type string: 'image', 'video', 'audio', 'document', 'archive', or 'other'
    """
    filename_str = str(filename)
    # Strip query string and fragment for URLs
    if "?" in filename_str:
        filename_str = filename_str.split("?")[0]
    if "#" in filename_str:
        filename_str = filename_str.split("#")[0]
    ext = Path(filename_str.lower()).suffix
    return DOWNLOADABLE_EXTENSIONS.get(ext, "other")


def get_extension(filename: str | Path) -> str:
    """
    Get the lowercase extension from a filename.

    Args:
        filename: The filename, path, or URL

    Returns:
        Extension including dot (e.g., '.jpg') or empty string
    """
    filename_str = str(filename)
    # Strip query string and fragment for URLs
    if "?" in filename_str:
        filename_str = filename_str.split("?")[0]
    if "#" in filename_str:
        filename_str = filename_str.split("#")[0]
    return Path(filename_str.lower()).suffix


def is_downloadable(filename: str | Path) -> bool:
    """
    Check if a file has a downloadable extension.

    Args:
        filename: The filename or URL to check

    Returns:
        True if extension is in the downloadable list
    """
    ext = get_extension(filename)
    return ext in DOWNLOADABLE_EXTENSIONS


def get_extensions_for_type(file_type: str) -> List[str]:
    """
    Get all extensions for a given file type.

    Args:
        file_type: The file type ('image', 'video', etc.)

    Returns:
        List of extensions for that type
    """
    return [ext for ext, ftype in DOWNLOADABLE_EXTENSIONS.items() if ftype == file_type]
