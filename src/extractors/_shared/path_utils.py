"""
Path utilities for browser extractors.

Provides utilities for resolving browser profile paths:
- Environment variable expansion (Windows-style)
- Glob pattern matching across filesystems
- Profile enumeration for multi-profile browsers

Design Principle:
    Browser paths use Windows-style environment variables even when
    parsing on Linux. These helpers handle cross-platform resolution.
"""

from __future__ import annotations

import fnmatch
import os
import re
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Callable, Iterable, Iterator, List, Optional, Set, Union


# Standard Windows environment variable mappings for browser paths
WINDOWS_ENV_DEFAULTS = {
    "LOCALAPPDATA": "AppData/Local",
    "APPDATA": "AppData/Roaming",
    "USERPROFILE": "",  # User home directory
    "PROGRAMFILES": "Program Files",
    "PROGRAMFILES(X86)": "Program Files (x86)",
    "SYSTEMROOT": "Windows",
    "WINDIR": "Windows",
}


def expand_windows_env_vars(
    path: str,
    user_home: Optional[str] = None,
    env_overrides: Optional[dict] = None,
) -> str:
    """
    Expand Windows-style environment variables in a path.

    Handles %LOCALAPPDATA%, %APPDATA%, %USERPROFILE%, etc.
    Common in browser path patterns from YAML rules.

    Args:
        path: Path string with %VAR% style variables
        user_home: User home directory for variable resolution
                  (e.g., "Users/john" without drive letter)
        env_overrides: Optional dict of VAR->value overrides

    Returns:
        Path with variables expanded

    Example:
        >>> expand_windows_env_vars("%LOCALAPPDATA%/Google/Chrome", "Users/john")
        "Users/john/AppData/Local/Google/Chrome"
    """
    if not path:
        return path

    env_map = dict(WINDOWS_ENV_DEFAULTS)
    if env_overrides:
        env_map.update(env_overrides)

    def replace_var(match: re.Match) -> str:
        var_name = match.group(1).upper()
        value = env_map.get(var_name, "")

        # Handle USERPROFILE specially - prepend user_home
        if var_name == "USERPROFILE" and user_home:
            return user_home

        # For other vars, prepend user_home if it's a relative user path
        if user_home and var_name in ("LOCALAPPDATA", "APPDATA"):
            return f"{user_home}/{value}"

        return value

    # Replace %VAR% patterns
    result = re.sub(r"%([^%]+)%", replace_var, path)

    # Normalize path separators
    result = result.replace("\\", "/")

    return result


def glob_pattern_to_regex(pattern: str) -> re.Pattern:
    """
    Convert glob pattern to regex for path matching.

    Supports:
    - * : matches any characters except /
    - ** : matches any characters including /
    - ? : matches single character
    - [abc] : character class

    Args:
        pattern: Glob pattern string

    Returns:
        Compiled regex pattern

    Example:
        >>> p = glob_pattern_to_regex("Users/*/AppData/Local/Google/Chrome/**")
        >>> p.match("Users/john/AppData/Local/Google/Chrome/Default/History")
    """
    # Escape regex special chars except our glob chars
    i = 0
    regex_parts = []
    pattern = pattern.replace("\\", "/")  # Normalize separators

    while i < len(pattern):
        c = pattern[i]

        if c == "*":
            if i + 1 < len(pattern) and pattern[i + 1] == "*":
                # ** matches everything including /
                regex_parts.append(".*")
                i += 2
                # Skip trailing / after **
                if i < len(pattern) and pattern[i] == "/":
                    i += 1
            else:
                # * matches everything except /
                regex_parts.append("[^/]*")
                i += 1
        elif c == "?":
            regex_parts.append("[^/]")
            i += 1
        elif c == "[":
            # Find closing bracket
            j = i + 1
            while j < len(pattern) and pattern[j] != "]":
                j += 1
            if j < len(pattern):
                regex_parts.append(pattern[i:j+1])
                i = j + 1
            else:
                regex_parts.append(re.escape(c))
                i += 1
        else:
            regex_parts.append(re.escape(c))
            i += 1

    regex_str = "^" + "".join(regex_parts) + "$"
    return re.compile(regex_str, re.IGNORECASE)


def find_matching_paths(
    root: Path,
    pattern: str,
    file_lister: Optional[Callable[[Path], Iterable[str]]] = None,
) -> Iterator[Path]:
    """
    Find paths matching a glob pattern under a root directory.

    Works with both local filesystems and evidence filesystems
    by accepting a custom file lister function.

    Args:
        root: Root directory to search under
        pattern: Glob pattern to match (can include **)
        file_lister: Optional function that lists paths under root.
                    If None, uses os.walk for local filesystem.

    Yields:
        Matching Path objects

    Example:
        # Local filesystem
        for p in find_matching_paths(Path("/mnt/evidence"), "Users/*/AppData/**"):
            print(p)

        # Evidence filesystem with custom lister
        def ewf_lister(root):
            for f in ewf_handle.iter_all_files():
                yield f.path

        for p in find_matching_paths(Path("/"), pattern, ewf_lister):
            print(p)
    """
    regex = glob_pattern_to_regex(pattern)

    if file_lister is not None:
        # Use custom file lister (for evidence filesystems)
        for path_str in file_lister(root):
            # Normalize path for matching
            normalized = path_str.replace("\\", "/").lstrip("/")
            if regex.match(normalized):
                yield root / normalized
    else:
        # Use local filesystem walk
        for dirpath, dirnames, filenames in os.walk(root):
            # Check directories
            for dirname in dirnames:
                full_path = Path(dirpath) / dirname
                rel_path = str(full_path.relative_to(root)).replace("\\", "/")
                if regex.match(rel_path):
                    yield full_path

            # Check files
            for filename in filenames:
                full_path = Path(dirpath) / filename
                rel_path = str(full_path.relative_to(root)).replace("\\", "/")
                if regex.match(rel_path):
                    yield full_path


def enumerate_browser_profiles(
    user_data_dir: Path,
    profile_patterns: Optional[List[str]] = None,
) -> Iterator[Path]:
    """
    Enumerate browser profile directories.

    Chromium browsers support multiple profiles (Default, Profile 1, etc.)
    Firefox uses random profile IDs.

    Args:
        user_data_dir: Browser's user data directory
                      (e.g., AppData/Local/Google/Chrome/User Data)
        profile_patterns: Glob patterns for profile dirs
                         Default: ["Default", "Profile *"] for Chromium

    Yields:
        Profile directory paths

    Example:
        for profile in enumerate_browser_profiles(chrome_user_data):
            history_db = profile / "History"
            if history_db.exists():
                process_history(history_db)
    """
    if profile_patterns is None:
        # Default Chromium profile patterns
        profile_patterns = ["Default", "Profile *"]

    if not user_data_dir.exists():
        return

    seen: Set[Path] = set()

    for pattern in profile_patterns:
        for item in user_data_dir.iterdir():
            if item.is_dir() and fnmatch.fnmatch(item.name, pattern):
                if item not in seen:
                    seen.add(item)
                    yield item


def normalize_evidence_path(
    path: Union[str, Path],
    to_posix: bool = True,
) -> str:
    """
    Normalize a path from evidence for consistent handling.

    Evidence paths may use Windows or Unix conventions depending
    on the source. This normalizes them for consistent processing.

    Args:
        path: Path to normalize
        to_posix: If True, convert to forward slashes

    Returns:
        Normalized path string

    Example:
        >>> normalize_evidence_path("Users\\john\\Documents")
        "Users/john/Documents"
    """
    path_str = str(path)

    if to_posix:
        # Convert backslashes to forward slashes
        path_str = path_str.replace("\\", "/")

        # Remove redundant slashes
        while "//" in path_str:
            path_str = path_str.replace("//", "/")

        # Remove leading slash for relative paths
        # but keep it for absolute Unix paths
        if path_str.startswith("/") and len(path_str) > 1:
            if path_str[1].isalpha() and (len(path_str) < 3 or path_str[2] == "/"):
                # Looks like /C/... - strip leading /
                path_str = path_str[1:]

    return path_str


def extract_username_from_path(path: str) -> Optional[str]:
    """
    Extract Windows username from a Users/username/... path.

    Useful for correlating artifacts to user accounts.

    Args:
        path: Path string

    Returns:
        Username if found, None otherwise

    Example:
        >>> extract_username_from_path("Users/john.doe/AppData/Local")
        "john.doe"
    """
    path = normalize_evidence_path(path)

    # Match Users/username pattern
    match = re.match(r"(?:.*?/)?Users/([^/]+)(?:/|$)", path, re.IGNORECASE)
    if match:
        username = match.group(1)
        # Filter out special directories that aren't usernames
        if username.lower() not in ("public", "default", "default user", "all users"):
            return username

    return None
