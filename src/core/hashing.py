from __future__ import annotations

import hashlib
from pathlib import Path


def hash_file(path: Path, alg: str = "sha256", chunk_size: int = 65536) -> str:
    """Compute a file hash using the requested algorithm."""
    hasher = hashlib.new(alg)
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()
