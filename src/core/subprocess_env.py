"""Clean environment for spawning external tool subprocesses.

PyInstaller sets ``LD_LIBRARY_PATH`` (Linux) and ``DYLD_LIBRARY_PATH`` /
``DYLD_FRAMEWORK_PATH`` (macOS) to its temporary ``_MEIPASS`` directory so the
frozen Python interpreter loads bundled shared libraries.  Child processes
inherit this and may load incompatible libraries, causing dynamic-linker errors.

:func:`clean_subprocess_env` returns a sanitised copy of ``os.environ`` when
running inside a frozen PyInstaller bundle, or ``None`` when running from source
(so ``subprocess.run(env=None)`` keeps the default behaviour with zero overhead).
"""

from __future__ import annotations

import os
import sys
from typing import Optional


_PYINSTALLER_LIB_VARS = (
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "DYLD_FRAMEWORK_PATH",
)


def clean_subprocess_env() -> Optional[dict[str, str]]:
    """Return an environment dict safe for spawning external tools.

    When running inside a PyInstaller bundle (``sys.frozen`` is *True*):
      * Restores each library-path variable from its ``*_ORIG`` backup
        (PyInstaller saves the original value before overwriting).
      * Falls back to removing the variable entirely if no backup exists.

    When running from source:
      * Returns ``None`` so callers can pass it straight to
        ``subprocess.run(env=…)`` without any overhead.
    """
    if not getattr(sys, "frozen", False):
        return None

    env = os.environ.copy()

    for var in _PYINSTALLER_LIB_VARS:
        orig_key = f"{var}_ORIG"
        if orig_key in env:
            orig_value = env[orig_key]
            if orig_value:
                env[var] = orig_value
            else:
                env.pop(var, None)
        elif var in env:
            env.pop(var)

    return env
