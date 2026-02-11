import sys
import os
from pathlib import Path

# Handle --version early, before heavy imports trigger optional-dependency warnings.
if "--version" in sys.argv or "-V" in sys.argv:
    src_dir = Path(__file__).resolve().parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from core.app_version import get_app_version
    print(f"SurfSifter {get_app_version()}")
    sys.exit(0)

# Add the src directory to sys.path so we can import app
src_dir = Path(__file__).resolve().parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from app.main import main

if __name__ == "__main__":
    sys.exit(main())
