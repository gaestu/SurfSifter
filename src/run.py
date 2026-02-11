import sys
import os
from pathlib import Path

# Add the src directory to sys.path so we can import app
src_dir = Path(__file__).resolve().parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from app.main import main

if __name__ == "__main__":
    sys.exit(main())
