#!/usr/bin/env python3
"""Generate SurfSifter branding assets from the source PNG.

Creates:
  - config/branding/logo.jpg    (report logo, from surfsifter.png)
  - config/branding/surfsifter.ico  (Windows multi-size icon)

Requires: Pillow
"""

from pathlib import Path
from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
BRANDING = ROOT / "config" / "branding"
SRC_PNG = BRANDING / "surfsifter.png"


def generate_logo_jpg() -> None:
    """Convert PNG → JPEG for use as default report logo."""
    img = Image.open(SRC_PNG).convert("RGB")
    out = BRANDING / "logo.jpg"
    img.save(out, "JPEG", quality=90)
    print(f"  ✓ {out.relative_to(ROOT)}")


def generate_ico() -> None:
    """Create multi-size ICO for Windows packaging."""
    img = Image.open(SRC_PNG)
    sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    out = BRANDING / "surfsifter.ico"
    img.save(out, format="ICO", sizes=sizes)
    print(f"  ✓ {out.relative_to(ROOT)}")


if __name__ == "__main__":
    print("Generating SurfSifter branding assets...")
    generate_logo_jpg()
    generate_ico()
    print("Done.")
