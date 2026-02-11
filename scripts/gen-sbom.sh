#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
mkdir -p "$DIST_DIR"

SBOM_OUTPUT="$DIST_DIR/sbom.json"
LICENSES_OUTPUT="$DIST_DIR/THIRD_PARTY_LICENSES.md"

if poetry run cyclonedx-py --help >/dev/null 2>&1; then
  poetry run cyclonedx-py environment --of JSON --output-reproducible -o "$SBOM_OUTPUT"
elif poetry run pip-audit --help >/dev/null 2>&1; then
  echo "cyclonedx-py not available; using pip-audit fallback" >&2
  poetry run pip-audit --format cyclonedx-json --output "$SBOM_OUTPUT"
else
  echo "Neither cyclonedx-py nor pip-audit is installed" >&2
  exit 1
fi

poetry run pip-licenses --format=markdown --with-authors --with-urls > "$LICENSES_OUTPUT"
test -s "$SBOM_OUTPUT"
test -s "$LICENSES_OUTPUT"

echo "SBOM written to $SBOM_OUTPUT"
echo "License report written to $LICENSES_OUTPUT"
