#!/bin/bash
# Clean generated artifacts from worktree
# Usage: ./scripts/clean.sh [--all] [--build] [--venv]
#   --all    Remove everything including .venv and dist
#   --build  Remove dist/ (release builds)
#   --venv   Remove .venv/ (virtual environment)
set -e

cd "$(dirname "$0")/.."

# Safe by default: only caches
echo "Cleaning cache artifacts..."
rm -rf .pytest_cache/ __pycache__/ logs/
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Optional: build artifacts
if [[ "$*" == *"--build"* ]] || [[ "$*" == *"--all"* ]]; then
    echo "Cleaning build artifacts (dist/)..."
    rm -rf dist/
fi

# Optional: virtual environment
if [[ "$*" == *"--venv"* ]] || [[ "$*" == *"--all"* ]]; then
    echo "Cleaning virtual environment (.venv/)..."
    rm -rf .venv/
fi

echo "Done."
