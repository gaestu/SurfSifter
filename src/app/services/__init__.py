"""Background services and IO helpers used by multiple features.

This package contains:
- workers.py: QRunnable task classes for background operations
- net_download.py: Network download utilities
- thumbnailer.py: Image thumbnail generation
"""

# Lazy imports to avoid circular dependencies during transition
# Direct imports like `from app.services.workers import X` are recommended
