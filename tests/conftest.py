"""Global pytest configuration (non-GUI fixtures only)."""

import pytest

pytest_plugins = ["tests.fixtures.db"]


@pytest.fixture(autouse=True)
def _clear_tool_version_cache():
    """Tool version probes are cached per process; isolate tests (issue #70)."""
    from core.tool_discovery import clear_tool_version_cache

    clear_tool_version_cache()
    yield
    clear_tool_version_cache()


def _build_extractor_registry():
    from extractors.extractor_registry import ExtractorRegistry
    return ExtractorRegistry()


def _extract_registry_names(extractors):
    return [extractor.metadata.name for extractor in extractors]


@pytest.fixture(scope="session")
def extractor_registry():
    """Session-wide extractor registry to avoid repeated discovery cost."""
    return _build_extractor_registry()


@pytest.fixture(scope="session")
def extractor_registry_all(extractor_registry):
    """Cached list of all extractors from the registry."""
    return extractor_registry.get_all()


@pytest.fixture(scope="session")
def extractor_registry_names(extractor_registry_all):
    """Cached extractor names for quick membership checks."""
    return _extract_registry_names(extractor_registry_all)
