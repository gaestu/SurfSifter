"""Contract tests for extractor metadata and availability."""
from __future__ import annotations

from dataclasses import dataclass
import importlib
from typing import Optional

import pytest


@dataclass(frozen=True)
class ExtractorContract:
    module: str
    class_name: str
    name: str
    category: str
    can_extract: bool = True
    can_ingest: bool = True
    stats_module: Optional[str] = None


CONTRACTS = [
    ExtractorContract(
        module="extractors.browser.chromium.cache",
        class_name="CacheSimpleExtractor",
        name="cache_simple",
        category="browser",
        stats_module="extractors.browser.chromium.cache.extractor",
    ),
    ExtractorContract(
        module="extractors.browser.firefox.cache",
        class_name="CacheFirefoxExtractor",
        name="cache_firefox",
        category="browser",
        stats_module="extractors.browser.firefox.cache.extractor",
    ),
    ExtractorContract(
        module="extractors.browser.chromium.media_history",
        class_name="MediaHistoryExtractor",
        name="media_history",
        category="browser",
    ),
    ExtractorContract(
        module="extractors.system.file_list",
        class_name="SystemFileListExtractor",
        name="file_list",
        category="system",
    ),
    ExtractorContract(
        module="extractors.system.registry",
        class_name="SystemRegistryExtractor",
        name="system_registry",
        category="system",
    ),
    ExtractorContract(
        module="extractors.media.filesystem_images",
        class_name="FilesystemImagesExtractor",
        name="filesystem_images",
        category="media",
        stats_module="extractors.media.filesystem_images.extractor",
    ),
    ExtractorContract(
        module="extractors.media.foremost_carver",
        class_name="ForemostCarverExtractor",
        name="foremost_carver",
        category="media",
        stats_module="extractors.media.foremost_carver.extractor",
    ),
    ExtractorContract(
        module="extractors.media.scalpel",
        class_name="ScalpelExtractor",
        name="scalpel",
        category="media",
        stats_module="extractors.media.scalpel.extractor",
    ),
    ExtractorContract(
        module="extractors.carvers.bulk_extractor",
        class_name="BulkExtractorExtractor",
        name="bulk_extractor",
        category="forensic",
        stats_module="extractors.carvers.bulk_extractor.extractor",
    ),
    ExtractorContract(
        module="extractors.carvers.browser_carver",
        class_name="BrowserCarverExtractor",
        name="browser_carver",
        category="forensic_tools",
        stats_module="extractors.carvers.browser_carver.extractor",
    ),
]


@pytest.mark.parametrize("contract", CONTRACTS)
def test_extractor_contract(contract: ExtractorContract) -> None:
    module = importlib.import_module(contract.module)
    extractor_cls = getattr(module, contract.class_name)
    extractor = extractor_cls()

    assert extractor.metadata.name == contract.name
    assert extractor.metadata.category == contract.category
    assert extractor.metadata.can_extract is contract.can_extract
    assert extractor.metadata.can_ingest is contract.can_ingest
    assert extractor.metadata.version
    assert "." in extractor.metadata.version

    if contract.stats_module:
        stats_module = importlib.import_module(contract.stats_module)
        assert hasattr(stats_module, "StatisticsCollector") or "StatisticsCollector" in dir(stats_module)
