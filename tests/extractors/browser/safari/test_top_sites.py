"""Tests for Safari Top Sites parser and extractor."""

from __future__ import annotations

import fnmatch
import json
import plistlib
from pathlib import Path

from extractors.browser.safari._parsers import get_top_site_stats, parse_top_sites
from extractors.browser.safari.top_sites import SafariTopSitesExtractor
from extractors.extractor_registry import ExtractorRegistry


class _Callbacks:
    def on_step(self, step_name: str) -> None:
        return None

    def on_log(self, message: str, level: str = "info") -> None:
        return None

    def on_error(self, error: str, details: str = "") -> None:
        return None

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        return None

    def is_cancelled(self) -> bool:
        return False


class _FakeEvidenceFS:
    def __init__(self, file_map: dict[str, bytes]):
        self.file_map = file_map
        self.fs_type = "APFS"
        self.source_path = "/tmp/evidence.E01"
        self.partition_index = 0

    def iter_paths(self, pattern: str):
        for path in self.file_map:
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(f"/{path}", pattern):
                yield path

    def read_file(self, path: str) -> bytes:
        if path in self.file_map:
            return self.file_map[path]
        alt = path.lstrip("/")
        if alt in self.file_map:
            return self.file_map[alt]
        raise FileNotFoundError(path)


def _plist_bytes(payload: object) -> bytes:
    return plistlib.dumps(payload, fmt=plistlib.FMT_BINARY)


def test_metadata_and_registry_discovery() -> None:
    extractor = SafariTopSitesExtractor()
    assert extractor.metadata.name == "safari_top_sites"
    assert extractor.metadata.can_extract is True
    assert extractor.metadata.can_ingest is True

    registry = ExtractorRegistry()
    assert "safari_top_sites" in registry.list_names()
    assert isinstance(registry.get("safari_top_sites"), SafariTopSitesExtractor)


def test_parse_top_sites_layout(tmp_path: Path) -> None:
    payload = {
        "TopSites": [
            {
                "TopSiteURLString": "https://example.com/",
                "TopSiteTitle": "Example",
                "TopSiteIsBuiltIn": False,
            },
            {
                "TopSiteURLString": "https://apple.com/",
                "TopSiteTitle": "Apple",
                "TopSiteIsBuiltIn": True,
            },
        ]
    }
    path = tmp_path / "TopSites.plist"
    path.write_bytes(_plist_bytes(payload))
    sites = parse_top_sites(path)

    assert len(sites) == 2
    assert sites[0].url == "https://example.com/"
    assert sites[0].title == "Example"
    assert sites[0].rank == 0
    assert sites[0].is_built_in is False
    assert sites[1].rank == 1
    assert sites[1].is_built_in is True


def test_parse_banner_list_and_fallback_keys(tmp_path: Path) -> None:
    banner_path = tmp_path / "banner.plist"
    banner_path.write_bytes(
        _plist_bytes(
            {
                "BannerList": [
                    {"TopSiteURLString": "https://news.example/", "TopSiteTitle": "News"},
                    {"URLString": "https://legacy.example/", "Title": "Legacy"},
                ]
            }
        )
    )
    flat_list_path = tmp_path / "flat.plist"
    flat_list_path.write_bytes(
        _plist_bytes(
            [
                {"URLString": "https://flat.example/", "Title": "Flat"},
                {"URLString": "", "Title": "Skip blank"},
            ]
        )
    )

    banner_sites = parse_top_sites(banner_path)
    flat_sites = parse_top_sites(flat_list_path)

    assert len(banner_sites) == 2
    assert banner_sites[1].url == "https://legacy.example/"
    assert banner_sites[1].title == "Legacy"
    assert len(flat_sites) == 1
    assert flat_sites[0].url == "https://flat.example/"


def test_parse_top_sites_empty_or_corrupt(tmp_path: Path) -> None:
    empty_path = tmp_path / "empty.plist"
    empty_path.write_bytes(_plist_bytes({"TopSites": []}))
    bad_path = tmp_path / "bad.plist"
    bad_path.write_bytes(b"not-a-plist")

    assert parse_top_sites(empty_path) == []
    assert parse_top_sites(bad_path) == []


def test_top_site_stats(tmp_path: Path) -> None:
    path = tmp_path / "stats.plist"
    path.write_bytes(
        _plist_bytes(
            {
                "TopSites": [
                    {"TopSiteURLString": "https://example.com/", "TopSiteTitle": "Example"},
                    {"TopSiteURLString": "https://example.com/", "TopSiteTitle": "Example 2"},
                    {"TopSiteURLString": "https://apple.com/", "TopSiteIsBuiltIn": True},
                ]
            }
        )
    )
    sites = parse_top_sites(path)

    stats = get_top_site_stats(sites)
    assert stats["total_sites"] == 3
    assert stats["unique_urls"] == 2
    assert stats["built_in_count"] == 1


def test_extraction_copies_top_sites_files(tmp_path: Path) -> None:
    plist_data = _plist_bytes(
        {
            "TopSites": [
                {
                    "TopSiteURLString": "https://example.com/",
                    "TopSiteTitle": "Example",
                }
            ]
        }
    )
    fs = _FakeEvidenceFS(
        {
            "Users/alice/Library/Safari/TopSites.plist": plist_data,
            "Users/alice/Library/Containers/com.apple.Safari/Data/Library/Safari/TopSites.plist": plist_data,
        }
    )

    extractor = SafariTopSitesExtractor()
    output_dir = tmp_path / "out"
    ok = extractor.run_extraction(fs, output_dir, {"evidence_id": 1}, _Callbacks())
    assert ok is True

    manifests = sorted(output_dir.glob("*/manifest.json"))
    assert manifests, "No extraction manifest generated"
    manifest = json.loads(manifests[-1].read_text())

    files = manifest["files"]
    assert len(files) == 2
    assert all(item["artifact_type"] == "top_sites_plist" for item in files)
    assert all(item.get("profile") == "alice" for item in files)
