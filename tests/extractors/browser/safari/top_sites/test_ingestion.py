from __future__ import annotations

import json
import plistlib
from pathlib import Path

from extractors.browser.safari.top_sites import SafariTopSitesExtractor


class _Callbacks:
    def __init__(self) -> None:
        self.steps: list[str] = []
        self.logs: list[str] = []

    def on_step(self, step_name: str) -> None:
        self.steps.append(step_name)

    def on_log(self, message: str, level: str = "info") -> None:
        self.logs.append(f"{level}:{message}")

    def on_error(self, error: str, details: str = "") -> None:
        self.logs.append(f"error:{error}:{details}")

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        return None

    def is_cancelled(self) -> bool:
        return False


def _top_sites_plist(path: Path) -> None:
    payload = {
        "TopSites": [
            {"TopSiteURLString": "https://example.com/", "TopSiteTitle": "Example"},
            {"TopSiteURLString": "topsites://favorites", "TopSiteTitle": "Internal"},
            {"TopSiteURLString": "javascript:alert(1)", "TopSiteTitle": "Script"},
            {
                "TopSiteURLString": "https://apple.com/",
                "TopSiteTitle": "Apple",
                "TopSiteIsBuiltIn": True,
            },
            {"TopSiteURLString": "", "TopSiteTitle": "Blank"},
        ]
    }
    path.write_bytes(plistlib.dumps(payload, fmt=plistlib.FMT_BINARY))


def test_ingestion_inserts_top_sites_urls_and_inventory(case_context, tmp_path: Path) -> None:
    run_id = "20260101T000000_abcd1234"
    output_dir = tmp_path / "safari_top_sites"
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True)

    plist_path = run_dir / "safari_alice_TopSites.plist"
    _top_sites_plist(plist_path)

    manifest = {
        "run_id": run_id,
        "status": "ok",
        "extraction_timestamp_utc": "2026-01-01T00:00:00+00:00",
        "files": [
            {
                "artifact_type": "top_sites_plist",
                "local_path": str(plist_path),
                "source_path": "Users/alice/Library/Safari/TopSites.plist",
                "profile": "alice",
                "user": "alice",
                "md5": "x",
                "sha256": "y",
                "size_bytes": plist_path.stat().st_size,
                "partition_index": 0,
                "fs_type": "APFS",
            }
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest))

    conn = case_context.manager.get_evidence_conn(case_context.evidence_id, case_context.evidence_label)
    try:
        extractor = SafariTopSitesExtractor()
        stats_first = extractor.run_ingestion(
            output_dir,
            conn,
            case_context.evidence_id,
            {"evidence_label": case_context.evidence_label},
            _Callbacks(),
        )
        stats_second = extractor.run_ingestion(
            output_dir,
            conn,
            case_context.evidence_id,
            {"evidence_label": case_context.evidence_label},
            _Callbacks(),
        )

        assert stats_first["top_sites"] == 4
        assert stats_first["urls"] == 2
        assert stats_first["built_in"] == 1
        assert stats_second["top_sites"] == 4
        assert stats_second["urls"] == 2
        assert stats_second["built_in"] == 1

        top_site_count = conn.execute(
            "SELECT COUNT(*) FROM top_sites WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert top_site_count == 4

        built_in_count = conn.execute(
            "SELECT COUNT(*) FROM top_sites WHERE evidence_id = ? AND run_id = ? AND notes = 'built-in'",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert built_in_count == 1

        url_rows = conn.execute(
            "SELECT url, context FROM urls WHERE evidence_id = ? AND run_id = ? ORDER BY id",
            (case_context.evidence_id, run_id),
        ).fetchall()
        assert len(url_rows) == 2
        assert all(row[1] == "top_sites:safari:alice" for row in url_rows)
        assert all(not row[0].startswith(("topsites://", "javascript:")) for row in url_rows)

        inventory_count = conn.execute(
            "SELECT COUNT(*) FROM browser_cache_inventory WHERE evidence_id = ? AND run_id = ? AND artifact_type = 'top_sites'",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert inventory_count == 2
    finally:
        conn.close()
