"""Deterministic synthetic URL fixtures for URL-tab tests and benchmarks.

Builds a case workspace with one or more evidences whose ``urls`` tables are
populated with reproducible rows (no randomness, no wall-clock values).
Shared by fast tests (small row counts) and the opt-in URL loading
benchmark (1M / 10M rows).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from core.database import DatabaseManager


@dataclass
class UrlFixtureCase:
    case_folder: Path
    case_db_path: Path
    evidence_ids: List[int] = field(default_factory=list)
    labels: Dict[int, str] = field(default_factory=dict)


def build_url_fixture_case(
    case_folder: Path,
    *,
    rows_per_evidence: int,
    evidence_count: int = 1,
    timestamp_every: int = 10,
    tag_every: int = 0,
    match_every: int = 0,
    domain_count: int = 1000,
    batch_size: int = 50_000,
) -> UrlFixtureCase:
    """Create a case with ``evidence_count`` evidences of synthetic URLs.

    Args:
        case_folder: Case workspace folder (created if missing).
        rows_per_evidence: URL rows inserted per evidence.
        evidence_count: Number of evidences to create.
        timestamp_every: Every Nth row gets a ``first_seen_utc`` value; others
            are NULL ("mostly NULL timestamps"). 0 disables timestamps.
        tag_every: Every Nth row is tagged ("dense" ~ 2, "sparse" ~ 100).
            0 disables tagging.
        match_every: Every Nth row gets a ``url_matches`` row. 0 disables.
        domain_count: Number of distinct domains.
        batch_size: Rows per ``executemany`` batch.
    """
    case_folder.mkdir(parents=True, exist_ok=True)
    case_db_path = case_folder / "bench_surfsifter.sqlite"
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    result = UrlFixtureCase(case_folder=case_folder, case_db_path=case_db_path)
    try:
        case_conn = manager.get_case_conn()
        with case_conn:
            case_id = case_conn.execute(
                "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                ("BENCH001", "URL Benchmark", "2025-01-01T00:00:00Z"),
            ).lastrowid
            for index in range(evidence_count):
                label = f"Bench Evidence {index + 1}"
                evidence_id = case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) "
                    "VALUES (?, ?, ?, ?)",
                    (case_id, label, f"/bench/evidence{index + 1}.E01", "2025-01-01T00:00:00Z"),
                ).lastrowid
                result.evidence_ids.append(evidence_id)
                result.labels[evidence_id] = label

        for evidence_id in result.evidence_ids:
            conn = manager.get_evidence_conn(evidence_id, result.labels[evidence_id])
            _populate_evidence(
                conn,
                evidence_id,
                rows=rows_per_evidence,
                timestamp_every=timestamp_every,
                tag_every=tag_every,
                match_every=match_every,
                domain_count=domain_count,
                batch_size=batch_size,
            )
    finally:
        manager.close_all()
    return result


def _populate_evidence(
    conn,
    evidence_id: int,
    *,
    rows: int,
    timestamp_every: int,
    tag_every: int,
    match_every: int,
    domain_count: int,
    batch_size: int,
) -> None:
    sources = ("chromium_history", "firefox_history", "chromium_cache", "bulk_extractor")
    conn.execute("PRAGMA synchronous = OFF")
    with conn:
        tag_ids = []
        if tag_every:
            for name in ("Relevant", "Review"):
                tag_ids.append(
                    conn.execute(
                        "INSERT INTO tags(evidence_id, name, name_normalized, created_at_utc) "
                        "VALUES (?, ?, ?, ?)",
                        (evidence_id, name, name.lower(), "2025-01-01T00:00:00Z"),
                    ).lastrowid
                )

        start = 0
        while start < rows:
            stop = min(start + batch_size, rows)
            url_batch = []
            for i in range(start, stop):
                domain = f"d{i % domain_count}.example.com"
                first_seen = None
                if timestamp_every and i % timestamp_every == 0:
                    first_seen = f"2024-{1 + (i // 7) % 12:02d}-{1 + i % 28:02d}T{i % 24:02d}:{i % 60:02d}:{(i // 60) % 60:02d}Z"
                url_batch.append(
                    (
                        evidence_id,
                        f"https://{domain}/path/{i}?q={i % 97}",
                        domain,
                        "https",
                        sources[i % len(sources)],
                        first_seen,
                        first_seen,
                        f"/Users/u/AppData/Profile/History#{i % 13}",
                    )
                )
            cursor = conn.executemany(
                "INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by, "
                "first_seen_utc, last_seen_utc, source_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                url_batch,
            )
            del cursor
            start = stop

        # url ids are assigned sequentially by AUTOINCREMENT; resolve the range.
        min_id, max_id = conn.execute(
            "SELECT MIN(id), MAX(id) FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        if min_id is None:
            return
        if tag_every:
            conn.executemany(
                "INSERT INTO tag_associations(tag_id, evidence_id, artifact_type, artifact_id, tagged_at_utc) "
                "VALUES (?, ?, 'url', ?, ?)",
                (
                    (tag_ids[(url_id // tag_every) % len(tag_ids)], evidence_id, url_id, "2025-01-01T00:00:00Z")
                    for url_id in range(min_id, max_id + 1)
                    if (url_id - min_id) % tag_every == 0
                ),
            )
        if match_every:
            conn.executemany(
                "INSERT INTO url_matches(evidence_id, url_id, list_name, match_type, matched_pattern) "
                "VALUES (?, ?, ?, 'domain', ?)",
                (
                    (evidence_id, url_id, f"list{(url_id // match_every) % 3}", "example.com")
                    for url_id in range(min_id, max_id + 1)
                    if (url_id - min_id) % match_every == 0
                ),
            )
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("ANALYZE")
