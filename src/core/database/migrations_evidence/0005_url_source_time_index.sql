-- Single-source URL browsing in stable sort order (issue #67).
--
-- Replaces idx_urls_evidence_source (evidence_id, discovered_by) with
-- (evidence_id, discovered_by, first_seen_utc DESC) under the same name, so
-- `discovered_by = ?` together with ORDER BY first_seen_utc DESC, id ASC is an
-- index seek without a sort step (the implicit rowid at the end of the index
-- provides the ascending id tie-breaker). Before, a source whose URLs have no
-- first_seen_utc (e.g. carved URLs) scanned every timestamped row of the
-- evidence for its first page. The (evidence_id, discovered_by) prefix still
-- serves the source COUNT and the DISTINCT source listing. The name is kept
-- because URL deduplication REINDEXes it by name.
--
-- Migration 0005: idx_urls_evidence_source

DROP INDEX IF EXISTS idx_urls_evidence_source;

CREATE INDEX IF NOT EXISTS idx_urls_evidence_source
    ON urls(evidence_id, discovered_by, first_seen_utc DESC);
