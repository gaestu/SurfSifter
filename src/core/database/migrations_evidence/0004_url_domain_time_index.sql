-- Exact-domain URL browsing in stable sort order (issue #61).
--
-- Serves `domain = ? COLLATE NOCASE` (exact, ASCII case-insensitive domain
-- filter) together with ORDER BY first_seen_utc DESC, id ASC: the implicit
-- rowid at the end of the index provides the ascending id tie-breaker, so
-- keyset pages and the exact-domain COUNT are index seeks without a sort step.
--
-- Migration 0004: idx_urls_evidence_domain_nocase_first_seen

CREATE INDEX IF NOT EXISTS idx_urls_evidence_domain_nocase_first_seen
    ON urls(evidence_id, domain COLLATE NOCASE, first_seen_utc DESC);
