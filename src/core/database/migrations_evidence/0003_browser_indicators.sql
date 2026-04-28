-- Browser usage indicators aggregated from multiple sources.
-- These are soft indicators (execution traces, URL patterns, etc.)
-- NOT parsed profile data. They complement artifact-level extractors.
--
-- Migration 0003: browser_indicators table

CREATE TABLE IF NOT EXISTS browser_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    indicator_type TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_id INTEGER,
    indicator_value TEXT NOT NULL,
    source_path TEXT,
    timestamp_utc TEXT,
    confidence TEXT NOT NULL DEFAULT 'medium',
    notes TEXT,
    created_at_utc TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_browser_indicators_evidence
    ON browser_indicators(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_indicators_browser
    ON browser_indicators(browser);
CREATE INDEX IF NOT EXISTS idx_browser_indicators_type
    ON browser_indicators(indicator_type);
CREATE INDEX IF NOT EXISTS idx_browser_indicators_run
    ON browser_indicators(run_id);
