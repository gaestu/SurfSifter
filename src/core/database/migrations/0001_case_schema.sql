-- Consolidated case database schema (v0.64.0 - November 2025)
-- This is the COMPLETE schema for case metadata storage.
-- Evidence artifact data lives in separate per-evidence databases.
--
-- IMPORTANT: This is a pre-production consolidation. Existing development
-- databases are NOT supported for upgrade. Users must create fresh cases.
--
-- Schema version: 1 (consolidated baseline)

-- Cases table: Core case metadata
CREATE TABLE IF NOT EXISTS cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    investigator TEXT,
    created_at_utc TEXT NOT NULL,
    notes TEXT,
    case_number TEXT DEFAULT NULL,
    case_name TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_cases_case_number ON cases(case_number);

-- Evidences table: Evidence item metadata and user preferences
CREATE TABLE IF NOT EXISTS evidences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    source_path TEXT NOT NULL,
    size INTEGER,
    ewf_info_json TEXT,
    added_at_utc TEXT NOT NULL,
    read_only INTEGER NOT NULL DEFAULT 1,
    partition_index INTEGER DEFAULT NULL,
    partition_info TEXT DEFAULT NULL,
    partition_selections TEXT DEFAULT NULL,
    scan_slack_space INTEGER DEFAULT 0,
    extractor_selections TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_evidences_extractor_selections 
    ON evidences(extractor_selections) WHERE extractor_selections IS NOT NULL;

-- Report sections: Per-evidence custom report sections
CREATE TABLE IF NOT EXISTS report_sections (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id      INTEGER NOT NULL,
    title            TEXT NOT NULL,
    section_type     TEXT NULL,
    description      TEXT NULL,
    order_index      INTEGER NOT NULL DEFAULT 0,
    artefact_filters TEXT NOT NULL,
    created_at_utc   TEXT NOT NULL,
    updated_at_utc   TEXT NOT NULL,
    FOREIGN KEY (evidence_id) REFERENCES evidences(id)
);

CREATE INDEX IF NOT EXISTS idx_report_sections_evidence_order
    ON report_sections (evidence_id, order_index, id);

-- Case audit log: Forensic audit trail for case lifecycle, evidence management, and settings changes
CREATE TABLE IF NOT EXISTS case_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,                 -- ISO 8601 UTC timestamp
    level TEXT NOT NULL,                   -- INFO, WARNING, ERROR
    category TEXT NOT NULL,                -- evidence, report, settings, tag, case
    action TEXT NOT NULL,                  -- added, removed, generated, changed, opened, closed
    target_type TEXT,                      -- evidence, report, tag, setting, case
    target_id INTEGER,                     -- ID of affected record (if applicable)
    details_json TEXT,                     -- JSON-encoded additional details
    investigator TEXT                      -- Username/identifier of investigator
);

-- Index for timestamp-based queries (timeline view)
CREATE INDEX IF NOT EXISTS idx_case_audit_ts ON case_audit_log(ts_utc);

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_case_audit_category ON case_audit_log(category);

-- Index for target lookups (e.g., all events for evidence id=5)
CREATE INDEX IF NOT EXISTS idx_case_audit_target ON case_audit_log(target_type, target_id);
