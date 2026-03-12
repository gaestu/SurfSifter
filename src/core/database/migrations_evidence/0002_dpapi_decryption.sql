-- DPAPI Decryption Support
-- Tracks master key discovery, Chromium app key extraction, and per-record decrypt status.

-- ============================================================================
-- Windows User Accounts (from SAM)
-- ============================================================================

CREATE TABLE IF NOT EXISTS windows_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    sid TEXT NOT NULL,
    rid INTEGER NOT NULL,
    username TEXT NOT NULL,
    profile_path TEXT,
    last_logon_utc TEXT,
    password_last_set_utc TEXT,
    account_flags INTEGER,
    ntlm_hash_available INTEGER DEFAULT 0,
    master_keys_found INTEGER DEFAULT 0,
    master_keys_unlocked INTEGER DEFAULT 0,
    run_id TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_windows_users_evidence ON windows_users(evidence_id);
CREATE INDEX IF NOT EXISTS idx_windows_users_sid ON windows_users(sid);
CREATE INDEX IF NOT EXISTS idx_windows_users_run_id ON windows_users(run_id);

-- ============================================================================
-- DPAPI Master Keys
-- ============================================================================

CREATE TABLE IF NOT EXISTS dpapi_master_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    sid TEXT NOT NULL,
    username TEXT,
    guid TEXT NOT NULL,
    source_path TEXT NOT NULL,
    file_hash_sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'locked',
    unlock_method TEXT,
    unlocked_at_utc TEXT,
    error_message TEXT,
    run_id TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_dpapi_mk_evidence ON dpapi_master_keys(evidence_id);
CREATE INDEX IF NOT EXISTS idx_dpapi_mk_sid ON dpapi_master_keys(sid);
CREATE INDEX IF NOT EXISTS idx_dpapi_mk_guid ON dpapi_master_keys(guid);
CREATE INDEX IF NOT EXISTS idx_dpapi_mk_run_id ON dpapi_master_keys(run_id);

-- ============================================================================
-- Chromium Application Keys
-- ============================================================================

CREATE TABLE IF NOT EXISTS chromium_app_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    sid TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile_root TEXT NOT NULL,
    local_state_path TEXT NOT NULL,
    local_state_hash_sha256 TEXT,
    master_key_guid TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    run_id TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_chromium_ak_evidence ON chromium_app_keys(evidence_id);
CREATE INDEX IF NOT EXISTS idx_chromium_ak_sid ON chromium_app_keys(sid);
CREATE INDEX IF NOT EXISTS idx_chromium_ak_run_id ON chromium_app_keys(run_id);
CREATE INDEX IF NOT EXISTS idx_chromium_ak_browser ON chromium_app_keys(browser);

-- ============================================================================
-- Decryption Audit Records
-- ============================================================================

CREATE TABLE IF NOT EXISTS decrypt_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    target_table TEXT NOT NULL,
    target_id INTEGER NOT NULL,
    chromium_app_key_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    blob_version TEXT,
    error_code TEXT,
    run_id TEXT NOT NULL,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_decrypt_audit_evidence ON decrypt_audit(evidence_id);
CREATE INDEX IF NOT EXISTS idx_decrypt_audit_target ON decrypt_audit(target_table, target_id);
CREATE INDEX IF NOT EXISTS idx_decrypt_audit_run_id ON decrypt_audit(run_id);
