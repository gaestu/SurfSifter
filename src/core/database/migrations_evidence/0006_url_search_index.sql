-- URL substring search index coverage (issue #62).
--
-- `indexed_through_id` (W) records that the FTS5 trigram index
-- `urls_search_index` holds exactly the URL rows with id <= W, with their
-- current url. Rows above W are not indexed yet and are scanned by search.
-- W = 0 means nothing is indexed: existing cases start there and the index
-- is built in the background, in batches, when the URLs tab is shown.
--
-- The FTS5 table and its triggers are not created here: FTS5 with the trigram
-- tokenizer needs SQLite >= 3.34, and a migration failure would block opening
-- the case. core.database.helpers.url_search_index.ensure_url_search_index
-- creates them when the evidence database is opened on a runtime that
-- supports them (see that module for the invariants).
--
-- Migration 0006: url_search_index_state

CREATE TABLE IF NOT EXISTS url_search_index_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    indexed_through_id INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO url_search_index_state (id, indexed_through_id) VALUES (1, 0);
