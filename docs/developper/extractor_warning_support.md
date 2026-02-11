## Extractor Integration Guide (For AI Agents)

This section explains how to add extraction warning support to any extractor.

### Quick Reference

**Import these from `extractors/_shared/extraction_warnings.py`:**
```python
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_columns,
    track_unknown_values,
    discover_unknown_json_keys,
)
```

### Implementation Pattern

#### Step 1: Create a Warning Collector at Ingestion Start

In your extractor's `run_ingestion()` method, create a collector:

```python
def run_ingestion(self, evidence_id, run_id, evidence_conn, callbacks, ...):
    warning_collector = ExtractionWarningCollector(
        extractor_name=self.metadata.name,  # e.g., "chromium_autofill"
        run_id=run_id,
        evidence_id=evidence_id,
    )
    
    try:
        # ... your extraction logic here ...
        pass
    finally:
        # ALWAYS flush warnings at the end, even on error
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings", warning_count)
```

#### Step 2: Detect Unknown Database Tables

For SQLite databases, define known tables and discover unknowns:

```python
# In your _schemas.py or at module level
KNOWN_TABLES = {"history", "visits", "urls", "downloads", "segments", ...}
TABLE_PATTERNS = ["history", "visit", "url"]  # Patterns to filter relevant unknowns

# In your parsing method
def _parse_database(self, conn, source_file, warning_collector, ...):
    if warning_collector:
        unknown_tables = discover_unknown_tables(
            conn, KNOWN_TABLES, TABLE_PATTERNS
        )
        for table_info in unknown_tables:
            warning_collector.add_unknown_table(
                table_name=table_info["name"],
                columns=table_info["columns"],
                source_file=source_file,
                artifact_type="history",  # Your artifact type
            )
```

#### Step 3: Track Unknown Enum/Token Values

When parsing tables with type codes or enums:

```python
# Define known values
VISIT_TYPES = {0: "link", 1: "typed", 2: "bookmark", ...}

def _parse_visits(self, cursor, source_file, warning_collector):
    found_types = set()
    
    for row in cursor:
        visit_type = row["visit_type"]
        found_types.add(visit_type)
        # ... parse the row ...
    
    # After parsing, report any unknown types
    if warning_collector:
        track_unknown_values(
            warning_collector=warning_collector,
            known_mapping=VISIT_TYPES,
            found_values=found_types,
            value_name="visit_type",
            source_file=source_file,
            artifact_type="history",
        )
```

#### Step 4: Report JSON Parse Errors

When parsing JSON files that might be malformed:

```python
def _parse_json_file(self, file_path, warning_collector):
    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        if warning_collector:
            warning_collector.add_json_parse_error(
                filename=str(file_path),
                error=str(e),
            )
        return None
    return data
```

#### Step 5: Discover Unknown JSON Keys

For JSON files with evolving schemas:

```python
KNOWN_BOOKMARK_KEYS = {"name", "url", "date_added", "type", "children", ...}

def _parse_bookmarks(self, data, source_file, warning_collector):
    if warning_collector:
        unknown_keys = discover_unknown_json_keys(data, KNOWN_BOOKMARK_KEYS)
        for key_info in unknown_keys:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type="bookmarks",
                source_file=source_file,
                item_name=key_info["path"],
                item_value=str(type(key_info["value"]).__name__),
            )
```

### Warning Collector API Summary

| Method | Use Case |
|--------|----------|
| `add_unknown_table(table_name, columns, source_file, artifact_type)` | SQLite table not in known list |
| `add_unknown_column(table_name, column_name, column_type, source_file, artifact_type)` | Column exists but not parsed |
| `add_unknown_token_type(token_type, source_file, artifact_type)` | Enum/type code not recognized |
| `add_json_parse_error(filename, error)` | JSON file failed to parse |
| `add_warning(warning_type, category, item_name, ...)` | Generic warning for any case |
| `flush_to_database(conn)` | Save all collected warnings (call once at end) |

### Severity Guidelines

| Severity | When to Use |
|----------|-------------|
| `info` | Data was still extracted, just noting something unknown |
| `warning` | Potentially missing forensic data |
| `error` | Data loss occurred (parse failure, corruption) |

### Categories

Use these category constants: `database`, `json`, `leveldb`, `binary`, `plist`, `registry`

### Pass Warning Collector Through Method Chain

Update method signatures to accept `warning_collector`:

```python
def _parse_web_data(
    self, db_path, browser, file_entry, run_id, evidence_id, evidence_conn, callbacks,
    *, warning_collector: Optional[ExtractionWarningCollector] = None,
) -> Dict[str, int]:
```

Use `Optional` type and default to `None` for backward compatibility.