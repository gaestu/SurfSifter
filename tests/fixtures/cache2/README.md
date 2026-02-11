# Firefox Cache2 Test Fixtures

Synthetic Firefox cache2 entry files for testing.

## Format Overview

Firefox cache2 uses a **body-first** layout with metadata at the END:

```
┌─────────────────────────────────────────────────────────────┐
│ Response Body (raw or compressed)                           │  ← Offset 0
├─────────────────────────────────────────────────────────────┤
│ Metadata Checksum (4 bytes, big-endian Jenkins Hash)        │  ← metaOffset
├─────────────────────────────────────────────────────────────┤
│ Hash Array (2 bytes × chunk_count)                          │
├─────────────────────────────────────────────────────────────┤
│ CacheFileMetadataHeader (28-32 bytes, BIG ENDIAN)           │
├─────────────────────────────────────────────────────────────┤
│ Key (cache key/URL) + null terminator                       │
├─────────────────────────────────────────────────────────────┤
│ Elements (key\0value\0 pairs)                               │
├─────────────────────────────────────────────────────────────┤
│ Metadata Offset (4 bytes, big-endian uint32)                │  ← LAST 4 bytes
└─────────────────────────────────────────────────────────────┘
```

## Test Files

- `valid_jpeg_v2.cache2` - Valid cache2 entry with JPEG image body
- `valid_png_v2.cache2` - Valid cache2 entry with PNG image body
- `valid_gzip_v2.cache2` - Cache2 entry with gzip-compressed body
- `invalid_small.cache2` - File too small (< 4 bytes)
- `invalid_offset.cache2` - Invalid metadata offset
- `empty_body.cache2` - Valid entry with empty body (meta_offset = 0)

## Header Fields (BIG ENDIAN)

| Field         | Size   | Description                      |
|---------------|--------|----------------------------------|
| version       | 4 bytes| Cache format version (1, 2, 3)   |
| fetch_count   | 4 bytes| Number of times fetched          |
| last_fetched  | 4 bytes| Unix timestamp of last fetch     |
| last_modified | 4 bytes| Unix timestamp of last mod       |
| frecency      | 4 bytes| Firefox frecency score           |
| expiration    | 4 bytes| Unix timestamp of expiration     |
| key_size      | 4 bytes| Size of cache key in bytes       |
| flags         | 4 bytes| (v2+ only) Cache entry flags     |

## Regenerating Fixtures

```bash
cd tests/fixtures/cache2
python generate_fixtures.py
```
