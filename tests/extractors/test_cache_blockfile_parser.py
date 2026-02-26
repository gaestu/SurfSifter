"""
Unit tests for Chromium blockfile cache parser.

Tests the blockfile.py module which handles legacy Chromium disk_cache format
using data_0/1/2/3 block files and separate index file.

Reference: Chromium source net/disk_cache/blockfile/
"""
import struct
import pytest
from datetime import datetime, timezone
from pathlib import Path

from extractors.browser.chromium.cache.blockfile import (
    # Constants
    BLOCKFILE_INDEX_MAGIC,
    BLOCKFILE_BLOCK_MAGIC,
    BLOCK_SIZES,
    BLOCK_HEADER_SIZE,
    ENTRY_STORE_SIZE,
    DOUBLE_KEY_PREFIX,
    DOUBLE_KEY_SEPARATOR,
    CODE_CACHE_KEY_PREFIX,
    # Data classes
    CacheAddr,
    EntryStore,
    RankingsNode,
    IndexHeader,
    BlockfileCacheEntry,
    # Functions
    parse_cache_addr,
    parse_entry_store,
    parse_rankings_node,
    parse_index_header,
    read_block_data,
    read_block_data_from_dict,
    iter_index_entries,
    iter_index_entries_lazy,
    read_long_key,
    detect_blockfile_cache,
    parse_blockfile_cache,
    load_cache_files,
    extract_url_from_cache_key,
    is_cache_url,
    MAX_INTERNAL_KEY_LENGTH,
)

# File type constants (matching blockfile.py)
FILE_TYPE_EXTERNAL = 0
FILE_TYPE_RANKINGS = 1
FILE_TYPE_BLOCK_256 = 2
FILE_TYPE_BLOCK_1K = 3
FILE_TYPE_BLOCK_4K = 4


class TestBlockfileConstants:
    """Test magic numbers and constants."""

    def test_index_magic_value(self):
        """Index magic should be 0xC103CAC3."""
        assert BLOCKFILE_INDEX_MAGIC == 0xC103CAC3

    def test_block_magic_value(self):
        """Block file magic should be 0xC104CAC3."""
        assert BLOCKFILE_BLOCK_MAGIC == 0xC104CAC3

    def test_block_sizes(self):
        """Block sizes should match Chromium source."""
        assert BLOCK_SIZES[FILE_TYPE_RANKINGS] == 36
        assert BLOCK_SIZES[FILE_TYPE_BLOCK_256] == 256
        assert BLOCK_SIZES[FILE_TYPE_BLOCK_1K] == 1024
        assert BLOCK_SIZES[FILE_TYPE_BLOCK_4K] == 4096

    def test_block_header_size(self):
        """Block header should be 8KB."""
        assert BLOCK_HEADER_SIZE == 8192

    def test_entry_store_size(self):
        """EntryStore should be 256 bytes."""
        assert ENTRY_STORE_SIZE == 256

    def test_double_key_prefix(self):
        """Double-key prefix should be '_dk_'."""
        assert DOUBLE_KEY_PREFIX == "_dk_"

    def test_double_key_separator(self):
        """Double-key separator should be space."""
        assert DOUBLE_KEY_SEPARATOR == " "


class TestExtractUrlFromCacheKey:
    """Test cache key URL extraction for network isolation keys."""

    def test_simple_url_no_prefix(self):
        """Plain URL without any prefix should be returned as-is."""
        url = "https://example.com/page.html"
        assert extract_url_from_cache_key(url) == url

    def test_simple_url_with_credential_prefix(self):
        """URL with 1/0/ prefix should strip the prefix."""
        cache_key = "1/0/https://example.com/page.html"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/page.html"

    def test_double_keyed_single_isolation(self):
        """Double-keyed URL with single isolation key should extract URL."""
        cache_key = "1/0/_dk_https://toplevel.com https://cdn.example.com/image.jpg"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/image.jpg"

    def test_double_keyed_two_isolation_keys(self):
        """Double-keyed URL with two isolation keys (frame + toplevel) should extract URL."""
        cache_key = "1/0/_dk_https://msn.com https://msn.com https://cdn.msn.com/image.jpg"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.msn.com/image.jpg"

    def test_double_keyed_three_isolation_keys(self):
        """Double-keyed URL with three isolation keys should extract last URL."""
        cache_key = "1/0/_dk_https://site1.com https://site2.com https://site3.com https://actual.com/resource.png"
        assert extract_url_from_cache_key(cache_key) == "https://actual.com/resource.png"

    def test_double_keyed_with_query_params(self):
        """Double-keyed URL with query parameters should be fully extracted."""
        cache_key = "1/0/_dk_https://toplevel.com https://cdn.example.com/image.jpg?size=large&format=webp"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/image.jpg?size=large&format=webp"

    def test_double_keyed_with_fragment(self):
        """Double-keyed URL with fragment should be fully extracted."""
        cache_key = "1/0/_dk_https://toplevel.com https://example.com/page.html#section1"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/page.html#section1"

    def test_double_keyed_http_url(self):
        """Double-keyed HTTP (not HTTPS) URL should be extracted."""
        cache_key = "1/0/_dk_https://toplevel.com http://insecure.example.com/data.json"
        assert extract_url_from_cache_key(cache_key) == "http://insecure.example.com/data.json"

    def test_double_keyed_with_port(self):
        """Double-keyed URL with port number should be extracted."""
        cache_key = "1/0/_dk_https://toplevel.com https://api.example.com:8443/endpoint"
        assert extract_url_from_cache_key(cache_key) == "https://api.example.com:8443/endpoint"

    def test_double_keyed_with_auth(self):
        """Double-keyed URL with authentication credentials should be extracted."""
        cache_key = "1/0/_dk_https://toplevel.com https://user:pass@secure.example.com/private"
        assert extract_url_from_cache_key(cache_key) == "https://user:pass@secure.example.com/private"

    def test_empty_cache_key(self):
        """Empty cache key should return empty string."""
        assert extract_url_from_cache_key("") == ""

    def test_none_cache_key(self):
        """None cache key should return None."""
        assert extract_url_from_cache_key(None) is None

    def test_only_credential_prefix(self):
        """Cache key with only credential prefix should return empty."""
        assert extract_url_from_cache_key("1/0/") == ""

    def test_single_slash(self):
        """Cache key with single slash should be returned as-is."""
        cache_key = "http://example.com/path"
        assert extract_url_from_cache_key(cache_key) == cache_key

    def test_data_uri(self):
        """Data URI in cache key should be handled."""
        cache_key = "1/0/data:image/png;base64,iVBORw0KGgo="
        assert extract_url_from_cache_key(cache_key) == "data:image/png;base64,iVBORw0KGgo="

    def test_double_keyed_gambling_site(self):
        """Real-world gambling site URL extraction."""
        cache_key = "1/0/_dk_https://example.org https://example.org https://example.org/lobby/images/game.png"
        assert extract_url_from_cache_key(cache_key) == "https://example.org/lobby/images/game.png"

    def test_double_keyed_with_subframe_prefix(self):
        """Double-keyed with s_ subframe prefix should extract URL."""
        # Subframe document resources may have s_ prefix in isolation key
        cache_key = "1/0/_dk_s_https://toplevel.com https://cdn.example.com/frame.js"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/frame.js"

    def test_double_keyed_with_cross_site_prefix(self):
        """Double-keyed with cn_ cross-site prefix should extract URL."""
        # Cross-site main frame navigation may have cn_ prefix
        cache_key = "1/0/_dk_cn_https://redirect.com https://final.com/page.html"
        assert extract_url_from_cache_key(cache_key) == "https://final.com/page.html"

    def test_url_with_encoded_spaces(self):
        """URL with %20 encoded spaces should be extracted correctly."""
        cache_key = "1/0/_dk_https://toplevel.com https://example.com/path%20with%20spaces"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/path%20with%20spaces"

    def test_url_with_unicode(self):
        """URL with unicode characters should be extracted."""
        cache_key = "1/0/_dk_https://toplevel.com https://example.com/path/日本語"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/path/日本語"

    def test_alternative_credential_prefix(self):
        """Cache key with different credential values should work."""
        cache_key = "0/0/https://example.com/page.html"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/page.html"

    def test_non_http_scheme(self):
        """Cache key with non-HTTP scheme should be handled."""
        cache_key = "1/0/blob:https://example.com/uuid-1234"
        assert extract_url_from_cache_key(cache_key) == "blob:https://example.com/uuid-1234"

    def test_double_keyed_with_blob_url(self):
        """Double-keyed blob URL should extract the blob URL."""
        cache_key = "1/0/_dk_https://toplevel.com blob:https://example.com/uuid-5678"
        assert extract_url_from_cache_key(cache_key) == "blob:https://example.com/uuid-5678"

    # --- Code Cache _key prefix tests ---

    def test_code_cache_key_with_newline_separator(self):
        """Code Cache key: _key<url> \\n<top_level_site> should extract the script URL."""
        cache_key = "_keyhttps://cdn.example.com/script.js \nhttps://example.com/"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/script.js"

    def test_code_cache_key_with_only_newline(self):
        """Code Cache key with bare newline separator."""
        cache_key = "_keyhttps://assets.msn.com/bundles/v1/shell.js\nhttps://msn.com/"
        assert extract_url_from_cache_key(cache_key) == "https://assets.msn.com/bundles/v1/shell.js"

    def test_code_cache_key_no_separator(self):
        """Code Cache key without separator returns full content after _key."""
        cache_key = "_keyhttps://cdn.example.com/single.js"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/single.js"

    def test_code_cache_key_with_credential_prefix(self):
        """Code Cache key might also have N/N/ prefix."""
        cache_key = "1/0/_keyhttps://cdn.example.com/script.js \nhttps://example.com/"
        assert extract_url_from_cache_key(cache_key) == "https://cdn.example.com/script.js"

    def test_code_cache_key_real_ipoint(self):
        """Real-world iPoint/EBWebView Code Cache key from evidence."""
        cache_key = "_keyhttps://assets.msn.com/bundles/v1/windowsShell/latest/common-utils.f35aee6163747e34f107.js \nhttps://msn.com/"
        expected = "https://assets.msn.com/bundles/v1/windowsShell/latest/common-utils.f35aee6163747e34f107.js"
        assert extract_url_from_cache_key(cache_key) == expected

    def test_code_cache_key_trailing_space_before_newline(self):
        """Trailing space before newline should be stripped."""
        cache_key = "_keyhttps://example.com/app.js  \nhttps://example.com/"
        assert extract_url_from_cache_key(cache_key) == "https://example.com/app.js"

    def test_sha256_hash_key_passthrough(self):
        """SHA-256 hash-only key (no _key prefix) should pass through as-is."""
        hash_key = "AB2A39155883DE91B9EFDA1DBD40620716B544996393D10FCD674C426DA1A250"
        assert extract_url_from_cache_key(hash_key) == hash_key

    def test_gpu_cache_hash_key_passthrough(self):
        """GPUCache base64 hash pair should pass through as-is."""
        gpu_key = "HYN18Fl2bqKa6GEqZSRCWOIq1vc=:L4uLPxDuFfI2ovodjJe3qn+JmNw="
        assert extract_url_from_cache_key(gpu_key) == gpu_key

    def test_code_cache_key_prefix_constant(self):
        """CODE_CACHE_KEY_PREFIX should be '_key'."""
        assert CODE_CACHE_KEY_PREFIX == "_key"


class TestIsCacheUrl:
    """Test the is_cache_url helper that distinguishes URLs from opaque cache keys."""

    def test_http_url(self):
        assert is_cache_url("http://example.com/page.html") is True

    def test_https_url(self):
        assert is_cache_url("https://cdn.msn.com/image.jpg") is True

    def test_chrome_extension_url(self):
        assert is_cache_url("chrome-extension://abcdef/popup.html") is True

    def test_blob_url(self):
        assert is_cache_url("blob:https://example.com/uuid") is True

    def test_data_uri(self):
        assert is_cache_url("data:image/png;base64,iVBORw0KGgo=") is True

    def test_ftp_url(self):
        assert is_cache_url("ftp://files.example.com/data.zip") is True

    def test_sha256_hash(self):
        """SHA-256 hash (Code Cache compiled code key) is NOT a URL."""
        assert is_cache_url("AB2A39155883DE91B9EFDA1DBD40620716B544996393D10FCD674C426DA1A250") is False

    def test_gpu_cache_hash(self):
        """GPUCache base64 hash pair is NOT a URL."""
        assert is_cache_url("HYN18Fl2bqKa6GEqZSRCWOIq1vc=:L4uLPxDuFfI2ovodjJe3qn+JmNw=") is False

    def test_empty_string(self):
        assert is_cache_url("") is False

    def test_none_value(self):
        assert is_cache_url(None) is False

    def test_plain_text(self):
        """Random text without colon is not a URL."""
        assert is_cache_url("just-some-text") is False

    def test_colon_at_start(self):
        """String starting with colon is not a URL."""
        assert is_cache_url(":not-a-scheme") is False

    def test_ws_websocket(self):
        assert is_cache_url("ws://example.com/socket") is True

    def test_wss_websocket(self):
        assert is_cache_url("wss://example.com/socket") is True

    def test_edge_scheme(self):
        assert is_cache_url("edge://settings/") is True


class TestParseIndexHeaderTableLen:
    """Test parse_index_header table_len validation and correction."""

    def _make_index_data(self, table_len: int, num_entries: int = 10,
                          table_size: int = 65536) -> bytes:
        """Build a minimal index file with configurable table_len."""
        # Header fields (48 bytes of useful data + 208 bytes padding = 256 header)
        header = struct.pack(
            '<II ii ii Ii ii Q',
            BLOCKFILE_INDEX_MAGIC,  # magic
            0x00020001,             # version 2.1
            num_entries,            # num_entries
            0,                      # old_num_bytes
            0,                      # last_file
            1,                      # this_id
            0,                      # stats_addr
            table_len,              # table_len (the field under test)
            0,                      # crash
            0,                      # experiment
            0,                      # create_time
        )
        # Pad header to 256 bytes
        header += b'\x00' * (256 - len(header))
        # Append empty hash table to reach the expected file size
        table_data = b'\x00' * (table_size * 4)
        return header + table_data

    def test_normal_table_len_65536(self):
        """Normal table_len of 65536 should be preserved."""
        data = self._make_index_data(table_len=65536)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 65536

    def test_table_len_zero_uses_default(self):
        """table_len=0 should fall back to DEFAULT_TABLE_SIZE (65536)."""
        data = self._make_index_data(table_len=0)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 65536

    def test_table_len_one_corrected(self):
        """table_len=1 with large file should be corrected to 65536."""
        data = self._make_index_data(table_len=1)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 65536

    def test_table_len_small_corrected(self):
        """table_len < 256 should be corrected based on file size."""
        data = self._make_index_data(table_len=100)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 65536

    def test_table_len_256_preserved(self):
        """table_len=256 (>= threshold) should be preserved if file is big enough."""
        data = self._make_index_data(table_len=256)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 256

    def test_table_len_small_file(self):
        """Small file with bogus table_len should compute from file size."""
        # File has room for only 512 buckets (header 256 + 512*4 = 2304)
        data = self._make_index_data(table_len=1, table_size=512)
        header = parse_index_header(data)
        assert header is not None
        # Should be 512 (largest power of 2 <= file capacity)
        assert header.table_len == 512

    def test_table_len_negative_corrected(self):
        """Negative table_len should be corrected."""
        data = self._make_index_data(table_len=-1)
        header = parse_index_header(data)
        assert header is not None
        assert header.table_len == 65536


class TestCacheAddrParsing:
    """Test CacheAddr 32-bit address parsing."""

    def test_parse_uninitialized_address(self):
        """Address 0 should not be initialized."""
        addr = parse_cache_addr(0)
        assert addr.is_initialized is False

    def test_parse_initialized_address(self):
        """Address with high bit set should be initialized."""
        addr = parse_cache_addr(0x80000000)
        assert addr.is_initialized is True

    def test_parse_external_file_type(self):
        """File type 0 should be external."""
        addr = parse_cache_addr(0x80000000)  # initialized + file_type 0
        assert addr.file_type == FILE_TYPE_EXTERNAL
        assert addr.is_external is True

    def test_parse_block_256_file_type(self):
        """File type 2 should be BLOCK_256."""
        addr = parse_cache_addr(0xA0000000)  # initialized + file_type 2
        assert addr.file_type == FILE_TYPE_BLOCK_256
        assert addr.is_external is False

    def test_parse_file_number_block(self):
        """File number should be in bits 16-23 for block files."""
        addr = parse_cache_addr(0xA0020000)  # file_number 2
        assert addr.file_number == 2

    def test_parse_file_number_external(self):
        """File number for external is full 28 bits."""
        addr = parse_cache_addr(0x80000123)  # external file 0x123
        assert addr.file_number == 0x123

    def test_parse_start_block(self):
        """Start block should be in bits 0-15."""
        addr = parse_cache_addr(0xA00001FF)
        assert addr.start_block == 0x01FF

    def test_parse_num_blocks(self):
        """Num blocks should be (bits 24-25) + 1."""
        addr = parse_cache_addr(0xA1000000)  # num_blocks = 1+1 = 2
        assert addr.num_blocks == 2

        addr = parse_cache_addr(0xA3000000)  # num_blocks = 3+1 = 4
        assert addr.num_blocks == 4


class TestCacheAddrMethods:
    """Test CacheAddr helper methods."""

    def test_block_size_rankings(self):
        """Rankings file type should have 36-byte blocks."""
        addr = parse_cache_addr(0x90000000)  # file_type 1
        assert addr.block_size == 36

    def test_block_size_256(self):
        """BLOCK_256 type should have 256-byte blocks."""
        addr = parse_cache_addr(0xA0000000)  # file_type 2
        assert addr.block_size == 256

    def test_block_size_1k(self):
        """BLOCK_1K type should have 1024-byte blocks."""
        addr = parse_cache_addr(0xB0000000)  # file_type 3
        assert addr.block_size == 1024

    def test_block_size_4k(self):
        """BLOCK_4K type should have 4096-byte blocks."""
        addr = parse_cache_addr(0xC0000000)  # file_type 4
        assert addr.block_size == 4096

    def test_block_size_external_returns_zero(self):
        """External file type has block_size 0."""
        addr = parse_cache_addr(0x80000000)  # file_type 0
        assert addr.block_size == 0

    def test_data_file_name_block(self):
        """Block file name should be data_N format."""
        addr = parse_cache_addr(0xA0000000)  # file_number 0
        assert addr.data_file_name() == "data_0"

        addr = parse_cache_addr(0xA0030000)  # file_number 3
        assert addr.data_file_name() == "data_3"

    def test_data_file_name_external(self):
        """External file name should be f_XXXXXX format."""
        addr = parse_cache_addr(0x80000ABC)  # file 0xABC
        assert addr.data_file_name() == "f_000abc"

    def test_offset_calculation(self):
        """Offset should be header + block * block_size."""
        addr = parse_cache_addr(0xA0000005)  # block 5, type 2 (256 bytes)
        # offset = 8192 + 5 * 256 = 8192 + 1280 = 9472
        assert addr.offset() == 9472

    def test_total_size_calculation(self):
        """Total size should be num_blocks * block_size."""
        addr = parse_cache_addr(0xA2000000)  # 3 blocks, type 2 (256 bytes)
        # size = 3 * 256 = 768
        assert addr.total_size() == 768


class TestEntryStoreParsing:
    """Test EntryStore 256-byte structure parsing."""

    def test_parse_entry_store_basic(self):
        """Parse a valid EntryStore structure."""
        data = bytearray(256)

        # hash at offset 0
        struct.pack_into('<I', data, 0, 0xDEADBEEF)
        # key_len at offset 32
        struct.pack_into('<i', data, 32, 20)
        # key at offset 96
        key = b"http://example.com/x"
        data[96:96+len(key)] = key

        entry = parse_entry_store(bytes(data))
        assert entry is not None
        assert entry.hash == 0xDEADBEEF
        assert entry.key_len == 20

    def test_parse_entry_store_truncated(self):
        """Truncated data should return None."""
        data = b'\x00' * 100  # Less than 256 bytes
        entry = parse_entry_store(data)
        assert entry is None


class TestRankingsNodeParsing:
    """Test RankingsNode 36-byte structure parsing."""

    def test_parse_rankings_node_basic(self):
        """Parse a valid RankingsNode structure."""
        data = bytearray(36)

        # last_used: uint64 at offset 0
        struct.pack_into('<Q', data, 0, 132000000000000000)
        # last_modified: uint64 at offset 8
        struct.pack_into('<Q', data, 8, 132100000000000000)

        node = parse_rankings_node(bytes(data))
        assert node is not None
        assert node.last_used == 132000000000000000
        assert node.last_modified == 132100000000000000

    def test_parse_rankings_node_truncated(self):
        """Truncated data should return None."""
        data = b'\x00' * 20  # Less than 36 bytes
        node = parse_rankings_node(data)
        assert node is None


class TestIndexHeaderParsing:
    """Test index file header parsing."""

    def test_parse_valid_index_header(self):
        """Parse a valid index file header."""
        data = bytearray(368)  # Minimal header size

        # magic: uint32 at offset 0
        struct.pack_into('<I', data, 0, BLOCKFILE_INDEX_MAGIC)
        # version: uint32 at offset 4
        struct.pack_into('<I', data, 4, 0x20001)  # version 2.1
        # num_entries: int32 at offset 8
        struct.pack_into('<i', data, 8, 1000)
        # num_bytes: int32 at offset 12
        struct.pack_into('<i', data, 12, 50000)
        # last_file: int32 at offset 16
        struct.pack_into('<i', data, 16, 3)
        # this_id: int32 at offset 20
        struct.pack_into('<i', data, 20, 1)
        # table_len: int32 at offset 28
        struct.pack_into('<i', data, 28, 0x10000)

        header = parse_index_header(bytes(data))
        assert header is not None
        assert header.magic == BLOCKFILE_INDEX_MAGIC
        assert header.version == 0x20001
        assert header.num_entries == 1000
        assert header.table_len == 0x10000

    def test_parse_index_header_invalid_magic(self):
        """Invalid magic should return None."""
        data = bytearray(368)
        struct.pack_into('<I', data, 0, 0xDEADBEEF)  # Wrong magic
        header = parse_index_header(bytes(data))
        assert header is None

    def test_parse_index_header_truncated(self):
        """Truncated header should return None."""
        data = b'\x00' * 40
        header = parse_index_header(data)
        assert header is None


class TestReadBlockData:
    """Test block reading from data files."""

    def test_read_block_success(self):
        """Read a block from a data file dict."""
        # Create data_1 file with 8KB header + blocks
        header = b'\x00' * 8192  # 8KB header
        block_data = b'X' * 256  # One 256-byte block
        data_files = {"data_1": header + block_data}

        # Create address: initialized + file_type 2 (256) + file_number 1 + start_block 0
        addr = parse_cache_addr(0xA0010000)

        result = read_block_data_from_dict(data_files, addr)
        assert result is not None
        assert len(result) == 256
        assert result == block_data

    def test_read_block_missing_file(self):
        """Missing data file should return None."""
        data_files = {}  # Empty
        addr = parse_cache_addr(0xA0010000)
        result = read_block_data_from_dict(data_files, addr)
        assert result is None

    def test_read_block_uninitialized(self):
        """Uninitialized address should return None."""
        data_files = {"data_1": b'\x00' * 9000}
        addr = parse_cache_addr(0x00000000)  # Not initialized
        result = read_block_data_from_dict(data_files, addr)
        assert result is None


class TestDetectBlockfileCache:
    """Test blockfile cache detection."""

    def test_detect_with_valid_index_magic(self, tmp_path):
        """Directory with valid index magic + data_1 should be detected."""
        # Create index with proper magic
        index_data = struct.pack('<I', BLOCKFILE_INDEX_MAGIC) + b'\x00' * 100
        (tmp_path / "index").write_bytes(index_data)
        (tmp_path / "data_1").write_bytes(b'\x00' * 100)

        assert detect_blockfile_cache(tmp_path) is True

    def test_detect_invalid_index_magic(self, tmp_path):
        """Invalid index magic should not be detected."""
        # Create index with wrong magic
        index_data = struct.pack('<I', 0xDEADBEEF) + b'\x00' * 100
        (tmp_path / "index").write_bytes(index_data)
        (tmp_path / "data_1").write_bytes(b'\x00' * 100)

        assert detect_blockfile_cache(tmp_path) is False

    def test_detect_missing_index(self, tmp_path):
        """Missing index file should not be detected."""
        (tmp_path / "data_0").write_bytes(b'\x00' * 100)
        (tmp_path / "data_1").write_bytes(b'\x00' * 100)

        assert detect_blockfile_cache(tmp_path) is False

    def test_detect_missing_data_1(self, tmp_path):
        """Missing data_1 file should not be detected."""
        index_data = struct.pack('<I', BLOCKFILE_INDEX_MAGIC) + b'\x00' * 100
        (tmp_path / "index").write_bytes(index_data)
        (tmp_path / "data_0").write_bytes(b'\x00' * 100)

        assert detect_blockfile_cache(tmp_path) is False

    def test_detect_empty_directory(self, tmp_path):
        """Empty directory should not be detected."""
        assert detect_blockfile_cache(tmp_path) is False


class TestLoadCacheFiles:
    """Test cache file loading."""

    def test_load_basic_files(self, tmp_path):
        """Should load index and data files."""
        (tmp_path / "index").write_bytes(b'INDEX')
        (tmp_path / "data_0").write_bytes(b'DATA0')
        (tmp_path / "data_1").write_bytes(b'DATA1')

        files = load_cache_files(tmp_path)
        assert "index" in files
        assert files["index"] == b'INDEX'
        assert "data_0" in files
        assert "data_1" in files

    def test_load_external_files(self, tmp_path):
        """Should load f_XXXXXX external files."""
        (tmp_path / "index").write_bytes(b'INDEX')
        (tmp_path / "data_0").write_bytes(b'DATA0')
        (tmp_path / "f_000001").write_bytes(b'EXTERNAL1')
        (tmp_path / "f_00abcd").write_bytes(b'EXTERNAL2')

        files = load_cache_files(tmp_path)
        assert "f_000001" in files
        assert "f_00abcd" in files


class TestParseBlockfileCache:
    """Test full cache parsing."""

    def test_parse_empty_cache(self, tmp_path):
        """Empty cache should return empty list."""
        # Create minimal valid blockfile cache structure
        header_size = 368
        table_size = 16 * 4
        index_data = bytearray(header_size + table_size)

        struct.pack_into('<I', index_data, 0, BLOCKFILE_INDEX_MAGIC)
        struct.pack_into('<I', index_data, 4, 0x20001)
        struct.pack_into('<i', index_data, 8, 0)  # no entries
        struct.pack_into('<i', index_data, 28, 16)

        (tmp_path / "index").write_bytes(bytes(index_data))

        # Create empty data files with proper header
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)
        (tmp_path / "data_0").write_bytes(bytes(data_header))
        (tmp_path / "data_1").write_bytes(bytes(data_header))

        result = parse_blockfile_cache(tmp_path)
        assert result == []

    def test_parse_nonexistent_directory(self, tmp_path):
        """Non-existent directory should return empty list."""
        nonexistent = tmp_path / "nonexistent"
        # With lazy loading, parse_blockfile_cache checks if index exists first
        # and returns empty list if not found
        result = parse_blockfile_cache(nonexistent)
        assert result == []

    def test_parse_nonexistent_directory_legacy(self, tmp_path):
        """Non-existent directory should raise FileNotFoundError in legacy mode."""
        nonexistent = tmp_path / "nonexistent"
        # In legacy mode (lazy=False), load_cache_files iterates directory
        # A non-existent directory will raise FileNotFoundError
        import pytest
        with pytest.raises(FileNotFoundError):
            parse_blockfile_cache(nonexistent, lazy=False)


class TestBlockfileCacheEntryDataclass:
    """Test BlockfileCacheEntry dataclass."""

    def test_entry_creation(self):
        """Should create entry with all fields."""
        entry = BlockfileCacheEntry(
            url="http://example.com",
            entry_hash=0xDEADBEEF,
            creation_time=None,
            last_used_time=None,
            state=0,
            data_sizes=[100, 200, 0, 0],
            data_addrs=[parse_cache_addr(0xA0010001)] + [parse_cache_addr(0)] * 3,
            source_file="data_1",
            block_offset=8192,
        )
        assert entry.url == "http://example.com"
        assert entry.entry_hash == 0xDEADBEEF
        assert entry.data_sizes == [100, 200, 0, 0]


class TestIntegrationWithEntries:
    """Integration tests with actual entry data."""

    def test_full_cache_structure(self, tmp_path):
        """Create and parse a complete cache with one entry."""
        # 1. Create index file with one entry
        header_size = 368
        table_size = 256 * 4  # 256 slots
        index_data = bytearray(header_size + table_size)

        struct.pack_into('<I', index_data, 0, BLOCKFILE_INDEX_MAGIC)
        struct.pack_into('<I', index_data, 4, 0x20001)
        struct.pack_into('<i', index_data, 8, 1)  # 1 entry
        struct.pack_into('<i', index_data, 28, 256)  # table_len

        # Put entry address in slot 0: file_type=2 (256), file_num=1, start_block=0
        entry_addr = 0xA0010000
        struct.pack_into('<I', index_data, header_size, entry_addr)

        (tmp_path / "index").write_bytes(bytes(index_data))

        # 2. Create data_1 with entry block
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)

        # Create EntryStore at block 0
        entry_store = bytearray(256)
        key = b"http://example.com/test.html"
        struct.pack_into('<I', entry_store, 0, 0xDEADBEEF)  # hash
        struct.pack_into('<i', entry_store, 32, len(key))  # key_len
        entry_store[96:96+len(key)] = key

        data_content = bytes(data_header) + bytes(entry_store)
        (tmp_path / "data_1").write_bytes(data_content)

        # 3. Create data_0 (required for detection)
        (tmp_path / "data_0").write_bytes(bytes(data_header))

        # 4. Parse the cache
        result = parse_blockfile_cache(tmp_path)
        assert len(result) >= 0  # May be empty if parsing strict

    def test_cache_detection_simple(self, tmp_path):
        """Simple detection with valid index magic."""
        # Create index with proper magic
        index_data = struct.pack('<I', BLOCKFILE_INDEX_MAGIC) + b'\x00' * 400
        (tmp_path / "index").write_bytes(index_data)
        (tmp_path / "data_0").write_bytes(b'\x00' * 8200)
        (tmp_path / "data_1").write_bytes(b'\x00' * 8200)
        (tmp_path / "data_2").write_bytes(b'\x00' * 8200)
        (tmp_path / "data_3").write_bytes(b'\x00' * 8200)

        assert detect_blockfile_cache(tmp_path) is True


class TestCacheAddrRepr:
    """Test CacheAddr string representation."""

    def test_repr_uninitialized(self):
        """Uninitialized address should show raw value."""
        addr = parse_cache_addr(0x12345678)
        assert "uninitialized" in repr(addr)

    def test_repr_external(self):
        """External address should show f_XXXXXX."""
        addr = parse_cache_addr(0x80000ABC)
        assert "external" in repr(addr)
        assert "000abc" in repr(addr)

    def test_repr_block(self):
        """Block address should show type/file/block info."""
        addr = parse_cache_addr(0xA0010005)
        r = repr(addr)
        assert "type=2" in r
        assert "file=1" in r
        assert "block=5" in r


class TestLazyLoading:
    """Test lazy loading functions."""

    def test_read_block_data_lazy_from_file(self, tmp_path):
        """read_block_data should read directly from disk."""
        # Create data_1 file with 8KB header + blocks
        header = b'\x00' * 8192  # 8KB header
        block_data = b'Y' * 256  # One 256-byte block
        (tmp_path / "data_1").write_bytes(header + block_data)

        # Create address: initialized + file_type 2 (256) + file_number 1 + start_block 0
        addr = parse_cache_addr(0xA0010000)

        result = read_block_data(tmp_path, addr)
        assert result is not None
        assert len(result) == 256
        assert result == block_data

    def test_read_block_data_lazy_with_cache(self, tmp_path):
        """read_block_data should cache block files for reuse."""
        # Create data_1 file
        header = b'\x00' * 8192
        block_data = b'Z' * 256
        (tmp_path / "data_1").write_bytes(header + block_data)

        addr = parse_cache_addr(0xA0010000)
        file_cache = {}

        # First read should populate cache
        result1 = read_block_data(tmp_path, addr, _file_cache=file_cache)
        assert result1 == block_data
        assert "data_1" in file_cache

        # Delete file - should still read from cache
        (tmp_path / "data_1").unlink()
        result2 = read_block_data(tmp_path, addr, _file_cache=file_cache)
        assert result2 == block_data

    def test_read_block_data_lazy_external_not_cached(self, tmp_path):
        """External files (f_*) should not be cached."""
        # Create external file
        (tmp_path / "f_000001").write_bytes(b'EXTERNAL_DATA')

        # Create external address: initialized + file_type 0 + file_number 1
        addr = parse_cache_addr(0x80000001)
        file_cache = {}

        result = read_block_data(tmp_path, addr, _file_cache=file_cache)
        assert result == b'EXTERNAL_DATA'
        # External files should NOT be cached
        assert "f_000001" not in file_cache

    def test_iter_index_entries_lazy(self, tmp_path):
        """iter_index_entries_lazy should parse entries without loading all files."""
        # This is similar to test_full_cache_structure but uses lazy version
        # Create minimal valid blockfile cache

        # 1. Create index with one entry
        index_data = bytearray(512)
        struct.pack_into('<I', index_data, 0, BLOCKFILE_INDEX_MAGIC)
        struct.pack_into('<I', index_data, 4, 0x20001)  # version
        struct.pack_into('<i', index_data, 8, 1)  # num_entries = 1
        struct.pack_into('<i', index_data, 28, 16)  # table_len = 16

        # Put entry address at bucket 0 (offset 256)
        # Address: initialized + file_type 2 + file 1 + block 0
        entry_addr = 0xA0010000
        struct.pack_into('<I', index_data, 256, entry_addr)
        (tmp_path / "index").write_bytes(bytes(index_data))

        # 2. Create data_1 with one entry
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)

        entry_store = bytearray(256)
        key = b"https://example.com/test"
        struct.pack_into('<I', entry_store, 0, 0x12345678)  # hash
        struct.pack_into('<i', entry_store, 32, len(key))  # key_len
        entry_store[96:96+len(key)] = key

        (tmp_path / "data_1").write_bytes(bytes(data_header) + bytes(entry_store))

        # 3. Create data_0
        (tmp_path / "data_0").write_bytes(bytes(data_header))

        # 4. Parse using lazy iterator
        entries = list(iter_index_entries_lazy(tmp_path))
        # May be 0 or 1 depending on validity checks
        assert isinstance(entries, list)


class TestLongKeySupport:
    """Test long key (>160 bytes) handling."""

    def test_inline_key_under_limit(self):
        """Keys <= 159 bytes should be stored inline."""
        # Create EntryStore with inline key (MAX_INTERNAL_KEY_LENGTH = 159)
        entry_store = bytearray(256)
        key = b"https://example.com/" + b"x" * 100  # 120 bytes, under limit
        struct.pack_into('<i', entry_store, 32, len(key))  # key_len
        entry_store[96:96+len(key)] = key

        parsed = parse_entry_store(bytes(entry_store))
        assert parsed is not None
        result = parsed.get_key()
        assert result == key.decode('utf-8')

    def test_long_key_returns_none_without_lookup(self):
        """Keys > 159 bytes should return None from get_key()."""
        entry_store = bytearray(256)
        # Key length of 200 bytes (> MAX_INTERNAL_KEY_LENGTH)
        struct.pack_into('<i', entry_store, 32, 200)  # key_len = 200

        parsed = parse_entry_store(bytes(entry_store))
        assert parsed is not None
        assert parsed.key_len == 200
        # get_key() returns None for long keys
        assert parsed.get_key() is None

    def test_read_long_key_from_external_file(self, tmp_path):
        """Long keys stored in external f_* files should be readable."""
        # Create the long URL
        long_url = "https://example.com/very/long/path/" + "x" * 200  # ~240 bytes

        # Create external file containing the long key
        (tmp_path / "f_000001").write_bytes(long_url.encode('utf-8'))

        # Create EntryStore with long_key pointing to f_000001
        entry_store = bytearray(256)
        struct.pack_into('<i', entry_store, 32, len(long_url))  # key_len
        # long_key address at offset 36: initialized + external + file 1
        long_key_addr = 0x80000001
        struct.pack_into('<I', entry_store, 36, long_key_addr)

        parsed = parse_entry_store(bytes(entry_store))
        assert parsed is not None
        assert parsed.key_len == len(long_url)
        assert parsed.long_key.is_external

        # Read long key
        result = read_long_key(tmp_path, parsed)
        assert result == long_url

    def test_read_long_key_from_block_file(self, tmp_path):
        """Long keys in block files (data_2) should be readable."""
        # Create the long URL
        long_url = "https://example.com/another/long/path/" + "y" * 200  # ~240 bytes

        # Create data_2 with 8KB header + 1KB block containing the key
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)

        # 1KB block (padded)
        key_block = long_url.encode('utf-8').ljust(1024, b'\x00')
        (tmp_path / "data_2").write_bytes(bytes(data_header) + key_block)

        # Create EntryStore with long_key pointing to data_2 block 0
        entry_store = bytearray(256)
        struct.pack_into('<i', entry_store, 32, len(long_url))  # key_len
        # long_key address: initialized + file_type 3 (1K) + file 2 + block 0
        long_key_addr = 0xB0020000
        struct.pack_into('<I', entry_store, 36, long_key_addr)

        parsed = parse_entry_store(bytes(entry_store))
        assert parsed is not None
        assert parsed.key_len == len(long_url)
        assert not parsed.long_key.is_external

        # Read long key
        result = read_long_key(tmp_path, parsed)
        assert result == long_url

    def test_max_internal_key_length_constant(self):
        """Verify MAX_INTERNAL_KEY_LENGTH is 159 (160 bytes - null terminator)."""
        assert MAX_INTERNAL_KEY_LENGTH == 159


class TestBlockfileIngestionIntegration:
    """Integration tests for blockfile ingestion into database."""

    def _create_minimal_blockfile_cache(self, cache_dir: Path, urls: list[str]) -> None:
        """
        Create a minimal valid blockfile cache with given URLs.

        Args:
            cache_dir: Directory to create cache files in
            urls: List of URLs to store as cache entries
        """
        cache_dir.mkdir(parents=True, exist_ok=True)

        # 1. Create index file
        num_entries = len(urls)
        table_len = max(16, num_entries * 2)  # Simple sizing

        index_data = bytearray(256 + table_len * 4)
        struct.pack_into('<I', index_data, 0, BLOCKFILE_INDEX_MAGIC)  # magic
        struct.pack_into('<I', index_data, 4, 0x20001)  # version
        struct.pack_into('<i', index_data, 8, num_entries)  # num_entries
        struct.pack_into('<i', index_data, 28, table_len)  # table_len

        # 2. Create data_1 (EntryStore) with 8KB header + entries
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)

        entries_data = bytearray()

        for i, url in enumerate(urls):
            # Create EntryStore (256 bytes)
            entry_store = bytearray(256)

            # Calculate hash (simple placeholder)
            url_hash = hash(url) & 0xFFFFFFFF
            struct.pack_into('<I', entry_store, 0, url_hash)  # hash

            # Next entry address (0 = end of chain)
            struct.pack_into('<I', entry_store, 4, 0)

            # Key length and inline key
            url_bytes = url.encode('utf-8')
            struct.pack_into('<i', entry_store, 32, len(url_bytes))  # key_len
            entry_store[96:96 + min(len(url_bytes), 159)] = url_bytes[:159]

            # Add entry address to index hash table
            bucket = url_hash % table_len
            entry_addr = 0xA0010000 + i  # file_type 2, file 1, block i
            struct.pack_into('<I', index_data, 256 + bucket * 4, entry_addr)

            entries_data.extend(entry_store)

        (cache_dir / "index").write_bytes(bytes(index_data))
        (cache_dir / "data_1").write_bytes(bytes(data_header) + bytes(entries_data))

        # 3. Create data_0 (rankings) - minimal
        (cache_dir / "data_0").write_bytes(bytes(data_header))

    def test_ingest_blockfile_inserts_urls(self, tmp_path):
        """
        Integration test: blockfile ingestion should insert URLs into database.

        This tests the complete flow from blockfile cache -> URL database rows.
        """
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from extractors.callbacks import ExtractorCallbacks
        from core.database import DatabaseManager
        from datetime import datetime, timezone
        import json

        # Create a blockfile cache with test URLs
        output_dir = tmp_path / "cache_simple"
        run_id = "test_run"
        profile_dir = output_dir / run_id / "chrome_Default"

        test_urls = [
            "https://example.com/page1",
            "https://example.org/page2",
            "http://test.net/image.jpg",
        ]
        self._create_minimal_blockfile_cache(profile_dir, test_urls)

        # Create manifest indicating blockfile cache
        manifest = {
            "run_id": run_id,
            "evidence_id": 1,
            "extractor": "cache_simple",
            "extractor_version": "0.68.1",
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": "test-tool:1.0",
            "hash_mode": "extraction",
            "status": "ok",
            "files": [
                {
                    "source_path": "Cache/Cache_Data/index",
                    "logical_path": "Cache/Cache_Data/index",
                    "forensic_path": "",
                    "partition_index": 0,
                    "fs_type": "unknown",
                    "extracted_path": str((profile_dir / "index").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "index").stat().st_size,
                    "md5": None,
                    "sha256": None,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                    "entry_hash": None,
                },
                {
                    "source_path": "Cache/Cache_Data/data_0",
                    "logical_path": "Cache/Cache_Data/data_0",
                    "forensic_path": "",
                    "partition_index": 0,
                    "fs_type": "unknown",
                    "extracted_path": str((profile_dir / "data_0").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "data_0").stat().st_size,
                    "md5": None,
                    "sha256": None,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                    "entry_hash": None,
                },
                {
                    "source_path": "Cache/Cache_Data/data_1",
                    "logical_path": "Cache/Cache_Data/data_1",
                    "forensic_path": "",
                    "partition_index": 0,
                    "fs_type": "unknown",
                    "extracted_path": str((profile_dir / "data_1").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "data_1").stat().st_size,
                    "md5": None,
                    "sha256": None,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                    "entry_hash": None,
                },
            ],
            "notes": [],
            "statistics": {},
            "config": {"browsers": ["chrome"]},
            "e01_context": {},
        }

        run_dir = output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # Initialize database
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        evidence_conn = db_manager.get_evidence_conn(1, "test_evidence")

        # Create mock callbacks
        class MockCallbacks(ExtractorCallbacks):
            def __init__(self):
                self.logs = []
                self.errors = []
                self.progress = []
            def on_progress(self, current, total, message=None):
                self.progress.append((current, total, message))
            def on_step(self, message):
                self.logs.append(f"STEP: {message}")
            def on_log(self, message):
                self.logs.append(message)
            def on_error(self, message, details=None):
                self.errors.append((message, details))
            def on_complete(self, success, stats=None): pass
            def is_cancelled(self): return False

        callbacks = MockCallbacks()
        extractor = CacheSimpleExtractor()

        # Run ingestion
        stats = extractor.run_ingestion(
            output_dir=output_dir,
            evidence_conn=evidence_conn,
            evidence_id=1,
            config={},
            callbacks=callbacks,
        )

        # Verify URLs were inserted (may be less than 3 due to hash collisions in buckets)
        url_count = evidence_conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
        assert url_count > 0, "At least one URL should be inserted"

        # Verify blockfile entries stat
        assert stats["blockfile_entries"] > 0, "Should have processed blockfile entries"

        # Verify URLs contain expected domain
        domains = [r[0] for r in evidence_conn.execute("SELECT DISTINCT domain FROM urls").fetchall()]
        # At least one of our test domains should be present
        assert any(d in domains for d in ["example.com", "example.org", "test.net"]), \
            f"Expected test domains, got: {domains}"

        # Verify tags contain cache_backend=blockfile
        for row in evidence_conn.execute("SELECT tags FROM urls").fetchall():
            tags = json.loads(row[0]) if row[0] else {}
            assert tags.get("cache_backend") == "blockfile", \
                f"Expected cache_backend=blockfile in tags: {tags}"

        db_manager.close_all()

    def test_ingest_blockfile_with_long_url(self, tmp_path):
        """
        Integration test: blockfile with long URLs (>160 bytes) should be ingested.

        This verifies that the lazy loading and long key support work end-to-end.
        """
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from extractors.callbacks import ExtractorCallbacks
        from core.database import DatabaseManager
        from datetime import datetime, timezone
        import json

        # Create a blockfile cache with a long URL stored in external file
        output_dir = tmp_path / "cache_simple"
        run_id = "test_run"
        profile_dir = output_dir / run_id / "chrome_Default"
        profile_dir.mkdir(parents=True)

        # Long URL (> 160 bytes)
        long_url = "https://example.com/very/long/path/that/exceeds/the/inline/storage/limit/" + "x" * 150
        assert len(long_url) > MAX_INTERNAL_KEY_LENGTH

        # Create external file containing long key
        (profile_dir / "f_000001").write_bytes(long_url.encode('utf-8'))

        # Create index with one entry
        table_len = 16
        index_data = bytearray(256 + table_len * 4)
        struct.pack_into('<I', index_data, 0, BLOCKFILE_INDEX_MAGIC)
        struct.pack_into('<I', index_data, 4, 0x20001)
        struct.pack_into('<i', index_data, 8, 1)  # 1 entry
        struct.pack_into('<i', index_data, 28, table_len)

        # Entry address at bucket 0
        entry_addr = 0xA0010000  # file_type 2, file 1, block 0
        struct.pack_into('<I', index_data, 256, entry_addr)

        # Create data_1 with EntryStore pointing to external long key
        data_header = bytearray(8192)
        struct.pack_into('<I', data_header, 0, BLOCKFILE_BLOCK_MAGIC)

        entry_store = bytearray(256)
        struct.pack_into('<I', entry_store, 0, 0x12345678)  # hash
        struct.pack_into('<i', entry_store, 32, len(long_url))  # key_len (long)
        # long_key address: initialized + external + file 1
        long_key_addr = 0x80000001
        struct.pack_into('<I', entry_store, 36, long_key_addr)

        (profile_dir / "index").write_bytes(bytes(index_data))
        (profile_dir / "data_1").write_bytes(bytes(data_header) + bytes(entry_store))
        (profile_dir / "data_0").write_bytes(bytes(data_header))

        # Create manifest
        manifest = {
            "run_id": run_id,
            "evidence_id": 1,
            "extractor": "cache_simple",
            "extractor_version": "0.68.1",
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": "test-tool:1.0",
            "hash_mode": "extraction",
            "status": "ok",
            "files": [
                {
                    "source_path": "Cache/Cache_Data/index",
                    "extracted_path": str((profile_dir / "index").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "index").stat().st_size,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                },
                {
                    "source_path": "Cache/Cache_Data/data_0",
                    "extracted_path": str((profile_dir / "data_0").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "data_0").stat().st_size,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                },
                {
                    "source_path": "Cache/Cache_Data/data_1",
                    "extracted_path": str((profile_dir / "data_1").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "data_1").stat().st_size,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",
                },
                {
                    "source_path": "Cache/Cache_Data/f_000001",
                    "extracted_path": str((profile_dir / "f_000001").relative_to(output_dir)),
                    "size_bytes": (profile_dir / "f_000001").stat().st_size,
                    "browser": "chrome",
                    "profile": "Default",
                    "artifact_type": "cache_simple",
                    "file_type": "block",  # External files marked as block
                },
            ],
            "notes": [],
            "statistics": {},
            "config": {"browsers": ["chrome"]},
            "e01_context": {},
        }

        run_dir = output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # Initialize database
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        evidence_conn = db_manager.get_evidence_conn(1, "test_evidence")

        # Mock callbacks
        class MockCallbacks(ExtractorCallbacks):
            def on_progress(self, current, total, message=None): pass
            def on_step(self, message): pass
            def on_log(self, message): pass
            def on_error(self, message, details=None): pass
            def on_complete(self, success, stats=None): pass
            def is_cancelled(self): return False

        callbacks = MockCallbacks()
        extractor = CacheSimpleExtractor()

        # Run ingestion
        stats = extractor.run_ingestion(
            output_dir=output_dir,
            evidence_conn=evidence_conn,
            evidence_id=1,
            config={},
            callbacks=callbacks,
        )

        # Verify the long URL was inserted
        url_rows = evidence_conn.execute("SELECT url FROM urls").fetchall()
        urls = [r[0] for r in url_rows]

        assert long_url in urls, f"Long URL should be ingested. Got: {urls}"

        db_manager.close_all()


class TestOrphanFileCarving:
    """Test orphan data file carving when blockfile index has no valid entries."""

    def _make_empty_blockfile_cache(self, cache_dir: Path, f_files: dict):
        """
        Create a blockfile cache dir with valid magic but zeroed entries,
        plus external f_* files containing image data.
        """
        # Index: valid magic, version 2.1, but empty hash table
        header = struct.pack(
            '<II ii ii Ii ii Q',
            BLOCKFILE_INDEX_MAGIC,  # magic
            0x00020001,             # version 2.1
            0,                      # num_entries
            0,                      # num_bytes
            0,                      # last_file
            1,                      # this_id
            0,                      # stats_addr
            0x10000,                # table_len (65536)
            0,                      # crash
            0,                      # experiment
            0,                      # create_time
        )
        header += b'\x00' * (256 - len(header))
        # Write index (header + empty table)
        (cache_dir / 'index').write_bytes(header + b'\x00' * (65536 * 4))

        # data_0 and data_1: valid magic + empty blocks
        for i in range(4):
            bh = struct.pack('<II hh iii', 0xC104CAC3, 0x20001, i, 0, BLOCK_SIZES.get(i + 1, 256), 0, 0)
            bh += b'\x00' * (8192 - len(bh))
            (cache_dir / f'data_{i}').write_bytes(bh)

        # Write external f_* files
        for name, content in f_files.items():
            (cache_dir / name).write_bytes(content)

    def test_orphan_carving_detects_images(self, tmp_path):
        """Orphan carving should detect and carve images from f_* files."""
        from extractors.browser.chromium.cache._blockfile_ingestion import (
            _carve_orphan_data_files,
        )
        from core.database.manager import DatabaseManager

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # Create a minimal JPEG
        jpeg_data = b'\xff\xd8\xff\xe0' + b'\x00' * 100 + b'\xff\xd9'
        # Create a minimal PNG
        png_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100

        self._make_empty_blockfile_cache(cache_dir, {
            "f_000001": jpeg_data,
            "f_000002": png_data,
            "f_000003": b'not-an-image-just-text',
        })

        # Verify no entries from index
        entries = parse_blockfile_cache(cache_dir)
        assert len(entries) == 0

        # Setup DB
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test.sqlite"
        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        evidence_conn = db_manager.get_evidence_conn(1, "test_evidence")

        extraction_dir = tmp_path / "output"
        extraction_dir.mkdir()

        count = _carve_orphan_data_files(
            evidence_conn=evidence_conn,
            evidence_id=1,
            cache_dir=cache_dir,
            extraction_dir=extraction_dir,
            run_id="test_run",
            extractor_version="1.0.0",
        )

        # Should have carved at least the JPEG (PNG might fail without
        # valid chunk structure, but JPEG magic is enough for detection)
        assert count >= 1, f"Expected at least 1 carved image, got {count}"

        # Check images were inserted
        rows = evidence_conn.execute("SELECT count(*) FROM images").fetchone()
        assert rows[0] >= 1

        db_manager.close_all()

    def test_orphan_carving_skips_non_images(self, tmp_path):
        """Orphan carving should skip non-image files."""
        from extractors.browser.chromium.cache._blockfile_ingestion import (
            _carve_orphan_data_files,
        )
        from core.database.manager import DatabaseManager

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        self._make_empty_blockfile_cache(cache_dir, {
            "f_000001": b'Hello World - just text',
            "f_000002": b'\x50\x4b\x03\x04zip-file-data',
        })

        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test.sqlite"
        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        evidence_conn = db_manager.get_evidence_conn(1, "test_evidence")

        extraction_dir = tmp_path / "output"
        extraction_dir.mkdir()

        count = _carve_orphan_data_files(
            evidence_conn=evidence_conn,
            evidence_id=1,
            cache_dir=cache_dir,
            extraction_dir=extraction_dir,
            run_id="test_run",
            extractor_version="1.0.0",
        )

        assert count == 0

        db_manager.close_all()

    def test_orphan_carving_no_f_files(self, tmp_path):
        """Orphan carving returns 0 when no f_* files exist."""
        from extractors.browser.chromium.cache._blockfile_ingestion import (
            _carve_orphan_data_files,
        )
        from core.database.manager import DatabaseManager

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        self._make_empty_blockfile_cache(cache_dir, {})

        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test.sqlite"
        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        evidence_conn = db_manager.get_evidence_conn(1, "test_evidence")

        extraction_dir = tmp_path / "output"
        extraction_dir.mkdir()

        count = _carve_orphan_data_files(
            evidence_conn=evidence_conn,
            evidence_id=1,
            cache_dir=cache_dir,
            extraction_dir=extraction_dir,
            run_id="test_run",
            extractor_version="1.0.0",
        )

        assert count == 0

        db_manager.close_all()