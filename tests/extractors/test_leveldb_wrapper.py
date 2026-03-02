"""
Tests for LevelDB wrapper — robustness against invalid KeyState values.

Covers:
- iterate_records_raw() handles ValueError from invalid KeyState gracefully
- Records yielded before the error are preserved
- Error count is incremented
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from extractors._shared.leveldb_wrapper import (
    LevelDBWrapper,
    LevelDBRecord,
    is_leveldb_available,
    CCL_AVAILABLE,
)


pytestmark = pytest.mark.skipif(
    not CCL_AVAILABLE,
    reason="ccl_chromium_reader not installed",
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _fake_record(user_key: bytes, value: bytes, seq: int, state=None):
    """Build a SimpleNamespace mimicking a ccl_leveldb Record."""
    return SimpleNamespace(user_key=user_key, value=value, seq=seq, state=state)


class _FakeRawLevelDb:
    """
    Fake RawLevelDb that yields a configurable sequence of records
    and can inject a ValueError at a specific position.
    """

    def __init__(self, records, error_at=None, error_msg="999 is not a valid KeyState"):
        self._records = records
        self._error_at = error_at
        self._error_msg = error_msg

    def iterate_records_raw(self):
        for i, rec in enumerate(self._records):
            if self._error_at is not None and i == self._error_at:
                raise ValueError(self._error_msg)
            yield rec

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIterateRecordsRawKeyState:
    """Test iterate_records_raw() resilience to invalid KeyState."""

    def test_normal_records_yielded(self, tmp_path):
        """All records yielded when no errors occur."""
        from extractors._shared import leveldb_wrapper as mod

        fake_db = _FakeRawLevelDb([
            _fake_record(b"k1", b"v1", 1),
            _fake_record(b"k2", b"v2", 2),
        ])

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = fake_db  # Inject fake DB

        records = list(wrapper.iterate_records_raw())
        assert len(records) == 2
        assert records[0].key == b"k1"
        assert records[1].key == b"k2"

    def test_valueerror_stops_iteration_gracefully(self, tmp_path):
        """ValueError (invalid KeyState) stops iteration without crashing."""
        fake_db = _FakeRawLevelDb(
            records=[
                _fake_record(b"k1", b"v1", 1),
                _fake_record(b"k2", b"v2", 2),  # Will not be reached
                _fake_record(b"k3", b"v3", 3),
            ],
            error_at=1,
            error_msg="114 is not a valid KeyState",
        )

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = fake_db

        records = list(wrapper.iterate_records_raw())
        # Only the first record (before the error) is yielded
        assert len(records) == 1
        assert records[0].key == b"k1"

    def test_valueerror_increments_error_count(self, tmp_path):
        """ValueError increments wrapper error count."""
        fake_db = _FakeRawLevelDb(
            records=[_fake_record(b"k1", b"v1", 1)],
            error_at=0,
            error_msg="114 is not a valid KeyState",
        )

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = fake_db
        assert wrapper._error_count == 0

        list(wrapper.iterate_records_raw())
        assert wrapper._error_count == 1

    def test_valueerror_at_first_record(self, tmp_path):
        """ValueError at very first record yields nothing."""
        fake_db = _FakeRawLevelDb(
            records=[_fake_record(b"k1", b"v1", 1)],
            error_at=0,
        )

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = fake_db

        records = list(wrapper.iterate_records_raw())
        assert records == []

    def test_empty_db_yields_nothing(self, tmp_path):
        """Empty database yields no records."""
        fake_db = _FakeRawLevelDb(records=[])

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = fake_db

        records = list(wrapper.iterate_records_raw())
        assert records == []
        assert wrapper._error_count == 0

    def test_generic_exception_stops_iteration(self, tmp_path):
        """Non-ValueError exceptions also stop iteration gracefully."""
        class _ErrorDb:
            def iterate_records_raw(self):
                yield _fake_record(b"k1", b"v1", 1)
                raise RuntimeError("disk error")

            def close(self):
                pass

        wrapper = LevelDBWrapper(tmp_path)
        wrapper._db = _ErrorDb()

        records = list(wrapper.iterate_records_raw())
        assert len(records) == 1
        assert wrapper._error_count == 1
