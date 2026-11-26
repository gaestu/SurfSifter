"""No option limits extraction to one partition (issue #78).

The former "Scan all partitions" option is gone: no extractor reads it, and
the extractors that used to honour it search the file list unconditionally.
"""
from __future__ import annotations

import importlib
import re
from contextlib import contextmanager
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"

# Extractors that limited discovery to the open partition when the option
# was off, with one browser each. One browser keeps the test independent of
# the number of path patterns a full browser selection produces.
FORMERLY_OPTIONAL = [
    ("chromium_autofill", "chrome"),
    ("chromium_bookmarks", "chrome"),
    ("chromium_config", "chrome"),
    ("chromium_cookies", "chrome"),
    ("chromium_downloads", "chrome"),
    ("chromium_favicons", "chrome"),
    ("chromium_history", "chrome"),
    ("chromium_permissions", "chrome"),
    ("chromium_sessions", "chrome"),
    ("chromium_site_engagement", "chrome"),
    ("chromium_sync_data", "chrome"),
    ("media_history", "chrome"),
    ("firefox_autofill", "firefox"),
    ("firefox_config", "firefox"),
    ("firefox_cookies", "firefox"),
    ("firefox_downloads", "firefox"),
    ("firefox_extensions", "firefox"),
    ("firefox_history", "firefox"),
    ("firefox_permissions", "firefox"),
    ("tor_state", "tor"),
]

# These two query the file list for other reasons too, so they are checked by
# the partition they open. For cache_simple only a value passed in the run
# config had an effect; its checkbox never reached the run.
OPENS_FILE_LIST_PARTITION = [
    (
        "chromium_extensions",
        "extractors.browser.chromium.extensions.extractor",
        "Users/A/AppData/Local/Google/Chrome/User Data/Default/Preferences",
    ),
    (
        "cache_simple",
        "extractors.browser.chromium.cache.extractor",
        "Users/A/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/index",
    ),
]

_FILE_LIST_QUERY = re.compile(r"\bFROM\s+file_list\b", re.IGNORECASE)
_COUNT_QUERY = re.compile(r"\bCOUNT\s*\(", re.IGNORECASE)


def _is_discovery_query(sql: str) -> bool:
    """A file list search, not the check whether a file list exists."""
    return bool(_FILE_LIST_QUERY.search(sql)) and not _COUNT_QUERY.search(sql)


class _EmptyOpenPartition:
    """Evidence filesystem whose open partition holds no artifacts."""

    partition_index = 1
    fs_type = "NTFS"
    ewf_paths = None
    source_path = None

    def iter_paths(self, pattern):  # noqa: ARG002
        return iter(())


class _EmptyOpenEwfPartition(_EmptyOpenPartition):
    """The same for an EWF image, whose other partitions can be opened."""

    ewf_paths = [Path("image.E01")]


class _Callbacks:
    def on_step(self, step_name):  # noqa: ARG002
        pass

    def on_progress(self, current, total, message=""):  # noqa: ARG002
        pass

    def on_log(self, message, level="info"):  # noqa: ARG002
        pass

    def on_error(self, error, details=""):  # noqa: ARG002
        pass

    def is_cancelled(self):
        return False


@pytest.fixture
def evidence_conn(case_context):
    """Evidence database with a file list entry on a second partition."""
    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id, case_context.evidence_label
    )
    with conn:
        conn.execute(
            "INSERT INTO file_list(evidence_id, file_path, file_name, extension,"
            " import_timestamp, partition_index) VALUES (?, ?, ?, ?, ?, ?)",
            (
                case_context.evidence_id,
                "Data/readme.txt",
                "readme.txt",
                ".txt",
                "2026-01-01T00:00:00+00:00",
                2,
            ),
        )
    return conn


def test_option_does_not_occur_in_source():
    """The option cannot come back unnoticed, shown or read."""
    hits = [
        f"{path.relative_to(SRC_DIR)}:{number}"
        for path in sorted(SRC_DIR.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if "scan_all_partitions" in line
    ]

    assert not hits, "scan_all_partitions found in:\n" + "\n".join(hits)


def test_formerly_optional_extractors_are_registered(extractor_registry_names):
    names = {name for name, _ in FORMERLY_OPTIONAL} | {row[0] for row in OPENS_FILE_LIST_PARTITION}
    assert len(names) == 22
    assert names <= set(extractor_registry_names)


@pytest.mark.parametrize("name,browser", FORMERLY_OPTIONAL)
def test_file_list_is_searched_even_if_the_old_option_is_off(
    name, browser, extractor_registry, case_context, evidence_conn, tmp_path
):
    statements = []
    evidence_conn.set_trace_callback(statements.append)
    config = {
        "evidence_id": case_context.evidence_id,
        "evidence_label": case_context.evidence_label,
        "evidence_conn": evidence_conn,
        "browsers": [browser],
        "scan_all_partitions": False,
    }

    try:
        extractor_registry.get(name).run_extraction(
            _EmptyOpenPartition(), tmp_path / name, config, _Callbacks()
        )
    finally:
        evidence_conn.set_trace_callback(None)

    assert any(_is_discovery_query(sql) for sql in statements), (
        f"{name} did not search the file list"
    )


@pytest.mark.parametrize(
    "name,module_name,file_path",
    OPENS_FILE_LIST_PARTITION,
    ids=[row[0] for row in OPENS_FILE_LIST_PARTITION],
)
def test_other_partition_is_opened_even_if_the_old_option_is_off(
    name, module_name, file_path, extractor_registry, case_context, evidence_conn,
    tmp_path, monkeypatch,
):
    with evidence_conn:
        evidence_conn.execute(
            "INSERT INTO file_list(evidence_id, file_path, file_name, extension,"
            " import_timestamp, partition_index) VALUES (?, ?, ?, ?, ?, ?)",
            (
                case_context.evidence_id,
                file_path,
                file_path.rsplit("/", 1)[-1],
                "",
                "2026-01-01T00:00:00+00:00",
                2,
            ),
        )
    opened = []

    @contextmanager
    def record_partition(evidence_fs_or_paths, partition_index):  # noqa: ARG001
        opened.append(partition_index)
        yield _EmptyOpenPartition()

    monkeypatch.setattr(
        importlib.import_module(module_name), "open_partition_for_extraction", record_partition
    )
    config = {
        "evidence_id": case_context.evidence_id,
        "evidence_label": case_context.evidence_label,
        "evidence_conn": evidence_conn,
        "browsers": ["chrome"],
        "scan_all_partitions": False,
    }

    extractor_registry.get(name).run_extraction(
        _EmptyOpenEwfPartition(), tmp_path / name, config, _Callbacks()
    )

    assert 2 in opened
