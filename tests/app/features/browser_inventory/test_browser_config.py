"""Tests for the Browser Config inventory subtab model and helpers."""
from __future__ import annotations

import pytest

from core.database import (
    get_browser_config_filter_values,
    get_browser_configs,
    insert_browser_configs,
)


def _sample_config_entries():
    return [
        {
            "run_id": "run-1",
            "browser": "chrome",
            "profile": "Default",
            "config_type": "preferences",
            "config_key": "download.default_directory",
            "config_value": "/Users/test/Downloads",
            "source_path": "/Users/test/Chrome/Default/Preferences",
            "partition_index": 2,
            "fs_type": "ntfs",
            "logical_path": "/Users/test/Chrome/Default/Preferences",
            "forensic_path": "/p2/Users/test/Chrome/Default/Preferences",
            "notes": "preference source",
        },
        {
            "run_id": "run-1",
            "browser": "tor",
            "profile": "Browser/TorBrowser/Data/Tor",
            "config_type": "torrc",
            "config_key": "SocksPort",
            "config_value": "9150",
            "source_path": "/Tor Browser/Browser/TorBrowser/Data/Tor/torrc",
            "partition_index": 1,
        },
        {
            "run_id": "run-2",
            "browser": "firefox",
            "profile": "release",
            "config_type": "prefs_js",
            "config_key": "browser.startup.homepage",
            "config_value": "https://example.test",
            "source_path": "/Users/test/Firefox/release/prefs.js",
        },
    ]


def test_browser_config_helper_filters_and_searches(evidence_db, case_context):
    """Helper should support Browser Config UI filters without UI raw SQL."""
    insert_browser_configs(evidence_db, case_context.evidence_id, _sample_config_entries())

    chrome_rows = get_browser_configs(
        evidence_db,
        case_context.evidence_id,
        browser="chrome",
        config_type="preferences",
    )
    assert len(chrome_rows) == 1
    assert chrome_rows[0]["config_key"] == "download.default_directory"

    homepage_rows = get_browser_configs(
        evidence_db,
        case_context.evidence_id,
        config_key_search="STARTUP",
    )
    assert [row["browser"] for row in homepage_rows] == ["firefox"]

    profile_rows = get_browser_configs(
        evidence_db,
        case_context.evidence_id,
        profile_search="torbrowser",
    )
    assert [row["config_type"] for row in profile_rows] == ["torrc"]


def test_browser_config_search_treats_like_metacharacters_as_literals(evidence_db, case_context):
    """Percent and underscore searches should not become SQL wildcards."""
    insert_browser_configs(
        evidence_db,
        case_context.evidence_id,
        [
            {
                "run_id": "run-1",
                "browser": "chrome",
                "profile": "QA_Profile",
                "config_type": "preferences",
                "config_key": "literal%key",
                "config_value": "match",
                "source_path": "/Preferences",
            },
            {
                "run_id": "run-1",
                "browser": "chrome",
                "profile": "QAxProfile",
                "config_type": "preferences",
                "config_key": "literalXkey",
                "config_value": "no-match",
                "source_path": "/Preferences",
            },
        ],
    )

    percent_rows = get_browser_configs(
        evidence_db,
        case_context.evidence_id,
        config_key_search="literal%",
    )
    assert [row["config_key"] for row in percent_rows] == ["literal%key"]

    underscore_rows = get_browser_configs(
        evidence_db,
        case_context.evidence_id,
        profile_search="QA_",
    )
    assert [row["profile"] for row in underscore_rows] == ["QA_Profile"]


def test_browser_config_filter_values_are_allow_listed(evidence_db, case_context):
    """Distinct filter values should be sorted and limited to known columns."""
    insert_browser_configs(evidence_db, case_context.evidence_id, _sample_config_entries())

    assert get_browser_config_filter_values(
        evidence_db,
        case_context.evidence_id,
        "browser",
    ) == ["chrome", "firefox", "tor"]
    assert get_browser_config_filter_values(
        evidence_db,
        case_context.evidence_id,
        "config_type",
        browser="chrome",
    ) == ["preferences"]

    with pytest.raises(ValueError):
        get_browser_config_filter_values(evidence_db, case_context.evidence_id, "run_id")


def test_browser_config_table_model_loads_filtered_rows(case_context):
    """Browser Config model should expose required columns and filtered rows."""
    pytest.importorskip("PySide6")
    from app.features.browser_inventory.browser_config.model import BrowserConfigTableModel

    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id,
        case_context.evidence_label,
    )
    try:
        insert_browser_configs(conn, case_context.evidence_id, _sample_config_entries())
        conn.commit()
    finally:
        conn.close()

    model = BrowserConfigTableModel(
        case_context.manager,
        case_context.evidence_id,
        case_context.evidence_label,
    )
    model.load(browser_filter="chrome", key_search="download", profile_search="default")

    assert model.HEADERS == [
        "Browser",
        "Profile",
        "Config Type",
        "Config Key",
        "Config Value",
        "Source Path",
        "Partition",
        "Notes",
    ]
    assert model.rowCount() == 1
    row = model.get_row_data(model.index(0, 0))
    assert row["browser"] == "chrome"
    assert row["partition_index"] == 2
    assert row["forensic_path"] == "/p2/Users/test/Chrome/Default/Preferences"