from __future__ import annotations

import sqlite3
from pathlib import Path

from extractors.browser.safari.favicons._parser import parse_favicons_db


class _WarningCollector:
    def __init__(self) -> None:
        self.unknown_tables: list[str] = []
        self.unknown_columns: list[tuple[str, str]] = []

    def add_unknown_table(self, table_name: str, columns: list[str], source_file: str, artifact_type: str) -> None:
        self.unknown_tables.append(table_name)

    def add_unknown_column(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        source_file: str,
        artifact_type: str,
    ) -> None:
        self.unknown_columns.append((table_name, column_name))


def _create_favicons_db(path: Path, *, add_unknown_table: bool = False, add_unknown_column: bool = False) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE icon_info (
            uuid TEXT PRIMARY KEY,
            url TEXT,
            timestamp REAL,
            width INTEGER,
            height INTEGER,
            has_generated_representations INTEGER
            {extra_col}
        )
        """.format(extra_col=", future_col TEXT" if add_unknown_column else "")
    )
    conn.execute(
        """
        CREATE TABLE page_url (
            uuid TEXT,
            url TEXT
        )
        """
    )
    if add_unknown_table:
        conn.execute("CREATE TABLE icon_future_data (id INTEGER, value TEXT)")

    conn.execute(
        """
        INSERT INTO icon_info(uuid, url, timestamp, width, height, has_generated_representations {extra_fields})
        VALUES ('uuid-1', 'https://example.com/favicon.ico', 730000000.0, 64, 64, 1 {extra_values})
        """.format(
            extra_fields=", future_col" if add_unknown_column else "",
            extra_values=", 'x'" if add_unknown_column else "",
        )
    )
    conn.execute(
        "INSERT INTO page_url(uuid, url) VALUES ('uuid-1', 'https://example.com/page1')"
    )
    conn.execute(
        "INSERT INTO page_url(uuid, url) VALUES ('uuid-1', 'https://example.com/page2')"
    )
    conn.commit()
    conn.close()


def test_parse_favicons_db_returns_icons_and_mappings(tmp_path: Path) -> None:
    db_path = tmp_path / "Favicons.db"
    _create_favicons_db(db_path)

    icons, mappings = parse_favicons_db(db_path)
    assert len(icons) == 1
    assert icons[0].uuid == "uuid-1"
    assert icons[0].icon_url == "https://example.com/favicon.ico"
    assert icons[0].width == 64
    assert mappings["uuid-1"] == [
        "https://example.com/page1",
        "https://example.com/page2",
    ]


def test_parse_favicons_db_handles_missing_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "Favicons.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE other_table(id INTEGER)")
    conn.commit()
    conn.close()

    icons, mappings = parse_favicons_db(db_path)
    assert icons == []
    assert mappings == {}


def test_parse_favicons_db_collects_schema_warnings(tmp_path: Path) -> None:
    db_path = tmp_path / "Favicons.db"
    _create_favicons_db(db_path, add_unknown_table=True, add_unknown_column=True)
    collector = _WarningCollector()

    parse_favicons_db(db_path, warning_collector=collector, source_file=str(db_path))

    assert "icon_future_data" in collector.unknown_tables
    assert ("icon_info", "future_col") in collector.unknown_columns
