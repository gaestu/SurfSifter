import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import core.database
from extractors.system.registry import ingestion
from extractors.system.registry.parser import RegistryFinding


def test_registry_ingestion_persists_original_hive_path(tmp_path, monkeypatch):
    """Registry findings should point to the evidence hive, not the local export."""
    output_dir = tmp_path / "registry"
    hives_dir = output_dir / "hives"
    hives_dir.mkdir(parents=True)
    local_hive = hives_dir / "NTUSER_0.hive"
    local_hive.write_bytes(b"test")

    original_path = "Users/Alice/NTUSER.DAT"
    manifest_data = {
        "run_id": "registry-run-1",
        "extracted_hives": [
            {
                "original_path": original_path,
                "local_path": "hives/NTUSER_0.hive",
                "filename": "NTUSER.DAT",
                "logical_hive": "NTUSER",
                "size": 4,
            }
        ],
    }
    target = {
        "name": "ntuser_test",
        "paths": ["Users/*/NTUSER.DAT"],
    }

    monkeypatch.setattr(
        ingestion,
        "load_registry_rules",
        lambda: SimpleNamespace(targets=[target]),
    )

    def fake_process_hive_file(hive_path: Path, target_info):
        assert hive_path == local_hive
        assert target_info == target
        return [
            RegistryFinding(
                detector_id="ntuser_test",
                name="browser:typed_url",
                value="https://example.com",
                confidence="0.85",
                provenance="registry",
                hive=str(hive_path),
                path="Software\\Microsoft\\Internet Explorer\\TypedURLs\\url1",
            )
        ]

    inserted_records = []

    def fake_insert_os_indicators(conn, evidence_id, records):
        inserted_records.extend(records)
        return len(records)

    monkeypatch.setattr(ingestion, "process_hive_file", fake_process_hive_file)
    monkeypatch.setattr(core.database, "insert_os_indicators", fake_insert_os_indicators)
    monkeypatch.setattr(core.database, "delete_os_indicators_by_run", lambda *args: 0)

    result = ingestion.run_registry_ingestion(
        manifest_data=manifest_data,
        evidence_conn=sqlite3.connect(":memory:"),
        evidence_id=1,
        callbacks=Mock(),
        output_dir=output_dir,
        config={"run_id": "registry-run-1"},
    )

    assert result == {"inserted": 1, "errors": 0}
    assert inserted_records[0]["hive"] == original_path
    assert inserted_records[0]["hive"] != str(local_hive)
