from core.database import validate_all_schemas


def test_table_schemas_match_baseline() -> None:
    errors = validate_all_schemas()
    assert errors == {}, f"Schema drift detected: {errors}"
