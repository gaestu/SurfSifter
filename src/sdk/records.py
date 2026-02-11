from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .base import EmittedRecord


def make_url_record(
    url: str,
    discovered_by: str,
    *,
    domain: Optional[str] = None,
    scheme: Optional[str] = None,
    source_path: Optional[str] = None,
    ts_utc: Optional[datetime] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> EmittedRecord:
    data: Dict[str, Any] = {
        "url": url,
        "discovered_by": discovered_by,
        "domain": domain,
        "scheme": scheme,
        "source_path": source_path,
        "ts_utc": _serialize_datetime(ts_utc),
    }
    if extra:
        data.update(extra)
    return EmittedRecord(kind="url", data=data)


def make_image_record(
    rel_path: str,
    filename: str,
    discovered_by: str,
    *,
    ts_utc: Optional[datetime] = None,
    hashes: Optional[Dict[str, str]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> EmittedRecord:
    data: Dict[str, Any] = {
        "rel_path": rel_path,
        "filename": filename,
        "discovered_by": discovered_by,
        "ts_utc": _serialize_datetime(ts_utc),
    }
    if hashes:
        data["hashes"] = hashes
    if extra:
        data.update(extra)
    return EmittedRecord(kind="image", data=data)


def make_timeline_record(
    ts_utc: datetime,
    kind: str,
    ref_table: str,
    ref_id: int,
    *,
    confidence: str = "medium",
    note: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> EmittedRecord:
    data: Dict[str, Any] = {
        "ts_utc": _serialize_datetime(ts_utc),
        "kind": kind,
        "ref_table": ref_table,
        "ref_id": ref_id,
        "confidence": confidence,
        "note": note,
    }
    if extra:
        data.update(extra)
    return EmittedRecord(kind="timeline", data=data)


def _serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
