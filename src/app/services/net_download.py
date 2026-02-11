from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Callable, Dict, Iterable, List, Optional

import requests


@dataclass
class DownloadRequest:
    item_id: int
    url: str
    dest_path: Path
    domain: str


@dataclass
class DownloadResult:
    item_id: int
    url: str
    dest_path: Optional[Path]
    ok: bool
    status_code: Optional[int]
    bytes_written: int
    sha256: Optional[str]
    md5: Optional[str]  # Added for hash matching
    error: Optional[str]
    duration_s: float
    attempts: int
    content_type: Optional[str]


def sanitize_filename(name: str) -> str:
    safe = [c if c.isalnum() or c in {"-", "_", "."} else "_" for c in name]
    return "".join(safe) or "download"


def _normalize_content_type(raw: Optional[str]) -> str:
    if not raw:
        return ""
    token = raw.split(";", 1)[0]
    return token.strip().lower()


def _is_allowed_content_type(value: str, allowed: Iterable[str]) -> bool:
    if not allowed:
        return False
    for pattern in allowed:
        token = (pattern or "").strip().lower()
        if not token:
            continue
        if token.endswith("*"):
            prefix = token[:-1]
            if value.startswith(prefix):
                return True
        elif token.endswith("/"):
            if value.startswith(token):
                return True
        elif value == token:
            return True
    return False


def download_items(
    requests_list: Iterable[DownloadRequest],
    *,
    concurrency: int,
    timeout_s: int,
    retries: int,
    max_bytes: int,
    allowed_content_types: List[str],
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[DownloadResult]:
    lock = threading.Lock()
    results: List[DownloadResult] = []
    normalized_allow = [ctype.strip().lower() for ctype in allowed_content_types if ctype and ctype.strip()]

    def report(item_id: int, pct: int, note: str) -> None:
        if progress_cb:
            progress_cb(item_id, pct, note)

    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    def worker(req: DownloadRequest) -> DownloadResult:
        if cancelled():
            return DownloadResult(
                item_id=req.item_id,
                url=req.url,
                dest_path=None,
                ok=False,
                status_code=None,
                bytes_written=0,
                sha256=None,
                md5=None,
                error="cancelled",
                duration_s=0.0,
                attempts=0,
                content_type=None,
            )
        attempt = 0
        last_error: Optional[str] = None
        last_status: Optional[int] = None
        last_duration = 0.0
        while attempt <= retries:
            if cancelled():
                return DownloadResult(
                    item_id=req.item_id,
                    url=req.url,
                    dest_path=None,
                    ok=False,
                    status_code=last_status,
                    bytes_written=0,
                    sha256=None,
                    md5=None,
                    error="cancelled",
                    duration_s=last_duration,
                    attempts=attempt,
                    content_type=None,
                )
            attempt += 1
            report(req.item_id, 5, f"Connecting (attempt {attempt})")
            start_time = time.perf_counter()
            try:
                with requests.get(req.url, stream=True, timeout=timeout_s) as resp:
                    status_code = resp.status_code
                    content_type = _normalize_content_type(resp.headers.get("Content-Type"))
                    if not _is_allowed_content_type(content_type, normalized_allow):
                        duration = time.perf_counter() - start_time
                        return DownloadResult(
                            item_id=req.item_id,
                            url=req.url,
                            dest_path=None,
                            ok=False,
                            status_code=status_code,
                            bytes_written=0,
                            sha256=None,
                            md5=None,
                            error=f"Blocked content-type {content_type or 'unknown'}",
                            duration_s=duration,
                            attempts=attempt,
                            content_type=content_type or None,
                        )
                    if status_code >= 500:
                        last_status = status_code
                        raise RuntimeError(f"HTTP {status_code}")
                    if status_code >= 400:
                        duration = time.perf_counter() - start_time
                        return DownloadResult(
                            item_id=req.item_id,
                            url=req.url,
                            dest_path=None,
                            ok=False,
                            status_code=status_code,
                            bytes_written=0,
                            sha256=None,
                            md5=None,
                            error=f"HTTP {status_code}",
                            duration_s=duration,
                            attempts=attempt,
                            content_type=content_type or None,
                        )
                    dest_dir = req.dest_path.parent
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    # Compute both SHA256 and MD5 for hash matching
                    hasher_sha256 = hashlib.sha256()
                    hasher_md5 = hashlib.md5()
                    bytes_written = 0
                    with req.dest_path.open("wb") as fh:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if cancelled():
                                fh.close()
                                req.dest_path.unlink(missing_ok=True)
                                duration = time.perf_counter() - start_time
                                return DownloadResult(
                                    item_id=req.item_id,
                                    url=req.url,
                                    dest_path=None,
                                    ok=False,
                                    status_code=None,
                                    bytes_written=bytes_written,
                                    sha256=None,
                                    md5=None,
                                    error="cancelled",
                                    duration_s=duration,
                                    attempts=attempt,
                                    content_type=content_type or None,
                                )
                            if not chunk:
                                continue
                            bytes_written += len(chunk)
                            if bytes_written > max_bytes:
                                fh.close()
                                req.dest_path.unlink(missing_ok=True)
                                duration = time.perf_counter() - start_time
                                return DownloadResult(
                                    item_id=req.item_id,
                                    url=req.url,
                                    dest_path=None,
                                    ok=False,
                                    status_code=status_code,
                                    bytes_written=bytes_written,
                                    sha256=None,
                                    md5=None,
                                    error=f"size limit exceeded ({max_bytes} bytes)",
                                    duration_s=duration,
                                    attempts=attempt,
                                    content_type=content_type or None,
                                )
                            fh.write(chunk)
                            hasher_sha256.update(chunk)
                            hasher_md5.update(chunk)
                            pct = min(95, int((bytes_written / max(1, max_bytes)) * 90) + 5)
                            report(req.item_id, pct, "Downloading")
                    duration = time.perf_counter() - start_time
                    if cancelled():
                        req.dest_path.unlink(missing_ok=True)
                        return DownloadResult(
                            item_id=req.item_id,
                            url=req.url,
                            dest_path=None,
                            ok=False,
                            status_code=None,
                            bytes_written=bytes_written,
                            sha256=None,
                            md5=None,
                            error="cancelled",
                            duration_s=duration,
                            attempts=attempt,
                            content_type=content_type or None,
                        )
                    return DownloadResult(
                        item_id=req.item_id,
                        url=req.url,
                        dest_path=req.dest_path,
                        ok=True,
                        status_code=status_code,
                        bytes_written=bytes_written,
                        sha256=hasher_sha256.hexdigest(),
                        md5=hasher_md5.hexdigest(),
                        error=None,
                        duration_s=duration,
                        attempts=attempt,
                        content_type=content_type or None,
                    )
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                last_duration = time.perf_counter() - start_time
                if cancelled():
                    return DownloadResult(
                        item_id=req.item_id,
                        url=req.url,
                        dest_path=None,
                        ok=False,
                        status_code=last_status,
                        bytes_written=0,
                        sha256=None,
                        md5=None,
                        error="cancelled",
                        duration_s=last_duration,
                        attempts=attempt,
                        content_type=None,
                    )
                if attempt > retries:
                    break
                backoff = min(0.5 * (2 ** (attempt - 1)), 5.0)
                report(req.item_id, 5, f"Retry in {backoff:.1f}s after error: {exc}")
                time.sleep(backoff)
        return DownloadResult(
            item_id=req.item_id,
            url=req.url,
            dest_path=None,
            ok=False,
            status_code=last_status,
            bytes_written=0,
            sha256=None,
            md5=None,
            error=last_error or "download failed",
            duration_s=last_duration,
            attempts=attempt,
            content_type=None,
        )

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        future_map = {executor.submit(worker, req): req.item_id for req in requests_list}
        for future in as_completed(future_map):
            result = future.result()
            with lock:
                results.append(result)
            report(result.item_id, 100 if result.ok else 0, "Completed")
    results.sort(key=lambda item: item.item_id)
    return results
