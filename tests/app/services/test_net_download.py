import hashlib
import io
from pathlib import Path
from typing import List

import requests

from app.services.net_download import DownloadRequest, download_items


class MockResponse:
    def __init__(self, status_code: int, content: bytes, headers=None):
        self.status_code = status_code
        self._content = content
        self.headers = headers or {"Content-Type": "image/png"}
        self.ok = status_code == 200
        self._buffer = io.BytesIO(content)

    def iter_content(self, chunk_size=8192):
        while True:
            chunk = self._buffer.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def test_download_respects_size_limit(monkeypatch, tmp_path: Path) -> None:
    responses: List[MockResponse] = [MockResponse(200, b"a" * (1024 * 1024 * 5))]

    def mock_get(url, stream=True, timeout=10):  # noqa: ANN001
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)
    req = DownloadRequest(item_id=1, url="http://example.com/file", dest_path=tmp_path / "file.bin", domain="example")
    results = download_items(
        [req],
        concurrency=1,
        timeout_s=5,
        retries=0,
        max_bytes=1024 * 1024,
        allowed_content_types=["image/"],
    )
    assert not results[0].ok
    assert results[0].error.startswith("size limit exceeded")
    assert results[0].bytes_written > 1024 * 1024
    assert results[0].sha256 is None


def test_download_filters_content_type(monkeypatch, tmp_path: Path) -> None:
    responses = [MockResponse(200, b"data", headers={"Content-Type": "application/octet-stream"})]

    def mock_get(url, stream=True, timeout=10):  # noqa: ANN001
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)
    req = DownloadRequest(item_id=1, url="http://example.com/file", dest_path=tmp_path / "file.bin", domain="example")
    results = download_items(
        [req],
        concurrency=1,
        timeout_s=5,
        retries=0,
        max_bytes=1024 * 1024,
        allowed_content_types=["image/"],
    )
    assert not results[0].ok
    assert "Blocked content-type" in results[0].error


def test_download_captures_sha(monkeypatch, tmp_path: Path) -> None:
    content = b"hello world"
    responses = [MockResponse(200, content)]

    def mock_get(url, stream=True, timeout=10):  # noqa: ANN001
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)
    req = DownloadRequest(item_id=1, url="http://example.com/file", dest_path=tmp_path / "file.bin", domain="example")
    results = download_items(
        [req],
        concurrency=1,
        timeout_s=5,
        retries=0,
        max_bytes=1024 * 1024,
        allowed_content_types=["image/"],
    )
    assert results[0].ok
    assert results[0].sha256 == hashlib.sha256(content).hexdigest()
    assert results[0].bytes_written == len(content)


def test_download_retries(monkeypatch, tmp_path: Path) -> None:
    calls = {"count": 0}

    def failing_then_success(url, stream=True, timeout=10):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.exceptions.ConnectionError("boom")
        return MockResponse(200, b"ok")

    monkeypatch.setattr(requests, "get", failing_then_success)
    monkeypatch.setattr("app.services.net_download.time.sleep", lambda _: None)

    req = DownloadRequest(item_id=1, url="http://example.com/file", dest_path=tmp_path / "file.bin", domain="example")
    results = download_items(
        [req],
        concurrency=1,
        timeout_s=5,
        retries=1,
        max_bytes=1024 * 1024,
        allowed_content_types=["image/"],
    )
    assert results[0].ok
    assert results[0].attempts == 2
    assert calls["count"] == 2
