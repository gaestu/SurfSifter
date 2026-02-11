"""
Tests for Firefox Storage Value Analyzer.

Tests extraction of forensic artifacts from browser storage values.
"""
import json
import pytest
from datetime import datetime, timezone

from extractors.browser.firefox.storage.analyzer import (
    StorageValueAnalyzer,
    ExtractedUrl,
    ExtractedEmail,
    ExtractedToken,
    ExtractedIdentifier,
    AnalysisResult,
)


class TestStorageValueAnalyzer:
    """Tests for StorageValueAnalyzer class."""

    def test_init_default_options(self):
        """Test default initialization enables all extraction options."""
        analyzer = StorageValueAnalyzer()
        assert analyzer.extract_urls is True
        assert analyzer.extract_emails is True
        assert analyzer.detect_tokens is True
        assert analyzer.extract_identifiers is True

    def test_init_custom_options(self):
        """Test initialization with custom options."""
        analyzer = StorageValueAnalyzer(
            extract_urls=False,
            extract_emails=False,
            detect_tokens=True,
            extract_identifiers=False,
        )
        assert analyzer.extract_urls is False
        assert analyzer.extract_emails is False
        assert analyzer.detect_tokens is True
        assert analyzer.extract_identifiers is False


class TestUrlExtraction:
    """Tests for URL extraction from storage values."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer for URL testing."""
        return StorageValueAnalyzer(
            extract_urls=True,
            extract_emails=False,
            detect_tokens=False,
            extract_identifiers=False,
        )

    def test_extract_simple_url(self, analyzer):
        """Test extraction of a simple HTTP URL."""
        value = '{"redirect_url": "https://example.com/page"}'
        result = analyzer.analyze_value("test_key", value)
        assert len(result.urls) == 1
        assert result.urls[0].url == "https://example.com/page"

    def test_extract_url_encoded(self, analyzer):
        """Test extraction of URL-encoded URL.

        Note:  removed URL deduplication for forensic completeness.
        The same URL may appear multiple times with different contexts
        (e.g., found via JSON parsing and regex).
        """
        value = '{"url": "https%3A%2F%2Fexample.com%2Fpath%3Fq%3Dtest"}'
        result = analyzer.analyze_value("test_key", value)
        # URL is found by both JSON field detection and regex scanning
        assert len(result.urls) >= 1
        assert any(u.url == "https://example.com/path?q=test" for u in result.urls)

    def test_extract_multiple_urls(self, analyzer):
        """Test extraction of multiple URLs from JSON."""
        value = json.dumps({
            "homepage": "https://site1.com",
            "callback": "https://site2.com/callback",
            "other": "not a url"
        })
        result = analyzer.analyze_value("test_key", value)
        assert len(result.urls) == 2
        urls = {u.url for u in result.urls}
        assert "https://site1.com" in urls
        assert "https://site2.com/callback" in urls

    def test_extract_url_from_nested_json(self, analyzer):
        """Test URL extraction from nested JSON structures."""
        value = json.dumps({
            "data": {
                "settings": {
                    "endpoint": "https://api.example.com/v1"
                }
            }
        })
        result = analyzer.analyze_value("test_key", value)
        assert len(result.urls) == 1
        assert result.urls[0].url == "https://api.example.com/v1"

    def test_skip_data_urls(self, analyzer):
        """Test that data: URLs are skipped."""
        value = '{"image": "data:image/png;base64,abc123"}'
        result = analyzer.analyze_value("test_key", value)
        assert len(result.urls) == 0

    def test_skip_javascript_urls(self, analyzer):
        """Test that javascript: URLs are skipped."""
        value = '{"onclick": "javascript:void(0)"}'
        result = analyzer.analyze_value("test_key", value)
        assert len(result.urls) == 0


class TestEmailExtraction:
    """Tests for email extraction from storage values."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer for email testing."""
        return StorageValueAnalyzer(
            extract_urls=False,
            extract_emails=True,
            detect_tokens=False,
            extract_identifiers=False,
        )

    def test_extract_simple_email(self, analyzer):
        """Test extraction of a simple email address."""
        value = '{"user_email": "user@example.com"}'
        result = analyzer.analyze_value("test_key", value)
        assert len(result.emails) == 1
        assert result.emails[0].email == "user@example.com"
        assert result.emails[0].source_key == "test_key"

    def test_extract_email_from_plain_text(self, analyzer):
        """Test email extraction from non-JSON text."""
        value = "Contact us at support@company.com for help"
        result = analyzer.analyze_value("test_key", value)
        assert len(result.emails) == 1
        assert result.emails[0].email == "support@company.com"


class TestTokenDetection:
    """Tests for authentication token detection."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer for token testing."""
        return StorageValueAnalyzer(
            extract_urls=False,
            extract_emails=False,
            detect_tokens=True,
            extract_identifiers=False,
        )

    def test_detect_jwt_token(self, analyzer):
        """Test detection of a JWT token."""
        # Minimal JWT (header.payload.signature)
        header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"  # {"alg":"HS256","typ":"JWT"}
        payload = "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
        signature = "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        jwt = f"{header}.{payload}.{signature}"

        value = json.dumps({"access_token": jwt})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.tokens) == 1
        assert result.tokens[0].token_type == "jwt"
        assert result.tokens[0].subject == "1234567890"

    def test_detect_session_token(self, analyzer):
        """Test detection of session tokens in auth-related fields."""
        session_token = "abc123def456ghi789jkl012mno345pqr"  # 32 chars

        value = json.dumps({"sessionToken": session_token})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.tokens) == 1
        assert result.tokens[0].token_type == "session"

    def test_extract_jwt_claims(self, analyzer):
        """Test extraction of JWT claims (exp, iss, sub)."""
        import base64

        # Create JWT with specific claims
        header = base64.urlsafe_b64encode(
            b'{"alg":"HS256","typ":"JWT"}'
        ).decode().rstrip('=')

        payload = base64.urlsafe_b64encode(json.dumps({
            "sub": "user123",
            "iss": "https://auth.example.com",
            "exp": 1893456000,  # Far future
            "email": "user@example.com"
        }).encode()).decode().rstrip('=')

        jwt = f"{header}.{payload}.signature"

        value = json.dumps({"id_token": jwt})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.tokens) == 1
        token = result.tokens[0]
        assert token.subject == "user123"
        assert token.issuer == "https://auth.example.com"
        assert token.associated_email == "user@example.com"
        assert token.is_expired is False


class TestIdentifierExtraction:
    """Tests for identifier/tracking ID extraction."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer for identifier testing."""
        return StorageValueAnalyzer(
            extract_urls=False,
            extract_emails=False,
            detect_tokens=False,
            extract_identifiers=True,
        )

    def test_extract_user_id(self, analyzer):
        """Test extraction of user IDs."""
        value = json.dumps({"user_id": "usr_12345abcde"})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.identifiers) == 1
        assert result.identifiers[0].identifier_type == "user_id"
        assert result.identifiers[0].identifier_value == "usr_12345abcde"

    def test_extract_device_id(self, analyzer):
        """Test extraction of device IDs."""
        value = json.dumps({"device_id": "550e8400-e29b-41d4-a716-446655440000"})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.identifiers) == 1
        assert result.identifiers[0].identifier_type == "device_id"

    def test_extract_tracking_id(self, analyzer):
        """Test extraction of tracking IDs."""
        value = json.dumps({"tracking_id": "UA-12345678-1"})
        result = analyzer.analyze_value("test_key", value)

        assert len(result.identifiers) == 1
        assert result.identifiers[0].identifier_type == "tracking_id"


class TestAnalysisResult:
    """Tests for AnalysisResult aggregation."""

    def test_all_extractions_enabled(self):
        """Test analysis with all extraction types enabled."""
        analyzer = StorageValueAnalyzer()

        value = json.dumps({
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature",
            "user_email": "test@example.com",
            "profile_url": "https://example.com/user/123",
            "user_id": "usr_abc123",
        })

        result = analyzer.analyze_value("test_key", value)

        assert len(result.tokens) >= 1
        assert len(result.emails) >= 1
        assert len(result.urls) >= 1
        assert len(result.identifiers) >= 1

    def test_disabled_extractions(self):
        """Test that disabled extraction types return empty results."""
        analyzer = StorageValueAnalyzer(
            extract_urls=False,
            extract_emails=False,
            detect_tokens=False,
            extract_identifiers=False,
        )

        value = json.dumps({
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature",
            "user_email": "test@example.com",
            "profile_url": "https://example.com/user/123",
            "user_id": "usr_abc123"
        })

        result = analyzer.analyze_value("test_key", value)

        assert len(result.tokens) == 0
        assert len(result.emails) == 0
        assert len(result.urls) == 0
        assert len(result.identifiers) == 0


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.fixture
    def analyzer(self):
        """Create default analyzer."""
        return StorageValueAnalyzer()

    def test_empty_value(self, analyzer):
        """Test handling of empty value."""
        result = analyzer.analyze_value("test_key", "")
        assert result.urls == []
        assert result.emails == []
        assert result.tokens == []
        assert result.identifiers == []

    def test_none_value(self, analyzer):
        """Test handling of None value."""
        result = analyzer.analyze_value("test_key", None)
        assert result.urls == []
        assert result.emails == []
        assert result.tokens == []
        assert result.identifiers == []

    def test_invalid_json(self, analyzer):
        """Test handling of invalid JSON - falls back to regex."""
        value = "not valid json { broken"
        result = analyzer.analyze_value("test_key", value)
        # Should not raise, just return empty or regex matches
        assert isinstance(result, AnalysisResult)

    def test_binary_value(self, analyzer):
        """Test handling of binary-like values."""
        value = "\x00\x01\x02binary data"
        result = analyzer.analyze_value("test_key", value)
        # Should not raise
        assert isinstance(result, AnalysisResult)

    def test_very_long_value(self, analyzer):
        """Test handling of very long values."""
        # Create a long but valid JSON
        value = json.dumps({"data": "x" * 100000})
        result = analyzer.analyze_value("test_key", value)
        # Should complete without error
        assert isinstance(result, AnalysisResult)

    def test_deeply_nested_json(self, analyzer):
        """Test handling of deeply nested JSON.

        Note:  removed URL deduplication for forensic completeness.
        The URL may be found multiple times with different contexts.
        """
        nested = {"url": "https://example.com"}
        for _ in range(50):
            nested = {"nested": nested}

        value = json.dumps(nested)
        result = analyzer.analyze_value("test_key", value)
        # Should find the URL despite nesting (may find via JSON and regex)
        assert len(result.urls) >= 1
        assert any(u.url == "https://example.com" for u in result.urls)


class TestDataClasses:
    """Tests for data classes used in analysis results."""

    def test_extracted_url(self):
        """Test ExtractedUrl dataclass."""
        url = ExtractedUrl(url="https://example.com", source_key="test")
        assert url.url == "https://example.com"
        assert url.source_key == "test"

    def test_extracted_email(self):
        """Test ExtractedEmail dataclass."""
        email = ExtractedEmail(email="user@example.com", source_key="test")
        assert email.email == "user@example.com"
        assert email.source_key == "test"

    def test_extracted_token(self):
        """Test ExtractedToken dataclass with hash."""
        import hashlib
        token_value = "test_token_value"
        token_hash = hashlib.sha256(token_value.encode()).hexdigest()

        token = ExtractedToken(
            token_type="jwt",
            token_value=token_value,
            token_hash=token_hash,
            source_key="access_token",
        )

        assert token.token_type == "jwt"
        assert token.token_hash == token_hash
        assert len(token.token_hash) == 64  # SHA256 hex

    def test_extracted_identifier(self):
        """Test ExtractedIdentifier dataclass."""
        identifier = ExtractedIdentifier(
            identifier_type="user_id",
            identifier_name="uid",
            identifier_value="user123",
            source_key="test"
        )

        assert identifier.identifier_type == "user_id"
        assert identifier.identifier_name == "uid"
        assert identifier.identifier_value == "user123"

    def test_analysis_result_merge(self):
        """Test AnalysisResult merge operation."""
        result1 = AnalysisResult(
            urls=[ExtractedUrl(url="https://example1.com")],
            emails=[ExtractedEmail(email="a@test.com")],
        )
        result2 = AnalysisResult(
            urls=[ExtractedUrl(url="https://example2.com")],
            tokens=[ExtractedToken(token_type="jwt", token_value="x", token_hash="hash")],
        )

        result1.merge(result2)

        assert len(result1.urls) == 2
        assert len(result1.emails) == 1
        assert len(result1.tokens) == 1

    def test_analysis_result_is_empty(self):
        """Test AnalysisResult.is_empty() method."""
        empty_result = AnalysisResult()
        assert empty_result.is_empty() is True

        non_empty = AnalysisResult(urls=[ExtractedUrl(url="https://test.com")])
        assert non_empty.is_empty() is False
