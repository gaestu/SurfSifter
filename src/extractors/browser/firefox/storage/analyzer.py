"""
Firefox Storage Value Analyzer

Analyzes LocalStorage and IndexedDB values to extract forensically valuable artifacts:
- URLs with timestamps
- Email addresses
- Authentication tokens (JWT, OAuth, session)
- User identifiers (tracking IDs, device IDs)

This module provides pure parsing functions that can be used during ingestion
to extract artifacts from raw storage values.

Removed deduplication for URLs and emails (forensic completeness)
        Tokens still deduplicated by hash (identical tokens have no additional forensic value)
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.storage.analyzer")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ExtractedUrl:
    """A URL extracted from storage value."""
    url: str
    first_seen_utc: Optional[str] = None
    last_seen_utc: Optional[str] = None
    context: str = ""  # JSON path or key name
    source_key: str = ""  # Original storage key


@dataclass
class ExtractedEmail:
    """An email extracted from storage value."""
    email: str
    context: str = ""
    source_key: str = ""


@dataclass
class ExtractedToken:
    """An authentication token extracted from storage value."""
    token_type: str  # jwt, microsoft_oauth, oauth, session, api_key, unknown
    token_value: str
    token_hash: str  # SHA256 for deduplication

    # JWT claims (if parsed)
    issuer: Optional[str] = None
    subject: Optional[str] = None
    audience: Optional[str] = None
    issued_at_utc: Optional[str] = None
    expires_at_utc: Optional[str] = None

    # Associated data
    associated_email: Optional[str] = None
    associated_user_id: Optional[str] = None

    # Risk assessment
    risk_level: str = "medium"  # high, medium, low
    is_expired: bool = False

    # Context
    source_key: str = ""
    notes: str = ""


@dataclass
class ExtractedIdentifier:
    """A user/device/tracking identifier extracted from storage value."""
    identifier_type: str  # user_id, device_id, tracking_id, visitor_id, session_id, email
    identifier_name: str  # The field name (e.g., "muid", "web_id")
    identifier_value: str
    first_seen_utc: Optional[str] = None
    last_seen_utc: Optional[str] = None
    source_key: str = ""


@dataclass
class AnalysisResult:
    """Complete analysis result for a storage value."""
    urls: List[ExtractedUrl] = field(default_factory=list)
    emails: List[ExtractedEmail] = field(default_factory=list)
    tokens: List[ExtractedToken] = field(default_factory=list)
    identifiers: List[ExtractedIdentifier] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.urls or self.emails or self.tokens or self.identifiers)

    def merge(self, other: "AnalysisResult") -> None:
        """Merge another result into this one."""
        self.urls.extend(other.urls)
        self.emails.extend(other.emails)
        self.tokens.extend(other.tokens)
        self.identifiers.extend(other.identifiers)


# =============================================================================
# Regex Patterns
# =============================================================================

# URL patterns (handles URL-encoded too)
URL_PATTERN = re.compile(
    r'https?://[^\s"\'<>\]})\\,]+|'  # Standard URLs
    r'https?%3A%2F%2F[^\s"\'<>\]})\\,]+',  # URL-encoded
    re.IGNORECASE
)

# Email pattern
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# JWT pattern (base64url encoded header starting with {"alg":...)
JWT_PATTERN = re.compile(r'eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*')

# Microsoft OAuth tokens (EwA/EwB prefix)
MS_OAUTH_PATTERN = re.compile(r'Ew[AB][A-Za-z0-9+/=_-]{50,}')

# Generic long token pattern (for API keys, session tokens)
LONG_TOKEN_PATTERN = re.compile(r'[A-Za-z0-9_-]{64,}')

# UUID pattern
UUID_PATTERN = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
    re.IGNORECASE
)


# =============================================================================
# Timestamp Fields to Look For
# =============================================================================

TIMESTAMP_FIELDS = {
    'time', 'timestamp', 'date', 'created', 'modified',
    'previousRequestTime', 'lastActiveTime', 'lastUpdated',
    'visitTime', 'accessTime', 'requestTime', 'expiration',
    'exp', 'iat', 'nbf', 'createdAt', 'updatedAt', 'lastUsed',
    'created_at', 'updated_at', 'last_used', 'expiresAt', 'issuedAt',
    'expires_at', 'issued_at', 'lastAccess', 'last_access',
}

URL_FIELDS = {
    'url', 'href', 'link', 'redirect', 'referrer', 'pageUrl', 'uri',
    'redirectUrl', 'returnUrl', 'callbackUrl', 'targetUrl', 'sourceUrl',
    'imageUrl', 'videoUrl', 'contentUrl', 'baseUrl', 'requestUrl',
}

EMAIL_FIELDS = {
    'email', 'mail', 'upn', 'userPrincipalName', 'user_email',
    'emailAddress', 'email_address', 'userEmail', 'account',
    'preferred_username', 'unique_name',
}

# Precomputed lowercase field sets for fast membership checks
URL_FIELDS_LOWER = {field.lower() for field in URL_FIELDS}
EMAIL_FIELDS_LOWER = {field.lower() for field in EMAIL_FIELDS}

IDENTIFIER_FIELDS = {
    # User IDs
    'user_id': 'user_id', 'userId': 'user_id', 'uid': 'user_id',
    'sub': 'user_id', 'subject': 'user_id', 'oid': 'user_id',
    # Device IDs
    'device_id': 'device_id', 'deviceId': 'device_id', 'did': 'device_id',
    'muid': 'device_id', 'mid': 'device_id', 'machine_id': 'device_id',
    # Tracking IDs
    'tracking_id': 'tracking_id', 'trackingId': 'tracking_id',
    '_ga': 'tracking_id', '_gid': 'tracking_id', 'ga_id': 'tracking_id',
    'web_id': 'tracking_id', 'webId': 'tracking_id',
    'visitor_id': 'visitor_id', 'visitorId': 'visitor_id',
    'client_id': 'tracking_id', 'clientId': 'tracking_id',
    # Session IDs
    'session_id': 'session_id', 'sessionId': 'session_id', 'sid': 'session_id',
}


# =============================================================================
# Storage Value Analyzer
# =============================================================================

class StorageValueAnalyzer:
    """Analyzes storage values to extract forensic artifacts."""

    def __init__(
        self,
        extract_urls: bool = True,
        extract_emails: bool = True,
        detect_tokens: bool = True,
        extract_identifiers: bool = True,
    ):
        self.extract_urls = extract_urls
        self.extract_emails = extract_emails
        self.detect_tokens = detect_tokens
        self.extract_identifiers = extract_identifiers

    def analyze_value(
        self,
        key: str,
        value: str,
        origin: str = "",
    ) -> AnalysisResult:
        """Analyze a storage key-value pair for forensic artifacts.

        Args:
            key: The storage key name
            value: The storage value (string)
            origin: The site origin (e.g., https://example.com)

        Returns:
            AnalysisResult with extracted artifacts
        """
        result = AnalysisResult()

        if not value:
            return result

        # Try to parse as JSON first for structured extraction
        json_data = None
        try:
            json_data = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass

        if json_data is not None:
            # Structured JSON analysis
            json_result = self._analyze_json(key, json_data, origin)
            result.merge(json_result)

        # Always do regex-based extraction for things JSON parsing might miss
        regex_result = self._analyze_with_regex(key, value, origin)
        result.merge(regex_result)

        # Deduplicate results
        self._deduplicate(result)

        return result

    def _analyze_json(
        self,
        key: str,
        data: Any,
        origin: str,
        path: str = "",
    ) -> AnalysisResult:
        """Recursively analyze JSON data for artifacts."""
        result = AnalysisResult()

        if isinstance(data, dict):
            # First pass: find timestamp in this object
            timestamp = self._find_timestamp_in_dict(data)

            # Second pass: extract artifacts
            for field_name, field_value in data.items():
                current_path = f"{path}.{field_name}" if path else field_name

                if isinstance(field_value, str):
                    field_name_lower = field_name.lower()
                    # Check for URLs
                    if self.extract_urls and field_name_lower in URL_FIELDS_LOWER:
                        url = self._clean_url(field_value)
                        if url:
                            result.urls.append(ExtractedUrl(
                                url=url,
                                first_seen_utc=timestamp,
                                last_seen_utc=timestamp,
                                context=f"json:{current_path}",
                                source_key=key,
                            ))

                    # Check for emails
                    if self.extract_emails and field_name_lower in EMAIL_FIELDS_LOWER:
                        if EMAIL_PATTERN.match(field_value):
                            result.emails.append(ExtractedEmail(
                                email=field_value.lower(),
                                context=f"json:{current_path}",
                                source_key=key,
                            ))

                    # Check for identifiers
                    if self.extract_identifiers and field_name in IDENTIFIER_FIELDS:
                        id_type = IDENTIFIER_FIELDS[field_name]
                        result.identifiers.append(ExtractedIdentifier(
                            identifier_type=id_type,
                            identifier_name=field_name,
                            identifier_value=field_value,
                            first_seen_utc=timestamp,
                            last_seen_utc=timestamp,
                            source_key=key,
                        ))

                    # Check for tokens
                    if self.detect_tokens:
                        token = self._detect_token(field_name, field_value, data)
                        if token:
                            token.source_key = key
                            result.tokens.append(token)

                elif isinstance(field_value, (dict, list)):
                    # Recurse into nested structures
                    nested = self._analyze_json(key, field_value, origin, current_path)
                    result.merge(nested)

        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{path}[{i}]"
                if isinstance(item, (dict, list)):
                    nested = self._analyze_json(key, item, origin, current_path)
                    result.merge(nested)

        return result

    def _analyze_with_regex(
        self,
        key: str,
        value: str,
        origin: str,
    ) -> AnalysisResult:
        """Extract artifacts using regex patterns."""
        result = AnalysisResult()

        # Extract URLs
        if self.extract_urls:
            # Fast pre-checks to avoid expensive regex scans on long strings
            if (
                "http" in value
                or "HTTP" in value
                or "%3A%2F%2F" in value
                or "%3a%2f%2f" in value
            ):
                for match in URL_PATTERN.finditer(value):
                    url = self._clean_url(match.group())
                    if url:
                        # Try to find a nearby timestamp
                        timestamp = self._find_nearby_timestamp(value, match.start())
                        result.urls.append(ExtractedUrl(
                            url=url,
                            first_seen_utc=timestamp,
                            last_seen_utc=timestamp,
                            context=f"regex:key={key}",
                            source_key=key,
                        ))

        # Extract emails
        if self.extract_emails and "@" in value:
            for match in EMAIL_PATTERN.finditer(value):
                email = match.group().lower()
                # Filter out common false positives
                if not self._is_false_positive_email(email):
                    result.emails.append(ExtractedEmail(
                        email=email,
                        context=f"regex:key={key}",
                        source_key=key,
                    ))

        # Detect tokens based on key name patterns
        if self.detect_tokens:
            key_lower = key.lower()
            is_token_key = any(kw in key_lower for kw in [
                'token', 'auth', 'session', 'jwt', 'oauth', 'access', 'refresh',
                'credential', 'secret', 'apikey', 'api_key', 'bearer',
            ])

            if is_token_key:
                token = self._detect_token(key, value, {})
                if token:
                    token.source_key = key
                    result.tokens.append(token)

        return result

    def _find_timestamp_in_dict(self, data: Dict[str, Any]) -> Optional[str]:
        """Find and parse a timestamp from a dictionary."""
        for field in TIMESTAMP_FIELDS:
            if field in data:
                ts = self._parse_timestamp(data[field])
                if ts:
                    return ts
            # Also check lowercase version
            if field.lower() in {k.lower() for k in data.keys()}:
                for k, v in data.items():
                    if k.lower() == field.lower():
                        ts = self._parse_timestamp(v)
                        if ts:
                            return ts
        return None

    def _find_nearby_timestamp(self, value: str, position: int) -> Optional[str]:
        """Try to find a timestamp near the given position in a string."""
        # Look for Unix milliseconds (13 digits) or seconds (10 digits)
        # within ~200 chars of the position
        start = max(0, position - 200)
        end = min(len(value), position + 200)
        window = value[start:end]

        # Look for numbers that could be timestamps
        for match in re.finditer(r'\b(\d{13})\b', window):
            ts = self._parse_timestamp(int(match.group(1)))
            if ts:
                return ts

        for match in re.finditer(r'\b(\d{10})\b', window):
            ts = self._parse_timestamp(int(match.group(1)))
            if ts:
                return ts

        # Look for ISO 8601 dates
        iso_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
        match = re.search(iso_pattern, window)
        if match:
            return self._parse_timestamp(match.group())

        return None

    def _parse_timestamp(self, value: Any) -> Optional[str]:
        """Parse various timestamp formats to ISO 8601 UTC."""
        if value is None:
            return None

        try:
            if isinstance(value, str):
                # ISO 8601 format
                if 'T' in value or '-' in value:
                    # Try parsing as ISO
                    for fmt in [
                        '%Y-%m-%dT%H:%M:%S.%fZ',
                        '%Y-%m-%dT%H:%M:%SZ',
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%d',
                    ]:
                        try:
                            dt = datetime.strptime(value.replace('+00:00', 'Z').rstrip('Z'), fmt.rstrip('Z'))
                            return dt.replace(tzinfo=timezone.utc).isoformat()
                        except ValueError:
                            continue

                # Try as numeric string
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        return None

            if isinstance(value, (int, float)):
                # Determine if milliseconds or seconds
                if value > 1e12:  # Milliseconds
                    dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                elif value > 1e9:  # Seconds
                    dt = datetime.fromtimestamp(value, tz=timezone.utc)
                else:
                    return None

                # Sanity check: should be between 2000 and 2050
                if 2000 <= dt.year <= 2050:
                    return dt.isoformat()

        except (OSError, OverflowError, ValueError):
            pass

        return None

    def _clean_url(self, url: str) -> Optional[str]:
        """Clean and validate a URL."""
        if not url:
            return None

        # URL decode if needed
        if '%3A%2F%2F' in url or '%3a%2f%2f' in url:
            url = urllib.parse.unquote(url)

        # Basic validation
        if not url.startswith(('http://', 'https://')):
            return None

        # Remove trailing garbage
        url = url.rstrip('.,;:!?)]\'"')

        # Check for valid structure
        try:
            parsed = urllib.parse.urlparse(url)
            if not parsed.netloc:
                return None
            # Reconstruct clean URL
            return urllib.parse.urlunparse(parsed)
        except Exception:
            return None

    def _is_false_positive_email(self, email: str) -> bool:
        """Check if an email is likely a false positive."""
        # Common false positives
        false_positives = {
            'example@example.com',
            'user@example.com',
            'test@test.com',
            'noreply@',
            '@example.',
            '@localhost',
        }

        email_lower = email.lower()
        for fp in false_positives:
            if fp in email_lower:
                return True

        # Too short domain
        parts = email.split('@')
        if len(parts) == 2 and len(parts[1]) < 4:
            return True

        return False

    def _detect_token(
        self,
        key: str,
        value: str,
        context_data: Dict[str, Any],
    ) -> Optional[ExtractedToken]:
        """Detect and parse authentication tokens."""
        if not value or len(value) < 20:
            return None

        token_type = None
        risk_level = "medium"
        issuer = None
        subject = None
        audience = None
        issued_at = None
        expires_at = None
        associated_email = None
        associated_user_id = None
        is_expired = False
        notes = ""

        # JWT detection
        jwt_match = JWT_PATTERN.search(value)
        if jwt_match:
            token_type = "jwt"
            risk_level = "high"
            jwt_token = jwt_match.group()

            # Try to decode JWT claims (without verification)
            claims = self._decode_jwt_claims(jwt_token)
            if claims:
                issuer = claims.get('iss')
                subject = claims.get('sub')
                audience = claims.get('aud')
                if isinstance(audience, list):
                    audience = ', '.join(audience)

                if 'iat' in claims:
                    issued_at = self._parse_timestamp(claims['iat'])
                if 'exp' in claims:
                    expires_at = self._parse_timestamp(claims['exp'])
                    # Check if expired
                    if expires_at:
                        try:
                            exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                            is_expired = exp_dt < datetime.now(timezone.utc)
                        except Exception:
                            pass

                # Extract email from common claims
                associated_email = (
                    claims.get('email') or
                    claims.get('upn') or
                    claims.get('preferred_username') or
                    claims.get('unique_name')
                )
                associated_user_id = claims.get('sub') or claims.get('oid')

        # Microsoft OAuth detection
        elif MS_OAUTH_PATTERN.search(value):
            token_type = "microsoft_oauth"
            risk_level = "high"
            notes = "Microsoft OAuth access/refresh token"

            # Try to get associated email from context
            if context_data:
                associated_email = (
                    context_data.get('email') or
                    context_data.get('upn') or
                    context_data.get('userPrincipalName')
                )

        # Key-based detection
        elif any(kw in key.lower() for kw in ['token', 'auth', 'session', 'bearer', 'access', 'refresh']):
            # Check if it looks like a token (long alphanumeric)
            if LONG_TOKEN_PATTERN.match(value) or len(value) > 32:
                if 'refresh' in key.lower():
                    token_type = "refresh_token"
                    risk_level = "high"
                elif 'access' in key.lower():
                    token_type = "access_token"
                    risk_level = "high"
                elif 'session' in key.lower():
                    token_type = "session"
                    risk_level = "medium"
                else:
                    token_type = "unknown"
                    risk_level = "low"

        if not token_type:
            return None

        # Generate hash for deduplication
        token_hash = hashlib.sha256(value.encode()).hexdigest()

        return ExtractedToken(
            token_type=token_type,
            token_value=value,
            token_hash=token_hash,
            issuer=issuer,
            subject=subject,
            audience=audience,
            issued_at_utc=issued_at,
            expires_at_utc=expires_at,
            associated_email=associated_email,
            associated_user_id=associated_user_id,
            risk_level=risk_level,
            is_expired=is_expired,
            notes=notes,
        )

    def _decode_jwt_claims(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode JWT payload without verification."""
        try:
            parts = token.split('.')
            if len(parts) < 2:
                return None

            # Decode payload (second part)
            payload = parts[1]
            # Add padding if needed
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding

            # Replace URL-safe chars
            payload = payload.replace('-', '+').replace('_', '/')

            decoded = base64.b64decode(payload)
            return json.loads(decoded)
        except Exception:
            return None

    def _deduplicate(self, result: AnalysisResult) -> None:
        """Remove duplicate tokens from result.

        Note: URLs, emails, and identifiers are NOT deduplicated as each
        occurrence may have forensic value (different contexts, timestamps,
        storage keys). Only tokens are deduplicated by hash since identical
        tokens provide no additional forensic information.
        """
        # Dedupe tokens by hash (tokens ARE deduplicated - identical tokens have same forensic value)
        seen_hashes = set()
        unique_tokens = []
        for token in result.tokens:
            if token.token_hash not in seen_hashes:
                seen_hashes.add(token.token_hash)
                unique_tokens.append(token)
        result.tokens = unique_tokens


# =============================================================================
# Convenience Functions
# =============================================================================

def analyze_storage_value(
    key: str,
    value: str,
    origin: str = "",
    extract_urls: bool = True,
    extract_emails: bool = True,
    detect_tokens: bool = True,
    extract_identifiers: bool = True,
) -> AnalysisResult:
    """Convenience function to analyze a single storage value.

    Args:
        key: Storage key name
        value: Storage value string
        origin: Site origin (e.g., https://example.com)
        extract_urls: Whether to extract URLs
        extract_emails: Whether to extract email addresses
        detect_tokens: Whether to detect auth tokens
        extract_identifiers: Whether to extract identifiers

    Returns:
        AnalysisResult with extracted artifacts
    """
    analyzer = StorageValueAnalyzer(
        extract_urls=extract_urls,
        extract_emails=extract_emails,
        detect_tokens=detect_tokens,
        extract_identifiers=extract_identifiers,
    )
    return analyzer.analyze_value(key, value, origin)


def batch_analyze_storage(
    records: List[Dict[str, Any]],
    key_field: str = "key",
    value_field: str = "value",
    origin_field: str = "origin",
    **analyzer_kwargs,
) -> Tuple[AnalysisResult, int]:
    """Analyze multiple storage records in batch.

    Args:
        records: List of storage record dicts
        key_field: Field name for storage key
        value_field: Field name for storage value
        origin_field: Field name for origin
        **analyzer_kwargs: Arguments passed to StorageValueAnalyzer

    Returns:
        Tuple of (combined AnalysisResult, number of records analyzed)
    """
    analyzer = StorageValueAnalyzer(**analyzer_kwargs)
    combined = AnalysisResult()
    count = 0

    for record in records:
        key = record.get(key_field, "")
        value = record.get(value_field, "")
        origin = record.get(origin_field, "")

        if value:
            result = analyzer.analyze_value(key, value, origin)
            combined.merge(result)
            count += 1

    # Final deduplication
    analyzer._deduplicate(combined)

    return combined, count
