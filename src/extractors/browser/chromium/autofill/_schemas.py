"""
Chromium Autofill schema variants across browser versions.

This module defines the various database schema variants used by different
Chromium versions for storing autofill address/profile data. The extractor
will try all variants and extract from whatever tables exist.

Schema Evolution Timeline:
- Chromium <100: autofill_profiles table with structured columns
- Chromium 100-130: contact_info_type_tokens, local_addresses_type_tokens
- Chromium 131+: addresses, address_type_tokens (unified schema)
- Edge: Additional edge_server_addresses_type_tokens for synced addresses

To add a new schema variant:
1. Add a tuple to the appropriate list below
2. Document the Chromium version range and source

References:
- Chromium source: components/autofill/core/browser/
- Field types: components/autofill/core/browser/field_types.h
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# =============================================================================
# Token-based Address Tables
# =============================================================================
# These tables use a normalized structure: (guid, type, value)
# where 'type' is an integer mapping to a field type (NAME_FIRST, ADDRESS_HOME_CITY, etc.)
#
# Format: (parent_table, token_table, browser_filter)
# - parent_table: The main table that tokens reference (for documentation)
# - token_table: The actual token table to query
# - browser_filter: None = all browsers, "edge" = Edge only, etc.

ADDRESS_TOKEN_TABLES: List[Tuple[str, str, Optional[str]]] = [
    # Modern Chromium 131+ unified schema
    ("addresses", "address_type_tokens", None),

    # Chromium 100-130 contact info schema
    ("contact_info", "contact_info_type_tokens", None),

    # Chromium 100-130 local addresses schema
    ("local_addresses", "local_addresses_type_tokens", None),

    # Edge-specific synced addresses (Microsoft account sync)
    ("edge_server_addresses", "edge_server_addresses_type_tokens", "edge"),
]


# =============================================================================
# Legacy Profile Tables (pre-token era)
# =============================================================================
# Older Chromium versions used tables with explicit columns for each field type.
# These are still present in newer versions but may be empty if user only uses
# the modern address picker.
#
# Format: (table_name, browser_filter)

LEGACY_PROFILE_TABLES: List[Tuple[str, Optional[str]]] = [
    # Standard autofill profiles - all Chromium versions
    ("autofill_profiles", None),

    # Server-synced addresses (older sync schema)
    ("server_addresses", None),
]


# =============================================================================
# Token Type Mappings
# =============================================================================
# Maps integer type codes to human-readable field names.
# Source: components/autofill/core/browser/field_types.h
#
# Note: This enum has grown over Chromium versions. Unknown types are handled
# gracefully by returning "UNKNOWN_{type_code}".

TOKEN_TYPES: Dict[int, str] = {
    # Unknown/empty
    0: "UNKNOWN_TYPE",

    # Name fields (1-7)
    1: "NAME_FULL",
    2: "NAME_FIRST",
    3: "NAME_MIDDLE",
    4: "NAME_LAST",
    5: "NAME_MIDDLE_INITIAL",
    6: "NAME_PREFIX",
    7: "NAME_SUFFIX",

    # Email (9)
    9: "EMAIL_ADDRESS",

    # Phone fields (10-14)
    10: "PHONE_HOME_NUMBER",
    11: "PHONE_HOME_CITY_CODE",
    12: "PHONE_HOME_COUNTRY_CODE",
    13: "PHONE_HOME_CITY_AND_NUMBER",
    14: "PHONE_HOME_WHOLE_NUMBER",

    # Address fields (30-37, 60-62)
    30: "ADDRESS_HOME_LINE1",
    31: "ADDRESS_HOME_LINE2",
    32: "ADDRESS_HOME_LINE3",
    33: "ADDRESS_HOME_APT_NUM",
    34: "ADDRESS_HOME_CITY",
    35: "ADDRESS_HOME_STATE",
    36: "ADDRESS_HOME_ZIP",
    37: "ADDRESS_HOME_COUNTRY",
    60: "ADDRESS_HOME_STREET_ADDRESS",
    61: "ADDRESS_HOME_SORTING_CODE",
    62: "ADDRESS_HOME_DEPENDENT_LOCALITY",

    # Additional address fields (Chromium 90+)
    63: "ADDRESS_HOME_SUBPREMISE",
    64: "ADDRESS_HOME_PREMISE_NAME",
    65: "ADDRESS_HOME_DEPENDENT_STREET_NAME",
    66: "ADDRESS_HOME_STREET_NAME",
    67: "ADDRESS_HOME_HOUSE_NUMBER",
    68: "ADDRESS_HOME_FLOOR",

    # Company (77)
    77: "COMPANY_NAME",

    # Credit card fields (51-59) - for reference, stored in credit_cards table
    51: "CREDIT_CARD_NAME_FULL",
    52: "CREDIT_CARD_NUMBER",
    53: "CREDIT_CARD_EXP_MONTH",
    54: "CREDIT_CARD_EXP_2_DIGIT_YEAR",
    55: "CREDIT_CARD_EXP_4_DIGIT_YEAR",
    56: "CREDIT_CARD_EXP_DATE_2_DIGIT_YEAR",
    57: "CREDIT_CARD_EXP_DATE_4_DIGIT_YEAR",
    58: "CREDIT_CARD_TYPE",
    59: "CREDIT_CARD_VERIFICATION_CODE",

    # Birthdate fields (Chromium 100+)
    78: "BIRTHDATE_DAY",
    79: "BIRTHDATE_MONTH",
    80: "BIRTHDATE_4_DIGIT_YEAR",

    # IBAN (Chromium 110+)
    81: "IBAN_VALUE",
}


def get_token_type_name(token_type: int) -> str:
    """
    Convert Chromium token type code to human-readable name.

    Args:
        token_type: Integer type code from the token table

    Returns:
        Human-readable field name, or "UNKNOWN_{code}" for unmapped types
    """
    return TOKEN_TYPES.get(token_type, f"UNKNOWN_{token_type}")


# =============================================================================
# Autofill Form Data Tables
# =============================================================================
# The main autofill table stores name-value pairs from web forms.
# This schema has been stable across Chromium versions.

AUTOFILL_TABLES: List[Tuple[str, Optional[str]]] = [
    # Standard autofill name-value pairs
    ("autofill", None),

    # Edge-specific autofill field values (Edge 90+)
    ("autofill_edge_field_values", "edge"),
]


# =============================================================================
# Credentials Tables
# =============================================================================
# Login Data database tables for saved passwords.

CREDENTIALS_TABLES: List[Tuple[str, Optional[str]]] = [
    # Main logins table - all Chromium versions
    ("logins", None),

    # Insecure/compromised credentials metadata (Chromium 85+)
    ("insecure_credentials", None),

    # Breached password indicators
    ("breached", None),

    # Password notes (Chromium 100+)
    ("password_notes", None),
]


# =============================================================================
# Search Engine Tables
# =============================================================================
# Web Data database tables for search engine keywords.

SEARCH_ENGINE_TABLES: List[Tuple[str, Optional[str]]] = [
    # Keywords/search engines - all Chromium versions
    ("keywords", None),
]


# =============================================================================
# Credit Card Tables
# =============================================================================
# Web Data database tables for saved payment methods.

CREDIT_CARD_TABLES: List[Tuple[str, Optional[str]]] = [
    # Local credit cards
    ("credit_cards", None),

    # Server-synced credit cards
    ("server_card_metadata", None),
    ("masked_credit_cards", None),
]


# =============================================================================
# Block List Tables
# =============================================================================
# Tables for sites where autofill is disabled.

BLOCK_LIST_TABLES: List[Tuple[str, Optional[str]]] = [
    # Never-save list for passwords
    ("insecure_credentials", None),  # Has phished/leaked flags

    # Autofill block list (Chromium 95+)
    # Note: May not exist in older versions
]


# =============================================================================
# Known Tables for Schema Discovery
# =============================================================================
# These sets define tables we know about. Any tables NOT in these sets
# will be flagged as unknown for investigator review.

KNOWN_WEB_DATA_TABLES: set[str] = {
    # SQLite internal
    "sqlite_sequence",
    "sqlite_stat1",

    # Schema versioning
    "meta",

    # Autofill core tables
    "autofill",
    "autofill_profiles",
    "autofill_profile_names",
    "autofill_profile_emails",
    "autofill_profile_phones",
    "autofill_profile_addresses",
    "autofill_profiles_trash",
    "autofill_sync_metadata",
    "autofill_model_type_state",

    # Edge-specific autofill
    "autofill_edge_field_values",
    "autofill_edge_block_list",

    # Modern Chromium addresses (131+)
    "addresses",
    "address_type_tokens",

    # Chromium 100-130 contact info
    "contact_info",
    "contact_info_type_tokens",
    "local_addresses",
    "local_addresses_type_tokens",

    # Edge server addresses
    "edge_server_addresses",
    "edge_server_addresses_type_tokens",

    # Server-synced profiles
    "server_addresses",
    "server_address_metadata",

    # Credit cards
    "credit_cards",
    "server_card_metadata",
    "masked_credit_cards",
    "server_card_cloud_token_data",
    "local_ibans",
    "masked_ibans",
    "masked_ibans_metadata",
    "masked_bank_accounts",
    "masked_bank_accounts_metadata",
    "payment_instruments",
    "payment_instrument_supported_rails",
    "payment_instrument_creation_options",
    "masked_credit_cards_benefits",
    "generic_payment_instruments",
    "local_stored_cvc",
    "server_stored_cvc",
    "payments_customer_data",
    "payments_upi_vpa",
    "offer_data",
    "offer_eligible_instrument",
    "offer_merchant_domain",

    # Search engines
    "keywords",
    "keyword_metadata",
    "builtin_keyword_metadata",
    "keyword_mode_params",

    # Token service
    "token_service",

    # Web apps
    "web_app_manifest_section",
    "web_apks",
    "web_intents",
    "web_intents_defaults",

    # Plus codes (location)
    "plus_address",
    "plus_address_sync_model_type_state",
    "plus_address_sync_entity_metadata",

    # Edge-specific autofill
    "autofill_edge_custom_data",
    "autofill_edge_extended",
    "autofill_edge_field_client_info",
    "autofill_edge_fieldid_clusterid_mapping",
    "autofill_edge_form_field_identifier",
    "autofill_edge_levo_value_mapping",
    "autofill_profile_edge_extended",

    # Credit cards - additional tables
    "credit_cards_edge_extended",
    "edge_tokenized_credit_cards",
    "unmasked_credit_cards",
    "virtual_card_usage_data",
    "credit_card_tags_v2",  # Credit card tag metadata

    # Payment method tables
    "payment_method_manifest",
    "secure_payment_confirmation_instrument",

    # Profile extensions
    "autofill_profile_birthdates",
    "autofill_travels",
}

KNOWN_LOGIN_DATA_TABLES: set[str] = {
    # SQLite internal
    "sqlite_sequence",
    "sqlite_stat1",

    # Schema versioning
    "meta",

    # Core credentials
    "logins",
    "ie7_logins",  # Legacy IE import
    "stats",

    # Security metadata
    "insecure_credentials",
    "breached",

    # Password features
    "password_notes",
    "field_info",
    "compromised_credentials",
    "password_issues",

    # Sync metadata
    "sync_entities_metadata",
    "sync_model_metadata",
    "sync_invalidations",
    "password_store_migration_data",

    # Edge-specific logins
    "logins_edge_extended",
}

# Pattern for discovering autofill-related unknown tables
AUTOFILL_TABLE_PATTERNS: list[str] = [
    "autofill",
    "address",
    "contact",
    "credit",
    "card",
    "payment",
    "login",
    "password",
    "credential",
]


# =============================================================================
# Known Columns for Schema Discovery
# =============================================================================
# These sets define columns we parse from each table. Unknown columns will be
# flagged for investigator review.

KNOWN_AUTOFILL_COLUMNS: set[str] = {
    # Core fields - stable across all Chromium versions
    "name",
    "value",
    "value_lower",  # Lowercase indexed value (Chromium 90+)
    "count",
    "date_created",
    "date_last_used",
}

KNOWN_AUTOFILL_PROFILES_COLUMNS: set[str] = {
    # Primary key
    "guid",
    # Name fields
    "full_name",
    "company_name",
    # Address fields
    "street_address",
    "city",
    "state",
    "zipcode",
    "country_code",
    "dependent_locality",  # Sub-locality field (some countries)
    "sorting_code",        # Postal sorting code (some countries)
    # Usage tracking
    "use_count",
    "use_date",
    "date_modified",
    "date_synced",  # Sync timestamp
    # Label/nickname (Chromium 90+)
    "label",
    # Language (for i18n)
    "language_code",
    # Legacy fields (older Chromium)
    "first_name",
    "middle_name",
    "last_name",
    "email",
    "phone_number",
    # Profile origin
    "origin",
    # Validity bitfield
    "validity_bitfield",
    "is_client_validity_states_updated",  # Client validity flag
    # Sync/preference
    "preferred",  # User-preferred address flag
}

KNOWN_CREDIT_CARDS_COLUMNS: set[str] = {
    # Primary key
    "guid",
    # Card fields
    "name_on_card",
    "expiration_month",
    "expiration_year",
    "card_number_encrypted",
    "nickname",
    # Usage tracking
    "use_count",
    "use_date",
    "date_modified",
    # Billing address link
    "billing_address_id",
    # Card metadata (Chromium 90+)
    "origin",
}

KNOWN_LOCAL_IBANS_COLUMNS: set[str] = {
    # Identity
    "guid",
    "instrument_id",
    # Value
    "value",
    "value_encrypted",
    # Display metadata
    "nickname",
    "prefix",
    "suffix",
    "length",
    # Usage tracking
    "use_count",
    "use_date",
    "date_modified",
}

KNOWN_MASKED_IBANS_COLUMNS: set[str] = {
    # Identity
    "instrument_id",
    # Display metadata
    "nickname",
    "prefix",
    "suffix",
    "length",
    # Usage tracking
    "use_count",
    "use_date",
    "date_modified",
}

KNOWN_LOGINS_COLUMNS: set[str] = {
    # URL/realm identification
    "origin_url",
    "action_url",
    "signon_realm",
    # Credentials
    "username_element",
    "username_value",
    "password_element",
    "password_value",
    # Timestamps
    "date_created",
    "date_last_used",
    "date_password_modified",
    # Usage tracking
    "times_used",
    # Metadata
    "blacklisted_by_user",
    "scheme",
    "password_type",
    "possible_username_pairs",
    "submit_element",
    "form_data",
    "display_name",
    "icon_url",
    "federation_url",
    "skip_zero_click",
    "generation_upload_status",
    "moving_blocked_for",
    # Primary key (some versions)
    "id",
}

KNOWN_KEYWORDS_COLUMNS: set[str] = {
    # Primary key
    "id",
    # Basic info
    "short_name",
    "keyword",
    "url",
    # Favicon
    "favicon_url",
    # Suggestions
    "suggest_url",
    # Prepopulated engines
    "prepopulate_id",
    # Usage tracking
    "usage_count",
    # Timestamps
    "date_created",
    "last_modified",
    "last_visited",
    # Status
    "is_active",
    # Additional URL templates
    "new_tab_url",
    "image_url",
    "search_url_post_params",
    "suggest_url_post_params",       # Older alias
    "suggestions_url_post_params",
    "image_url_post_params",
    # Sync
    "sync_guid",
    # Search hints
    "safe_for_autoreplace",
    "originating_url",
    "input_encodings",
    "alternate_urls",
    "starter_pack_id",
    "enforced_by_policy",
    "featured_by_policy",
    "created_by_policy",
    "created_from_play_api",
}

KNOWN_TOKEN_TABLE_COLUMNS: set[str] = {
    # Standard token table columns (all variants)
    "guid",
    "type",
    "value",
}

# =============================================================================
# Edge-Specific Known Columns
# =============================================================================

KNOWN_EDGE_AUTOFILL_FIELD_VALUES_COLUMNS: set[str] = {
    "field_id",
    "value",
    "count",
    "date_created",
    "date_last_used",
}

KNOWN_EDGE_FIELD_CLIENT_INFO_COLUMNS: set[str] = {
    "field_id",
    "label",
    "domain_value",
}

KNOWN_EDGE_AUTOFILL_BLOCK_LIST_COLUMNS: set[str] = {
    "guid",
    "block_value",
    "block_value_type",
    "attribute_flag",
    "meta_data",
    "device_model",
    "date_created",
    "date_modified",
}

# =============================================================================
# Edge Block List Enum Values
# =============================================================================
# Track known values for block_value_type and attribute_flag fields.

EDGE_BLOCK_VALUE_TYPES: Dict[int, str] = {
    0: "DOMAIN",           # Domain-level block
    1: "URL",              # Specific URL block
    2: "FORM_FIELD",       # Specific form field
}

EDGE_ATTRIBUTE_FLAGS: Dict[int, str] = {
    0: "NONE",             # No special flags
    1: "PASSWORDS",        # Block password autofill
    2: "ADDRESSES",        # Block address autofill
    3: "CREDIT_CARDS",     # Block credit card autofill
    4: "ALL",              # Block all autofill
}


def get_block_value_type_name(value_type: int) -> str:
    """Get human-readable name for Edge block_value_type."""
    return EDGE_BLOCK_VALUE_TYPES.get(value_type, f"UNKNOWN_{value_type}")


def get_attribute_flag_name(flag: int) -> str:
    """Get human-readable name for Edge attribute_flag."""
    return EDGE_ATTRIBUTE_FLAGS.get(flag, f"UNKNOWN_{flag}")
