"""
Compatibility boundary for legacy `core.database` exports.

This module centralizes backward-compatibility aliases that map historical API
names to canonical helper functions. New code should import canonical names
from `core.database` (or helper modules) instead of these aliases.

Deprecation policy:
- Aliases remain available through the public-release transition period.
- Removal target: first stable major release after `0.2.x` (planned ``).
"""

from __future__ import annotations

from .helpers.artifacts import (
    insert_bitcoins,
    get_bitcoins,
    insert_ethereums,
    get_ethereums,
    insert_ips,
    get_ips,
    insert_phones,
    get_phones,
)
from .helpers.autofill import insert_autofill_entries, get_autofill_entries
from .helpers.extensions import (
    insert_extension,
    insert_extensions,
    get_extensions,
    get_extension_stats,
    delete_extensions_by_run,
)
from .helpers.jump_lists import delete_jump_list_by_run
from .helpers.permissions import insert_permissions, get_permissions
from .helpers.storage import (
    insert_local_storage,
    insert_local_storages,
    insert_session_storage,
    insert_session_storages,
    insert_indexeddb_database,
    insert_indexeddb_entry,
    delete_indexeddb_entries_by_run,
)

# Artifacts aliases
insert_bitcoin_addresses = insert_bitcoins
get_bitcoin_addresses = get_bitcoins
insert_ethereum_addresses = insert_ethereums
get_ethereum_addresses = get_ethereums
insert_ip_addresses = insert_ips
get_ip_addresses = get_ips
insert_telephone_numbers = insert_phones
get_telephone_numbers = get_phones

# Autofill aliases
insert_autofill = insert_autofill_entries
get_autofill = get_autofill_entries

# Permissions aliases
insert_site_permissions = insert_permissions
get_site_permissions = get_permissions

# Jump lists alias
delete_jump_lists_by_run = delete_jump_list_by_run

# Extensions aliases
insert_browser_extension_row = insert_extension
insert_browser_extensions = insert_extensions
get_browser_extensions = get_extensions
get_browser_extension_stats = get_extension_stats
delete_browser_extensions_by_run = delete_extensions_by_run

# Storage aliases
insert_local_storage_row = insert_local_storage
insert_local_storage_rows = insert_local_storages
insert_session_storage_row = insert_session_storage
insert_session_storage_rows = insert_session_storages
insert_indexeddb_database_row = insert_indexeddb_database
insert_indexeddb_entry_row = insert_indexeddb_entry
delete_indexeddb_by_run = delete_indexeddb_entries_by_run

__all__ = [
    "insert_bitcoin_addresses",
    "get_bitcoin_addresses",
    "insert_ethereum_addresses",
    "get_ethereum_addresses",
    "insert_ip_addresses",
    "get_ip_addresses",
    "insert_telephone_numbers",
    "get_telephone_numbers",
    "insert_autofill",
    "get_autofill",
    "insert_site_permissions",
    "get_site_permissions",
    "delete_jump_lists_by_run",
    "insert_browser_extension_row",
    "insert_browser_extensions",
    "get_browser_extensions",
    "get_browser_extension_stats",
    "delete_browser_extensions_by_run",
    "insert_local_storage_row",
    "insert_local_storage_rows",
    "insert_session_storage_row",
    "insert_session_storage_rows",
    "insert_indexeddb_database_row",
    "insert_indexeddb_entry_row",
    "delete_indexeddb_by_run",
]

