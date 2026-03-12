# DPAPI Decrypt (Tab)

Windows DPAPI credential decryption results viewer.

## Purpose
- Displays the results of offline DPAPI (Data Protection API) decryption performed by the SystemDpapiDecryptExtractor.
- Allows investigators to review Windows user accounts, DPAPI master keys, Chromium application-bound encryption keys, and decryption outcomes.
- Provides a summary of decryption success/failure across credential types.

## When to use
- After running the DPAPI Decrypt extractor on Windows evidence containing Chromium-based browsers.
- To review which browser credentials (passwords, cookies, credit cards) were successfully decrypted.
- To verify decryption coverage and identify gaps (locked master keys, missing hives).

## Prerequisites
- The **SystemDpapiDecryptExtractor** must be run first (via the Extraction tab).
- Windows registry hives (SYSTEM, SAM, SECURITY) must be present in the evidence.
- Chromium browser profiles with v10-encrypted credentials must exist.

## Subtabs

### Users & Keys
- **Top panel:** Windows user accounts table — shows username, SID, NTLM hash status, and master key count.
- **Bottom panel:** DPAPI master keys table — filtered by the selected user. Shows key GUID, status (locked/unlocked), unlock method, and file hash.
- Select a user in the top table to filter master keys in the bottom table.

### Chromium Keys
- Displays application-bound Chromium AES encryption keys extracted from Local State JSON files.
- Shows browser, profile, key status (pending/decrypted/failed), and the associated browser path.

### Decrypt Summary
- **Stat cards:** Total decryption attempts, successful decryptions, failures, and items with no available key.
- **Per-table breakdown:** Separate counts for credentials (passwords), cookies, and credit cards.
- Provides at-a-glance decryption coverage assessment.

## Data sources
- Evidence database tables: `windows_users`, `dpapi_master_keys`, `chromium_app_keys`, `decrypt_audit`.
- Updated tables: `credentials`, `cookies`, `credit_cards` (with `decrypted_value`, `decrypt_method`, `decrypt_status` fields).

## Key controls
- Lazy loading — data loads on first tab visit or after extraction completes.
- Refresh on mark stale — automatically reloads when new extraction data is available.

## Notes
- All decryption attempts are logged in the `decrypt_audit` table for forensic accountability.
- Decryption supports multiple unlock methods: examiner-provided password, NTLM hash, empty password fallback, and DPAPI_SYSTEM machine/user keys.
- Multi-user evidence is handled with fallback key matching across all available AES keys.
