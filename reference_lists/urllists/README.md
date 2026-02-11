# URL Reference Lists

This directory contains URL reference lists for matching against discovered URLs in browser forensics.

## Overview

- **Included:** `sample_demo.txt` — a demo/template list using RFC 2606 reserved domains
- **Purpose:** Users create their own domain-specific lists for each investigation

## Format

Each `.txt` file contains one domain, URL, or IP address per line:

```
# List Name - URL Reference List
# Comments start with #
# Blank lines are ignored

example.com
subdomain.example.com
192.168.1.1
admin.example.com/login
```

## Matching Modes

When using URL lists in the application, you can choose:

1. **Wildcard (default):** Pattern matching with `*` wildcards
   - `example` → matches URLs containing "example" (case-insensitive)
   - `example.com` → matches URLs containing "example.com"
   - `192.168.1.1` → matches exact IP address

2. **Regex:** Full regular expression support
   - `^https://example\.` → matches URLs starting with https://example.
   - `(admin|login)` → matches URLs containing "admin" or "login"

## Usage

1. **Preferences > URL Lists:** Install bundled lists or upload custom lists
2. **URLs Tab:** Click "Match Against URL Lists" to match discovered URLs
3. **Filter by Match:** Use the Match column filter to view matched URLs

## List Management

- **Install:** Preferences > URL Lists > Install Bundled Lists
- **Upload Custom:** Preferences > URL Lists > Upload Custom List
- **Remove:** Preferences > URL Lists > Select list > Remove

## Notes

- All matching is case-insensitive by default
- IP addresses are preserved in lists and matched exactly
- Subdomains are matched (e.g., "example.com" matches "www.example.com")
- Paths are included where relevant (e.g., admin URLs)

## Forensic Considerations

- Lists are read-only after installation (tamper-evident)
- Match results are stored in case database (`url_matches` table)
- List name and matched pattern are logged for audit trail
