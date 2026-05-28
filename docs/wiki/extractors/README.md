# Extractors

This section documents all extractors under `src/extractors/`.

## Browser Families
- Chromium (`chromium.md`) — 17 extractors for Chrome, Chromium, Edge, Brave, Opera, Vivaldi, and embedded CEF/CefSharp
- Firefox (`firefox.md`) — 14 extractors for Firefox, Firefox ESR, and Tor Browser
- Safari (`safari.md`) — 9 extractors for macOS Safari (experimental)
- IE Legacy (`ie-legacy.md`) — 12 extractors for Internet Explorer and Legacy Edge

## Other Extractor Groups
- Carvers (`carvers.md`) — bulk_extractor, SwiftBeaver, and browser carver
- Media (`media.md`) — filesystem image extraction and file carving (Foremost, Scalpel)
- System (`system.md`) — registry, Jump Lists, file list enumeration, DPAPI decryption, and macOS plist parsing

## Conventions
- Each family page starts with a high-level overview.
- Each extractor uses an ELT-style split: **Extraction (source)** and **Ingestion (transform + store)**.
- Each extractor has its own subsection with extraction, ingestion, outputs, and notes.
- Patterns are documented either per-extractor or in a shared section for the family.
