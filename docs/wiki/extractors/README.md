# Browser Extractors

This section documents browser-family extractors under `src/extractors/browser/`.

## Families
- Chromium (`chromium.md`)
- Firefox (`firefox.md`)
- Safari (`safari.md`)
- IE Legacy (`ie-legacy.md`)
- Carvers (`carvers.md`)
- Media (`media.md`)
- System (`system.md`)

## Conventions
- Each family page starts with a high-level overview.
- Each extractor uses an ELT-style split: **Extraction (source)** and **Ingestion (transform + store)**.
- Each extractor has its own subsection with extraction, ingestion, outputs, and notes.
- Patterns are documented either per-extractor or in a shared section for the family.
