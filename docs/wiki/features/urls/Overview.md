# URLs (Tab)

Browse, filter, and tag extracted URLs with optional matching against reference lists.

## Purpose
- Provide a searchable list of all URLs extracted from the evidence.
- Support grouping, tagging, and list matching to triage large URL sets.

## When to use
- When you need to review or filter extracted URLs by domain, source, or tag.
- When you want to match URLs against reference lists or deduplicate entries.

## Data sources
- Case database URL tables (including sources, tags, and match results).

## Key controls
- Filters: domain, source, tag, match status, and text search (Search button applies text).
- Domain filtering has two modes:
  - **Domain dropdown — exact match.** Selects URLs whose domain is exactly the chosen value, ignoring ASCII letter case (`Example.COM` matches `example.com`; non-ASCII letters, e.g. in internationalized domains, must match exactly). Subdomains are not included: `example.com` does not match `sub.example.com`.
    Domains that differ only in letter case are listed as one entry. The entry shows one spelling, and its count includes every spelling. The list is ordered by count, and domains with equal counts are ordered alphabetically.
  - **Domain contains — substring match.** Type part of a domain and press Enter or Search. It also ignores ASCII letter case. `%`, `_` and `\` are matched as literal characters, not as wildcards. Use this for domains that are not in the top-200 dropdown.
  - If both are set, a URL must match both.
- **Text search — substring match.** Type part of a URL and press Enter or Search. It ignores ASCII letter case. `%`, `_` and `\` are matched as literal characters, not as wildcards: `utm_source` does not match `utmXsource`, and `%` only matches URLs that contain a `%`.
- **Tag filter — exact match.** Shows URLs that have the selected tag. Letter case is ignored in the same way as when tags are created. `%` and `_` in tag names are literal characters, so `a_b` does not match `axb`.
- **Editing tags in the list.** Double-click a **Tags** cell (table or grouped view) to edit that URL's tags as a comma-separated list. The text you enter replaces the URL's tags: tags you remove from the list are untagged. Letter case is ignored, so changing only the case of a tag keeps it unchanged; rename tags in the Tags tab instead. A tag whose name contains a comma is kept as long as its full name is still in the list. Only a URL's Tags cell can be edited; all other cells, including **Occurrences** and the domain rows of the grouped view, are read-only.
- Paging: Previous/Next, and a **Rows per page** selector (100, 500, 1,000, 5,000 or 10,000; default 500). Your choice is remembered.
- Group by domain toggle (table vs grouped tree view).
- Actions: Match Against URL Lists, Deduplicate, Tag Selected, Export CSV.

## Outputs
- CSV export of the current URL list.
- Updated tags and match results stored in the case database.

## Subtabs
- None

## Notes
- Downloads are managed in the Downloads tab (URL tab no longer triggers downloads).
- The flat URL list loads in the background, both for the first page and when you filter, page with Previous/Next, or refresh. The page counter shows "Loading..." until the new page arrives, and the UI stays responsive. A newer filter or page request cancels the older one, so only the latest request is shown.
- URLs are listed newest first by First Seen (UTC). URLs without a First Seen timestamp come after all timestamped URLs. Rows with the same timestamp, and all rows without one, are ordered by their database ID. This order is stable, so walking all pages forward or backward shows every matching URL exactly once while the data does not change.
- Rows appear before the exact total is known. Until the total is counted, the counter reads `Showing 1-500 of … URLs (counting)`; it never shows an estimate. If counting fails, it reads `(total unavailable)` and the rows stay usable. The total is remembered for the current filters, so Previous/Next and changing the page size do not count again.
- After tagging in the table view (with Tag Selected or by editing a Tags cell), you stay on the current page and the total is recounted. Tagging in the grouped view returns the table to page 1. If the evidence database was changed elsewhere (for example a tag renamed or merged in the Tags tab), the next page load notices it and recounts the total. If that happens beyond page 1, earlier pages may have changed, so the counter shows the page number instead of row numbers (`Page 3: 500 of 1,234 URLs; data changed, row numbers hidden`) until you return to page 1. Returning to the URLs tab while the table view (not the grouped view) is shown also reloads the current page in the background, so such changes are noticed without changing pages. The counter keeps its old text until the page is reloaded, and if nothing changed, the table and your selection stay as they are. After extraction/ingestion, deduplication or list matching, the list returns to page 1 and the total is recounted, because earlier pages may have changed.
- Refresh and filter changes reload only the visible view (table or grouped tree). The other view reloads when you switch to it.
