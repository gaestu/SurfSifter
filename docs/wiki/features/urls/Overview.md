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
- The first rows do not wait for the filter dropdowns. The domain, source, tag and match lists load separately in the background. Until they arrive, the domain, source and tag dropdowns show only the current selection and are disabled; search, Domain contains and paging can be used meanwhile. Reloading the lists never changes your selection: a selected value that is no longer in the list (for example a domain outside the top 200 or a deleted tag) stays selected. The lists are reloaded after extraction/ingestion, tagging, deduplication and list matching, and when the tab notices that the evidence database was changed elsewhere. They are remembered per evidence while the case is open, so returning to an evidence does not count its domains again.

## Search index
Text search on large evidence uses a search index. The index only makes search faster. The matched URLs, their order and the total are the same with or without it.
- **When it is built:** when the URLs tab is shown for an evidence that has URLs not indexed yet, the index is built in the background. This happens once for an existing case, and again for new URLs after extraction or ingestion. While it builds, a status next to the Search button shows `Search index: building 45%`. Search is complete meanwhile: URLs not indexed yet are searched without the index. The build stops after the batch it is working on when you leave the URLs tab, open another case, or close the evidence tab or the application (minimizing the window does not stop it). It also pauses while a case export runs, so the evidence database does not change while it is exported: the export waits for running builds to stop, and if one is still finishing after 15 seconds, the export is not started and you are asked to try again. It resumes where it stopped the next time the URLs tab is shown. The status is hidden once every URL is indexed.
- **When a build finishes:** the index build writes to the evidence database, and the URLs tab cannot tell those writes apart from changes made elsewhere (for example a tag renamed or merged in the Tags tab). While the build runs, it therefore does not react to such writes, and paging behaves normally. When a build that indexed URLs finishes, the shown table page is reloaded and the total is recounted, so changes made elsewhere in the meantime are picked up (in the grouped view, this happens with the next table page load). If you are beyond page 1 at that moment, the counter shows `data changed, row numbers hidden` until you return to page 1, even if nothing but the index changed.
- **When it is used:** only for search text of three or more characters, and only when it matches a limited number of URLs (about 25,000). For other searches, including one- and two-character searches and very common text such as `https`, search reads the URLs directly. Those are as fast as before, because the first page of a common search is found quickly.
- **Unavailable:** if the SQLite library of your installation lacks the FTS5 trigram tokenizer (SQLite 3.34 or newer is needed; release builds include it), the status shows `Search index: unavailable`. It also shows `unavailable` if the index could not be set up when the evidence database was opened (the reason is in the log); reopening the case retries. Search then reads every URL: results are complete, but searches for rare text are slower on large evidence. If a build fails, the status shows `Search index: build failed` with the error in its tooltip; search stays complete, and the build is retried the next time the tab is shown.
- **Storage:** the index is stored in the evidence database in the case workspace, never in the evidence image. It does not copy URL text. It takes about 50 MB per million short URLs (about 45 characters) and about 110 MB per million longer, more varied URLs (about 100 characters). Building it takes about 10 to 40 seconds per million URLs on 1 million URLs, and longer per URL on larger evidence: about 13 minutes for 10 million short URLs.
- **Ingest and delete cost:** adding URLs costs nothing extra, because new URLs are indexed by the background build. Deleting URLs that are already indexed (re-extraction cleanup, deduplication, purging evidence data) is slower, and more so on larger evidence: deleting 100,000 indexed URLs took 4.3 s instead of 2.8 s on 1 million URLs (1.5 to 1.8 times slower over two measurements), and 12.6 s instead of 3.7 s on 10 million URLs (3.4 times slower). These are same-file comparisons by the URL query benchmark in `tests/app/test_url_query_benchmark.py`. Deletes are slower because the deleted URLs are removed from the index in the same transaction.
- **Audit:** each build session that indexes URLs is recorded in the evidence process log (`url_search_index_build`), with the number of URLs indexed and whether it completed, was stopped, or failed. A failed build is recorded with exit code 1 and the error, and appears as a failed tool run in case validation. These entries do not change the evidence's "Last Extraction" time.
