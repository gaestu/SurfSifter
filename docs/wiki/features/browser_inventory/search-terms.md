# Search Terms (Subtab)

Extracted browser search terms from browsing history and URL analysis.

## Purpose
- Displays search terms extracted from browser history URLs across all supported browsers.
- Allows filtering by browser, search engine, and term text.

## When to use
- When investigating what a user searched for across different search engines.
- To correlate search activity with timeline events or other artifacts.

## Data sources
- Evidence database: search terms parsed from URL query parameters in browsing history.
- Supported search engines are auto-detected from URL patterns (Google, Bing, DuckDuckGo, Yahoo, etc.).

## Key controls
- **Browser filter** — restrict results to a specific browser.
- **Search engine filter** — filter by detected search engine.
- **Term text filter** — free-text search within extracted terms.

## Columns
| Column | Description |
| --- | --- |
| Term | The extracted search query text |
| URL | The full search URL |
| Search Time | Timestamp of the search |
| Browser | Browser that recorded the search |
| Profile | Browser profile name |
| Search Engine | Detected search provider |
| Tags | Applied tags |

## Context menu
- **View Details** — open the search term detail dialog.
- **Copy search term** — copy the term text to clipboard.
- **Sandbox URL actions** — open the URL in a sandboxed browser.
- **Copy URL** — copy the full URL to clipboard.
- **Tag selected** — apply tags to selected entries.

## Notes
- Search terms are identified by parsing URL query parameters for known search engine patterns.
- Tagging uses artifact type `browser_search_term`.
