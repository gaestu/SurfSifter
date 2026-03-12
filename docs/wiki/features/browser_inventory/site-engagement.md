# Site Engagement (Subtab)

Chromium browser site and media engagement scores.

## Purpose
- Displays site engagement metrics that Chromium browsers internally track to measure user interaction with websites.
- Shows both site engagement (browsing activity scores) and media engagement (audio/video playback metrics).
- High-engagement sites are prioritized by browsers for features like offline support, notifications, and media autoplay.

## When to use
- When investigating which websites a user interacted with most frequently.
- To identify high-value sites beyond what browsing history alone reveals.
- To discover media consumption patterns (frequently watched video sites, music services).

## Data sources
- Evidence database tables: `site_engagements` and `media_engagements`.
- Populated by the ChromiumSiteEngagementExtractor from Preferences JSON files of Chrome, Edge, Brave, Opera, and Vivaldi.

## Key controls
- **Browser filter** — restrict results to a specific Chromium browser.
- **Type filter** — Site Engagement or Media Engagement.
- **Min Score filter** — Any, 1+, 5+, 10+, 25+, 50+ (higher scores indicate more interaction).

## Columns
| Column | Description |
| --- | --- |
| Origin | The website origin (scheme + domain) |
| Type | Site Engagement or Media Engagement |
| Browser | Browser that recorded the engagement |
| Profile | Browser profile name |
| Score | Engagement score (0–100+, higher = more interaction) |
| Visits | Number of recorded visits |
| Playbacks | Media playback count (media engagement only) |
| Last Engagement | Timestamp of last engagement activity |
| Tags | Applied tags |

## Status bar
Shows total record count, site vs. media engagement breakdown, and maximum engagement score in the current view.

## Context menu
- **View Details** — open the engagement detail dialog with full JSON data.
- **Sandbox URL actions** — open the origin in a sandboxed browser.
- **Copy origin** — copy the origin URL to clipboard.
- **Tag selected** — apply tags to selected entries.

## Notes
- Engagement scores are computed internally by Chromium browsers based on user actions (clicks, scrolls, typing, media playback).
- A score above 5 typically indicates meaningful user interaction; scores above 25 suggest regular use.
- Media engagement tracks audio/video playback separately from general site engagement.
- Tagging uses artifact type `site_engagement`.
- Firefox and IE/Edge Legacy do not maintain engagement scores.
