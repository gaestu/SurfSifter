# Site Engagement (Report Module)

Displays Chromium site engagement scores measuring user interaction with websites.

## Purpose
- Show which websites a user interacted with most, based on Chromium's internal engagement metrics.
- Include both site engagement (browsing scores) and media engagement (playback metrics).

## Inputs
- Site engagement records populated by the ChromiumSiteEngagementExtractor.
- Tags applied to site engagement entries.

## Filters and controls
- Section Title: Custom heading text.
- Show Description: Toggle explanatory text.
- Limit: Maximum number of entries (10/25/50/100/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Engagement Type: All, Site Engagement only, or Media Engagement only.
- Minimum Score: Filter by minimum engagement score.
- Column visibility: type, score, visits, browser, profile, last engagement.
- Sort By: Order by score, visits, last engagement, or origin.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of engagement records with origin, type, score, visits, last engagement, browser, and profile.

## Notes
- Chromium-only; Firefox and IE/Edge Legacy do not maintain engagement scores.
- Higher scores indicate more user interaction (scores above 25 suggest regular use).
