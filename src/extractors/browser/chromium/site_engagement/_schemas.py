"""
Schema definitions for Chromium Site Engagement data.

Site Engagement Reference:
- Chrome stores site engagement data in Preferences JSON under:
  - profile.content_settings.exceptions.site_engagement
  - profile.content_settings.exceptions.media_engagement

- These track user interaction metrics to measure "site importance"

Site Engagement Fields:
- rawScore: 0-100 engagement score (browsing, clicking, typing, etc.)
- pointsAddedToday: Points added in current decay period
- lastEngagementTime: WebKit timestamp of last interaction
- lastShortcutLaunchTime: WebKit timestamp of shortcut launch
- hasHighScore: Boolean flag for high engagement sites

Media Engagement Fields:
- visits: Number of media playback visits
- mediaPlaybacks: Number of media playbacks (significant)
- lastMediaPlaybackTime: WebKit timestamp of last media play
- hasHighScore: Boolean flag

Both types share:
- last_modified: WebKit timestamp
- expiration: WebKit timestamp (if time-limited)
- model: Engagement model version

Known Engagement Keys:
- setting: A dict containing engagement metrics
- last_modified: WebKit timestamp
- expiration: Optional WebKit timestamp
- model: Version identifier (int)
"""

# Known keys in site_engagement/media_engagement entries
ENGAGEMENT_SETTING_KEYS = frozenset({
    "setting",
    "last_modified",
    "expiration",
    "model",
})

# Known keys inside the "setting" dict for site_engagement
SITE_ENGAGEMENT_SETTING_FIELDS = frozenset({
    "rawScore",
    "pointsAddedToday",
    "lastEngagementTime",
    "lastShortcutLaunchTime",
    "hasHighScore",
})

# Known keys inside the "setting" dict for media_engagement
MEDIA_ENGAGEMENT_SETTING_FIELDS = frozenset({
    "visits",
    "mediaPlaybacks",
    "lastMediaPlaybackTime",
    "hasHighScore",
})

# All known setting fields (union of both types)
ALL_ENGAGEMENT_SETTING_FIELDS = SITE_ENGAGEMENT_SETTING_FIELDS | MEDIA_ENGAGEMENT_SETTING_FIELDS | frozenset({
    # Some versions use these alternate names
    "lastSignificantPlaybackTime",
    "audiblePlaybacks",
    "significantPlaybacks",
    "highScoreChanges",
})
