# Tools (Tab)

Enhanced Tools Tab for Preferences Dialog

## Purpose
- Configure and validate external forensic tools and Python libraries.
- View tool status, version, and capabilities.

## When to use
- When configuring external forensic tools or Python dependencies.
- When troubleshooting missing tools or incorrect paths.

## Data sources
- Tool registry auto-discovery results.
- User-configured tool paths from preferences.

## Key controls
- Tool tables (forensic tools and Python libraries) with status and version.
- Actions: Refresh All, Test Tool, Set Custom Path, Reset to Auto.
- Download Tools Guide button for setup help.

## Outputs
- Updated tool configuration saved in preferences.
- Tool status and version details refreshed in the UI.

## Subtabs
- None

## Notes
- dependency (settings importing from tools).
- This module re-exports for backward compatibility.
