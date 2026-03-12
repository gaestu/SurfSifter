# Tools (Tab)

View and configure external forensic tools and Python library status.

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
- Tool paths are saved in `~/.config/surfsifter/tool_paths.json`.
- See the **External Tools** page for installation guides.
