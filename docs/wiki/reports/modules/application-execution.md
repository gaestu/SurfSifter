# Application Execution (Report Module)

Displays Windows application execution artifacts from UserAssist registry data.

## Purpose
- Show which applications were executed, with run counts and focus time.
- Provide evidence of user activity based on registry-level execution tracking.

## Inputs
- OS indicator records from registry extraction (UserAssist hive entries).
- Tags applied to application execution entries.

## Filters and controls
- Section Title and Description: Custom heading and explanatory text.
- Show Default Description: Toggle descriptive text for non-technical readers.
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Show Run Count: Toggle run count column.
- Show Focus Time: Toggle focus time column.
- Show Focus Count: Toggle focus count column.
- Show Source: Toggle source hive column.

## Output
- Table of application execution records with application path, last run time, run count, focus time, focus count, and source hive.

## Notes
- Windows-only; requires registry extraction with UserAssist data.
- UserAssist data is stored in NTUSER.DAT hives and tracks GUI application execution.
