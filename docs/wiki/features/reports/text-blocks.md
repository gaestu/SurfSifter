# Text Blocks

Text Blocks are reusable, plain-text templates for report custom sections.

## Purpose
- Avoid repetitive typing for standard section language (methodology, disclaimers, common findings).
- Keep section wording consistent across cases.

## Where they are managed
- Open `Settings -> Preferences -> Text Blocks`.
- Or use `Reports -> Custom Sections -> Manage Text Blocks`.

## What a text block contains
- `Title`
- `Content` (plain text)
- `Tags` (comma-separated)

## Core actions
- Create, edit, and delete text blocks.
- Filter by search text (title/content/tags).
- Filter by tag.
- Import blocks from JSON.
- Export all blocks or selected blocks to JSON.

## Import behavior
- Import adds to the current library.
- Duplicate titles can be handled as:
  - Skip
  - Rename
  - Overwrite
- Invalid entries are skipped and reported.

## Storage
- Linux: `~/.config/surfsifter/text_blocks.json`
- Windows: `%APPDATA%/surfsifter/text_blocks.json`

## Using in custom sections
1. Open `Reports` and click `Add Section` (or edit an existing section).
2. Select a block from `From Text Block`.
3. Title/content fields are prefilled and remain editable before saving.
