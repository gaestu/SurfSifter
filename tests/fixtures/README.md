# Test Fixtures

This directory contains test data files used by the test suite.

## Pattern Files

- **test_patterns.txt** - Sample wildcard patterns for file list matching tests
- **test_simple_patterns.txt** - Simplified wildcard patterns for basic testing

## Usage

These files are referenced by test cases in the parent `tests/` directory, typically for:
- File list pattern matching
- Reference list functionality
- Dialog testing (AddFileListDialog)

## Format

Pattern files use simple text format with one pattern per line:
- Wildcard patterns: `window*`, `*.dll`, `temp*`
- Empty lines are ignored
- No comment syntax (yet)
