# Evidence and Cases

## Supported evidence formats
- **E01/EWF disk images** (including multi-part images)
- **Multi-partition** evidence is supported with automatic segment detection

## Evidence handling
- Evidence is accessed via a read-only filesystem abstraction.
- Source images are never modified.

## Case directories
- When you create a case, you select a directory to store all outputs.
- The case directory contains extracted artifacts, analysis databases, reports, and logs.

## Preserved context
- Extracted artifacts retain source context such as paths and timestamps **when available**.
- Timeline and correlation views rely on the original metadata recorded at extraction time.
