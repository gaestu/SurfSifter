# Images (Tab)

## Purpose
- Review image artifacts extracted from evidence.
- Filter, tag, cluster, and export images for analysis.

## When to use
- When you need to triage large image sets or find related images.
- When checking images against known hash lists.

## Data sources
- Evidence database image tables (including tags and hash matches).
- Case folder image files and thumbnails.

## Key controls
- Filters: tags, source, extension, size, and hash match status.
- Subtabs: Grid, Clusters, Table views.
- Actions: Tag Selected/Checked, Export Selected, Export Clusters CSV, Check Known Hashes.

## Outputs
- Exported images and cluster CSV files saved to disk.
- Tags and hash match results stored in the case database.

## Subtabs
- [[./grid|Grid]] - thumbnail grid with quick review and tagging.
- [[./clusters|Clusters]] - perceptual hash clustering of similar images.
- [[./table|Table]] - sortable table view of image metadata.

## Notes
- Workers extracted to separate module.
