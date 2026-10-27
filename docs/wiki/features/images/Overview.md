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
- Filters: tags, source, extension, size, hash match status, URL text, and cache key text, each text field with an Exclude field next to it (see [Filters](#filters)).
- Subtabs: Grid, Clusters, Table views.
- Actions: Tag Selected/Checked, Export Selected, Export Clusters CSV, Check Known Hashes.

## Filters
The filters apply to the Grid and Table views (not to Clusters) and combine with each other. **↻ Reset** clears them all.

- **Tags**: **All tags**, **Untagged** (images without any tag), then the evidence's tags. A tag that is itself named `Untagged` is listed separately with its count and filters as a normal tag.
- **URL** / **Cache Key**: show images with a discovery whose cache URL / cache key contains the text.
- **URL exclude** / **Cache Key exclude**: hide images with any discovery whose cache URL / cache key contains the text, for example `favicon`. Images without a URL / cache key (carved or filesystem images) are kept, so URL `X` and URL exclude `X` together show every image exactly once. URL `cdn` with URL exclude `favicon` shows images from a `cdn` URL, none of whose URLs contains `favicon`.
- The four text fields match one literal substring: ASCII letter case is ignored, `%`, `_` and `\` are ordinary characters, and leading and trailing spaces are ignored. They apply 400 ms after you stop typing; an empty field filters nothing.
- An image found several times (several discoveries) matches URL / Cache Key if **any** discovery matches, and is hidden by an Exclude field if **any** discovery contains the text.

## Outputs
- Exported images and cluster CSV files saved to disk.
- Tags and hash match results stored in the case database.

## Subtabs
- [[./grid|Grid]] - thumbnail grid with quick review and tagging.
- [[./clusters|Clusters]] - perceptual hash clustering of similar images.
- [[./table|Table]] - sortable table view of image metadata.

## Notes
- Workers extracted to separate module.
