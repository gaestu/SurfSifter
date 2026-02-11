# Extractor Doc Agent Instructions

## Copy/Paste Prompt (for agents)
```
You are documenting extractor code. Use only the pasted code and the pasted stub file.
Do not infer or invent details. If a detail is missing, write "Unknown/TBD".
Fill in the Overview/Extractors/Patterns sections in the stub.
Use the ELT split: Extraction (source) and Ingestion (transform + store).
Keep each extractor entry to 6–10 short bullet lines.
```

## Goal
Produce crisp, forensic‑oriented documentation for a single extractor or a small extractor section. The output must tell a forensic specialist **what the extractor does**, **where it extracts data from**, and **how it transforms + stores data during ingestion**, without bloated prose. The agent must **not hallucinate**: if a detail is not present in the provided code, mark it as **Unknown/TBD**.

## Scope Per Run
- **Input:** a family stub file (e.g., `docs/extractors/chromium.md`) go into the source and get the extractors and the files for the extractors (e.g. Source: `src/extractors/browser/chromium`). Each extractor has his own folder with the code.
- **Output:** fill in the stub sections in that same file with verified facts only.
- **Do not** attempt to document multiple families in a single run.

## How To Read The Code
1. Use only the pasted code. Do not assume anything outside it.
2. Locate the class that implements the extractor (usually `class <Name>Extractor`).
3. Identify:
   - **Extraction**: file types, DBs, registry hives, or artifacts read.
   - **Transformation**: key parsing, normalization, filtering, or joins done during ingestion.
   - **Ingestion**: where records are stored (tables, helpers, manifests, callbacks).
   - **Outputs**: any files/manifests written in the extraction phase.
   - **Dependencies**: external tools or shared helpers (if directly referenced).
4. If a detail is not present in the pasted code, write **Unknown/TBD** instead of guessing.

## Writing Rules
- Keep each extractor entry **short**: 6–10 lines total.
- Prefer bullet points; no long paragraphs.
- Avoid internal implementation minutiae (function names, loop details) unless essential.
- Use neutral, factual language suitable for forensic reporting.
- If extraction and ingestion are split (e.g., WebCache → ingestion), explicitly state the dependency.
- Add a short **Evidence** note only when the user asks for it, listing file paths/line hints from the pasted snippet.

## Output Template (per extractor)
```
### <ExtractorName>
- Purpose: <one sentence>.
- Extraction (source): <file/db/registry/artifact list or Unknown/TBD>.
- Ingestion (transform + store): <short transform + where stored or Unknown/TBD>.
- Outputs: <files/manifests emitted during extraction or Unknown/TBD>.
- Notes: <dependencies, caveats, prerequisites or Unknown/TBD>.
```

## Placement Rules
- Fill in the stub sections in the provided family file: **Overview**, **Extractors**, **Patterns**.
- Keep ordering stable (match the order of extractors observed in the pasted `__init__.py`, if provided).
- If the family file has a **Patterns** section, add a single short line there **only when the pasted code shows explicit patterns**.

## Example (Format Only)
```
### FirefoxHistoryExtractor
- Purpose: Extract visit‑level browsing history.
- Inputs: places.sqlite, favicons.sqlite (read‑only).
- Processing: Parses visits/places, resolves URLs, normalizes timestamps.
- Ingestion: Writes `browser_history` rows via helper API.
- Outputs: None besides DB rows.
- Notes: Requires Firefox profile directory discovery.
```
