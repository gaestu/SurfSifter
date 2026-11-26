# Implement a GitHub Issue

You are implementing a GitHub issue for **SurfSifter**.
Read [`AGENTS.md`](../AGENTS.md) first and follow it strictly.

## Workflow

### 1. Research
- Read the issue and extract: problem statement, requirements, acceptance criteria, constraints.
- Use search subagents to gather codebase context: affected files, existing patterns, migration state, test coverage.
- **Internet research is permitted** for file format specs, library APIs, forensic artifact references.
- If anything is ambiguous, **ask the user** before proceeding.

### 2. Implementation Plan
Write a structured plan and **post it as a comment on the GitHub issue**:
- Affected files and planned changes
- New files / migrations to create
- Key design decisions and trade-offs

### 3. Implement

For **single-layer changes**: one focused implementation pass.

For **multi-layer changes** (5+ files across DB / core / UI / tests): split into up to 3 coordinated subagents:

| Agent | Scope | Order |
|-------|-------|-------|
| **DB & Core** | Migrations, helpers, `core/`, extractors | First |
| **UI & Services** | `app/features/`, `common/`, `services/`, `data/` | After DB & Core |
| **Tests** | New / modified tests | After DB & Core and UI & Services |

Each subagent receives: task description, research findings, constraints, and what the other agents own (to avoid overlap).

Implementation discipline:
- Preserve backward compatibility unless the issue explicitly requires otherwise
- Follow existing abstractions — don't invent parallel ones
- Keep changes focused; no opportunistic refactors
- Never write to evidence; preserve provenance fields on all output rows
- Don't hardcode version strings or migration counts

### 4. Tests
Add or update deterministic tests:
- Regression tests for bug fixes
- Integration tests for cross-module changes
- Schema/migration tests for DB changes
- Helper tests for new helpers

### 5. Docs
Update only if user-facing behavior changed:
- `docs/wiki/` for extractors, features, reports, general guides
- `planning/wip/` for behavioral spec changes
- `README.md` for new commands / flags / setup
- `CHANGELOG.md` Check if the CHANGELOG.md needs a Entry in Unreleased and add one if needed.

### 6. Specialist Review (parallel)

Always run as parallel subagents:
- [`review_correctness.md`](review_correctness.md)
- [`review_forensic_safety.md`](review_forensic_safety.md)
- [`review_security.md`](review_security.md)
- [`review_architecture.md`](review_architecture.md)

Run if applicable:
- [`review_documentation.md`](review_documentation.md) — when docs / user-facing behavior changed
- [`review_issue_completeness.md`](review_issue_completeness.md) — always for issue work

Launching reviewers:

- **Models:** correctness, forensic safety, security, architecture → Opus. Documentation, issue completeness → Sonnet. This sets the model per reviewer subagent; implementation stays on the session model. Tools without per-subagent model selection ignore it.
- **Input:** the task description, the list of changed/created file paths, and the specialist prompt. Do **not** paste file contents or the diff — each reviewer runs `git diff` on those paths and reads the files itself (new untracked files don't appear in `git diff`; they are read by path).

Each reviewer returns **PASS** or **ISSUES: [numbered list]**.
If any reviewer flags real issues → fix them and re-run **only the failed reviewers**. Continue the same reviewer subagent (it keeps its context) and give it only what changed since its last pass — the paths and a one-line note per fix. Launch a fresh reviewer only when the earlier one can no longer be continued. Max 3 iterations before escalating.

### 7. Finalize
```bash
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q
```
If tests fail: fix and re-run (max 3 attempts before escalating).

### 8. Return
- Summary of changes
- Files modified / created
- Tests added or updated
- Docs updated (if any)
- Reviewers run and verdicts
- Remaining risks or follow-ups
- Proposed commit message
