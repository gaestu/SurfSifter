# Implement a Non-Issue Task

You are implementing an ad-hoc task or `planning/wip/` spec for **SurfSifter**.
Read [`AGENTS.md`](../AGENTS.md) first and follow it strictly.

Use this prompt for tasks that don't originate from a GitHub issue (bug fixes, refactors, planning specs). For issue-driven work use [`implement_issue.md`](implement_issue.md) instead.

## Workflow

### 1. Research
- Use search subagents to find affected files, existing patterns, migration state, test coverage.
- **Internet research is permitted** for specs and library APIs.
- If requirements are ambiguous, **ask the user** before proceeding.

### 2. Implement

For **simple, single-layer changes**: make the change directly. No plan needed.

For **multi-layer changes** (5+ files across DB / core / UI / tests): split into coordinated subagents:

| Agent | Scope | Order |
|-------|-------|-------|
| **DB & Core** | Migrations, helpers, `core/`, extractors | First |
| **UI & Services** | `app/features/`, `common/`, `services/`, `data/` | After DB & Core |
| **Tests** | New / modified tests | After DB & Core and UI & Services |

Discipline:
- Only make changes directly requested or clearly necessary
- No opportunistic refactors, docstring additions, or unrelated improvements
- Follow existing patterns; don't invent parallel abstractions
- Never write to evidence; preserve provenance fields
- Don't hardcode version strings or migration counts

### 3. Tests
Add deterministic tests for any new behavior or bug fix.

### 4. Docs
Update `docs/wiki/`, `planning/wip/`, or `README.md` only if user-facing behavior changed.

### 5. Specialist Review (parallel)

Always run:
- [`review_correctness.md`](review_correctness.md)
- [`review_forensic_safety.md`](review_forensic_safety.md)
- [`review_security.md`](review_security.md)
- [`review_architecture.md`](review_architecture.md)

Run if docs/user-facing behavior changed:
- [`review_documentation.md`](review_documentation.md)

Each reviewer returns **PASS** or **ISSUES: [numbered list]**.
If a reviewer flags issues → fix and re-run only the failed reviewer. Max 3 iterations before escalating.

For trivial single-file changes (typo fix, log message tweak), reviewers may be skipped — use judgment.

### 6. Finalize
```bash
poetry run pytest -m "not gui_offscreen and not gui_live and not slow and not compat" -q
```

### 7. Return
- Summary of changes
- Files modified / created
- Tests added or updated
- Docs updated (if any)
- Reviewers run and verdicts
- Proposed commit message
