# Review Uncommitted Changes

Orchestrator for full review of local uncommitted changes in **SurfSifter**.
Read [`AGENTS.md`](../AGENTS.md) first.

## Procedure

1. Run `git diff --stat` and `git diff` (and `git diff --cached`) to gather all uncommitted changes.
2. For each changed file, read enough surrounding context (≥20 lines around each hunk) to understand the change.
3. **Launch all 5 specialist reviewers in parallel as subagents.** Each receives:
   - The task description (what the change is meant to do)
   - The list of changed/created file paths — **not** the file contents or the diff. Each reviewer runs `git diff` / `git diff --cached` on those paths and reads the files itself (new untracked files don't appear in `git diff`; they are read by path).
   - Pointer to its specialist prompt (below)

| Reviewer | Specialist prompt | Model |
|----------|-------------------|-------|
| Correctness | [`review_correctness.md`](review_correctness.md) | Opus |
| Forensic Safety | [`review_forensic_safety.md`](review_forensic_safety.md) | Opus |
| Security | [`review_security.md`](review_security.md) | Opus |
| Architecture | [`review_architecture.md`](review_architecture.md) | Opus |
| Documentation | [`review_documentation.md`](review_documentation.md) | Sonnet |

For issue-driven work, additionally run [`review_issue_completeness.md`](review_issue_completeness.md) (Sonnet) with the issue body and implementation plan.

The Model column sets the model per reviewer subagent, regardless of the model the orchestrating session runs on. Tools without per-subagent model selection ignore it.

## Re-review after fixes

Re-run only the reviewers that reported issues. Continue the same reviewer subagent (it keeps its context) and give it only what changed since its last pass — the paths and a one-line note per fix. Launch a fresh reviewer only when the earlier one can no longer be continued.

## Aggregation

Each reviewer returns **PASS** or **ISSUES: [numbered list]**.
Aggregate findings grouped by severity:

- **🔴 CRITICAL** — bugs, data corruption, forensic violations. Must fix before commit.
- **🟠 WARNING** — likely problems or maintenance burdens.
- **🟡 SUGGESTION** — style, readability, minor improvements.

## Final verdict

Exactly one of:
- **✅ PASS** — no critical/warning issues. List minor suggestions.
- **⚠️ NEEDS FIXES** — warnings should be addressed. Numbered list.
- **❌ ISSUES** — critical issues. Must resolve.

Include:
- Total findings by severity and by domain
- Files reviewed with no findings
- Specialist reviewers run and their individual verdicts
