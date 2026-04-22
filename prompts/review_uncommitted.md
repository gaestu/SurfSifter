# Review Uncommitted Changes

Orchestrator for full review of local uncommitted changes in **SurfSifter**.
Read [`AGENTS.md`](../AGENTS.md) first.

## Procedure

1. Run `git diff --stat` and `git diff` (and `git diff --cached`) to gather all uncommitted changes.
2. For each changed file, read enough surrounding context (≥20 lines around each hunk) to understand the change.
3. **Launch all 5 specialist reviewers in parallel as subagents.** Each receives:
   - The task description (what the change is meant to do)
   - The full content of every changed/created file
   - Pointer to its specialist prompt (below)

| Reviewer | Specialist prompt |
|----------|-------------------|
| Correctness | [`review_correctness.md`](review_correctness.md) |
| Forensic Safety | [`review_forensic_safety.md`](review_forensic_safety.md) |
| Security | [`review_security.md`](review_security.md) |
| Architecture | [`review_architecture.md`](review_architecture.md) |
| Documentation | [`review_documentation.md`](review_documentation.md) |

For issue-driven work, additionally run [`review_issue_completeness.md`](review_issue_completeness.md) with the issue body and implementation plan.

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
