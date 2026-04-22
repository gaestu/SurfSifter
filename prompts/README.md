# SurfSifter Prompt Set

[`AGENTS.md`](../AGENTS.md) is the base instruction set for every SurfSifter task.
Add **exactly one** task prompt from this directory when useful.

## Task prompts

- [`implement_issue.md`](implement_issue.md) — implement a GitHub issue end-to-end
- [`implement_task.md`](implement_task.md) — implement an ad-hoc task or `planning/wip/` spec

## Review prompts

- [`review_uncommitted.md`](review_uncommitted.md) — full review of local uncommitted changes (orchestrates the 5 specialist reviewers)
- [`review_correctness.md`](review_correctness.md) — correctness-only specialist review
- [`review_forensic_safety.md`](review_forensic_safety.md) — forensic-safety-only specialist review
- [`review_security.md`](review_security.md) — security-only specialist review
- [`review_architecture.md`](review_architecture.md) — architecture-only specialist review
- [`review_documentation.md`](review_documentation.md) — documentation-only specialist review
- [`review_issue_completeness.md`](review_issue_completeness.md) — verify a GitHub issue was fully implemented

## Composition rules

- `AGENTS.md` defines repository invariants and global rules.
- Task prompts add workflow and output expectations.
- Review prompts are specialist reviewer briefs — used in parallel by `review_uncommitted.md`.
- **Do not duplicate global rules from `AGENTS.md` into prompts.** Reference them.
