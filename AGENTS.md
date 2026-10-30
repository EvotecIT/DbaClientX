# Agent Instructions

- Work that changes this repository should be done on a feature branch and opened as a pull request against `master`.
- Do not push directly to `master` unless the repository owner explicitly asks for a direct push for that specific change.
- Keep PRs ready for automated CI checks and human/agent review by Claude/Codex before merge.
- Prefer focused changes that preserve provider portability across SQL Server, PostgreSQL, Oracle, MySQL, and SQLite.
- Run the most relevant build and test commands before publishing a PR.

Before adding or changing tests, read `$CODEX_HOME/skills/test-audit/SKILL.md#authoring-gate`
and `$CODEX_HOME/skills/contract-focused-tests/SKILL.md` (default CODEX_HOME: `~/.codex`).
For an explicitly requested pruning campaign, read `$CODEX_HOME/skills/test-audit/CAMPAIGN.md`.
