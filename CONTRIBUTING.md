# Contributing

## Branch and Pull Request Flow

Create a feature branch for repository changes and open a pull request against `master`.
Do not push directly to `master` unless the repository owner explicitly asks for a direct push for that specific change.
Keep pull requests ready for automated CI checks and Claude/Codex review before merge.

## Local Validation

Run the most relevant build and test commands for the provider or shared layer being changed before opening a pull request.
For cross-provider changes, include SQL Server plus any provider-specific tests that are affected.
