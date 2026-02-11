# Contributing to Hookaido

Thanks for contributing to Hookaido.

## Before You Start

- Be respectful and collaborative. See `CODE_OF_CONDUCT.md`.
- For security issues, do **not** open a public issue. See `SECURITY.md`.
- Check `BACKLOG.md` before starting larger work to align with project priorities.

## Development Setup

1. Install Go `1.25.x` (see `go.mod`).
2. Clone the repository.
3. Run tests:

```bash
go test ./...
```

## Change Expectations

- Keep changes focused and small where possible.
- Add or update tests for behavior changes.
- Keep docs in sync for user-visible behavior:
  - `DESIGN.md` for DSL/API/runtime semantics.
  - `CHANGELOG.md` for user-visible behavior changes.
  - `BACKLOG.md` for priority and completion tracking.
  - `STATUS.md` only for milestone-level shifts.

## Pull Requests

1. Create a branch from `main`.
2. Make changes with tests and docs as needed.
3. Open a pull request with:
   - what changed,
   - why it changed,
   - how it was validated.

PRs must pass CI and branch protection checks before merge.

