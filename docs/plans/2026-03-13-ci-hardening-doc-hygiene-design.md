# CI Hardening & Documentation Hygiene: Design

**Date:** 2026-03-13
**Status:** Approved

## Problem

Post-v2.0.0 audit identified two categories of issues:

**CI Pipeline (HIGH risk):**
- No `-race` detector in CI tests
- No linting step (golangci-lint, go vet)
- No coverage reporting
- Dockerfile uses `golang:1.26-alpine` vs go.mod `go 1.25.8`

**Documentation (HIGH visibility):**
- 8 releases (v1.0.1 through v1.5.1) missing from CHANGELOG
- README doesn't mention Postgres backend or gRPC Worker API
- DESIGN.md header says "Status: draft", Non-Goals contradict Admin API
- BACKLOG.md header says "v1.x", P0 items not marked complete
- SECURITY.md supported versions table shows only v1.x
- HMAC canonical string order wrong in docs/ingress.md (TIMESTAMP first vs code: METHOD first)
- Various stale version references and diagram labels

## Approach

Two parallel streams targeting independent files:

**Stream 1: CI Hardening** — ci.yml, Dockerfile
**Stream 2: Documentation Hygiene** — CHANGELOG, README, DESIGN.md, BACKLOG.md, SECURITY.md, docs/

## Stream 1: CI Changes

1. Add `-race` flag for Linux test runner
2. Add `golangci-lint` job (ubuntu-only)
3. Add coverage profile as artifact (Linux-only)
4. Fix Dockerfile Go version to match go.mod

## Stream 2: Documentation Changes

1. Reconstruct 8 missing CHANGELOG releases from git history
2. Redistribute v2.0.0 entries to correct releases
3. Add comparison link references
4. Update README: Postgres, gRPC Worker, Docker pin, diagram
5. Fix DESIGN.md: header, non-goals, roadmap, MVP language
6. Update BACKLOG.md: header, P0 items, logo item
7. Update SECURITY.md: v2.x support table
8. Fix HMAC canonical string in docs/ingress.md
9. Minor fixes: docs/docker.md version, deployment diagram, CLI reference

## HMAC Spec Resolution

Code (`internal/dispatcher/http_deliverer.go:129`):
```
METHOD\nPATH\nTIMESTAMP\nSHA256(body)
```

DESIGN.md matches the code. docs/ingress.md has TIMESTAMP first — docs/ingress.md must be fixed.
