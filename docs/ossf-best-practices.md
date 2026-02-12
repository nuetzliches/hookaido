# OpenSSF Best Practices Badge

This page tracks evidence and draft answers for the OpenSSF Best Practices Badge questionnaire.

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/11921/badge)](https://www.bestpractices.dev/projects/11921)

Badge status source of truth (SoT): <https://www.bestpractices.dev/projects/11921>.  
This document is a project-maintained evidence map and answer-prep checklist.

## Badge Entry

- Badge page (SoT): <https://www.bestpractices.dev/projects/11921>
- Portal: <https://www.bestpractices.dev/>
- Project repo: <https://github.com/nuetzliches/hookaido>
- Last reviewed: 2026-02-12

## Source of Criteria IDs

Passing criteria IDs were validated against current `bestpractices.dev` pages (retrieved 2026-02-12).

## Evidence Links (Quick Reference)

### Project Basics

- Description and usage: <https://github.com/nuetzliches/hookaido/blob/main/README.md>
- License (Apache-2.0): <https://github.com/nuetzliches/hookaido/blob/main/LICENSE>
- Changelog: <https://github.com/nuetzliches/hookaido/blob/main/CHANGELOG.md>

### Security

- Security policy: <https://github.com/nuetzliches/hookaido/blob/main/SECURITY.md>
- Security architecture guidance: <https://github.com/nuetzliches/hookaido/blob/main/docs/security.md>
- Signed release process: <https://github.com/nuetzliches/hookaido/blob/main/RELEASE.md>
- Release workflow: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/release.yml>
- Security code scanning: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/codeql.yml>
- Dependency vulnerability scanning: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/dependency-health.yml>

### Development Process

- Contributing guide: <https://github.com/nuetzliches/hookaido/blob/main/CONTRIBUTING.md>
- Code of conduct: <https://github.com/nuetzliches/hookaido/blob/main/CODE_OF_CONDUCT.md>
- Governance: <https://github.com/nuetzliches/hookaido/blob/main/GOVERNANCE.md>
- Support policy: <https://github.com/nuetzliches/hookaido/blob/main/SUPPORT.md>
- Code owners: <https://github.com/nuetzliches/hookaido/blob/main/.github/CODEOWNERS>
- PR template: <https://github.com/nuetzliches/hookaido/blob/main/.github/PULL_REQUEST_TEMPLATE.md>
- Issue templates: <https://github.com/nuetzliches/hookaido/tree/main/.github/ISSUE_TEMPLATE>
- Branch protection snapshot: <https://github.com/nuetzliches/hookaido/blob/main/.artifacts/main-protection.json>

### CI / Quality

- CI tests: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/ci.yml>
- Dependency health and vuln scan: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/dependency-health.yml>
- Scorecard workflow: <https://github.com/nuetzliches/hookaido/blob/main/.github/workflows/scorecard.yml>
- Dependency update automation: <https://github.com/nuetzliches/hookaido/blob/main/.github/dependabot.yml>
- Release packaging and verification: <https://github.com/nuetzliches/hookaido/blob/main/RELEASE.md>

## Draft Answers (Passing Criteria)

Legend:

- `Yes (evidence)` - high confidence from repository artifacts.
- `Manual confirm` - likely yes, but requires operational/project-history confirmation in portal.
- `Review carefully` - wording-sensitive or potentially weak; verify before submitting.

### High Confidence (`Yes (evidence)`)

- `description_good`, `interact`, `english`, `documentation_basics`, `documentation_interface`
  - Evidence: `README.md`, `docs/`
- `repo_public`, `repo_track`, `repo_distributed`, `repo_interim`
  - Evidence: public GitHub repo with full git history
- `floss_license`, `floss_license_osi`, `license_location`
  - Evidence: `LICENSE` (Apache-2.0), `README.md`
- `version_tags`, `version_unique`, `version_semver`
  - Evidence: release/tag workflow in `.github/workflows/release.yml`, SemVer release usage in `README.md`, changelog style in `CHANGELOG.md`
- `release_notes`
  - Evidence: `CHANGELOG.md`, GitHub release workflow with generated notes
- `contribution`, `contribution_requirements`, `discussion`, `report_tracker`, `report_archive`, `report_process`
  - Evidence: `CONTRIBUTING.md`, issue templates, PR template
- `sites_https`
  - Evidence: GitHub and docs site served over HTTPS
- `test`, `test_invocation`, `test_continuous_integration`, `test_most`, `test_policy`, `tests_are_added`, `tests_documented_added`
  - Evidence: `go test ./...` in `CONTRIBUTING.md`, CI in `.github/workflows/ci.yml`, PR checklist in `.github/PULL_REQUEST_TEMPLATE.md`
- `build`, `build_common_tools`, `build_floss_tools`
  - Evidence: `go build ./cmd/hookaido`, `Makefile`, OSS toolchain in `go.mod`
- `static_analysis`, `static_analysis_often`, `static_analysis_common_vulnerabilities`
  - Evidence: CodeQL (`.github/workflows/codeql.yml`), govulncheck (`.github/workflows/dependency-health.yml`)
- `vulnerability_report_process`, `vulnerability_report_private`
  - Evidence: `SECURITY.md` with private advisory link and coordinated disclosure policy
- `delivery_unsigned`, `delivery_mitm`
  - Evidence: release verification/signatures/attestations in `RELEASE.md` and `.github/workflows/release.yml`
- `know_secure_design`, `know_common_errors`
  - Evidence: `docs/security.md`, `DESIGN.md`, validation guidance in docs
- `no_leaked_credentials`
  - Evidence: `.env.example` uses dev placeholders; process guardrails in PR template and docs
- `maintained`
  - Evidence: active CI, release, dependency, and security workflows under `.github/workflows/`

### Portal Text Snippets (High-Confidence Criteria)

- `description_good`, `interact`, `english`, `documentation_basics`, `documentation_interface`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido provides clear, English, user-facing documentation of purpose, usage, and external interfaces in `README.md` and `docs/` (including configuration, ingress, pull/admin APIs, delivery, and deployment guidance)."
- `repo_public`, `repo_track`, `repo_distributed`, `repo_interim`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Development is tracked in a public, distributed Git repository on GitHub with full commit history and interim development snapshots."
- `floss_license`, `floss_license_osi`, `license_location`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido is released under an OSI-approved license (Apache-2.0), with license text available in `LICENSE` and referenced from `README.md`."
- `version_tags`, `version_unique`, `version_semver`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Releases are uniquely tagged in GitHub using semantic versioning (`vMAJOR.MINOR.PATCH`), with release automation and changelog alignment."
- `release_notes`
  - Suggested answer: `Yes`
  - Suggested text:
    - "User-visible release changes are documented via `CHANGELOG.md` and GitHub Releases."
- `contribution`, `contribution_requirements`, `discussion`, `report_tracker`, `report_archive`, `report_process`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Contribution and reporting processes are documented (`CONTRIBUTING.md`, issue/PR templates), and all reports are tracked in the public GitHub issue/PR system."
- `sites_https`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Project sites and artifacts are served via HTTPS (GitHub repository/docs/releases/workflows)."
- `test`, `test_invocation`, `test_continuous_integration`, `test_most`, `test_policy`, `tests_are_added`, `tests_documented_added`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Automated tests are documented (`go test ./...`), run in CI on push/PR, and contribution guidance requires tests for behavior changes."
- `build`, `build_common_tools`, `build_floss_tools`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido builds with common, FLOSS tooling (`go build`, `Makefile`, Go toolchain), with no proprietary build requirement."
- `vulnerability_report_process`, `vulnerability_report_private`
  - Suggested answer: `Yes`
  - Suggested text:
    - "A documented vulnerability disclosure process exists in `SECURITY.md`, including private reporting through GitHub security advisories."
- `delivery_unsigned`, `delivery_mitm`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Release delivery is protected by HTTPS transport and signed artifact verification (`verify-release` with required signature/SBOM checks)."
- `know_secure_design`, `know_common_errors`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Secure design and common misuse/error guidance are documented in `docs/security.md` and `DESIGN.md`."
- `no_leaked_credentials`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Repository policy and PR checks require that secrets are not committed; tracked examples use placeholder development values."
- `maintained`
  - Suggested answer: `Yes`
  - Suggested text:
    - "The project is actively maintained with regular CI runs, release workflows, dependency updates, and security scanning."

### Likely `Yes`, But `Manual confirm`

- `report_responses`, `enhancement_responses`
  - Confirm from issue/PR response history in GitHub UI.
- `vulnerability_report_response`
  - `SECURITY.md` defines response targets; confirm process adherence in practice.
- `static_analysis_fixed`
  - Confirm from current alert backlog/triage state at submission time.
- `vulnerabilities_critical_fixed`, `vulnerabilities_fixed_60_days`
  - Confirm vulnerability history/remediation timelines in advisories/issues.
- `release_notes_vulns`
  - Confirm release/advisory practice when vulnerabilities occur.

### Portal Text Snippets (Manual Criteria)

Use these snippets directly in the questionnaire comment/evidence fields and adjust wording if the portal asks for strict yes/no.

- `report_responses`
  - Suggested text:
    - "The repository has public issue tracking enabled (`has_issues=true`) and explicit support guidance in `SUPPORT.md`, including expected maintainer acknowledgement within a few business days. At the time of submission (2026-02-12), the public issue tracker has no open issues (`open_issues_count=0`), so there are no delayed responses to report."
- `enhancement_responses`
  - Suggested text:
    - "Feature requests are supported through issue templates (`.github/ISSUE_TEMPLATE/feature_request.yml`) and reviewed through the normal PR/issue process (`CONTRIBUTING.md`, `GOVERNANCE.md`). At submission time, no enhancement issues are currently open in the public tracker."
- `vulnerability_report_response`
  - Suggested text:
    - "Private vulnerability reporting is documented in `SECURITY.md` via GitHub private advisories, with explicit response targets (acknowledgement within 3 business days, triage within 7 business days)."
- `vulnerabilities_critical_fixed`
  - Suggested text:
    - "As of 2026-02-12, the repository shows no published GitHub security advisories (`security_advisories_count=0`). The project policy in `SECURITY.md` commits to coordinated disclosure and remediation when vulnerabilities are reported."
- `vulnerabilities_fixed_60_days`
  - Suggested text:
    - "As of 2026-02-12, there are no published security advisories for this repository, so there are no known unfixed vulnerabilities older than 60 days."
- `release_notes_vulns`
  - Suggested text:
    - "Release notes are published through GitHub Releases and `CHANGELOG.md`; `SECURITY.md` states that once a fix is available, maintainers publish release notes and a security advisory when applicable."

### Portal Text Snippets (Static Analysis Criteria)

- `static_analysis`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido uses static analysis in CI, including CodeQL (`.github/workflows/codeql.yml`) and `go vet` (`make lint`)."
- `static_analysis_often`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Static analysis runs regularly on push/PR and scheduled workflows (CodeQL and dependency-health/govulncheck jobs)."
- `static_analysis_common_vulnerabilities`
  - Suggested answer: `Yes`
  - Suggested text:
    - "CodeQL and govulncheck are used to detect common vulnerability classes in Go code and dependencies."
- `static_analysis_fixed`
  - Suggested answer: `Manual confirm`
  - Suggested text:
    - "Before submission, confirm that current static-analysis findings are fixed or explicitly triaged/accepted per project policy."

### Portal Text Snippets (Requested Security Criteria)

Use these snippets for the criteria that were flagged as wording-sensitive during review.

- `crypto_call`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido uses standard, well-vetted cryptography libraries (Go `crypto/*`) and does not implement custom cryptographic primitives."
- `crypto_floss`
  - Suggested answer: `Yes`
  - Suggested text:
    - "The cryptographic functionality used by Hookaido is provided by FLOSS components (Go standard library and open-source dependencies)."
- `crypto_keylength`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido uses modern cryptographic mechanisms (HMAC-SHA256, Ed25519, TLS from Go's maintained stack) and does not rely on weak key-length defaults."
- `crypto_working`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido's security-sensitive paths use currently accepted algorithms (for example HMAC-SHA256 and Ed25519), not deprecated broken primitives."
- `crypto_weaknesses`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Security mechanisms in Hookaido avoid algorithms and modes with known severe weaknesses; defaults and documentation focus on standard secure primitives."
- `crypto_published`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido relies on publicly documented and peer-reviewed cryptographic standards and implementations (including HMAC-SHA256, TLS, and Ed25519)."
- `crypto_pfs`
  - Suggested answer: `Yes`
  - Suggested text:
    - "When TLS is enabled, Hookaido uses Go's modern TLS implementation, which supports forward secrecy through ephemeral key exchange (noting deployment may terminate TLS at a reverse proxy)."
- `crypto_password_storage`
  - Suggested answer: `N/A`
  - Suggested text:
    - "Hookaido does not provide a user password database or account password storage feature; authentication is based on configured tokens/secrets and optional route basic-auth credentials."
- `crypto_random`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Hookaido uses cryptographically secure randomness (`crypto/rand`) in security-relevant code paths."
- `delivery_mitm`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Project delivery uses HTTPS channels (GitHub/GitHub Actions) and signed release verification steps, reducing MITM risk during artifact distribution."
- `delivery_unsigned`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Release checksums are signed with Ed25519, and verification (`hookaido verify-release --require-signature`) is part of CI/release policy."
- `vulnerabilities_fixed_60_days`
  - Suggested answer: `Yes`
  - Suggested text:
    - "As of 2026-02-12, the repository has no published GitHub security advisories, so there are no known unresolved vulnerabilities older than 60 days."
- `vulnerabilities_critical_fixed`
  - Suggested answer: `Yes`
  - Suggested text:
    - "As of 2026-02-12, there are no published critical GitHub security advisories for this repository; `SECURITY.md` documents coordinated disclosure and remediation commitments."
- `no_leaked_credentials`
  - Suggested answer: `Yes`
  - Suggested text:
    - "Repository policy and PR checklist explicitly require that no credentials are committed, and tracked examples use development placeholders rather than production secrets."

### Review Carefully (wording-sensitive)

- `dynamic_analysis`, `dynamic_analysis_fixed`, `dynamic_analysis_unsafe`
  - Suggested answer:
    - `dynamic_analysis`: `Yes`
    - `dynamic_analysis_fixed`: `Manual confirm`
    - `dynamic_analysis_unsafe`: `Yes`
  - Suggested text:
    - "Hookaido runs automated dynamic analysis via Go fuzz tests in `.github/workflows/dependency-health.yml` (e.g., `FuzzParseFormatRoundTrip`, `FuzzPullAPIServer`, `FuzzHMACAuthVerify`). These targets exercise untrusted/unsafe input paths; confirm at submission time that any discovered dynamic-analysis findings are fixed or explicitly tracked."
- `dynamic_analysis_enable_assertions`
  - Suggested answer: `N/A`
  - Suggested text:
    - "Go does not provide a universal project-level assertions build mode comparable to C/C++ assertions toggles; equivalent invariants are covered through tests and runtime checks."
- `warnings`, `warnings_fixed`, `warnings_strict`
  - Suggested answer:
    - `warnings`: `Yes`
    - `warnings_fixed`: `Manual confirm`
    - `warnings_strict`: `No` (current state)
  - Suggested text:
    - "Static analysis is enabled (CodeQL workflow and `go vet` via `make lint`). Confirm at submission time that warning-level findings are fixed or explicitly accepted. Branch protection currently enforces test and release-package checks, but does not require dedicated warning/static-analysis jobs as strict merge blockers."
- `crypto_call`, `crypto_floss`, `crypto_keylength`, `crypto_random`, `crypto_working`, `crypto_weaknesses`, `crypto_published`, `crypto_pfs`, `crypto_password_storage`
  - Suggested answer: see dedicated snippets above
  - Suggested text:
    - "Use the dedicated crypto snippets above and validate final wording against the exact portal criterion text at submission time."

## Suggested Fill Order

1. Fill all high-confidence criteria first (fast path).
2. For manual-confirm items, verify recent issue/advisory timelines in GitHub and then answer.
3. For wording-sensitive criteria, use criterion help text in portal and keep notes in this file.

## Remaining Manual Steps

1. Keep `bestpractices.dev` answers aligned with current repository state after security/process changes.
2. Re-review this page when workflows, policies, or release/security controls change.
3. Keep README/docs badge links aligned with the current badge URL.
