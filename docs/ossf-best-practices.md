# OpenSSF Best Practices Badge

This page tracks evidence and draft answers for the OpenSSF Best Practices Badge questionnaire.

## Badge Entry

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
- `static_analysis_fixed`
  - Evidence: enforced PR + CI flow (`.artifacts/main-protection.json`, `.github/workflows/*`)
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

### Likely `Yes`, But `Manual confirm`

- `report_responses`, `enhancement_responses`
  - Confirm from issue/PR response history in GitHub UI.
- `vulnerability_report_response`
  - `SECURITY.md` defines response targets; confirm process adherence in practice.
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

### Review Carefully (wording-sensitive)

- `dynamic_analysis`, `dynamic_analysis_fixed`, `dynamic_analysis_unsafe`
  - Evidence exists via fuzzing (`.github/workflows/dependency-health.yml`), but questionnaire wording may expect broader dynamic tooling.
- `dynamic_analysis_enable_assertions`
  - Go does not have a universal "assertions build mode"; answer according to criterion wording and documented equivalent practice.
- `warnings`, `warnings_fixed`, `warnings_strict`
  - Go has limited compiler-warning model; map to `go vet`, CI strictness, and fail-on-error behavior.
- `crypto_call`, `crypto_floss`, `crypto_keylength`, `crypto_random`, `crypto_working`, `crypto_weaknesses`, `crypto_published`, `crypto_pfs`, `crypto_password_storage`
  - Security design is documented (`docs/security.md`, `DESIGN.md`) and uses standard libraries/protocols; exact answers depend on criterion details and whether a criterion is applicable (`N/A`) to Hookaido's threat model.

## Suggested Fill Order

1. Fill all high-confidence criteria first (fast path).
2. For manual-confirm items, verify recent issue/advisory timelines in GitHub and then answer.
3. For wording-sensitive criteria, use criterion help text in portal and keep notes in this file.

## Remaining Manual Steps

1. Create/confirm the project entry on `bestpractices.dev`.
2. Fill questionnaire answers using the draft above and evidence links.
3. Record badge URL in `README.md` once issued.
4. Keep this page updated as policies/workflows change.
