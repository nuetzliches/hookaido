# Release Checklist

This checklist is for creating public Hookaido releases with reproducible, signed artifacts.

## 1. Pre-Release Validation

1. Ensure `STATUS.md` reflects current behavior and missing work.
2. Ensure `CHANGELOG.md` has user-visible `Unreleased` entries.
3. Run release checks:
   - `make release-check`
4. Optional unsigned packaging dry-run:
   - `make dist`
5. Signed packaging dry-run (recommended):
   - `HOOKAIDO_SIGNING_KEY_FILE=./release-signing-key.pem make dist-signed`

## 2. Version Metadata

Release artifacts embed build metadata into `hookaido version`:

- `version`
- `commit`
- `build_date` (RFC3339)

Inspect with:

- `hookaido version`
- `hookaido version --long`
- `hookaido version --json`

## 3. Signing Key Policy

1. Use an Ed25519 private key in PEM/PKCS#8 format.
2. Store the private key outside the repo (secret manager or CI secret).
3. For GitHub Actions release automation, set repository secret:
   - `HOOKAIDO_RELEASE_SIGNING_KEY_PEM`
4. Keep private keys rotated and access-restricted. Publish only derived public key material.

Example key generation (OpenSSL):

```bash
openssl genpkey -algorithm ED25519 -out release-signing-key.pem
openssl pkey -in release-signing-key.pem -pubout -out release-signing-key.pub.pem
```

## 4. Build Signed Release Artifacts

Bash:

```bash
go run ./internal/tools/release \
  -version v0.1.0 \
  -commit "$(git rev-parse --short HEAD)" \
  -date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -out dist \
  -signing-key ./release-signing-key.pem
```

PowerShell:

```powershell
go run ./internal/tools/release `
  -version v0.1.0 `
  -commit (git rev-parse --short HEAD) `
  -date (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") `
  -out dist `
  -signing-key .\release-signing-key.pem
```

Environment fallbacks:

- `HOOKAIDO_VERSION`
- `HOOKAIDO_COMMIT`
- `HOOKAIDO_BUILD_DATE`
- `HOOKAIDO_SIGNING_KEY_FILE`

If version/commit are omitted, git-based defaults are attempted.

## 5. Artifact Output

The packager writes to `dist/`:

- Cross-platform archives:
  - `hookaido_<version>_linux_amd64.tar.gz`
  - `hookaido_<version>_linux_arm64.tar.gz`
  - `hookaido_<version>_darwin_amd64.tar.gz`
  - `hookaido_<version>_darwin_arm64.tar.gz`
  - `hookaido_<version>_windows_amd64.zip`
  - `hookaido_<version>_windows_arm64.zip`
- Checksums:
  - `hookaido_<version>_checksums.txt`
- Detached checksum signature:
  - `hookaido_<version>_checksums.txt.sig` (Base64, Ed25519)
- Release public key material:
  - `hookaido_<version>_checksums.txt.pub.pem`
- Provenance manifest:
  - `hookaido_<version>_manifest.json`
- SPDX SBOM:
  - `hookaido_<version>_sbom.spdx.json`

Each archive contains:

- `hookaido` (or `hookaido.exe`)
- `LICENSE`
- `README.md`
- `CHANGELOG.md`

## 6. Verify Artifact Integrity

Use the built-in verifier:

```bash
hookaido verify-release \
  --checksums ./dist/hookaido_v0.1.0_checksums.txt \
  --require-signature \
  --require-sbom
```

PowerShell:

```powershell
hookaido verify-release `
  --checksums .\dist\hookaido_v0.1.0_checksums.txt `
  --require-signature `
  --require-sbom
```

Optional machine-readable output:

```bash
hookaido verify-release --checksums ./dist/hookaido_v0.1.0_checksums.txt --require-signature --require-sbom --json
```

Notes:

- If `--signature` and `--public-key` are omitted, verifier auto-detects:
  - `<checksums>.sig`
  - `<checksums>.pub.pem`
- If `--sbom` is omitted, verifier auto-detects one `*_sbom.spdx.json` entry from checksums.
- If `--provenance` is omitted, verifier auto-detects `*_provenance.intoto.jsonl` and falls back to `*_provenance.attestation.json`.
- If `--sbom-attestation` is omitted, verifier auto-detects `*_sbom.intoto.jsonl` and falls back to `*_sbom.attestation.json`.
- `--base-dir` can be used when artifact files are not beside the checksums file.

## 7. Upload Automation Policy

Tag-based release workflow is defined in `.github/workflows/release.yml`.

- Trigger: `push` tag matching `v*`
- Guardrail: fails closed when `HOOKAIDO_RELEASE_SIGNING_KEY_PEM` is missing
- Build: signed artifacts via `internal/tools/release`
- Verify: `hookaido verify-release --require-signature --require-sbom` must pass before publish
- Attestations:
  - `actions/attest-build-provenance@v3` for release artifacts (`subject-checksums`)
  - `actions/attest-sbom@v3` bound to `hookaido_<version>_sbom.spdx.json`
- Exported attestation bundles:
  - `hookaido_<version>_provenance.intoto.jsonl`
  - `hookaido_<version>_sbom.intoto.jsonl`
  - `hookaido_<version>_provenance.attestation.json`
  - `hookaido_<version>_sbom.attestation.json`
- Publish: uploads all `dist/*` files to GitHub Releases

Container publish workflow is defined in `.github/workflows/container.yml`.

- Trigger: `push` tag matching `v*` (and manual `workflow_dispatch`)
- Registry: GHCR (`ghcr.io/nuetzliches/hookaido`)
- Platforms: `linux/amd64`, `linux/arm64`
- Tags: semver tags (`vX.Y.Z`, `vX.Y`, `vX`), `latest`, and short `sha-*`
- Attestation: `actions/attest-build-provenance@v3` with `push-to-registry: true`
- Metadata: OCI labels include source URL and image description
- Build metadata: Docker build args wire to `hookaido version --long` (`version`, `commit`, `build_date`)

First-time package setup:

- Ensure the GHCR package visibility is `public` if anonymous pulls should work.

## 8. Key Rotation + Revocation Playbook

1. Generate a new Ed25519 PKCS#8 key pair outside the repo.
2. Update the repository secret `HOOKAIDO_RELEASE_SIGNING_KEY_PEM` with the new private key.
3. Keep old release public keys published for historical verification.
4. If key compromise is suspected:
   - rotate secret immediately,
   - mark old key as revoked in release notes/security advisory,
   - publish a re-signed follow-up release with the new key.
5. Restrict who can modify the signing secret and require audit trail on secret changes.

## 9. Attestation Verification Runbook

For each release, verify that:

1. `hookaido verify-release --require-signature --require-sbom` succeeds locally against the downloaded assets.
2. Release assets include:
   - `hookaido_<version>_provenance.intoto.jsonl`
   - `hookaido_<version>_sbom.intoto.jsonl`
   - `hookaido_<version>_provenance.attestation.json`
   - `hookaido_<version>_sbom.attestation.json`
3. Attestation bundles reference the same release subject checksums file that was published.
4. Verification outcome is recorded in release operations notes/ticket before rollout.

## 10. Tag + Publish

1. Create release commit with finalized `CHANGELOG.md`.
2. Push release tag:
   - `git tag v0.1.0`
   - `git push origin main --tags`
3. Confirm workflow `release` succeeded and uploaded:
   - archives
   - checksums
   - checksum signature/public key
   - manifest
   - sbom
   - provenance/sbom attestation bundles (`*.intoto.jsonl` + compatibility `*.attestation.json`)
4. Confirm workflow `container` succeeded and published:
   - `ghcr.io/nuetzliches/hookaido:vX.Y.Z`
   - `ghcr.io/nuetzliches/hookaido:latest`
   - multi-arch manifest (`linux/amd64`, `linux/arm64`)
