# Hookaido VS Code Extension

Syntax highlighting and snippets for `Hookaidofile` — the Hookaido webhook queue DSL.

## Features

- **Syntax highlighting** for the full Hookaidofile DSL: top-level blocks, route paths, directives, auth keywords, channel types, placeholders, durations, built-in constants, and comments.
- **Snippets** for common blocks: `ingress`, `pull_api`, `admin_api`, `route-pull`, `route-deliver`, `observability`, `defaults`, `secrets`, and more.
- **File association** for `Hookaidofile`, `*.hookaido`, and `*.hkd` files.
- **Bracket matching and auto-closing** for `{ }` and `" "`.
- **Code folding** on brace-delimited blocks.

## Installation

### From source (development)

```bash
cd editors/vscode
npm install -g @vscode/vsce
vsce package
code --install-extension hookaido-0.1.0.vsix
```

### Manual

Copy the `editors/vscode` folder to `~/.vscode/extensions/hookaido-0.1.0/`.

## Supported file names

| Pattern | Example |
|---------|---------|
| `Hookaidofile` | Project root config |
| `*.hookaido` | Named config files |
| `*.hkd` | Short extension |

## Snippet prefixes

| Prefix | Description |
|--------|-------------|
| `ingress` | Ingress listener block |
| `ingress-tls` | Ingress with TLS |
| `pull_api` | Pull API block |
| `admin_api` | Admin API block |
| `route-pull` | Route with pull endpoint |
| `route-deliver` | Route with push delivery |
| `route-hmac` | Route with HMAC auth |
| `outbound` | Outbound channel wrapper |
| `internal` | Internal channel wrapper |
| `observability` | Logging + metrics |
| `tracing` | OpenTelemetry tracing |
| `defaults` | Global defaults |
| `secrets` | Secrets block |
| `queue_limits` | Queue limits |
| `vars` | Config variables |
| `egress` | Egress policy |
| `env` | Env placeholder |

## License

Apache-2.0
