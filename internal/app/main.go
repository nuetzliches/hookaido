package app

import (
	"fmt"
	"os"
)

var (
	version   = "0.0.0-dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func Main(args []string) int {
	if len(args) < 2 {
		printHelp()
		return 2
	}

	switch args[1] {
	case "run":
		return run()
	case "config":
		return configCmd(args[2:])
	case "mcp":
		return mcpCmd(args[2:])
	case "verify-release":
		return verifyReleaseCmd(args[2:])
	case "version":
		return versionCmd(args[2:])
	case "help", "-h", "--help":
		printHelp()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[1])
		printHelp()
		return 2
	}
}

func printHelp() {
	fmt.Fprintln(os.Stdout, "hookaido")
	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, "Usage:")
	fmt.Fprintln(os.Stdout, "  hookaido run --config ./Hookaidofile [--db ./.data/hookaido.db] [--postgres-dsn postgres://user:pass@host:5432/db] [--pid-file ./hookaido.pid] [--watch] [--log-level info] [--dotenv ./.env]")
	fmt.Fprintln(os.Stdout, "  hookaido config fmt --config ./Hookaidofile")
	fmt.Fprintln(os.Stdout, "  hookaido config validate --config ./Hookaidofile --format json|text [--strict-secrets]")
	fmt.Fprintln(os.Stdout, "  hookaido mcp serve --config ./Hookaidofile --db ./.data/hookaido.db [--enable-mutations] [--enable-runtime-control] [--admin-endpoint-allowlist host:port,https://host/admin]")
	fmt.Fprintln(os.Stdout, "  hookaido verify-release --checksums ./dist/hookaido_<version>_checksums.txt [--require-signature] [--require-sbom] [--json]")
	fmt.Fprintln(os.Stdout, "  hookaido version [--long] [--json]")
}
