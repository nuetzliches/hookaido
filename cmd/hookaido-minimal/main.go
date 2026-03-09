// Binary hookaido-minimal is a minimal build of Hookaido without optional modules.
//
// This build includes only the core runtime with the in-memory queue backend.
// Optional modules (sqlite, postgres, grpcworker, otel, mcp) are not compiled in,
// resulting in a smaller binary with fewer dependencies.
//
// To build: make build-minimal
package main

import (
	"os"

	"github.com/nuetzliches/hookaido/internal/app"
)

func main() {
	code := app.Main(os.Args)
	os.Exit(code)
}
