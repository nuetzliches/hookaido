package main

import (
	"os"

	"github.com/nuetzliches/hookaido/v2/internal/app"

	// Module imports: register modules via init().
	_ "github.com/nuetzliches/hookaido/v2/modules/grpcworker"
	_ "github.com/nuetzliches/hookaido/v2/modules/mcp"
	_ "github.com/nuetzliches/hookaido/v2/modules/otel"
	_ "github.com/nuetzliches/hookaido/v2/modules/postgres"
	_ "github.com/nuetzliches/hookaido/v2/modules/sqlite"
)

func main() {
	code := app.Main(os.Args)
	os.Exit(code)
}
