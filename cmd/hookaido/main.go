package main

import (
	"os"

	"github.com/nuetzliches/hookaido/internal/app"

	// Module imports: register queue backends via init().
	_ "github.com/nuetzliches/hookaido/modules/postgres"
	_ "github.com/nuetzliches/hookaido/modules/sqlite"
)

func main() {
	code := app.Main(os.Args)
	os.Exit(code)
}
