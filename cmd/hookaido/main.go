package main

import (
	"os"

	"github.com/nuetzliches/hookaido/internal/app"
)

func main() {
	code := app.Main(os.Args)
	os.Exit(code)
}
