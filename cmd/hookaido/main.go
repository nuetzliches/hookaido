package main

import (
	"os"

	"hookaido/internal/app"
)

func main() {
	code := app.Main(os.Args)
	os.Exit(code)
}
