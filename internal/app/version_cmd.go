package app

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type versionPayload struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

func versionCmd(args []string) int {
	return runVersionCmd(args, os.Stdout, os.Stderr)
}

func runVersionCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	longOutput := fs.Bool("long", false, "")
	jsonOutput := fs.Bool("json", false, "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "version: %v\n", err)
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "version: unexpected positional arguments")
		return 2
	}

	payload := versionPayload{
		Version:   strings.TrimSpace(version),
		Commit:    strings.TrimSpace(commit),
		BuildDate: strings.TrimSpace(buildDate),
	}

	if *jsonOutput {
		enc := json.NewEncoder(stdout)
		if err := enc.Encode(payload); err != nil {
			fmt.Fprintf(stderr, "version: %v\n", err)
			return 1
		}
		return 0
	}

	if *longOutput {
		fmt.Fprintf(stdout, "%s (commit=%s, build_date=%s)\n", payload.Version, payload.Commit, payload.BuildDate)
		return 0
	}

	fmt.Fprintln(stdout, payload.Version)
	return 0
}
