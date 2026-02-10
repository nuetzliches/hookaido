package app

import (
	"flag"
	"fmt"
	"os"

	"github.com/nuetzliches/hookaido/internal/config"
)

func configCmd(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "missing subcommand: fmt | validate | diff")
		return 2
	}

	switch args[0] {
	case "fmt":
		return configFormat(args[1:])
	case "validate":
		return configValidate(args[1:])
	case "diff":
		return configDiff(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n", args[0])
		return 2
	}
}

func configFormat(args []string) int {
	fs := flag.NewFlagSet("config fmt", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "./Hookaidofile", "path to config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	out, err := config.Format(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	_, _ = os.Stdout.Write(out)
	return 0
}

func configValidate(args []string) int {
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "./Hookaidofile", "path to config file")
	format := fs.String("format", "json", "output format: json|text")
	strictSecrets := fs.Bool("strict-secrets", false, "load and verify all configured secret refs during validation")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		return configValidateError(*format, err.Error())
	}

	cfg, err := config.Parse(data)
	if err != nil {
		return configValidateError(*format, err.Error())
	}

	res := config.ValidateWithResultOptions(cfg, config.ValidationOptions{
		SecretPreflight: *strictSecrets,
	})
	if *format == "text" {
		msg := config.FormatValidationText(res)
		if res.OK {
			fmt.Fprintln(os.Stdout, msg)
			return 0
		}
		fmt.Fprintln(os.Stderr, msg)
		return 1
	}

	out, err := config.FormatValidationJSON(res)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	if res.OK {
		fmt.Fprintln(os.Stdout, out)
		return 0
	}
	fmt.Fprintln(os.Stderr, out)
	return 1
}

// configValidateError emits a validation failure in the requested format.
func configValidateError(format, msg string) int {
	res := config.ValidationResult{
		OK:     false,
		Errors: []string{msg},
	}
	if format == "text" {
		fmt.Fprintln(os.Stderr, config.FormatValidationText(res))
		return 1
	}
	out, err := config.FormatValidationJSON(res)
	if err != nil {
		fmt.Fprintln(os.Stderr, msg)
		return 1
	}
	fmt.Fprintln(os.Stderr, out)
	return 1
}

func configDiff(args []string) int {
	fs := flag.NewFlagSet("config diff", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	contextLines := fs.Int("context", 3, "number of unified diff context lines")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	posArgs := fs.Args()
	if len(posArgs) != 2 {
		fmt.Fprintln(os.Stderr, "usage: hookaido config diff [--context N] <old> <new>")
		return 2
	}

	oldPath, newPath := posArgs[0], posArgs[1]

	oldData, err := os.ReadFile(oldPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	newData, err := os.ReadFile(newPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}

	diff, err := config.FormatDiff(oldData, newData, *contextLines, oldPath, newPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}

	if diff == "" {
		return 0
	}

	fmt.Fprintln(os.Stdout, diff)
	return 1
}
