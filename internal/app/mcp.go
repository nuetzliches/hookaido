package app

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/nuetzliches/hookaido/internal/mcp"
)

func mcpCmd(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "missing subcommand: serve")
		return 2
	}

	switch args[0] {
	case "serve":
		return mcpServe(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown mcp subcommand: %s\n", args[0])
		return 2
	}
}

func mcpServe(args []string) int {
	fs := flag.NewFlagSet("mcp serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "./Hookaidofile", "path to config file (read-only tools)")
	dbPath := fs.String("db", "./hookaido.db", "path to sqlite db file (read-only tools)")
	roleName := fs.String("role", "read", "MCP tool authorization role (read|operate|admin)")
	principal := fs.String("principal", "", "principal identity bound to MCP mutation/runtime-control audit events")
	enableMutations := fs.Bool("enable-mutations", false, "enable MCP mutation tools (dlq/messages)")
	enableRuntimeControl := fs.Bool("enable-runtime-control", false, "enable MCP runtime control tools (instance_start/stop/reload)")
	adminEndpointAllowlist := fs.String("admin-endpoint-allowlist", "", "comma-separated allowlist for MCP admin proxy endpoints (host:port or URL prefix)")
	pidFile := fs.String("pid-file", "./hookaido.pid", "pid file path used by runtime control tools")
	runBinary := fs.String("run-binary", "", "hookaido binary used by instance_start (default: current executable)")
	runWatch := fs.Bool("run-watch", true, "include --watch when instance_start launches hookaido run")
	runLogLevel := fs.String("run-log-level", "info", "include --log-level when instance_start launches hookaido run")
	runDotenv := fs.String("run-dotenv", "", "include --dotenv when instance_start launches hookaido run")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	role, err := mcp.ParseRole(*roleName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}

	bin := *runBinary
	if bin == "" {
		if exe, err := os.Executable(); err == nil {
			bin = exe
		}
	}

	server := mcp.NewServer(
		os.Stdin,
		os.Stdout,
		*configPath,
		*dbPath,
		mcp.WithRole(role),
		mcp.WithPrincipal(*principal),
		mcp.WithAuditWriter(os.Stderr),
		mcp.WithMutationsEnabled(*enableMutations),
		mcp.WithRuntimeControlEnabled(*enableRuntimeControl),
		mcp.WithRuntimeControlPIDFile(*pidFile),
		mcp.WithRuntimeControlRunBinary(bin),
		mcp.WithRuntimeControlRunWatch(*runWatch),
		mcp.WithRuntimeControlRunLogLevel(*runLogLevel),
		mcp.WithRuntimeControlRunDotenv(*runDotenv),
		mcp.WithAdminProxyEndpointAllowlist(parseCSVList(*adminEndpointAllowlist)),
	)
	if err := server.Serve(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	return 0
}

func parseCSVList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		out = append(out, entry)
	}
	return out
}
