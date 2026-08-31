package mcp

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// toolPullConsumers lists the pull consumers currently attached to a route.
//
// Unlike the backlog and message tools it has no local fallback. Those read
// durable queue state and can open the SQLite file directly when the MCP server
// runs beside it; an SSE connection is not durable state — it exists only in
// the memory of the process serving it. So the running instance is the only
// possible source, and the Admin API is the only way to ask it.
func (s *Server) toolPullConsumers(args map[string]any) (any, error) {
	route, err := parseString(args, "route")
	if err != nil {
		return nil, err
	}
	if err := validateOptionalRoutePath(route); err != nil {
		return nil, err
	}

	if strings.TrimSpace(s.ConfigPath) == "" {
		return nil, errors.New("pull_consumers needs a config path: live consumer state is only available from a running instance via the Admin API")
	}
	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return nil, errors.New("pull_consumers is unavailable: " + s.ConfigPath + " does not compile")
	}

	query := url.Values{}
	if route != "" {
		query.Set("route", route)
	}
	return s.callAdminJSON(compiled, http.MethodGet, "/pull/consumers", query, nil, nil, defaultAdminProxyTimeout)
}
