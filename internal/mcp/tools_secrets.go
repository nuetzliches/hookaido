package mcp

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
)

// rotateSecretAllowedKeys enumerates every argument the rotate_secret tool
// accepts across its three operations. Per-operation requiredness is validated
// at runtime.
var rotateSecretAllowedKeys = map[string]struct{}{
	"operation":  {},
	"name":       {},
	"id":         {},
	"value":      {},
	"not_before": {},
	"not_after":  {},
	"reason":     {},
	"actor":      {},
	"request_id": {},
}

// toolRotateSecret proxies to the admin /secrets/{name}[/{id}] endpoints. It
// never mutates the Hookaidofile — the pool registry lives in the running
// Hookaido process, so the call must go over admin HTTP.
func (s *Server) toolRotateSecret(args map[string]any) (any, error) {
	if err := validateAllowedKeys(args, rotateSecretAllowedKeys, "arguments"); err != nil {
		return nil, err
	}

	operation, err := parseRequiredString(args, "operation")
	if err != nil {
		return nil, err
	}
	operation = strings.ToLower(strings.TrimSpace(operation))

	name, err := parseRequiredString(args, "name")
	if err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if !config.IsValidManagementLabel(name) {
		return nil, fmt.Errorf("name %q does not match management label pattern %s", name, config.ManagementLabelPattern())
	}

	compiled, res, err := s.loadCompiledConfig()
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return nil, fmt.Errorf("config compile failed: %s", strings.Join(res.Errors, "; "))
	}

	switch operation {
	case "add":
		return s.rotateSecretAdd(compiled, name, args)
	case "list":
		return s.rotateSecretList(compiled, name, args)
	case "delete":
		return s.rotateSecretDelete(compiled, name, args)
	default:
		return nil, fmt.Errorf("operation must be one of add|list|delete (got %q)", operation)
	}
}

func (s *Server) rotateSecretAdd(compiled config.Compiled, name string, args map[string]any) (any, error) {
	value, err := parseRequiredString(args, "value")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("value must not be empty")
	}

	notBefore, err := parseOptionalTimestamp(args, "not_before")
	if err != nil {
		return nil, err
	}
	notAfter, err := parseOptionalTimestamp(args, "not_after")
	if err != nil {
		return nil, err
	}
	if !notAfter.IsZero() && !notBefore.IsZero() && !notAfter.After(notBefore) {
		return nil, errors.New("not_after must be after not_before")
	}

	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}

	body := map[string]any{"value": value}
	if !notBefore.IsZero() {
		body["not_before"] = notBefore.UTC().Format(time.RFC3339Nano)
	}
	if !notAfter.IsZero() {
		body["not_after"] = notAfter.UTC().Format(time.RFC3339Nano)
	}

	out, err := s.callAdminJSON(
		compiled,
		http.MethodPost,
		"/secrets/"+name,
		nil,
		body,
		mutationAuditHeaders(audit),
		defaultAdminProxyTimeout,
	)
	if err != nil {
		return nil, err
	}
	return withAuditPrincipal(map[string]any{
		"operation": "add",
		"name":      name,
		"response":  out,
	}, s.auditPrincipal()), nil
}

func (s *Server) rotateSecretList(compiled config.Compiled, name string, args map[string]any) (any, error) {
	// list is read-only; reject mutation-only fields if present.
	for _, mutOnly := range []string{"value", "id", "not_before", "not_after", "reason", "actor", "request_id"} {
		if _, present := args[mutOnly]; present {
			return nil, fmt.Errorf("list operation does not accept %q", mutOnly)
		}
	}
	out, err := s.callAdminJSON(
		compiled,
		http.MethodGet,
		"/secrets/"+name,
		nil,
		nil,
		nil,
		defaultAdminProxyTimeout,
	)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"operation": "list",
		"name":      name,
		"response":  out,
	}, nil
}

func (s *Server) rotateSecretDelete(compiled config.Compiled, name string, args map[string]any) (any, error) {
	id, err := parseRequiredString(args, "id")
	if err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if !config.IsValidManagementLabel(id) {
		return nil, fmt.Errorf("id %q does not match management label pattern %s", id, config.ManagementLabelPattern())
	}

	audit, err := parseMutationAuditArgs(args, s.auditPrincipal())
	if err != nil {
		return nil, err
	}

	out, err := s.callAdminJSON(
		compiled,
		http.MethodDelete,
		"/secrets/"+name+"/"+id,
		nil,
		nil,
		mutationAuditHeaders(audit),
		defaultAdminProxyTimeout,
	)
	if err != nil {
		return nil, err
	}
	return withAuditPrincipal(map[string]any{
		"operation": "delete",
		"name":      name,
		"id":        id,
		"response":  out,
	}, s.auditPrincipal()), nil
}

// parseOptionalTimestamp parses an RFC3339 timestamp from args[key]. Empty /
// missing returns zero time with no error. Invalid values return an error.
func parseOptionalTimestamp(args map[string]any, key string) (time.Time, error) {
	raw, err := parseString(args, key)
	if err != nil {
		return time.Time{}, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t2, err2 := time.Parse(time.RFC3339, raw)
		if err2 != nil {
			return time.Time{}, fmt.Errorf("%s must be RFC3339 (got %q)", key, raw)
		}
		t = t2
	}
	return t.UTC(), nil
}
