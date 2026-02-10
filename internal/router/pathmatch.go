package router

import "strings"

// MatchPath implements the MVP routing rule:
// - exact match OR
// - prefix match on a segment boundary ("/path" matches "/path" and "/path/...")
//
// Query strings are not considered (caller should pass URL.Path only).
func MatchPath(requestPath, routePath string) bool {
	if routePath == "" {
		return false
	}
	if routePath == "/" {
		return true
	}
	if requestPath == routePath {
		return true
	}
	if strings.HasPrefix(requestPath, routePath) && len(requestPath) > len(routePath) && requestPath[len(routePath)] == '/' {
		return true
	}
	return false
}
