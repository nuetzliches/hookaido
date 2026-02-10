package router

import "net/http"

type Route struct {
	Name   string
	Path   string
	Target string
}

type MatchResult struct {
	Route Route
}

type Router interface {
	Match(r *http.Request) (MatchResult, bool)
}
