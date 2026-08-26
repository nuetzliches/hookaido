package app

import (
	"net/http"
	"testing"

	"github.com/nuetzliches/hookaido/v2/internal/config"
)

// The matchers double as the only credential check available to an event source
// whose whole configuration surface is a URL, so their value comparison is
// constant-time. These tests pin the behaviour that comparison has to preserve.

func TestMatchHeaderValues(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
		want     bool
	}{
		{name: "single exact", values: []string{"push"}, expected: "push", want: true},
		{name: "single mismatch", values: []string{"pull"}, expected: "push", want: false},
		{name: "no values", values: nil, expected: "push", want: false},
		{name: "second of several", values: []string{"pull", "push"}, expected: "push", want: true},
		{name: "comma list first", values: []string{"push, pull"}, expected: "push", want: true},
		{name: "comma list last", values: []string{"pull, push"}, expected: "push", want: true},
		{name: "comma list mismatch", values: []string{"pull, tag"}, expected: "push", want: false},
		{name: "surrounding space", values: []string{"  push  "}, expected: "push", want: true},
		{name: "empty expected matches empty value", values: []string{""}, expected: "", want: true},
		{name: "prefix is not a match", values: []string{"pushed"}, expected: "push", want: false},
		{name: "expected longer than value", values: []string{"pus"}, expected: "push", want: false},
		{name: "case sensitive", values: []string{"PUSH"}, expected: "push", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchHeaderValues(tt.values, tt.expected); got != tt.want {
				t.Fatalf("matchHeaderValues(%q, %q) = %v, want %v", tt.values, tt.expected, got, tt.want)
			}
		})
	}
}

func TestMatchQueryValues(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
		want     bool
	}{
		{name: "single exact", values: []string{"s3cr3t"}, expected: "s3cr3t", want: true},
		{name: "single mismatch", values: []string{"nope"}, expected: "s3cr3t", want: false},
		{name: "no values", values: nil, expected: "s3cr3t", want: false},
		{name: "second of several", values: []string{"nope", "s3cr3t"}, expected: "s3cr3t", want: true},
		{name: "comma is not split", values: []string{"nope,s3cr3t"}, expected: "s3cr3t", want: false},
		{name: "space is not trimmed", values: []string{" s3cr3t"}, expected: "s3cr3t", want: false},
		{name: "prefix is not a match", values: []string{"s3cr3"}, expected: "s3cr3t", want: false},
		{name: "empty expected matches empty value", values: []string{""}, expected: "", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchQueryValues(tt.values, tt.expected); got != tt.want {
				t.Fatalf("matchQueryValues(%q, %q) = %v, want %v", tt.values, tt.expected, got, tt.want)
			}
		})
	}
}

func TestMatchHeadersUsesConstantTimeCompare(t *testing.T) {
	h := http.Header{}
	h.Set("X-Token", "s3cr3t")

	if !matchHeaders(h, []config.HeaderMatchConfig{{Name: "X-Token", Value: "s3cr3t"}}, nil) {
		t.Fatal("expected the configured token to match")
	}
	if matchHeaders(h, []config.HeaderMatchConfig{{Name: "X-Token", Value: "s3cr3u"}}, nil) {
		t.Fatal("expected a one-byte difference not to match")
	}
	if matchHeaders(h, []config.HeaderMatchConfig{{Name: "X-Token", Value: ""}}, nil) {
		t.Fatal("expected an empty expectation not to match a non-empty value")
	}
}

func TestMatchQueryUsesConstantTimeCompare(t *testing.T) {
	values := map[string][]string{"t": {"s3cr3t"}}

	if !matchQuery(values, []config.QueryMatchConfig{{Name: "t", Value: "s3cr3t"}}, nil) {
		t.Fatal("expected the configured token to match")
	}
	if matchQuery(values, []config.QueryMatchConfig{{Name: "t", Value: "s3cr3u"}}, nil) {
		t.Fatal("expected a one-byte difference not to match")
	}
	if matchQuery(values, []config.QueryMatchConfig{{Name: "t", Value: "s3cr3t "}}, nil) {
		t.Fatal("expected a trailing space not to match")
	}
}

func TestConstantTimeEqual(t *testing.T) {
	if !constantTimeEqual("", "") {
		t.Fatal("empty strings should compare equal")
	}
	if !constantTimeEqual("abc", "abc") {
		t.Fatal("identical strings should compare equal")
	}
	if constantTimeEqual("abc", "abd") {
		t.Fatal("differing strings should not compare equal")
	}
	if constantTimeEqual("abc", "abcd") {
		t.Fatal("differing lengths should not compare equal")
	}
	if constantTimeEqual("abc", "") {
		t.Fatal("a non-empty string should not equal the empty string")
	}
}
