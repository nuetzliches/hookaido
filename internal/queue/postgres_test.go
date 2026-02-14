package queue

import (
	"strings"
	"testing"
)

func TestNewPostgresStore_EmptyDSN(t *testing.T) {
	_, err := NewPostgresStore("   ")
	if err == nil {
		t.Fatalf("expected error for empty dsn")
	}
	if !strings.Contains(err.Error(), "empty postgres dsn") {
		t.Fatalf("error = %v, want contains %q", err, "empty postgres dsn")
	}
}
