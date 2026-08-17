package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"strings"
	"testing"
)

func TestNewHTTPServer_SetsReadAndIdleTimeouts(t *testing.T) {
	srv := newHTTPServer(":8080", http.NotFoundHandler())

	if srv.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout=%s, want %s -- without it a client can hold a connection "+
			"open indefinitely by dribbling header bytes", srv.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if srv.IdleTimeout != httpIdleTimeout {
		t.Fatalf("IdleTimeout=%s, want %s", srv.IdleTimeout, httpIdleTimeout)
	}
	if srv.Addr != ":8080" {
		t.Fatalf("Addr=%q, want :8080", srv.Addr)
	}
	if srv.Handler == nil {
		t.Fatal("Handler is nil")
	}

	// Deliberately unset, not an oversight: WriteTimeout would truncate the
	// Pull API's SSE stream, and ReadTimeout would cap the time available to
	// read a legitimate max_body-sized payload over a slow link.
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout=%s, want 0 -- a write deadline truncates the SSE stream", srv.WriteTimeout)
	}
	if srv.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout=%s, want 0 -- a read deadline caps large-body uploads over slow links", srv.ReadTimeout)
	}
}

// Every listener in this package must be built through newHTTPServer so it
// inherits the timeouts above. A bare &http.Server{} literal elsewhere would
// silently reintroduce the unbounded-read exposure the helper exists to close,
// which is exactly how the original gap arose: two constructions, neither with
// a timeout, and nothing pointing that out.
func TestNoBareHTTPServerLiteralsOutsideHelper(t *testing.T) {
	fset := token.NewFileSet()
	// Test files are exempt: they stand up throwaway servers to exercise
	// serveOnListener and have no exposure to bound.
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if fn.Name.Name == "newHTTPServer" {
					continue
				}
				ast.Inspect(fn, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok {
						return true
					}
					sel, ok := lit.Type.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					ident, ok := sel.X.(*ast.Ident)
					if !ok || ident.Name != "http" || sel.Sel.Name != "Server" {
						return true
					}
					t.Errorf("%s: %s constructs http.Server directly; use newHTTPServer so the "+
						"read and idle timeouts are applied", fset.Position(lit.Pos()), fn.Name.Name)
					return true
				})
			}
		}
	}
}
