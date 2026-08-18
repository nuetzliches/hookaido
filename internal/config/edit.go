package config

import (
	"bytes"
	"errors"
	"slices"
	"strings"
)

// ErrRewriteUnsupported reports that a source-preserving rewrite could not be
// produced for this edit. Callers fall back to Format, which is always correct
// but regenerates the file from the AST and therefore drops in-body comments,
// blank lines and operator formatting.
var ErrRewriteUnsupported = errors.New("config: source-preserving rewrite not supported for this edit")

// RewriteManagedEndpoints returns src with the `application` and
// `endpoint_name` directives of every route adjusted to match updated, leaving
// every other byte of the file untouched.
//
// This exists because the Admin API rewrites the Hookaidofile on every applied
// managed-endpoint mutation. Doing that through Format regenerates the file
// from the AST, and the AST keeps comments only in Config.Preamble -- so a
// single admin call used to delete every in-body comment (route annotations,
// rotation notes) from the file the project declares the source of truth.
//
// The rewrite is deliberately narrow. It only ever touches the two managed
// endpoint directives, and it verifies its own output: the spliced source is
// reparsed and its canonical form compared against the canonical form of
// updated. Any difference -- an edit this function does not model, an
// unexpected source shape -- yields ErrRewriteUnsupported rather than a file
// that does not match the intended config.
//
// src is normalized the way Parse normalizes it (BOM stripped, CRLF folded to
// LF), so a CRLF file comes back with LF endings, as it would from Format.
func RewriteManagedEndpoints(src []byte, updated *Config) ([]byte, error) {
	if updated == nil {
		return nil, ErrRewriteUnsupported
	}

	norm := normalizeInput(src)
	original, err := Parse(norm)
	if err != nil {
		return nil, err
	}
	if len(original.Routes) != len(updated.Routes) {
		return nil, ErrRewriteUnsupported
	}

	spans, err := scanRouteSpans(string(norm))
	if err != nil {
		return nil, err
	}
	if len(spans) != len(original.Routes) {
		return nil, ErrRewriteUnsupported
	}

	var edits []sourceEdit
	for i := range original.Routes {
		before, after := original.Routes[i], updated.Routes[i]
		if before.Path != after.Path || spans[i].path != before.Path {
			return nil, ErrRewriteUnsupported
		}
		routeEdits, err := managedEndpointEdits(string(norm), spans[i], before, after)
		if err != nil {
			return nil, err
		}
		edits = append(edits, routeEdits...)
	}

	out, err := applyEdits(norm, edits)
	if err != nil {
		return nil, err
	}

	// Self-check: the spliced file must mean exactly what updated means.
	// Comparing canonical forms rather than the ASTs keeps this honest without
	// depending on struct equality across every block type.
	want, err := Format(updated)
	if err != nil {
		return nil, err
	}
	reparsed, err := Parse(out)
	if err != nil {
		return nil, ErrRewriteUnsupported
	}
	got, err := Format(reparsed)
	if err != nil {
		return nil, ErrRewriteUnsupported
	}
	if !bytes.Equal(got, want) {
		return nil, ErrRewriteUnsupported
	}
	return out, nil
}

// HasInBodyComments reports whether src carries comments that Format would
// drop, i.e. comments that are not part of the leading preamble.
func HasInBodyComments(src []byte) bool {
	lex := newLexer(string(normalizeInput(src)))
	sawStmt := false
	for {
		tok, err := lex.nextToken()
		if err != nil || tok.kind == tokEOF {
			return false
		}
		if tok.kind == tokComment {
			if sawStmt {
				return true
			}
			continue
		}
		sawStmt = true
	}
}

// routeSpan locates one route block and its managed-endpoint directives in the
// source, as byte offsets.
type routeSpan struct {
	path string

	headStart int // first byte of the route path token
	bodyStart int // first byte after the opening '{'
	bodyEnd   int // offset of the closing '}'

	application  *stmtSpan
	endpointName *stmtSpan
}

// stmtSpan covers a directive from its name to the end of its value.
type stmtSpan struct {
	start int
	end   int
}

type sourceEdit struct {
	start int
	end   int
	text  string
}

// scanRouteSpans walks the token stream and records, for each top-level route
// block, where it starts and where its managed-endpoint directives sit.
func scanRouteSpans(src string) ([]routeSpan, error) {
	lex := newLexer(src)

	var (
		spans   []routeSpan
		depth   int
		prev    token
		hasPrev bool
		inRoute bool
	)

	for {
		tok, err := lex.nextToken()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			break
		}
		if tok.kind == tokComment {
			continue
		}

		switch tok.kind {
		case tokLBrace:
			depth++
			if depth == 1 && hasPrev && strings.HasPrefix(prev.text, "/") {
				spans = append(spans, routeSpan{
					path:      prev.text,
					headStart: prev.off,
					bodyStart: tok.end,
				})
				inRoute = true
			}
		case tokRBrace:
			depth--
			if depth < 0 {
				return nil, ErrRewriteUnsupported
			}
			if depth == 0 && inRoute {
				spans[len(spans)-1].bodyEnd = tok.off
				inRoute = false
			}
		case tokIdent:
			// Only directives at the route body's own level are managed
			// endpoints; anything deeper belongs to a nested block.
			if !inRoute || depth != 1 {
				break
			}
			if tok.text != "application" && tok.text != "endpoint_name" {
				break
			}
			val, err := lex.nextToken()
			if err != nil {
				return nil, err
			}
			if val.kind != tokIdent && val.kind != tokString {
				return nil, ErrRewriteUnsupported
			}
			stmt := &stmtSpan{start: tok.off, end: val.end}
			cur := &spans[len(spans)-1]
			if tok.text == "application" {
				if cur.application != nil {
					return nil, ErrRewriteUnsupported
				}
				cur.application = stmt
			} else {
				if cur.endpointName != nil {
					return nil, ErrRewriteUnsupported
				}
				cur.endpointName = stmt
			}
			prev = val
			hasPrev = true
			continue
		}

		prev = tok
		hasPrev = true
	}

	if depth != 0 || inRoute {
		return nil, ErrRewriteUnsupported
	}
	return spans, nil
}

// managedEndpointEdits produces the splices that turn before's managed-endpoint
// directives into after's.
func managedEndpointEdits(src string, span routeSpan, before, after Route) ([]sourceEdit, error) {
	var edits []sourceEdit

	type directive struct {
		name   string
		span   *stmtSpan
		set    bool
		value  string
		quoted bool
		change bool
	}
	directives := []directive{
		{
			name:   "application",
			span:   span.application,
			set:    after.ApplicationSet,
			value:  after.Application,
			quoted: after.ApplicationQuoted,
			change: before.ApplicationSet != after.ApplicationSet || before.Application != after.Application || before.ApplicationQuoted != after.ApplicationQuoted,
		},
		{
			name:   "endpoint_name",
			span:   span.endpointName,
			set:    after.EndpointNameSet,
			value:  after.EndpointName,
			quoted: after.EndpointNameQuoted,
			change: before.EndpointNameSet != after.EndpointNameSet || before.EndpointName != after.EndpointName || before.EndpointNameQuoted != after.EndpointNameQuoted,
		},
	}

	// Insertions all land at the top of the block body, so they are emitted in
	// declaration order to keep application above endpoint_name.
	indent := bodyIndent(src, span)
	insertAt := span.bodyStart
	var inserted []string

	for _, d := range directives {
		if !d.change {
			continue
		}
		switch {
		case d.set && d.span != nil:
			edits = append(edits, sourceEdit{
				start: d.span.start,
				end:   d.span.end,
				text:  d.name + " " + formatValue(d.value, d.quoted),
			})
		case d.set:
			if !insertableBody(src, span) {
				return nil, ErrRewriteUnsupported
			}
			inserted = append(inserted, "\n"+indent+d.name+" "+formatValue(d.value, d.quoted))
		case d.span != nil:
			edits = append(edits, removeStatement(src, *d.span))
		}
	}

	if len(inserted) > 0 {
		edits = append(edits, sourceEdit{start: insertAt, end: insertAt, text: strings.Join(inserted, "")})
	}
	return edits, nil
}

// insertableBody reports whether the block body starts on its own line, which
// is what makes inserting a directive line after the opening brace safe. A
// single-line block (`/hooks { publish }`) falls back to the canonical rewrite
// rather than producing a mangled line.
func insertableBody(src string, span routeSpan) bool {
	for i := span.bodyStart; i < span.bodyEnd && i < len(src); i++ {
		switch src[i] {
		case ' ', '\t':
		case '\n':
			return true
		default:
			return false
		}
	}
	return false
}

// bodyIndent returns the indentation the block's existing directives use, or
// the route's own indentation plus one level when the body has none.
func bodyIndent(src string, span routeSpan) string {
	body := src[span.bodyStart:min(span.bodyEnd, len(src))]
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		return line[:len(line)-len(strings.TrimLeft(line, " \t"))]
	}
	return lineIndent(src, span.headStart) + "  "
}

// lineIndent returns the leading whitespace of the line that off sits on.
func lineIndent(src string, off int) string {
	start := strings.LastIndexByte(src[:off], '\n') + 1
	prefix := src[start:off]
	return prefix[:len(prefix)-len(strings.TrimLeft(prefix, " \t"))]
}

// removeStatement deletes a directive, taking its whole line with it when the
// line holds nothing else. A trailing comment on the same line is kept.
func removeStatement(src string, stmt stmtSpan) sourceEdit {
	start, end := stmt.start, stmt.end

	lineStart := strings.LastIndexByte(src[:start], '\n') + 1
	if strings.TrimLeft(src[lineStart:start], " \t") != "" {
		return sourceEdit{start: start, end: end}
	}

	rest := src[end:]
	nl := strings.IndexByte(rest, '\n')
	tail := rest
	if nl >= 0 {
		tail = rest[:nl]
	}
	if strings.TrimSpace(tail) != "" {
		// Something else follows on the line (typically a comment). Drop only
		// the directive itself so the comment survives.
		return sourceEdit{start: start, end: end}
	}
	if nl < 0 {
		return sourceEdit{start: lineStart, end: len(src)}
	}
	return sourceEdit{start: lineStart, end: end + nl + 1}
}

func applyEdits(src []byte, edits []sourceEdit) ([]byte, error) {
	if len(edits) == 0 {
		return append([]byte(nil), src...), nil
	}
	// Stable, so two edits at the same offset (the paired insertions) keep the
	// order they were produced in.
	sorted := slices.Clone(edits)
	slices.SortStableFunc(sorted, func(a, b sourceEdit) int { return a.start - b.start })

	var out bytes.Buffer
	prev := 0
	for _, e := range sorted {
		if e.start < prev || e.end < e.start || e.end > len(src) {
			return nil, ErrRewriteUnsupported
		}
		out.Write(src[prev:e.start])
		out.WriteString(e.text)
		prev = e.end
	}
	out.Write(src[prev:])
	return out.Bytes(), nil
}
