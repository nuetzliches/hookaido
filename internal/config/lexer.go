package config

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokString
	tokLBrace
	tokRBrace
	tokComment
)

type token struct {
	kind tokenKind
	text string
	pos  position
}

type position struct {
	line int
	col  int
}

func (p position) String() string {
	return fmt.Sprintf("%d:%d", p.line, p.col)
}

type lexer struct {
	src  string
	i    int
	line int
	col  int
}

func newLexer(src string) *lexer {
	return &lexer{
		src:  src,
		i:    0,
		line: 1,
		col:  1,
	}
}

func (l *lexer) nextToken() (token, error) {
	for {
		if l.i >= len(l.src) {
			return token{kind: tokEOF, pos: position{line: l.line, col: l.col}}, nil
		}

		r, size := utf8.DecodeRuneInString(l.src[l.i:])
		if r == utf8.RuneError && size == 1 {
			return token{}, fmt.Errorf("invalid utf-8 at %d:%d", l.line, l.col)
		}

		if isSpace(r) {
			l.consumeRune(r, size)
			continue
		}

		pos := position{line: l.line, col: l.col}
		switch r {
		case '{':
			if text, ok, err := l.readPlaceholder(); err != nil {
				return token{}, err
			} else if ok {
				return token{kind: tokIdent, text: text, pos: pos}, nil
			}
			l.consumeRune(r, size)
			return token{kind: tokLBrace, text: "{", pos: pos}, nil
		case '#':
			// Comment until end of line (including the leading '#').
			start := l.i
			for l.i < len(l.src) {
				r2, size2 := utf8.DecodeRuneInString(l.src[l.i:])
				if r2 == '\n' {
					break
				}
				l.consumeRune(r2, size2)
			}
			return token{kind: tokComment, text: l.src[start:l.i], pos: pos}, nil
		case '}':
			l.consumeRune(r, size)
			return token{kind: tokRBrace, text: "}", pos: pos}, nil
		case '"':
			s, err := l.readString()
			if err != nil {
				return token{}, err
			}
			return token{kind: tokString, text: s, pos: pos}, nil
		default:
			ident := l.readIdent()
			return token{kind: tokIdent, text: ident, pos: pos}, nil
		}
	}
}

func (l *lexer) readPlaceholder() (string, bool, error) {
	start := l.i
	if strings.HasPrefix(l.src[start:], "{$") {
		// ok
	} else if strings.HasPrefix(l.src[start:], "{env.") {
		// ok
	} else if strings.HasPrefix(l.src[start:], "{file.") {
		// ok
	} else {
		return "", false, nil
	}

	// Current rune is '{'
	ti := l.i
	r, size := utf8.DecodeRuneInString(l.src[ti:])
	if r == utf8.RuneError && size == 1 {
		return "", false, fmt.Errorf("invalid utf-8 at %d:%d", l.line, l.col)
	}
	ti += size

	for ti < len(l.src) {
		r2, size2 := utf8.DecodeRuneInString(l.src[ti:])
		if r2 == utf8.RuneError && size2 == 1 {
			return "", false, fmt.Errorf("invalid utf-8 at %d:%d", l.line, l.col)
		}
		if isSpace(r2) || r2 == '{' {
			return "", false, nil
		}
		ti += size2
		if r2 == '}' {
			for l.i < ti {
				r3, size3 := utf8.DecodeRuneInString(l.src[l.i:])
				l.consumeRune(r3, size3)
			}
			return l.src[start:ti], true, nil
		}
	}
	return "", false, nil
}

func (l *lexer) readIdent() string {
	start := l.i
	for l.i < len(l.src) {
		r, size := utf8.DecodeRuneInString(l.src[l.i:])
		if isSpace(r) || r == '{' || r == '}' || r == '"' || r == '#' {
			break
		}
		l.consumeRune(r, size)
	}
	return l.src[start:l.i]
}

func (l *lexer) readString() (string, error) {
	// Consume opening quote.
	r, size := utf8.DecodeRuneInString(l.src[l.i:])
	if r != '"' {
		return "", fmt.Errorf("internal error: expected '\"' at %d:%d", l.line, l.col)
	}
	l.consumeRune(r, size)

	var out []rune
	for {
		if l.i >= len(l.src) {
			return "", fmt.Errorf("unterminated string at %d:%d", l.line, l.col)
		}
		r, size := utf8.DecodeRuneInString(l.src[l.i:])
		if r == utf8.RuneError && size == 1 {
			return "", fmt.Errorf("invalid utf-8 at %d:%d", l.line, l.col)
		}
		if r == '\n' {
			return "", fmt.Errorf("unterminated string at %d:%d", l.line, l.col)
		}
		if r == '"' {
			l.consumeRune(r, size)
			return string(out), nil
		}
		if r == '\\' {
			l.consumeRune(r, size)
			if l.i >= len(l.src) {
				return "", fmt.Errorf("unterminated escape at %d:%d", l.line, l.col)
			}
			er, esize := utf8.DecodeRuneInString(l.src[l.i:])
			if er == utf8.RuneError && esize == 1 {
				return "", fmt.Errorf("invalid utf-8 at %d:%d", l.line, l.col)
			}
			l.consumeRune(er, esize)
			switch er {
			case '\\', '"':
				out = append(out, er)
			case 'n':
				out = append(out, '\n')
			case 't':
				out = append(out, '\t')
			case 'r':
				out = append(out, '\r')
			default:
				// Keep unknown escapes as-is (best-effort).
				out = append(out, er)
			}
			continue
		}

		l.consumeRune(r, size)
		out = append(out, r)
	}
}

func (l *lexer) consumeRune(r rune, size int) {
	l.i += size
	if r == '\n' {
		l.line++
		l.col = 1
		return
	}
	l.col++
}

func isSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}
