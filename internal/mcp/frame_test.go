package mcp

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Content-Length sized the payload buffer with only a negative check, so one
// header line could ask the process to reserve an arbitrary amount of memory
// before any content was read.
func TestReadFrame_RejectsOversizedContentLength(t *testing.T) {
	// Declare far more than the cap, and send no body at all: if the limit is
	// missing, this allocates the declared size before failing on the short read.
	frame := fmt.Sprintf("Content-Length: %d\r\n\r\n", int64(maxFrameBytes)+1)
	r := bufio.NewReader(strings.NewReader(frame))

	payload, err := readFrame(r)
	if err == nil {
		t.Fatalf("expected an error for a %d byte frame, got %d bytes", int64(maxFrameBytes)+1, len(payload))
	}
	if !strings.Contains(err.Error(), "frame limit") {
		t.Fatalf("expected a frame-limit error, got %v", err)
	}
}

// A frame exactly at the cap is still accepted, so the limit rejects only what
// is genuinely over it.
func TestReadFrame_AcceptsFrameAtTheLimit(t *testing.T) {
	body := strings.Repeat("a", 64)
	frame := fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
	r := bufio.NewReader(strings.NewReader(frame))

	payload, err := readFrame(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(payload) != body {
		t.Fatalf("payload = %q, want %q", payload, body)
	}
}

func TestReadFrame_RejectsNegativeAndMissingContentLength(t *testing.T) {
	cases := []struct {
		name  string
		frame string
		want  string
	}{
		{name: "negative", frame: "Content-Length: -1\r\n\r\n", want: "invalid content length"},
		{name: "not a number", frame: "Content-Length: abc\r\n\r\n", want: "invalid content length"},
		{name: "missing", frame: "X-Other: 1\r\n\r\n", want: "missing content length"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tc.frame))
			_, err := readFrame(r)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestReadFrame_EOFIsPropagated(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(""))
	if _, err := readFrame(r); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}
