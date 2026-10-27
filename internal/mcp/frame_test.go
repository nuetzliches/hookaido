package mcp

import (
	"bufio"
	"context"
	"encoding/json"
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

// The MCP stdio transport is newline-delimited JSON. A server that only spoke
// Content-Length framing read such a message as a header line and then waited
// for a blank line that never came, so Claude Code and OpenClaw timed out.
func TestReadMessage_DetectsFramingPerMessage(t *testing.T) {
	cl := `{"id":1}`
	stream := fmt.Sprintf("{\"id\":0}\n\r\nContent-Length: %d\r\n\r\n%s{\"id\":2}\r\n{\"id\":3}", len(cl), cl)
	r := bufio.NewReader(strings.NewReader(stream))

	want := []struct {
		payload string
		mode    frameMode
	}{
		{`{"id":0}`, frameNewline},
		{`{"id":1}`, frameContentLength},
		{`{"id":2}`, frameNewline},
		// The last message may arrive without a trailing newline.
		{`{"id":3}`, frameNewline},
	}
	for i, w := range want {
		payload, mode, err := readMessage(r)
		if err != nil {
			t.Fatalf("message %d: unexpected error: %v", i, err)
		}
		if string(payload) != w.payload || mode != w.mode {
			t.Fatalf("message %d = (%q, %v), want (%q, %v)", i, payload, mode, w.payload, w.mode)
		}
	}
	if _, _, err := readMessage(r); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

// An oversized newline-delimited message is rejected without buffering it, and
// the stream stays usable: the next message is read normally.
func TestReadMessage_RejectsOversizedLineAndResyncs(t *testing.T) {
	big := "{" + strings.Repeat("a", maxFrameBytes+1) + "}\n"
	r := bufio.NewReader(strings.NewReader(big + `{"id":7}` + "\n"))

	if _, mode, err := readMessage(r); err == nil || !strings.Contains(err.Error(), "frame limit") || mode != frameNewline {
		t.Fatalf("expected a frame-limit error in newline mode, got mode %v, err %v", mode, err)
	}
	payload, mode, err := readMessage(r)
	if err != nil || string(payload) != `{"id":7}` || mode != frameNewline {
		t.Fatalf("next message = (%q, %v, %v), want the following message", payload, mode, err)
	}
}

func TestServe_AnswersInTheClientsFraming(t *testing.T) {
	initialize := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	ping := `{"jsonrpc":"2.0","id":2,"method":"ping"}`

	t.Run("newline-delimited", func(t *testing.T) {
		var out strings.Builder
		s := NewServer(strings.NewReader(initialize+"\n"+ping+"\n"), &out, "", "")
		if err := s.Serve(context.Background()); err != nil {
			t.Fatalf("serve: %v", err)
		}
		lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 newline-delimited responses, got %q", out.String())
		}
		for i, line := range lines {
			var resp map[string]any
			if err := json.Unmarshal([]byte(line), &resp); err != nil {
				t.Fatalf("response %d is not a single JSON line: %q (%v)", i, line, err)
			}
			if id, _ := resp["id"].(float64); int(id) != i+1 {
				t.Fatalf("response %d id = %v, want %d", i, resp["id"], i+1)
			}
		}
	})

	t.Run("content-length", func(t *testing.T) {
		var out strings.Builder
		in := fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(initialize), initialize)
		s := NewServer(strings.NewReader(in), &out, "", "")
		if err := s.Serve(context.Background()); err != nil {
			t.Fatalf("serve: %v", err)
		}
		if !strings.HasPrefix(out.String(), "Content-Length: ") {
			t.Fatalf("expected a Content-Length framed response, got %q", out.String())
		}
	})

	t.Run("parse error keeps the framing", func(t *testing.T) {
		var out strings.Builder
		s := NewServer(strings.NewReader("{not json}\n"), &out, "", "")
		if err := s.Serve(context.Background()); err != nil {
			t.Fatalf("serve: %v", err)
		}
		if !strings.HasPrefix(out.String(), "{") || !strings.Contains(out.String(), "-32700") {
			t.Fatalf("expected a newline-delimited parse error, got %q", out.String())
		}
	})
}
