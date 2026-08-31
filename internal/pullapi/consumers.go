package pullapi

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sort"
	"time"
)

// ConsumerConnection describes one pull consumer that currently holds an open
// SSE stream.
//
// The connection gauge (`hookaido_pull_sse_connection_active`) answers how many
// consumers are attached; this answers which. That distinction matters because
// an unexpected second consumer on a route is not a visible failure: the queue
// is competing-consumer, so the two split the traffic and each of them observes
// a fluctuating fraction of the events arriving, which is indistinguishable
// from delivery loss from the inside. The count says there is a problem; the
// identity is what turns it into a fix.
//
// TokenRef names the configured secret reference the request authenticated
// with (`env.PULL_TOKEN`, `file./run/secrets/pull`), never the token value. It
// is empty when the Pull API runs without tokens, and it is deliberately the
// reference rather than a hash: an operator can map it back to a Hookaidofile
// line, which is what they need in order to decide whose credential is in use.
type ConsumerConnection struct {
	ID            string
	Route         string
	Endpoint      string
	RemoteAddr    string
	UserAgent     string
	TokenRef      string
	ConnectedAt   time.Time
	MessagesSent  int64
	LastMessageAt time.Time
}

// consumerConn is the mutable registry entry behind a ConsumerConnection.
//
// Only the two counters change over the life of a stream, and they are written
// by the stream's own goroutine while Consumers() reads them from an Admin API
// request. The registry mutex covers both, so a snapshot never observes a torn
// message count.
type consumerConn struct {
	id          string
	route       string
	endpoint    string
	remoteAddr  string
	userAgent   string
	tokenRef    string
	connectedAt time.Time

	messagesSent  int64
	lastMessageAt time.Time
}

func (s *Server) registerConsumer(r *http.Request, route string) *consumerConn {
	now := s.nowTime()
	c := &consumerConn{
		id:          newConsumerID(),
		route:       route,
		connectedAt: now,
	}
	if r != nil {
		c.remoteAddr = r.RemoteAddr
		c.userAgent = r.Header.Get("User-Agent")
		if r.URL != nil {
			c.endpoint = endpointFromPath(r.URL.Path)
		}
	}
	if s.IdentifyToken != nil {
		c.tokenRef = s.IdentifyToken(r)
	}

	s.consumersMu.Lock()
	if s.consumers == nil {
		s.consumers = make(map[string]*consumerConn)
	}
	s.consumers[c.id] = c
	snap := c.snapshotLocked()
	s.consumersMu.Unlock()

	s.observeSSEConnect(route)
	if s.ObserveConsumerConnect != nil {
		s.ObserveConsumerConnect(snap)
	}
	return c
}

// recordMessagesSent accounts for a batch that has been written to the stream.
func (s *Server) recordMessagesSent(c *consumerConn, n int) {
	if c == nil || n <= 0 {
		return
	}
	now := s.nowTime()
	s.consumersMu.Lock()
	c.messagesSent += int64(n)
	c.lastMessageAt = now
	s.consumersMu.Unlock()
}

// unregisterConsumer removes the stream from the registry and reports it.
//
// It is idempotent so callers can defer it without tracking whether an earlier
// return path already ran it.
func (s *Server) unregisterConsumer(c *consumerConn, statusCode int) {
	if c == nil {
		return
	}

	s.consumersMu.Lock()
	if _, ok := s.consumers[c.id]; !ok {
		s.consumersMu.Unlock()
		return
	}
	delete(s.consumers, c.id)
	snap := c.snapshotLocked()
	s.consumersMu.Unlock()

	duration := s.nowTime().Sub(snap.ConnectedAt)
	s.observeSSEDisconnect(snap.Route, statusCode, int(snap.MessagesSent), duration)
	if s.ObserveConsumerDisconnect != nil {
		s.ObserveConsumerDisconnect(snap, statusCode, duration)
	}
}

// Consumers returns the pull consumers with an open SSE stream right now,
// ordered by route and then by how long they have been connected.
//
// Only SSE streams appear here. A consumer that polls `POST .../dequeue` holds
// no connection between calls, so there is nothing to list and nothing the
// connection gauge counts either.
func (s *Server) Consumers() []ConsumerConnection {
	if s == nil {
		return nil
	}

	s.consumersMu.Lock()
	out := make([]ConsumerConnection, 0, len(s.consumers))
	for _, c := range s.consumers {
		out = append(out, c.snapshotLocked())
	}
	s.consumersMu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Route != out[j].Route {
			return out[i].Route < out[j].Route
		}
		if !out[i].ConnectedAt.Equal(out[j].ConnectedAt) {
			return out[i].ConnectedAt.Before(out[j].ConnectedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// snapshotLocked copies the entry. The caller must hold the registry mutex:
// the two counters are written by the stream's own goroutine, so an unlocked
// read here would race a concurrent send.
func (c *consumerConn) snapshotLocked() ConsumerConnection {
	return ConsumerConnection{
		ID:            c.id,
		Route:         c.route,
		Endpoint:      c.endpoint,
		RemoteAddr:    c.remoteAddr,
		UserAgent:     c.userAgent,
		TokenRef:      c.tokenRef,
		ConnectedAt:   c.connectedAt,
		MessagesSent:  c.messagesSent,
		LastMessageAt: c.lastMessageAt,
	}
}

func newConsumerID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "con_" + hex.EncodeToString(b[:])
}
