package ingress

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrUnauthorized = errors.New("unauthorized")

type HMACAuth struct {
	Secrets       [][]byte
	SelectSecrets func(at time.Time) [][]byte

	SignatureHeader string
	TimestampHeader string
	NonceHeader     string
	Tolerance       time.Duration
	Provider        string // "github", "gitea", or "" (canonical)

	Now func() time.Time

	nonce *nonceCache
}

func NewHMACAuth(secrets [][]byte) *HMACAuth {
	a := &HMACAuth{
		Secrets:         cloneByteSlices(secrets),
		SignatureHeader: "X-Signature",
		TimestampHeader: "X-Timestamp",
		NonceHeader:     "X-Nonce",
		Tolerance:       5 * time.Minute,
		Now:             time.Now,
	}
	a.nonce = newNonceCache(a.Now)
	return a
}

// AdoptNonceCache makes a share prev's replay-protection state.
//
// A config reload rebuilds every route's HMACAuth from scratch, and a fresh
// HMACAuth starts with an empty nonce cache -- so every reload used to forget
// every nonce seen inside the tolerance window and reopen the replay window
// for that long. Admin API managed-endpoint mutations reload too, so an
// attacker holding one captured signed request only had to wait for the next
// mutation, on any unrelated route, to replay it successfully.
//
// Sharing rather than copying is deliberate: if the reload fails after this
// point, the still-live old authorizer and the discarded new one address the
// same cache, so no claim is lost either way.
func (a *HMACAuth) AdoptNonceCache(prev *HMACAuth) {
	if a == nil || prev == nil || prev.nonce == nil {
		return
	}
	a.nonce = prev.nonce
}

// Verify checks:
// - timestamp header is present and within tolerance
// - nonce header is present and not reused within tolerance window
// - signature matches any configured secret
//
// String-to-sign:
//
//	ts + "\n" + method + "\n" + path + "\n" + hex(sha256(body))
//
// On success it returns a *NonceClaim, which the caller must Commit once the
// request is durably enqueued, or Release if it is not. The claim is nil for
// provider modes and for routes without HMAC auth; both methods are nil-safe,
// so `defer claim.Release()` is always valid.
func (a *HMACAuth) Verify(r *http.Request, requestPath string, body []byte) (*NonceClaim, error) {
	if a == nil {
		return nil, nil
	}
	if a.Provider != "" {
		return nil, a.verifyProvider(r, body)
	}
	if len(a.Secrets) == 0 && a.SelectSecrets == nil {
		return nil, nil
	}

	now := time.Now
	if a.Now != nil {
		now = a.Now
	}

	sigHex := strings.TrimSpace(r.Header.Get(a.SignatureHeader))
	tsStr := strings.TrimSpace(r.Header.Get(a.TimestampHeader))
	nonce := strings.TrimSpace(r.Header.Get(a.NonceHeader))
	if sigHex == "" || tsStr == "" || nonce == "" {
		return nil, ErrUnauthorized
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, ErrUnauthorized
	}
	t := time.Unix(ts, 0).UTC()
	if a.Tolerance > 0 {
		d := now().UTC().Sub(t)
		if d < -a.Tolerance || d > a.Tolerance {
			return nil, ErrUnauthorized
		}
	}

	if a.nonce == nil {
		a.nonce = newNonceCache(now)
	} else {
		a.nonce.setNow(now)
	}
	// Reject a nonce we could not remember before doing any work with it: the
	// route opted into replay protection, so a nonce we cannot track is not
	// something to wave through.
	if len(nonce) > nonceMaxLen {
		return nil, ErrUnauthorized
	}

	gotSig, err := hex.DecodeString(sigHex)
	if err != nil || len(gotSig) == 0 {
		return nil, ErrUnauthorized
	}

	bodyHash := sha256.Sum256(body)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s", tsStr, r.Method, requestPath, hex.EncodeToString(bodyHash[:]))
	msg := []byte(stringToSign)

	secrets := a.Secrets
	if a.SelectSecrets != nil {
		secrets = a.SelectSecrets(t)
	}
	if len(secrets) == 0 {
		return nil, ErrUnauthorized
	}

	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(msg)
		want := mac.Sum(nil)
		if subtle.ConstantTimeCompare(gotSig, want) == 1 {
			// Claim the nonce only now. Claiming it before verification let any
			// unauthenticated caller grow the cache, and let anyone who could
			// observe or predict a nonce claim it first so that the genuine
			// signed webhook carrying it was rejected as a replay.
			//
			// The claim is provisional: it blocks a concurrent replay
			// immediately, but only Commit makes it survive. See NonceClaim.
			seq, ok := a.nonce.claim(nonce, t.Add(a.Tolerance))
			if !ok {
				return nil, ErrUnauthorized
			}
			return &NonceClaim{cache: a.nonce, nonce: nonce, seq: seq}, nil
		}
	}

	return nil, ErrUnauthorized
}

func cloneByteSlices(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, b := range in {
		if len(b) == 0 {
			continue
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		out = append(out, cp)
	}
	return out
}

// allSecrets returns static secrets combined with SelectSecrets at the given time.
func (a *HMACAuth) allSecrets(at time.Time) [][]byte {
	var out [][]byte
	if a.SelectSecrets != nil {
		out = append(out, a.SelectSecrets(at)...)
	}
	out = append(out, a.Secrets...)
	return out
}

func (a *HMACAuth) verifyProvider(r *http.Request, body []byte) error {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	secrets := a.allSecrets(now())
	if len(secrets) == 0 {
		return ErrUnauthorized
	}

	switch a.Provider {
	case "github":
		return a.verifyGitHub(r, body, secrets)
	case "gitea":
		return a.verifyGitea(r, body, secrets)
	case "stripe":
		return a.verifyStripeLike(r, body, secrets, stripeProviderConfig)
	case "cituro":
		return a.verifyStripeLike(r, body, secrets, cituroProviderConfig)
	default:
		return ErrUnauthorized
	}
}

// stripeLikeConfig captures the wire-format differences between providers
// that otherwise share the Stripe-invented signing scheme: a header of the
// form "t=<unix-ts>,<sigTag>=<hex>[,...]" and a signed payload of
// "<ts>.<body>" hashed via HMAC-SHA256. Adding a new such provider is a
// one-line config addition plus a switch case -- no new code path.
type stripeLikeConfig struct {
	// Header is the HTTP header carrying the signature value.
	Header string
	// SigTag is the key within the header value holding the hex signature
	// (Stripe uses "v1" / "v0" for rotation, Cituro uses "s").
	SigTag string
	// TSUnit is the unit of the numeric t= field. Stripe emits Unix seconds,
	// Cituro emits Unix milliseconds; the PDF example "t=1592..." is
	// truncated and does not show the difference. Determined from live
	// headers.
	TSUnit time.Duration
}

var (
	// stripeProviderConfig matches docs.stripe.com/webhooks/signatures:
	// Stripe-Signature header, sig tag v1, 10-digit Unix seconds.
	stripeProviderConfig = stripeLikeConfig{
		Header: "Stripe-Signature",
		SigTag: "v1",
		TSUnit: time.Second,
	}
	// cituroProviderConfig matches Cituro's webhook API (cituro_API.pdf
	// §7.3): X-CITURO-SIGNATURE header, sig tag s, 13-digit Unix
	// milliseconds.
	cituroProviderConfig = stripeLikeConfig{
		Header: "X-CITURO-SIGNATURE",
		SigTag: "s",
		TSUnit: time.Millisecond,
	}
)

func (a *HMACAuth) verifyGitHub(r *http.Request, body []byte, secrets [][]byte) error {
	sigHeader := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256"))
	if sigHeader == "" {
		return ErrUnauthorized
	}
	if !strings.HasPrefix(sigHeader, "sha256=") {
		return ErrUnauthorized
	}
	gotSig, err := hex.DecodeString(sigHeader[len("sha256="):])
	if err != nil || len(gotSig) == 0 {
		return ErrUnauthorized
	}
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(body)
		want := mac.Sum(nil)
		if hmac.Equal(gotSig, want) {
			return nil
		}
	}
	return ErrUnauthorized
}

func (a *HMACAuth) verifyGitea(r *http.Request, body []byte, secrets [][]byte) error {
	sigHeader := strings.TrimSpace(r.Header.Get("X-Gitea-Signature"))
	if sigHeader == "" {
		return ErrUnauthorized
	}
	gotSig, err := hex.DecodeString(sigHeader)
	if err != nil || len(gotSig) == 0 {
		return ErrUnauthorized
	}
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(body)
		want := mac.Sum(nil)
		if hmac.Equal(gotSig, want) {
			return nil
		}
	}
	return ErrUnauthorized
}

// verifyStripeLike verifies HMAC signatures in the Stripe-invented scheme
// used by Stripe, Cituro, and compatible providers.
//
// Wire format (per provider's cfg):
//
//	Header: cfg.Header (e.g. Stripe-Signature, X-CITURO-SIGNATURE)
//	Value:  "t=<unix-ts>,<cfg.SigTag>=<hex>[,<cfg.SigTag>=<hex>...]"
//	  ts is interpreted per cfg.TSUnit (seconds for Stripe, ms for Cituro)
//
// String-to-sign: "<ts>.<body>" hashed via HMAC-SHA256, lowercase hex. The
// ts used in the signed payload is the *raw string* from the header, so
// senders that emit ms-precision and those emitting second-precision both
// work as long as their cfg.TSUnit matches.
//
// Replay protection: the timestamp must be within a.Tolerance of the
// current time (default 5m). No nonce — retries within the tolerance
// window are accepted, which matches Stripe/Cituro semantics.
//
// Diagnostic WARN logs fire on every rejection path (header_missing,
// parse_incomplete, ts_parse_error, ts_out_of_tolerance,
// sig_hex_decode_error, no_secret_matched). They describe only what the
// caller sent — byte counts and bounded prefixes of received values.
//
// Nothing derived from a secret is logged, not even truncated. Both halves
// of the signed message are attacker-controlled, so any value computed as
// HMAC(secret, msg) would be a chosen-message oracle regardless of how few
// bits of it were emitted. "never raw secrets or full signatures" is not a
// strong enough rule here, and reading it that way is what previously
// admitted a truncated expected-MAC into the no_secret_matched line.
func (a *HMACAuth) verifyStripeLike(r *http.Request, body []byte, secrets [][]byte, cfg stripeLikeConfig) error {
	raw := strings.TrimSpace(r.Header.Get(cfg.Header))
	if raw == "" {
		slog.Warn("hmac_stripe_failed", "reason", "header_missing", "header", cfg.Header)
		return ErrUnauthorized
	}

	// Every value carrying the configured tag is a candidate, not just the
	// first. Stripe signs with *all* active secrets while an endpoint secret is
	// rolled with an expiration window, emitting several `v1=` entries in one
	// header, and its own libraries compare against each of them. Keeping only
	// the first rejected validly signed webhooks with 401 for the whole roll
	// window -- up to 24h -- whenever the signature matching the configured
	// secret was not listed first.
	var tsStr string
	var sigHexes []string
	pairs := 0
	for _, part := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		pairs++
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "t":
			if tsStr == "" {
				tsStr = val
			}
		case cfg.SigTag:
			if val != "" && len(sigHexes) < stripeMaxSignatures {
				sigHexes = append(sigHexes, val)
			}
		}
	}
	if tsStr == "" || len(sigHexes) == 0 {
		// Never log `raw` here: this path is reached with attacker-controlled
		// input, and a header that is missing "t=" can still carry a full
		// signature (and vice versa). `pairs_parsed` alongside the *configured*
		// `sig_tag` pins down the realistic cause -- the sender emits a
		// different tag name than the route expects -- without moving header
		// material into the log.
		slog.Warn("hmac_stripe_failed", "reason", "parse_incomplete",
			"header", cfg.Header, "sig_tag", cfg.SigTag,
			"ts_present", tsStr != "", "sig_present", len(sigHexes) > 0,
			"pairs_parsed", pairs, "header_value_len", len(raw))
		return ErrUnauthorized
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		// tsStr is attacker-controlled header material, so none of it reaches the
		// log: only its length and a classification of why it would not parse.
		// This matches the parse_incomplete path above, which reports
		// pairs_parsed and header_value_len rather than the header value.
		//
		// err.Error() is deliberately not logged: strconv.ParseInt returns a
		// *strconv.NumError whose message embeds the entire input, so passing
		// it through would reintroduce the echo by the back door. The
		// classification below carries the same diagnostic value -- a sender
		// emitting a non-numeric t= is not_a_number, one emitting an oversized
		// numeric is out_of_range.
		cause := "invalid"
		switch {
		case errors.Is(err, strconv.ErrRange):
			cause = "out_of_range"
		case errors.Is(err, strconv.ErrSyntax):
			cause = "not_a_number"
		}
		slog.Warn("hmac_stripe_failed", "reason", "ts_parse_error",
			"header", cfg.Header,
			"ts_len", len(tsStr),
			"cause", cause)
		return ErrUnauthorized
	}

	tolerance := a.Tolerance
	if tolerance <= 0 {
		tolerance = 5 * time.Minute
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	// time.Unix(0, ns) takes nanoseconds; cfg.TSUnit (a time.Duration =
	// int64 ns) scales the raw numeric ts into the correct absolute time.
	t := time.Unix(0, ts*int64(cfg.TSUnit)).UTC()
	if d := now().UTC().Sub(t); d < -tolerance || d > tolerance {
		slog.Warn("hmac_stripe_failed", "reason", "ts_out_of_tolerance",
			"header", cfg.Header, "ts_unix", ts, "ts_unit", cfg.TSUnit.String(),
			"delta_seconds", d.Seconds(), "tolerance_seconds", tolerance.Seconds())
		return ErrUnauthorized
	}

	// An entry that is not hex is skipped rather than fatal: the header may
	// legitimately carry several signatures, and one malformed value must not
	// reject a request that another value signs correctly.
	gotSigs := make([][]byte, 0, len(sigHexes))
	for _, sigHex := range sigHexes {
		sig, err := hex.DecodeString(sigHex)
		if err != nil || len(sig) == 0 {
			slog.Warn("hmac_stripe_failed", "reason", "sig_hex_decode_error",
				"header", cfg.Header, "sig_hex_len", len(sigHex), "sig_hex_prefix", safePrefix(sigHex, 6))
			continue
		}
		gotSigs = append(gotSigs, sig)
	}
	if len(gotSigs) == 0 {
		return ErrUnauthorized
	}

	msg := make([]byte, 0, len(tsStr)+1+len(body))
	msg = append(msg, tsStr...)
	msg = append(msg, '.')
	msg = append(msg, body...)

	// One HMAC per secret, then a constant-time compare against each candidate
	// signature: the cost stays linear in the number of secrets no matter how
	// many signatures the header carries.
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(msg)
		want := mac.Sum(nil)
		for _, gotSig := range gotSigs {
			if hmac.Equal(gotSig, want) {
				return nil
			}
		}
	}

	// No secret matched. The fields below describe what arrived; none is derived
	// from a secret.
	//
	// This deliberately does not log the *expected* signature, not even a
	// prefix. Both halves of the signed message are attacker-controlled, so
	// emitting HMAC(secret, msg) for a caller-chosen msg -- truncated or not --
	// is a chosen-message oracle: each rejected request would hand out verified
	// bits of the correct MAC for a message of the attacker's choosing, which
	// materially assists offline brute force of a weak secret. A mismatch is
	// diagnosable from secrets_tried, body_len and got_prefix without it.
	first := gotSigs[0]
	slog.Warn("hmac_stripe_failed", "reason", "no_secret_matched",
		"header", cfg.Header, "sig_tag", cfg.SigTag,
		"secrets_tried", len(secrets), "signatures_tried", len(gotSigs),
		"ts_len", len(tsStr), "body_len", len(body),
		"got_prefix", hex.EncodeToString(first)[:min(8, 2*len(first))])
	return ErrUnauthorized
}

// safePrefix returns the first n characters of s, or all of s if shorter.
// Used for diagnostic logging where we want a fingerprint without leaking
// the full value (e.g. signature prefix, not full signature).
func safePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

const (
	// nonceMaxLen bounds a nonce we are willing to remember. Nonces are opaque
	// uniqueness tokens -- a UUID is 36 characters -- so this is generous,
	// while stopping a single entry from being megabytes wide. Without it the
	// only bound was Go's default 1 MiB header limit.
	nonceMaxLen = 256

	// nonceMaxEntries caps the cache. Entries are written only after a
	// signature verifies, so reaching this takes a legitimate sender at very
	// high sustained volume rather than an attacker; it is a memory backstop,
	// not the primary defence.
	nonceMaxEntries = 100_000

	// stripeMaxSignatures bounds how many signature candidates are taken from
	// one header. A secret roll puts a handful there -- Stripe signs with every
	// active secret -- so this is generous, while stopping a caller from
	// pushing thousands of values into the compare loop.
	stripeMaxSignatures = 16

	// nonceSweepInterval bounds how often the expired-entry sweep runs. The
	// sweep is O(entries) while holding the lock, so running it on every call
	// made each request on the route pay for the whole cache.
	nonceSweepInterval = 30 * time.Second
)

// NonceClaim is a replay-protection claim on the nonce of a request whose
// signature has verified. It blocks a concurrent replay from the moment Verify
// returns, but it is not permanent until Commit.
//
// The distinction matters because the ingress ACKs only after a durable
// enqueue. A permanently burned nonce is a hidden ACK: when the enqueue then
// failed and the server answered 503 -- explicitly inviting a retry -- the
// sender's identical signed retry, which is exactly what webhook senders
// replay, hit the claimed nonce and got 401 for the rest of the tolerance
// window. Transient backpressure became permanent webhook loss, and the sender
// saw "unauthorized", so it typically stopped retrying.
//
// Commit after the enqueue succeeds; Release on every other path. A released
// nonce lets the retry through, which is the point: at-least-once delivery
// makes a duplicate acceptable, a dropped webhook is not.
//
// A NonceClaim belongs to the goroutine handling the request and is not safe
// for concurrent use. Both methods are nil-safe.
type NonceClaim struct {
	cache *nonceCache
	nonce string
	seq   uint64
	done  bool
}

// Commit makes the claim permanent for the rest of the tolerance window.
func (c *NonceClaim) Commit() {
	if c == nil {
		return
	}
	c.done = true
}

// Release drops the claim so an identical signed retry is accepted. It is a
// no-op after Commit or a previous Release, so `defer claim.Release()` next to
// Verify is always correct.
func (c *NonceClaim) Release() {
	if c == nil || c.done || c.cache == nil {
		return
	}
	c.done = true
	c.cache.release(c.nonce, c.seq)
}

type nonceEntry struct {
	expiresAt time.Time
	// seq identifies one claim. Release compares it so a claim released late
	// cannot delete the entry of a later claim on the same nonce.
	seq uint64
}

type nonceCache struct {
	mu        sync.Mutex
	now       func() time.Time
	m         map[string]nonceEntry
	lastSweep time.Time
	nextSeq   uint64
}

func newNonceCache(now func() time.Time) *nonceCache {
	if now == nil {
		now = time.Now
	}
	return &nonceCache{
		now: now,
		m:   make(map[string]nonceEntry),
	}
}

func (c *nonceCache) setNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

// claim takes nonce until expiresAt and reports whether it was previously
// unclaimed, along with the sequence number identifying this claim.
//
// This is the authority on replay detection: call it only after the signature
// has verified, and reject the request when it returns false. Claiming a nonce
// before verification let any unauthenticated caller grow the cache, and let
// anyone able to observe or predict a nonce claim it first so that the genuine
// signed webhook carrying it was rejected as a replay.
func (c *nonceCache) claim(nonce string, expiresAt time.Time) (uint64, bool) {
	if nonce == "" || len(nonce) > nonceMaxLen {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now().UTC()
	c.sweepLocked(now)

	if e, ok := c.m[nonce]; ok && now.Before(e.expiresAt) {
		return 0, false
	}
	if len(c.m) >= nonceMaxEntries {
		// Still full after sweeping. Drop the entry closest to expiring: its
		// replay window is nearly over anyway, and refusing a validly signed
		// request would turn a capacity problem into dropped webhooks.
		c.evictEarliestLocked()
	}
	c.nextSeq++
	c.m[nonce] = nonceEntry{expiresAt: expiresAt.UTC(), seq: c.nextSeq}
	return c.nextSeq, true
}

// release drops a claim that was never committed, so that an identical signed
// retry is accepted. The seq check means a claim released after the same nonce
// was legitimately claimed again cannot delete the newer entry.
func (c *nonceCache) release(nonce string, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.m[nonce]; ok && e.seq == seq {
		delete(c.m, nonce)
	}
}

// sweepLocked removes expired entries, at most once per nonceSweepInterval.
// Entries that outlive their expiry until the next sweep are harmless: the
// lookup in recordIfAbsent compares against the stored expiry rather than
// treating mere presence as a replay.
func (c *nonceCache) sweepLocked(now time.Time) {
	if !c.lastSweep.IsZero() && now.Sub(c.lastSweep) < nonceSweepInterval {
		return
	}
	c.lastSweep = now
	for k, e := range c.m {
		if !now.Before(e.expiresAt) {
			delete(c.m, k)
		}
	}
}

func (c *nonceCache) evictEarliestLocked() {
	var (
		earliestKey string
		earliestExp time.Time
	)
	for k, e := range c.m {
		if earliestKey == "" || e.expiresAt.Before(earliestExp) {
			earliestKey, earliestExp = k, e.expiresAt
		}
	}
	if earliestKey != "" {
		delete(c.m, earliestKey)
	}
}
