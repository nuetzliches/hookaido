package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nuetzliches/hookaido/v2/internal/secrets"
)

// maxDrainBytes bounds how much of a delivery response body is read before the
// connection is closed.
//
// The body is discarded — it is read only so keep-alive can reuse the
// connection — but the read itself was unbounded, while ingress caps inbound
// bodies at 2 MiB. A target that answers 2xx and then streams at line rate held
// a delivery goroutine for the whole per-attempt timeout (10s by default) and
// still had its message acked, so deliver_concurrency goroutines (20 by default)
// could all be parked on one uncooperative target. Past the cap the body is left
// unread and the connection is dropped rather than reused, which is the right
// trade for a target behaving that way.
const maxDrainBytes = 64 << 10

// signingSecretTTL bounds how long a cached file:- or vault:-backed outbound
// signing secret is reused before being re-read. It is the window in which a
// revoked key still signs; short enough that rotation takes effect without an
// operator action, long enough that Vault is not consulted per delivery.
const signingSecretTTL = 60 * time.Second

// cachedSigningSecret is one memoized signing secret plus when it was read, so
// the entry can expire.
type cachedSigningSecret struct {
	value    []byte
	loadedAt time.Time
}

type HTTPDeliverer struct {
	Client *http.Client
	Policy EgressPolicy

	Resolver resolver

	Now func() time.Time

	secretCache sync.Map
}

func NewHTTPDeliverer(client *http.Client, policy EgressPolicy) *HTTPDeliverer {
	if client == nil {
		client = &http.Client{}
	}
	// A caller that did not bring its own transport gets the guarded one, so
	// the address-level policy is enforced at connect time rather than only
	// against addresses a prior lookup happened to return.
	if client.Transport == nil {
		client.Transport = NewEgressTransport(policy)
	}
	d := &HTTPDeliverer{
		Client:   client,
		Policy:   policy,
		Resolver: nil,
		Now:      time.Now,
	}
	if policy.Redirects {
		client.CheckRedirect = d.checkRedirect
	} else {
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	return d
}

func (d *HTTPDeliverer) Deliver(ctx context.Context, delivery Delivery) Result {
	method := delivery.Method
	if method == "" {
		method = http.MethodPost
	}

	if err := checkEgressPolicy(ctx, delivery.URL, d.Policy, d.Resolver); err != nil {
		return Result{Err: err}
	}

	req, err := http.NewRequestWithContext(ctx, method, delivery.URL, bytes.NewReader(delivery.Body))
	if err != nil {
		return Result{Err: err}
	}
	for k, v := range delivery.Header {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	for _, h := range delivery.CustomHeaders {
		req.Header.Set(h.Name, h.Value)
	}
	if err := d.applyDeliverySigning(req, delivery); err != nil {
		return Result{Err: err}
	}

	resp, err := d.Client.Do(req)
	if err != nil {
		return Result{Err: err}
	}
	defer resp.Body.Close()
	// Read the hint before the body is discarded: Result used to carry only a
	// status and an error, so the response headers were dropped here and
	// Retry-After never reached the retry scheduler.
	retryAfter := parseRetryAfter(resp.Header, d.now())
	// io.EOF is the ordinary outcome for a body shorter than the cap.
	_, _ = io.CopyN(io.Discard, resp.Body, maxDrainBytes)
	return Result{StatusCode: resp.StatusCode, RetryAfter: retryAfter}
}

func (d *HTTPDeliverer) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return http.ErrUseLastResponse
	}
	if err := checkEgressPolicyURL(req.Context(), req.URL, d.Policy, d.Resolver); err != nil {
		return err
	}
	return nil
}

func (d *HTTPDeliverer) applyDeliverySigning(req *http.Request, delivery Delivery) error {
	if delivery.Sign == nil {
		return nil
	}

	cfg := delivery.Sign
	signatureHeader := strings.TrimSpace(cfg.SignatureHeader)
	timestampHeader := strings.TrimSpace(cfg.TimestampHeader)
	if signatureHeader == "" || timestampHeader == "" {
		return fmt.Errorf("delivery signing headers are not configured")
	}

	signedAt := d.now().UTC()
	timestamp := strconv.FormatInt(signedAt.Unix(), 10)

	secretRef, err := selectSigningSecretRef(cfg, signedAt)
	if err != nil {
		return err
	}
	secret, err := d.loadSigningSecret(secretRef)
	if err != nil {
		return fmt.Errorf("delivery signing secret %q: %w", secretRef, err)
	}
	if len(secret) == 0 {
		return fmt.Errorf("delivery signing secret %q is empty", secretRef)
	}

	bodyHash := sha256.Sum256(delivery.Body)
	reqPath := req.URL.EscapedPath()
	if reqPath == "" {
		reqPath = "/"
	}
	canonical := strings.ToUpper(req.Method) + "\n" + reqPath + "\n" + timestamp + "\n" + hex.EncodeToString(bodyHash[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	signature := hex.EncodeToString(mac.Sum(nil))

	req.Header.Set(timestampHeader, timestamp)
	req.Header.Set(signatureHeader, signature)
	return nil
}

func selectSigningSecretRef(cfg *HMACSigningConfig, at time.Time) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("delivery signing config is not configured")
	}
	if len(cfg.SecretVersions) == 0 {
		secretRef := strings.TrimSpace(cfg.SecretRef)
		if secretRef == "" {
			return "", fmt.Errorf("delivery signing secret is not configured")
		}
		return secretRef, nil
	}

	selection := strings.ToLower(strings.TrimSpace(cfg.SecretSelection))
	if selection == "" {
		selection = "newest_valid"
	}
	if selection != "newest_valid" && selection != "oldest_valid" {
		return "", fmt.Errorf("delivery signing secret_selection %q is not supported", selection)
	}

	selectedIdx := -1
	for i := range cfg.SecretVersions {
		v := cfg.SecretVersions[i]
		if !isSigningSecretVersionValidAt(v, at) {
			continue
		}
		if selectedIdx < 0 {
			selectedIdx = i
			continue
		}
		selected := cfg.SecretVersions[selectedIdx]
		replace := false
		switch selection {
		case "newest_valid":
			replace = v.ValidFrom.After(selected.ValidFrom)
		case "oldest_valid":
			replace = v.ValidFrom.Before(selected.ValidFrom)
		}
		if !replace && v.ValidFrom.Equal(selected.ValidFrom) && v.ID < selected.ID {
			replace = true
		}
		if replace {
			selectedIdx = i
		}
	}
	if selectedIdx < 0 {
		return "", fmt.Errorf("delivery signing secret_ref has no version valid at timestamp")
	}
	selected := cfg.SecretVersions[selectedIdx]

	secretRef := strings.TrimSpace(selected.Ref)
	if secretRef == "" {
		return "", fmt.Errorf("delivery signing secret_ref %q resolved to empty value", selected.ID)
	}
	return secretRef, nil
}

func isSigningSecretVersionValidAt(v HMACSigningSecretVersion, at time.Time) bool {
	if v.ValidFrom.IsZero() || at.Before(v.ValidFrom) {
		return false
	}
	if !v.HasUntil {
		return true
	}
	return at.Before(v.ValidUntil)
}

// loadSigningSecret resolves an outbound signing secret, caching the result.
//
// The cache used to have no TTL and no invalidation, and secrets.LoadRef does
// not cache, so the dispatcher was the sole source of staleness. It was
// discarded only when the whole HTTPDeliverer was rebuilt, which happens only on
// a compiled-config change — so rotating a vault: or file: backed signing secret
// without editing the Hookaidofile had no effect, SIGHUP included, and revoking
// a leaked signing key required a full process restart.
//
// env: and raw: refs are still cached for the life of the deliverer: their value
// is fixed for the process, so re-reading could not observe a rotation. file:
// and vault: refs are re-read once their entry passes signingSecretTTL, which
// bounds how long a revoked key stays usable without putting a filesystem or
// Vault round trip on every delivery.
func (d *HTTPDeliverer) loadSigningSecret(ref string) ([]byte, error) {
	rereadable := signingSecretRefIsRereadable(ref)
	now := d.now()
	if v, ok := d.secretCache.Load(ref); ok {
		if entry, ok := v.(cachedSigningSecret); ok {
			if !rereadable || now.Sub(entry.loadedAt) < signingSecretTTL {
				return entry.value, nil
			}
		}
	}
	b, err := secrets.LoadRef(ref)
	if err != nil {
		return nil, err
	}
	secret := append([]byte(nil), b...)
	d.secretCache.Store(ref, cachedSigningSecret{value: secret, loadedAt: now})
	return secret, nil
}

// signingSecretRefIsRereadable reports whether a ref can change while the
// process runs. env: is fixed at exec time and raw: is literal config; file: and
// vault: are both mutable behind Hookaido's back.
func signingSecretRefIsRereadable(ref string) bool {
	ref = strings.TrimSpace(ref)
	return strings.HasPrefix(ref, "file:") || strings.HasPrefix(ref, "vault:")
}

// now resolves the deliverer's clock, defaulting to time.Now.
func (d *HTTPDeliverer) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// parseRetryAfter resolves a Retry-After header to a wait duration. RFC 7231
// allows either delta-seconds or an HTTP-date, and both appear in the wild.
//
// Zero means "no usable hint": absent, unparseable, non-positive, or a date
// already in the past. Callers treat zero as "use the retry schedule", so an
// unreadable header degrades to today's behaviour rather than to an immediate
// redelivery.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	raw := strings.TrimSpace(h.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	at, err := http.ParseTime(raw)
	if err != nil {
		return 0
	}
	if d := at.Sub(now); d > 0 {
		return d
	}
	return 0
}
