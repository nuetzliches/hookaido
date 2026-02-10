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

	"github.com/nuetzliches/hookaido/internal/secrets"
)

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
	if err := d.applyDeliverySigning(req, delivery); err != nil {
		return Result{Err: err}
	}

	resp, err := d.Client.Do(req)
	if err != nil {
		return Result{Err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return Result{StatusCode: resp.StatusCode}
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

	nowFn := d.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	signedAt := nowFn().UTC()
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

func (d *HTTPDeliverer) loadSigningSecret(ref string) ([]byte, error) {
	if v, ok := d.secretCache.Load(ref); ok {
		if b, ok := v.([]byte); ok {
			return b, nil
		}
	}
	b, err := secrets.LoadRef(ref)
	if err != nil {
		return nil, err
	}
	secret := append([]byte(nil), b...)
	d.secretCache.Store(ref, secret)
	return secret, nil
}
