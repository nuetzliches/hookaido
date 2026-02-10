package secrets

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var ErrSecretRef = errors.New("invalid secret reference")

// ValidateRef validates a secret reference format without loading its value.
//
// Supported forms:
// - env:NAME
// - file:/path/to/secret
// - raw:literal-value
// - vault:secret/path[#field]
func ValidateRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return fmt.Errorf("%w: empty", ErrSecretRef)
	}

	switch {
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env:"))
		if name == "" {
			return fmt.Errorf("%w: env var name is empty", ErrSecretRef)
		}
		return nil
	case strings.HasPrefix(ref, "file:"):
		path := strings.TrimSpace(strings.TrimPrefix(ref, "file:"))
		if path == "" {
			return fmt.Errorf("%w: file path is empty", ErrSecretRef)
		}
		return nil
	case strings.HasPrefix(ref, "raw:"):
		val := strings.TrimPrefix(ref, "raw:")
		if val == "" {
			return fmt.Errorf("%w: raw value is empty", ErrSecretRef)
		}
		return nil
	case strings.HasPrefix(ref, "vault:"):
		vref := strings.TrimSpace(strings.TrimPrefix(ref, "vault:"))
		_, _, err := parseVaultRef(vref)
		return err
	default:
		return fmt.Errorf("%w: unsupported scheme (use env:, file:, raw:, or vault:)", ErrSecretRef)
	}
}

// LoadRef loads a secret value from a reference string.
//
// Supported forms:
// - env:NAME
// - file:/path/to/secret
// - raw:literal-value (intended for tests/dev only)
// - vault:secret/path[#field] (reads from Vault HTTP API using env-configured address/token)
func LoadRef(ref string) ([]byte, error) {
	if err := ValidateRef(ref); err != nil {
		return nil, err
	}
	ref = strings.TrimSpace(ref)

	switch {
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env:"))
		if name == "" {
			return nil, fmt.Errorf("%w: env var name is empty", ErrSecretRef)
		}
		val := os.Getenv(name)
		if val == "" {
			return nil, fmt.Errorf("%w: env var %q is empty or missing", ErrSecretRef, name)
		}
		return []byte(val), nil
	case strings.HasPrefix(ref, "file:"):
		path := strings.TrimSpace(strings.TrimPrefix(ref, "file:"))
		if path == "" {
			return nil, fmt.Errorf("%w: file path is empty", ErrSecretRef)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		val := strings.TrimSpace(string(b))
		if val == "" {
			return nil, fmt.Errorf("%w: file %q is empty", ErrSecretRef, path)
		}
		return []byte(val), nil
	case strings.HasPrefix(ref, "raw:"):
		val := strings.TrimPrefix(ref, "raw:")
		if val == "" {
			return nil, fmt.Errorf("%w: raw value is empty", ErrSecretRef)
		}
		return []byte(val), nil
	case strings.HasPrefix(ref, "vault:"):
		vref := strings.TrimSpace(strings.TrimPrefix(ref, "vault:"))
		return loadVaultRef(vref)
	default:
		return nil, fmt.Errorf("%w: unsupported scheme (use env:, file:, raw:, or vault:)", ErrSecretRef)
	}
}

const (
	defaultVaultTimeout = 5 * time.Second

	vaultAddrEnv               = "HOOKAIDO_VAULT_ADDR"
	vaultTokenEnv              = "HOOKAIDO_VAULT_TOKEN"
	vaultNamespaceEnv          = "HOOKAIDO_VAULT_NAMESPACE"
	vaultTimeoutEnv            = "HOOKAIDO_VAULT_TIMEOUT"
	vaultCACertEnv             = "HOOKAIDO_VAULT_CACERT"
	vaultClientCertEnv         = "HOOKAIDO_VAULT_CLIENT_CERT"
	vaultClientKeyEnv          = "HOOKAIDO_VAULT_CLIENT_KEY"
	vaultInsecureSkipVerifyEnv = "HOOKAIDO_VAULT_INSECURE_SKIP_VERIFY"
)

type vaultConfig struct {
	addr               string
	token              string
	namespace          string
	timeout            time.Duration
	caCertPath         string
	clientCertPath     string
	clientKeyPath      string
	insecureSkipVerify bool
}

func loadVaultRef(rawRef string) ([]byte, error) {
	apiPath, field, err := parseVaultRef(rawRef)
	if err != nil {
		return nil, err
	}

	cfg, err := loadVaultConfigFromEnv()
	if err != nil {
		return nil, err
	}

	reqURL, err := buildVaultRequestURL(cfg.addr, apiPath)
	if err != nil {
		return nil, fmt.Errorf("%w: vault request URL: %v", ErrSecretRef, err)
	}
	client, err := newVaultHTTPClient(cfg)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build vault request: %v", ErrSecretRef, err)
	}
	req.Header.Set("X-Vault-Token", cfg.token)
	if cfg.namespace != "" {
		req.Header.Set("X-Vault-Namespace", cfg.namespace)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: vault request failed: %v", ErrSecretRef, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: read vault response: %v", ErrSecretRef, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: vault request failed (%d): %s", ErrSecretRef, resp.StatusCode, vaultErrorDetail(body))
	}

	secret, err := extractVaultSecret(body, field)
	if err != nil {
		return nil, err
	}
	return []byte(secret), nil
}

func parseVaultRef(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("%w: vault ref is empty", ErrSecretRef)
	}

	field := "value"
	pathPart := raw
	if idx := strings.IndexByte(raw, '#'); idx >= 0 {
		pathPart = strings.TrimSpace(raw[:idx])
		field = strings.TrimSpace(raw[idx+1:])
		if field == "" {
			return "", "", fmt.Errorf("%w: vault field is empty", ErrSecretRef)
		}
	}

	if strings.Contains(pathPart, "://") {
		return "", "", fmt.Errorf("%w: vault ref must be path-based (no URL scheme)", ErrSecretRef)
	}

	pathPart = strings.Trim(pathPart, "/ ")
	if pathPart == "" {
		return "", "", fmt.Errorf("%w: vault path is empty", ErrSecretRef)
	}
	for _, seg := range strings.Split(pathPart, "/") {
		if seg == "." || seg == ".." {
			return "", "", fmt.Errorf("%w: vault path must not contain dot segments", ErrSecretRef)
		}
	}
	if strings.HasPrefix(pathPart, "v1/") {
		return "/" + pathPart, field, nil
	}
	return "/v1/" + pathPart, field, nil
}

func loadVaultConfigFromEnv() (vaultConfig, error) {
	cfg := vaultConfig{
		addr:       strings.TrimSpace(os.Getenv(vaultAddrEnv)),
		token:      strings.TrimSpace(os.Getenv(vaultTokenEnv)),
		namespace:  strings.TrimSpace(os.Getenv(vaultNamespaceEnv)),
		timeout:    defaultVaultTimeout,
		caCertPath: strings.TrimSpace(os.Getenv(vaultCACertEnv)),
	}
	cfg.clientCertPath = strings.TrimSpace(os.Getenv(vaultClientCertEnv))
	cfg.clientKeyPath = strings.TrimSpace(os.Getenv(vaultClientKeyEnv))

	if cfg.addr == "" {
		return vaultConfig{}, fmt.Errorf("%w: %s is required for vault refs", ErrSecretRef, vaultAddrEnv)
	}
	if cfg.token == "" {
		return vaultConfig{}, fmt.Errorf("%w: %s is required for vault refs", ErrSecretRef, vaultTokenEnv)
	}
	if (cfg.clientCertPath == "") != (cfg.clientKeyPath == "") {
		return vaultConfig{}, fmt.Errorf("%w: %s and %s must be set together", ErrSecretRef, vaultClientCertEnv, vaultClientKeyEnv)
	}

	if raw := strings.TrimSpace(os.Getenv(vaultTimeoutEnv)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return vaultConfig{}, fmt.Errorf("%w: %s must be a positive duration", ErrSecretRef, vaultTimeoutEnv)
		}
		cfg.timeout = d
	}

	if raw := strings.TrimSpace(os.Getenv(vaultInsecureSkipVerifyEnv)); raw != "" {
		v, ok := parseBoolString(raw)
		if !ok {
			return vaultConfig{}, fmt.Errorf("%w: %s must be true|false|on|off|1|0", ErrSecretRef, vaultInsecureSkipVerifyEnv)
		}
		cfg.insecureSkipVerify = v
	}

	parsed, err := url.Parse(cfg.addr)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return vaultConfig{}, fmt.Errorf("%w: %s must be a valid http(s) URL", ErrSecretRef, vaultAddrEnv)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return vaultConfig{}, fmt.Errorf("%w: %s must use http or https", ErrSecretRef, vaultAddrEnv)
	}

	return cfg, nil
}

func buildVaultRequestURL(addr, apiPath string) (string, error) {
	base, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimSuffix(base.Path, "/")
	base.Path = path.Clean(basePath + apiPath)
	return base.String(), nil
}

func newVaultHTTPClient(cfg vaultConfig) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("%w: default HTTP transport is not *http.Transport", ErrSecretRef)
	}
	t := transport.Clone()

	useCustomTLS := cfg.insecureSkipVerify || cfg.caCertPath != "" || cfg.clientCertPath != ""
	if useCustomTLS {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.insecureSkipVerify,
		}

		if cfg.caCertPath != "" {
			b, err := os.ReadFile(cfg.caCertPath)
			if err != nil {
				return nil, fmt.Errorf("%w: read %s: %v", ErrSecretRef, vaultCACertEnv, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				return nil, fmt.Errorf("%w: parse %s: no certificates found", ErrSecretRef, vaultCACertEnv)
			}
			tlsCfg.RootCAs = pool
		}
		if cfg.clientCertPath != "" {
			cert, err := tls.LoadX509KeyPair(cfg.clientCertPath, cfg.clientKeyPath)
			if err != nil {
				return nil, fmt.Errorf("%w: load %s/%s: %v", ErrSecretRef, vaultClientCertEnv, vaultClientKeyEnv, err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		t.TLSClientConfig = tlsCfg
	}

	return &http.Client{
		Timeout:   cfg.timeout,
		Transport: t,
	}, nil
}

func extractVaultSecret(body []byte, field string) (string, error) {
	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return "", fmt.Errorf("%w: decode vault response: %v", ErrSecretRef, err)
	}

	dataMap, ok := payload["data"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("%w: vault response missing data object", ErrSecretRef)
	}

	if nested, ok := dataMap["data"].(map[string]any); ok {
		if v, ok := selectVaultFieldValue(nested, field); ok {
			return normalizeVaultSecretValue(v)
		}
	}
	if v, ok := selectVaultFieldValue(dataMap, field); ok {
		return normalizeVaultSecretValue(v)
	}

	return "", fmt.Errorf("%w: vault field %q not found", ErrSecretRef, field)
}

func selectVaultFieldValue(data map[string]any, field string) (any, bool) {
	if v, ok := data[field]; ok {
		return v, true
	}
	if field != "value" {
		return nil, false
	}

	var (
		candidate any
		count     int
	)
	for k, v := range data {
		if strings.EqualFold(strings.TrimSpace(k), "metadata") {
			continue
		}
		candidate = v
		count++
		if count > 1 {
			return nil, false
		}
	}
	if count == 1 {
		return candidate, true
	}
	return nil, false
}

func normalizeVaultSecretValue(value any) (string, error) {
	var out string
	switch v := value.(type) {
	case string:
		out = v
	case json.Number:
		out = v.String()
	case float64:
		out = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		out = strconv.FormatBool(v)
	default:
		return "", fmt.Errorf("%w: vault field must be a scalar value", ErrSecretRef)
	}
	if out == "" {
		return "", fmt.Errorf("%w: vault field is empty", ErrSecretRef)
	}
	return out, nil
}

func vaultErrorDetail(body []byte) string {
	type vaultErrorPayload struct {
		Errors []string `json:"errors"`
	}

	var p vaultErrorPayload
	if err := json.Unmarshal(body, &p); err == nil && len(p.Errors) > 0 {
		return strings.Join(p.Errors, "; ")
	}

	msg := strings.TrimSpace(string(body))
	if msg != "" {
		return msg
	}
	return "unknown error"
}

func parseBoolString(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on":
		return true, true
	case "0", "false", "off":
		return false, true
	default:
		return false, false
	}
}
