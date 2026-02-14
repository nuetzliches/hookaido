package config

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/httpheader"
	"github.com/nuetzliches/hookaido/internal/secrets"
)

const (
	defaultIngressListen                  = ":8080"
	defaultPullListen                     = ":9443"
	defaultPullAPIMaxBatch                = 100
	defaultPullAPIDefaultLeaseTTL         = 30 * time.Second
	defaultAdminListen                    = "127.0.0.1:2019"
	defaultMetricsListen                  = "127.0.0.1:9900"
	defaultMetricsPrefix                  = "/metrics"
	defaultMaxBodyBytes                   = 2 << 20  // 2 MiB
	defaultMaxHeaderBytes                 = 64 << 10 // 64 KiB
	defaultDeliverRetryMax                = 8
	defaultDeliverRetryBase               = 2 * time.Second
	defaultDeliverRetryCap                = 2 * time.Minute
	defaultDeliverRetryJitter             = 0.2
	defaultDeliverTimeout                 = 10 * time.Second
	defaultIngressHMACSignatureHeader     = "X-Signature"
	defaultIngressHMACTimestampHeader     = "X-Timestamp"
	defaultIngressHMACNonceHeader         = "X-Nonce"
	defaultDeliverSignHMACSignatureHeader = "X-Hookaido-Signature"
	defaultDeliverSignHMACTimestampHeader = "X-Hookaido-Timestamp"
	defaultDeliverSignHMACSecretSelection = "newest_valid"
	defaultDeliverConcurrency             = 20
	defaultTrendSignalsWindow             = 15 * time.Minute
	defaultTrendSignalsCaptureInterval    = time.Minute
	defaultTrendSignalsStaleGrace         = 3
	defaultTrendSustainedConsecutive      = 3
	defaultTrendSustainedMinSamples       = 5
	defaultTrendSustainedMinDelta         = 10
	defaultTrendSurgeMinTotal             = 20
	defaultTrendSurgeMinDelta             = 10
	defaultTrendSurgePercent              = 50
	defaultTrendDeadMinTotal              = 10
	defaultTrendDeadPercent               = 20
	defaultTrendPressureMinTotal          = 20
	defaultTrendPressurePercent           = 75
	defaultTrendPressureLeasedFactor      = 2
	defaultAdaptiveBackpressureEnabled    = false
	defaultAdaptiveBackpressureMinTotal   = 200
	defaultAdaptiveBackpressureQueuedPct  = 80
	defaultAdaptiveBackpressureReadyLag   = 30 * time.Second
	defaultAdaptiveBackpressureOldestAge  = 60 * time.Second
	defaultAdaptiveBackpressureSustained  = true
	defaultQueueMaxDepth                  = 10000
	defaultQueueDropPolicy                = "reject"
	defaultQueueRetentionMaxAge           = 7 * 24 * time.Hour
	defaultQueueRetentionPruneInterval    = 5 * time.Minute
	defaultDeliveredRetentionMaxAge       = 0
	defaultDLQRetentionMaxAge             = 30 * 24 * time.Hour
	defaultDLQRetentionMaxDepth           = 10000
	defaultEgressHTTPSOnly                = true
	defaultEgressRedirects                = false
	defaultEgressDNSRebindProtection      = true
	defaultPublishDirectEnabled           = true
	defaultPublishManagedEnabled          = true
	defaultPublishAllowPullRoutes         = true
	defaultPublishAllowDeliverRoutes      = true
	defaultPublishRequireActor            = false
	defaultPublishRequireRequestID        = false
	defaultPublishFailClosed              = false
	defaultTracingRetryEnabled            = true
	defaultTracingRetryInitialInterval    = 5 * time.Second
	defaultTracingRetryMaxInterval        = 30 * time.Second
	defaultTracingRetryMaxElapsedTime     = 1 * time.Minute
	defaultForwardAuthTimeout             = 2 * time.Second
)

const managementLabelPatternExpr = `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`

var varNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var managementLabelPattern = regexp.MustCompile(managementLabelPatternExpr)

func ManagementLabelPattern() string {
	return managementLabelPatternExpr
}

func IsValidManagementLabel(raw string) bool {
	return managementLabelPattern.MatchString(strings.TrimSpace(raw))
}

type Compiled struct {
	Ingress            IngressConfig
	Defaults           DefaultsConfig
	Vars               map[string]string
	Secrets            map[string]SecretConfig
	PullAPI            APIConfig
	AdminAPI           APIConfig
	Observability      ObservabilityConfig
	QueueRetention     QueueRetentionConfig
	DeliveredRetention DeliveredRetentionConfig
	DLQRetention       DLQRetentionConfig
	QueueLimits        QueueLimitsConfig
	SharedListener     bool
	HasPullRoutes      bool
	HasDeliverRoutes   bool

	// Routes are in evaluation order (top-down).
	Routes []CompiledRoute

	// PathToRoute maps Pull paths (relative to PullAPI.Prefix) to route paths.
	PathToRoute map[string]string
}

type IngressConfig struct {
	Listen string
	TLS    TLSConfig

	RateLimit RateLimitConfig
}

type DefaultsConfig struct {
	MaxBodyBytes   int64
	MaxHeaderBytes int

	DeliverRetry         RetryConfig
	DeliverTimeout       time.Duration
	DeliverConcurrency   int
	TrendSignals         TrendSignalsConfig
	AdaptiveBackpressure AdaptiveBackpressureConfig

	EgressPolicy  EgressPolicyConfig
	PublishPolicy PublishPolicyConfig
}

type TrendSignalsConfig struct {
	Window                  time.Duration
	ExpectedCaptureInterval time.Duration
	StaleGraceFactor        int

	SustainedGrowthConsecutive int
	SustainedGrowthMinSamples  int
	SustainedGrowthMinDelta    int

	RecentSurgeMinTotal int
	RecentSurgeMinDelta int
	RecentSurgePercent  int

	DeadShareHighMinTotal int
	DeadShareHighPercent  int

	QueuedPressureMinTotal         int
	QueuedPressurePercent          int
	QueuedPressureLeasedMultiplier int
}

type AdaptiveBackpressureConfig struct {
	Enabled bool

	MinTotal      int
	QueuedPercent int

	ReadyLag        time.Duration
	OldestQueuedAge time.Duration

	SustainedGrowth bool
}

type APIConfig struct {
	Listen string
	Prefix string

	GRPCListen string

	TLS TLSConfig

	AuthTokens []string

	MaxBatch        int
	DefaultLeaseTTL time.Duration
	MaxLeaseTTL     time.Duration
	DefaultMaxWait  time.Duration
	MaxWait         time.Duration
}

type CompiledRoute struct {
	ChannelType ChannelType
	Path        string
	Pull        *PullConfig

	Application    string
	EndpointName   string
	Publish        bool
	PublishDirect  bool
	PublishManaged bool

	Match MatchConfig

	RateLimit RateLimitConfig

	AuthBasic               map[string]string
	AuthForward             ForwardAuthConfig
	AuthHMACSecrets         []string
	AuthHMACSecretRefs      []string
	AuthHMACSignatureHeader string
	AuthHMACTimestampHeader string
	AuthHMACNonceHeader     string
	AuthHMACTolerance       time.Duration
	QueueBackend            string

	MaxBodyBytes   int64
	MaxHeaderBytes int

	DeliverConcurrency int
	Deliveries         []CompiledDeliver
}

type PullConfig struct {
	Path       string
	AuthTokens []string
}

type MatchConfig struct {
	Methods []string
	Hosts   []string
	Headers []HeaderMatchConfig
	Query   []QueryMatchConfig

	RemoteIPs []netip.Prefix

	HeaderExists []string
	QueryExists  []string
}

type HeaderMatchConfig struct {
	Name  string
	Value string
}

type QueryMatchConfig struct {
	Name  string
	Value string
}

type CompiledDeliver struct {
	URL         string
	Timeout     time.Duration
	Retry       RetryConfig
	SigningHMAC DeliverSigningHMACConfig
}

type DeliverSigningHMACConfig struct {
	Enabled bool

	SecretRef      string
	SecretVersions []DeliverSigningSecretVersion

	SecretSelection string

	SignatureHeader string
	TimestampHeader string
}

type DeliverSigningSecretVersion struct {
	ID string

	ValueRef string

	ValidFrom  time.Time
	ValidUntil time.Time
	HasUntil   bool
}

type RetryConfig struct {
	Type   string
	Max    int
	Base   time.Duration
	Cap    time.Duration
	Jitter float64
}

type RateLimitConfig struct {
	Enabled bool
	RPS     float64
	Burst   int
}

type ForwardAuthConfig struct {
	Enabled bool

	URL string

	Timeout time.Duration

	CopyHeaders []string

	BodyLimitBytes int64
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string

	ClientCA   string
	ClientAuth tls.ClientAuthType
}

type EgressPolicyConfig struct {
	HTTPSOnly           bool
	Redirects           bool
	DNSRebindProtection bool

	Allow []EgressRule
	Deny  []EgressRule
}

type PublishPolicyConfig struct {
	DirectEnabled      bool
	ManagedEnabled     bool
	AllowPullRoutes    bool
	AllowDeliverRoutes bool
	RequireActor       bool
	RequireRequestID   bool
	FailClosed         bool

	ActorAllowlist []string
	ActorPrefixes  []string
}

type EgressRule struct {
	Host       string
	Subdomains bool
	CIDR       netip.Prefix
	IsCIDR     bool
}

type ObservabilityConfig struct {
	AccessLogEnabled             bool
	AccessLogOutput              string
	AccessLogPath                string
	RuntimeLogLevel              string
	RuntimeLogDisabled           bool
	RuntimeLogOutput             string
	RuntimeLogPath               string
	RuntimeLogSet                bool
	Metrics                      MetricsConfig
	TracingEnabled               bool
	TracingCollector             string
	TracingURLPath               string
	TracingCompression           string
	TracingInsecure              bool
	TracingTimeout               time.Duration
	TracingTimeoutSet            bool
	TracingProxyURL              string
	TracingTLSCAFile             string
	TracingTLSCertFile           string
	TracingTLSKeyFile            string
	TracingTLSServerName         string
	TracingTLSInsecureSkipVerify bool
	TracingRetry                 *TracingRetryConfig
	TracingHeaders               []TracingHeaderConfig
}

type SecretConfig struct {
	ID         string
	ValueRef   string
	ValidFrom  time.Time
	ValidUntil time.Time
	HasUntil   bool
}

type MetricsConfig struct {
	Enabled bool
	Listen  string
	Prefix  string
}

type TracingHeaderConfig struct {
	Name  string
	Value string
}

type TracingRetryConfig struct {
	Enabled         bool
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
}

type QueueLimitsConfig struct {
	MaxDepth   int
	DropPolicy string
}

type QueueRetentionConfig struct {
	MaxAge        time.Duration
	PruneInterval time.Duration
	Enabled       bool
}

type DeliveredRetentionConfig struct {
	MaxAge  time.Duration
	Enabled bool
}

type DLQRetentionConfig struct {
	MaxAge   time.Duration
	MaxDepth int
	Enabled  bool
}

// Compile validates the config and returns a runtime-ready, normalized version
// with defaults applied.
func Compile(cfg *Config) (Compiled, ValidationResult) {
	var res ValidationResult
	if cfg == nil {
		res.Errors = append(res.Errors, "nil config")
		return Compiled{}, res
	}

	vars, varsRes := compileVars(cfg.Vars)
	res.Errors = append(res.Errors, varsRes.Errors...)
	res.Warnings = append(res.Warnings, varsRes.Warnings...)

	if len(vars) > 0 {
		cfgWithVars, err := cloneConfig(cfg)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("clone config for vars resolution: %v", err))
		} else {
			applyVarsToConfig(cfgWithVars, vars, &res)
			cfg = cfgWithVars
		}
	}

	ing, ingRes := compileIngress(cfg.Ingress, defaultIngressListen)
	defs, defsRes := compileDefaults(cfg.Defaults)
	secs, secsRes := compileSecrets(cfg.Secrets)
	pull, pullRes := compileAPI("pull_api", cfg.PullAPI, defaultPullListen)
	admin, adminRes := compileAPI("admin_api", cfg.AdminAPI, defaultAdminListen)
	obs, obsRes := compileObservability(cfg.Observability)
	qr, qrRes := compileQueueRetention(cfg.QueueRetention)
	dr, drRes := compileDeliveredRetention(cfg.DeliveredRetention)
	dlq, dlqRes := compileDLQRetention(cfg.DLQRetention)
	ql, qlRes := compileQueueLimits(cfg.QueueLimits)
	res.Errors = append(res.Errors, ingRes.Errors...)
	res.Errors = append(res.Errors, defsRes.Errors...)
	res.Errors = append(res.Errors, secsRes.Errors...)
	res.Errors = append(res.Errors, pullRes.Errors...)
	res.Errors = append(res.Errors, adminRes.Errors...)
	res.Errors = append(res.Errors, obsRes.Errors...)
	res.Errors = append(res.Errors, qrRes.Errors...)
	res.Errors = append(res.Errors, drRes.Errors...)
	res.Errors = append(res.Errors, dlqRes.Errors...)
	res.Errors = append(res.Errors, qlRes.Errors...)
	res.Warnings = append(res.Warnings, ingRes.Warnings...)
	res.Warnings = append(res.Warnings, defsRes.Warnings...)
	res.Warnings = append(res.Warnings, secsRes.Warnings...)
	res.Warnings = append(res.Warnings, pullRes.Warnings...)
	res.Warnings = append(res.Warnings, adminRes.Warnings...)
	res.Warnings = append(res.Warnings, obsRes.Warnings...)
	res.Warnings = append(res.Warnings, qrRes.Warnings...)
	res.Warnings = append(res.Warnings, drRes.Warnings...)
	res.Warnings = append(res.Warnings, dlqRes.Warnings...)
	res.Warnings = append(res.Warnings, qlRes.Warnings...)

	compiled := Compiled{
		Ingress:            ing,
		Defaults:           defs,
		Vars:               vars,
		Secrets:            secs,
		PullAPI:            pull,
		AdminAPI:           admin,
		Observability:      obs,
		QueueRetention:     qr,
		DeliveredRetention: dr,
		DLQRetention:       dlq,
		QueueLimits:        ql,
	}

	if len(cfg.Routes) == 0 {
		res.Errors = append(res.Errors, "no routes defined")
	}

	namedMatchers := make(map[string]MatchConfig, len(cfg.NamedMatchers))
	for i, m := range cfg.NamedMatchers {
		if strings.TrimSpace(m.Name) == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("matcher[%d] name must not be empty", i))
			continue
		}
		if _, ok := namedMatchers[m.Name]; ok {
			res.Errors = append(res.Errors, fmt.Sprintf("duplicate matcher @%s", m.Name))
			continue
		}
		matchCfg, ok := compileMatch("@"+m.Name, m.Match, &res)
		if !ok {
			continue
		}
		namedMatchers[m.Name] = matchCfg
	}

	pathToRoute := make(map[string]string, len(cfg.Routes))
	managedEndpointToRoute := make(map[string]string, len(cfg.Routes))
	routes := make([]CompiledRoute, 0, len(cfg.Routes))
	hasPullRoutes := false
	hasDeliverRoutes := false
	pullRoutesMissingAuth := false
	seenPaths := make(map[string]struct{}, len(cfg.Routes))
	for i, r := range cfg.Routes {
		rawPath := resolveValue(r.Path, fmt.Sprintf("routes[%d].path", i), &res)
		rPath, err := normalizePathValue(rawPath)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("routes[%d].path: %s", i, err.Error()))
			continue
		}
		if _, ok := seenPaths[rPath]; ok {
			res.Errors = append(res.Errors, fmt.Sprintf("duplicate route path %q", rPath))
			continue
		}
		seenPaths[rPath] = struct{}{}

		application := ""
		endpointName := ""
		if r.ApplicationSet || r.EndpointNameSet {
			if !r.ApplicationSet || !r.EndpointNameSet {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q application and endpoint_name must be set together", rPath))
			} else {
				appRaw := strings.TrimSpace(resolveValue(r.Application, fmt.Sprintf("route %q application", rPath), &res))
				endpointRaw := strings.TrimSpace(resolveValue(r.EndpointName, fmt.Sprintf("route %q endpoint_name", rPath), &res))
				if appRaw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q application must not be empty", rPath))
				}
				if endpointRaw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q endpoint_name must not be empty", rPath))
				}
				if appRaw != "" && !managementLabelPattern.MatchString(appRaw) {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q application must match %s", rPath, managementLabelPattern.String()))
				}
				if endpointRaw != "" && !managementLabelPattern.MatchString(endpointRaw) {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q endpoint_name must match %s", rPath, managementLabelPattern.String()))
				}
				if appRaw != "" && endpointRaw != "" {
					key := appRaw + "\x00" + endpointRaw
					if prev, ok := managedEndpointToRoute[key]; ok {
						res.Errors = append(res.Errors, fmt.Sprintf("duplicate management endpoint (%q, %q) for routes %q and %q", appRaw, endpointRaw, prev, rPath))
					} else {
						managedEndpointToRoute[key] = rPath
						application = appRaw
						endpointName = endpointRaw
					}
				}
			}
		}

		maxBodyBytes := defs.MaxBodyBytes
		if r.MaxBodySet {
			raw := strings.TrimSpace(resolveValue(r.MaxBody, fmt.Sprintf("route %q max_body", rPath), &res))
			size, err := parseByteSize(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q max_body %s", rPath, err.Error()))
			} else {
				maxBodyBytes = size
			}
		}

		maxHeaderBytes := defs.MaxHeaderBytes
		if r.MaxHeadersSet {
			raw := strings.TrimSpace(resolveValue(r.MaxHeaders, fmt.Sprintf("route %q max_headers", rPath), &res))
			size, err := parseByteSize(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q max_headers %s", rPath, err.Error()))
			} else if size > int64(^uint(0)>>1) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q max_headers must fit in int", rPath))
			} else {
				maxHeaderBytes = int(size)
			}
		}

		// ── Channel type constraints ──────────────────────────────────
		hasAuth := len(r.AuthBasic) > 0 || len(r.AuthHMACSecrets) > 0 || r.AuthForward != nil
		hasMatch := r.Match != nil || len(r.MatchRefs) > 0
		channelErr := false
		switch r.ChannelType {
		case ChannelOutbound:
			if r.Pull != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("outbound route %q: pull is forbidden", rPath))
				channelErr = true
			}
			if len(r.Deliveries) == 0 {
				res.Errors = append(res.Errors, fmt.Sprintf("outbound route %q: deliver is required", rPath))
				channelErr = true
			}
			if hasMatch {
				res.Errors = append(res.Errors, fmt.Sprintf("outbound route %q: match is forbidden", rPath))
				channelErr = true
			}
			if hasAuth {
				res.Errors = append(res.Errors, fmt.Sprintf("outbound route %q: auth is forbidden", rPath))
				channelErr = true
			}
			if r.RateLimit != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("outbound route %q: rate_limit is forbidden", rPath))
				channelErr = true
			}
		case ChannelInternal:
			if len(r.Deliveries) > 0 {
				res.Errors = append(res.Errors, fmt.Sprintf("internal route %q: deliver is forbidden", rPath))
				channelErr = true
			}
			if r.Pull == nil {
				res.Errors = append(res.Errors, fmt.Sprintf("internal route %q: pull is required", rPath))
				channelErr = true
			}
			if hasMatch {
				res.Errors = append(res.Errors, fmt.Sprintf("internal route %q: match is forbidden", rPath))
				channelErr = true
			}
			if hasAuth {
				res.Errors = append(res.Errors, fmt.Sprintf("internal route %q: auth is forbidden", rPath))
				channelErr = true
			}
			if r.RateLimit != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("internal route %q: rate_limit is forbidden", rPath))
				channelErr = true
			}
		}
		if channelErr {
			continue
		}

		var hmacSecrets []string
		var hmacSecretRefs []string
		seenSecretRefs := make(map[string]struct{})
		for j, ref := range r.AuthHMACSecrets {
			ref = strings.TrimSpace(resolveValue(ref, fmt.Sprintf("route %q auth hmac secret[%d]", rPath, j), &res))
			if ref == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac secret must not be empty", rPath))
				continue
			}
			if r.AuthHMACSecretIsRef != nil && j < len(r.AuthHMACSecretIsRef) && r.AuthHMACSecretIsRef[j] {
				if _, ok := compiled.Secrets[ref]; !ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac secret_ref %q not found", rPath, ref))
					continue
				}
				if _, ok := seenSecretRefs[ref]; ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac secret_ref duplicate %q", rPath, ref))
					continue
				}
				seenSecretRefs[ref] = struct{}{}
				hmacSecretRefs = append(hmacSecretRefs, ref)
				continue
			}
			if err := secrets.ValidateRef(ref); err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac secret[%d] %v", rPath, j, err))
				continue
			}
			hmacSecrets = append(hmacSecrets, ref)
		}
		hmacSignatureHeader := ""
		if r.AuthHMACSignatureHeaderSet {
			raw := strings.TrimSpace(resolveValue(r.AuthHMACSignatureHeader, fmt.Sprintf("route %q auth hmac signature_header", rPath), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac signature_header must not be empty", rPath))
			} else if !isMethodToken(raw) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac signature_header %q must be a valid header token", rPath, raw))
			} else {
				hmacSignatureHeader = http.CanonicalHeaderKey(raw)
			}
		}
		hmacTimestampHeader := ""
		if r.AuthHMACTimestampHeaderSet {
			raw := strings.TrimSpace(resolveValue(r.AuthHMACTimestampHeader, fmt.Sprintf("route %q auth hmac timestamp_header", rPath), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac timestamp_header must not be empty", rPath))
			} else if !isMethodToken(raw) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac timestamp_header %q must be a valid header token", rPath, raw))
			} else {
				hmacTimestampHeader = http.CanonicalHeaderKey(raw)
			}
		}
		hmacNonceHeader := ""
		if r.AuthHMACNonceHeaderSet {
			raw := strings.TrimSpace(resolveValue(r.AuthHMACNonceHeader, fmt.Sprintf("route %q auth hmac nonce_header", rPath), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac nonce_header must not be empty", rPath))
			} else if !isMethodToken(raw) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac nonce_header %q must be a valid header token", rPath, raw))
			} else {
				hmacNonceHeader = http.CanonicalHeaderKey(raw)
			}
		}
		hmacTolerance := time.Duration(0)
		if r.AuthHMACToleranceSet {
			raw := strings.TrimSpace(resolveValue(r.AuthHMACTolerance, fmt.Sprintf("route %q auth hmac tolerance", rPath), &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac tolerance %s", rPath, err.Error()))
			} else {
				hmacTolerance = d
			}
		}
		hasHMACOptions := r.AuthHMACSignatureHeaderSet || r.AuthHMACTimestampHeaderSet || r.AuthHMACNonceHeaderSet || r.AuthHMACToleranceSet
		if hasHMACOptions && len(hmacSecrets) == 0 && len(hmacSecretRefs) == 0 {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac options require at least one secret or secret_ref", rPath))
		}
		if len(hmacSecrets) > 0 || len(hmacSecretRefs) > 0 {
			effectiveSignatureHeader := hmacSignatureHeader
			if effectiveSignatureHeader == "" {
				effectiveSignatureHeader = defaultIngressHMACSignatureHeader
			}
			effectiveTimestampHeader := hmacTimestampHeader
			if effectiveTimestampHeader == "" {
				effectiveTimestampHeader = defaultIngressHMACTimestampHeader
			}
			effectiveNonceHeader := hmacNonceHeader
			if effectiveNonceHeader == "" {
				effectiveNonceHeader = defaultIngressHMACNonceHeader
			}
			if strings.EqualFold(effectiveSignatureHeader, effectiveTimestampHeader) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac signature_header and timestamp_header must differ", rPath))
			}
			if strings.EqualFold(effectiveSignatureHeader, effectiveNonceHeader) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac signature_header and nonce_header must differ", rPath))
			}
			if strings.EqualFold(effectiveTimestampHeader, effectiveNonceHeader) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth hmac timestamp_header and nonce_header must differ", rPath))
			}
		}

		basicAuth := make(map[string]string)
		for j, auth := range r.AuthBasic {
			user := strings.TrimSpace(resolveValue(auth.User, fmt.Sprintf("route %q auth basic user[%d]", rPath, j), &res))
			pass := strings.TrimSpace(resolveValue(auth.Pass, fmt.Sprintf("route %q auth basic pass[%d]", rPath, j), &res))
			if user == "" || pass == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth basic must include user and password", rPath))
				continue
			}
			if _, ok := basicAuth[user]; ok {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q auth basic duplicate user %q", rPath, user))
				continue
			}
			basicAuth[user] = pass
		}

		if len(basicAuth) > 0 && (len(hmacSecrets) > 0 || len(hmacSecretRefs) > 0 || hasHMACOptions) {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q auth basic cannot be combined with auth hmac", rPath))
		}
		if len(basicAuth) == 0 {
			basicAuth = nil
		}
		forwardAuth := compileForwardAuthConfig(fmt.Sprintf("route %q auth forward", rPath), r.AuthForward, &res)
		if forwardAuth.Enabled && (len(basicAuth) > 0 || len(hmacSecrets) > 0 || len(hmacSecretRefs) > 0 || hasHMACOptions) {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q auth forward cannot be combined with auth basic or auth hmac", rPath))
		}

		matchCfg, matchOK := compileMatch(rPath, r.Match, &res)
		for _, ref := range r.MatchRefs {
			ref = strings.TrimSpace(resolveValue(ref, fmt.Sprintf("route %q match @%s", rPath, ref), &res))
			if ref == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q match reference must not be empty", rPath))
				matchOK = false
				continue
			}
			nm, ok := namedMatchers[ref]
			if !ok {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q match references unknown matcher @%s", rPath, ref))
				matchOK = false
				continue
			}
			matchCfg.Methods = append(matchCfg.Methods, nm.Methods...)
			matchCfg.Hosts = append(matchCfg.Hosts, nm.Hosts...)
			matchCfg.Headers = append(matchCfg.Headers, nm.Headers...)
			matchCfg.Query = append(matchCfg.Query, nm.Query...)
			matchCfg.RemoteIPs = append(matchCfg.RemoteIPs, nm.RemoteIPs...)
			matchCfg.HeaderExists = append(matchCfg.HeaderExists, nm.HeaderExists...)
			matchCfg.QueryExists = append(matchCfg.QueryExists, nm.QueryExists...)
		}
		routeRateLimit := compileRateLimitConfig(fmt.Sprintf("route %q rate_limit", rPath), r.RateLimit, &res)

		queueBackend := "sqlite"
		if r.Queue != nil && r.Queue.BackendSet {
			raw := strings.TrimSpace(resolveValue(r.Queue.Backend, fmt.Sprintf("route %q queue.backend", rPath), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q queue.backend must not be empty", rPath))
			} else {
				raw = strings.ToLower(raw)
				if raw != "sqlite" && raw != "memory" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q queue.backend must be one of: sqlite, memory", rPath))
				} else {
					queueBackend = raw
				}
			}
		}

		deliveries := make([]CompiledDeliver, 0, len(r.Deliveries))
		deliverTargets := make(map[string]struct{}, len(r.Deliveries))
		deliverOK := true
		for j, d := range r.Deliveries {
			raw := strings.TrimSpace(resolveValue(d.URL, fmt.Sprintf("route %q deliver[%d]", rPath, j), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver target must not be empty", rPath))
				deliverOK = false
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || u == nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] target must be a valid URL", rPath, j))
				deliverOK = false
				continue
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] target must use http or https", rPath, j))
				deliverOK = false
				continue
			}
			if strings.TrimSpace(u.Host) == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] target must include host", rPath, j))
				deliverOK = false
				continue
			}
			if _, ok := deliverTargets[raw]; ok {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q duplicate deliver target %q", rPath, raw))
				deliverOK = false
				continue
			}
			deliverTargets[raw] = struct{}{}

			timeout := defs.DeliverTimeout
			if d.TimeoutSet {
				tRaw := strings.TrimSpace(resolveValue(d.Timeout, fmt.Sprintf("route %q deliver[%d].timeout", rPath, j), &res))
				dur, err := parsePositiveDuration(tRaw)
				if err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].timeout %s", rPath, j, err.Error()))
					deliverOK = false
					continue
				}
				timeout = dur
			}

			retry := defs.DeliverRetry
			if d.Retry != nil {
				rCfg, ok := compileRetry(fmt.Sprintf("route %q deliver[%d].retry", rPath, j), d.Retry, retry, &res)
				if !ok {
					deliverOK = false
					continue
				}
				retry = rCfg
			}

			signing := DeliverSigningHMACConfig{}
			if d.SignHMACSecretSet {
				raw := strings.TrimSpace(resolveValue(d.SignHMACSecret, fmt.Sprintf("route %q deliver[%d].sign hmac", rPath, j), &res))
				if raw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign hmac must not be empty", rPath, j))
					deliverOK = false
				} else {
					if err := secrets.ValidateRef(raw); err != nil {
						res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign hmac %v", rPath, j, err))
						deliverOK = false
						continue
					}
					signing.Enabled = true
					signing.SecretRef = raw
				}
			}
			if len(d.SignHMACSecretRefs) > 0 {
				if d.SignHMACSecretSet {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] sign hmac and sign hmac secret_ref cannot be combined", rPath, j))
					deliverOK = false
				}
				seenSecretRefs := make(map[string]struct{}, len(d.SignHMACSecretRefs))
				for k, rawRef := range d.SignHMACSecretRefs {
					secretID := strings.TrimSpace(resolveValue(rawRef, fmt.Sprintf("route %q deliver[%d].sign hmac secret_ref[%d]", rPath, j, k), &res))
					if secretID == "" {
						res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign hmac secret_ref[%d] must not be empty", rPath, j, k))
						deliverOK = false
						continue
					}
					if _, exists := seenSecretRefs[secretID]; exists {
						res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign hmac secret_ref duplicate %q", rPath, j, secretID))
						deliverOK = false
						continue
					}
					seenSecretRefs[secretID] = struct{}{}

					sc, ok := compiled.Secrets[secretID]
					if !ok {
						res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign hmac secret_ref %q not found", rPath, j, secretID))
						deliverOK = false
						continue
					}

					signing.SecretVersions = append(signing.SecretVersions, DeliverSigningSecretVersion{
						ID:         sc.ID,
						ValueRef:   sc.ValueRef,
						ValidFrom:  sc.ValidFrom,
						ValidUntil: sc.ValidUntil,
						HasUntil:   sc.HasUntil,
					})
				}
				if len(signing.SecretVersions) > 0 {
					signing.Enabled = true
				}
			}
			if d.SignHMACSignatureHeaderSet {
				raw := strings.TrimSpace(resolveValue(d.SignHMACSignatureHeader, fmt.Sprintf("route %q deliver[%d].sign signature_header", rPath, j), &res))
				if raw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign signature_header must not be empty", rPath, j))
					deliverOK = false
				} else if !isMethodToken(raw) {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign signature_header %q must be a valid header token", rPath, j, raw))
					deliverOK = false
				} else {
					signing.SignatureHeader = http.CanonicalHeaderKey(raw)
				}
			}
			if d.SignHMACTimestampHeaderSet {
				raw := strings.TrimSpace(resolveValue(d.SignHMACTimestampHeader, fmt.Sprintf("route %q deliver[%d].sign timestamp_header", rPath, j), &res))
				if raw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign timestamp_header must not be empty", rPath, j))
					deliverOK = false
				} else if !isMethodToken(raw) {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign timestamp_header %q must be a valid header token", rPath, j, raw))
					deliverOK = false
				} else {
					signing.TimestampHeader = http.CanonicalHeaderKey(raw)
				}
			}
			if d.SignHMACSecretSelectionSet {
				raw := strings.ToLower(strings.TrimSpace(resolveValue(d.SignHMACSecretSelection, fmt.Sprintf("route %q deliver[%d].sign secret_selection", rPath, j), &res)))
				switch raw {
				case "newest_valid", "oldest_valid":
					signing.SecretSelection = raw
				case "":
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign secret_selection must not be empty", rPath, j))
					deliverOK = false
				default:
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign secret_selection %q must be newest_valid or oldest_valid", rPath, j, raw))
					deliverOK = false
				}
			}
			if (d.SignHMACSignatureHeaderSet || d.SignHMACTimestampHeaderSet || d.SignHMACSecretSelectionSet) && !d.SignHMACSecretSet && len(d.SignHMACSecretRefs) == 0 {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] sign options require sign hmac", rPath, j))
				deliverOK = false
			}
			if d.SignHMACSecretSelectionSet && d.SignHMACSecretSet {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d] sign secret_selection requires sign hmac secret_ref", rPath, j))
				deliverOK = false
			}
			if signing.Enabled {
				if signing.SecretSelection == "" {
					signing.SecretSelection = defaultDeliverSignHMACSecretSelection
				}
				if signing.SignatureHeader == "" {
					signing.SignatureHeader = defaultDeliverSignHMACSignatureHeader
				}
				if signing.TimestampHeader == "" {
					signing.TimestampHeader = defaultDeliverSignHMACTimestampHeader
				}
				if strings.EqualFold(signing.SignatureHeader, signing.TimestampHeader) {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver[%d].sign signature_header and timestamp_header must differ", rPath, j))
					deliverOK = false
				}
			}
			if !deliverOK {
				continue
			}

			deliveries = append(deliveries, CompiledDeliver{
				URL:         raw,
				Timeout:     timeout,
				Retry:       retry,
				SigningHMAC: signing,
			})
		}
		if !deliverOK {
			continue
		}

		hasPull := r.Pull != nil
		hasDeliver := len(deliveries) > 0
		if hasPull && hasDeliver {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q: pull and deliver are mutually exclusive", rPath))
			continue
		}
		if !hasPull && !hasDeliver {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q: missing pull or deliver block", rPath))
			continue
		}

		deliverConcurrency := defs.DeliverConcurrency
		if r.DeliverConcurrencySet {
			raw := strings.TrimSpace(resolveValue(r.DeliverConcurrency, fmt.Sprintf("route %q deliver_concurrency", rPath), &res))
			if raw == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver_concurrency must not be empty", rPath))
			} else {
				v, err := strconv.Atoi(raw)
				if err != nil || v <= 0 {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver_concurrency must be a positive integer", rPath))
				} else if v > 10000 {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver_concurrency must not exceed 10000", rPath))
				} else {
					deliverConcurrency = v
				}
			}
		}
		if r.DeliverConcurrencySet && !hasDeliver {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q deliver_concurrency requires deliver targets", rPath))
		}

		publishEnabled := true
		publishDirectEnabled := true
		publishManagedEnabled := true
		if r.Publish != nil {
			if r.Publish.EnabledSet {
				raw := strings.TrimSpace(resolveValue(r.Publish.Enabled, fmt.Sprintf("route %q publish", rPath), &res))
				val, ok := parseBoolValue(raw)
				if !ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q publish must be on|off|true|false|1|0", rPath))
				} else {
					publishEnabled = val
				}
			}
			if r.Publish.DirectSet {
				raw := strings.TrimSpace(resolveValue(r.Publish.Direct, fmt.Sprintf("route %q publish.direct", rPath), &res))
				val, ok := parseBoolValue(raw)
				if !ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q publish.direct must be on|off|true|false|1|0", rPath))
				} else {
					publishDirectEnabled = val
				}
			}
			if r.Publish.ManagedSet {
				raw := strings.TrimSpace(resolveValue(r.Publish.Managed, fmt.Sprintf("route %q publish.managed", rPath), &res))
				val, ok := parseBoolValue(raw)
				if !ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q publish.managed must be on|off|true|false|1|0", rPath))
				} else {
					publishManagedEnabled = val
				}
			}
		}

		var pullCfg *PullConfig
		if hasPull {
			hasPullRoutes = true
			rawPath := resolveValue(r.Pull.Path, fmt.Sprintf("route %q pull.path", rPath), &res)
			endpoint, err := normalizePathValue(rawPath)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q pull.path: %s", rPath, err.Error()))
				continue
			}

			if compiled.PullAPI.Prefix != "" && hasPathPrefix(endpoint, compiled.PullAPI.Prefix) {
				res.Errors = append(res.Errors, fmt.Sprintf("route %q pull.path %q must be relative to pull_api.prefix %q", rPath, endpoint, compiled.PullAPI.Prefix))
				continue
			}

			if prev, ok := pathToRoute[endpoint]; ok {
				res.Errors = append(res.Errors, fmt.Sprintf("duplicate pull path %q (routes %q and %q)", endpoint, prev, rPath))
				continue
			}
			pathToRoute[endpoint] = rPath

			pullOK := true
			var pullTokens []string
			seenPullTokens := make(map[string]struct{})
			for j, t := range r.Pull.AuthTokens {
				raw := strings.TrimSpace(resolveValue(t, fmt.Sprintf("route %q pull.auth token[%d]", rPath, j), &res))
				if raw == "" {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q pull.auth token must not be empty", rPath))
					pullOK = false
					continue
				}
				if err := secrets.ValidateRef(raw); err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q pull.auth token[%d] %v", rPath, j, err))
					pullOK = false
					continue
				}
				if _, ok := seenPullTokens[raw]; ok {
					res.Errors = append(res.Errors, fmt.Sprintf("route %q pull.auth token duplicate %q", rPath, raw))
					pullOK = false
					continue
				}
				seenPullTokens[raw] = struct{}{}
				pullTokens = append(pullTokens, raw)
			}
			if len(pullTokens) == 0 {
				pullRoutesMissingAuth = true
			}
			if !pullOK {
				continue
			}

			pullCfg = &PullConfig{Path: endpoint, AuthTokens: pullTokens}
		}

		if !matchOK {
			continue
		}

		routes = append(routes, CompiledRoute{
			ChannelType:             r.ChannelType,
			Path:                    rPath,
			Pull:                    pullCfg,
			Application:             application,
			EndpointName:            endpointName,
			Publish:                 publishEnabled,
			PublishDirect:           publishDirectEnabled,
			PublishManaged:          publishManagedEnabled,
			Match:                   matchCfg,
			RateLimit:               routeRateLimit,
			AuthBasic:               basicAuth,
			AuthForward:             forwardAuth,
			AuthHMACSecrets:         hmacSecrets,
			AuthHMACSecretRefs:      hmacSecretRefs,
			AuthHMACSignatureHeader: hmacSignatureHeader,
			AuthHMACTimestampHeader: hmacTimestampHeader,
			AuthHMACNonceHeader:     hmacNonceHeader,
			AuthHMACTolerance:       hmacTolerance,
			QueueBackend:            queueBackend,
			MaxBodyBytes:            maxBodyBytes,
			MaxHeaderBytes:          maxHeaderBytes,
			DeliverConcurrency:      deliverConcurrency,
			Deliveries:              deliveries,
		})
		if hasDeliver {
			hasDeliverRoutes = true
		}
	}

	compiled.Routes = routes
	compiled.PathToRoute = pathToRoute
	compiled.HasPullRoutes = hasPullRoutes
	compiled.HasDeliverRoutes = hasDeliverRoutes

	if len(compiled.Routes) > 1 {
		selected := strings.TrimSpace(compiled.Routes[0].QueueBackend)
		if selected == "" {
			selected = "sqlite"
		}
		for _, rt := range compiled.Routes[1:] {
			backend := strings.TrimSpace(rt.QueueBackend)
			if backend == "" {
				backend = "sqlite"
			}
			if backend != selected {
				res.Errors = append(res.Errors, "mixed route queue backends are not supported yet (all routes must use the same queue.backend)")
				break
			}
		}
	}

	// Ingress is intentionally separate from Pull/Admin APIs in this MVP slice.
	if compiled.Ingress.Listen == compiled.PullAPI.Listen || compiled.Ingress.Listen == compiled.AdminAPI.Listen {
		res.Errors = append(res.Errors, "ingress.listen must not share a listener with pull_api/admin_api")
	}
	if compiled.PullAPI.GRPCListen != "" {
		if compiled.PullAPI.GRPCListen == compiled.Ingress.Listen ||
			compiled.PullAPI.GRPCListen == compiled.PullAPI.Listen ||
			compiled.PullAPI.GRPCListen == compiled.AdminAPI.Listen {
			res.Errors = append(res.Errors, "pull_api.grpc_listen must not share a listener with ingress/pull_api/admin_api")
		}
	}

	if compiled.Observability.Metrics.Enabled {
		if compiled.Observability.Metrics.Listen == compiled.Ingress.Listen ||
			compiled.Observability.Metrics.Listen == compiled.PullAPI.Listen ||
			compiled.Observability.Metrics.Listen == compiled.AdminAPI.Listen {
			res.Errors = append(res.Errors, "observability.metrics.listen must not share a listener with ingress/pull_api/admin_api")
		}
		if compiled.PullAPI.GRPCListen != "" && compiled.Observability.Metrics.Listen == compiled.PullAPI.GRPCListen {
			res.Errors = append(res.Errors, "pull_api.grpc_listen must not share a listener with observability.metrics.listen")
		}
	}

	if compiled.QueueRetention.PruneInterval <= 0 && (compiled.QueueRetention.Enabled || compiled.DeliveredRetention.Enabled || compiled.DLQRetention.Enabled) {
		res.Errors = append(res.Errors, "queue_retention.prune_interval must be a positive duration when retention is enabled")
	}

	if hasPullRoutes && compiled.PullAPI.Listen == compiled.AdminAPI.Listen {
		compiled.SharedListener = true
		if compiled.PullAPI.Prefix == "" || compiled.AdminAPI.Prefix == "" {
			res.Errors = append(res.Errors, "shared listener requires non-empty pull_api.prefix and admin_api.prefix")
		} else if compiled.PullAPI.Prefix == compiled.AdminAPI.Prefix {
			res.Errors = append(res.Errors, "shared listener requires distinct pull_api.prefix and admin_api.prefix")
		} else if hasPathPrefix(compiled.PullAPI.Prefix, compiled.AdminAPI.Prefix) || hasPathPrefix(compiled.AdminAPI.Prefix, compiled.PullAPI.Prefix) {
			res.Errors = append(res.Errors, "shared listener requires non-overlapping prefixes")
		}
		if compiled.PullAPI.TLS.Enabled != compiled.AdminAPI.TLS.Enabled {
			res.Errors = append(res.Errors, "shared listener requires matching pull_api/admin_api tls settings")
		} else if compiled.PullAPI.TLS.Enabled {
			if compiled.PullAPI.TLS.CertFile != compiled.AdminAPI.TLS.CertFile ||
				compiled.PullAPI.TLS.KeyFile != compiled.AdminAPI.TLS.KeyFile ||
				compiled.PullAPI.TLS.ClientCA != compiled.AdminAPI.TLS.ClientCA ||
				compiled.PullAPI.TLS.ClientAuth != compiled.AdminAPI.TLS.ClientAuth {
				res.Errors = append(res.Errors, "shared listener requires identical pull_api/admin_api tls configuration")
			}
		}
	}

	// Pull API is required only when pull routes are configured.
	if hasPullRoutes && cfg.PullAPI == nil {
		res.Errors = append(res.Errors, "pull_api block is required when using pull routes")
	}
	if compiled.PullAPI.GRPCListen != "" && !hasPullRoutes {
		res.Errors = append(res.Errors, "pull_api.grpc_listen requires at least one pull route")
	}

	// Pull API auth is required when pull routes exist or when pull_api is configured.
	if (hasPullRoutes || cfg.PullAPI != nil) && len(compiled.PullAPI.AuthTokens) == 0 {
		if !hasPullRoutes || pullRoutesMissingAuth {
			res.Errors = append(res.Errors, "pull_api requires auth token allowlist (use: pull_api { auth token \"env:VAR\" } or pull { auth token \"env:VAR\" })")
		}
	}

	res.OK = len(res.Errors) == 0
	return compiled, res
}

func compileDefaults(in *DefaultsBlock) (DefaultsConfig, ValidationResult) {
	var res ValidationResult
	out := DefaultsConfig{
		MaxBodyBytes:   defaultMaxBodyBytes,
		MaxHeaderBytes: defaultMaxHeaderBytes,
		DeliverRetry: RetryConfig{
			Type:   "exponential",
			Max:    defaultDeliverRetryMax,
			Base:   defaultDeliverRetryBase,
			Cap:    defaultDeliverRetryCap,
			Jitter: defaultDeliverRetryJitter,
		},
		DeliverTimeout:     defaultDeliverTimeout,
		DeliverConcurrency: defaultDeliverConcurrency,
		TrendSignals: TrendSignalsConfig{
			Window:                         defaultTrendSignalsWindow,
			ExpectedCaptureInterval:        defaultTrendSignalsCaptureInterval,
			StaleGraceFactor:               defaultTrendSignalsStaleGrace,
			SustainedGrowthConsecutive:     defaultTrendSustainedConsecutive,
			SustainedGrowthMinSamples:      defaultTrendSustainedMinSamples,
			SustainedGrowthMinDelta:        defaultTrendSustainedMinDelta,
			RecentSurgeMinTotal:            defaultTrendSurgeMinTotal,
			RecentSurgeMinDelta:            defaultTrendSurgeMinDelta,
			RecentSurgePercent:             defaultTrendSurgePercent,
			DeadShareHighMinTotal:          defaultTrendDeadMinTotal,
			DeadShareHighPercent:           defaultTrendDeadPercent,
			QueuedPressureMinTotal:         defaultTrendPressureMinTotal,
			QueuedPressurePercent:          defaultTrendPressurePercent,
			QueuedPressureLeasedMultiplier: defaultTrendPressureLeasedFactor,
		},
		AdaptiveBackpressure: AdaptiveBackpressureConfig{
			Enabled:         defaultAdaptiveBackpressureEnabled,
			MinTotal:        defaultAdaptiveBackpressureMinTotal,
			QueuedPercent:   defaultAdaptiveBackpressureQueuedPct,
			ReadyLag:        defaultAdaptiveBackpressureReadyLag,
			OldestQueuedAge: defaultAdaptiveBackpressureOldestAge,
			SustainedGrowth: defaultAdaptiveBackpressureSustained,
		},
		EgressPolicy: EgressPolicyConfig{
			HTTPSOnly:           defaultEgressHTTPSOnly,
			Redirects:           defaultEgressRedirects,
			DNSRebindProtection: defaultEgressDNSRebindProtection,
		},
		PublishPolicy: PublishPolicyConfig{
			DirectEnabled:      defaultPublishDirectEnabled,
			ManagedEnabled:     defaultPublishManagedEnabled,
			AllowPullRoutes:    defaultPublishAllowPullRoutes,
			AllowDeliverRoutes: defaultPublishAllowDeliverRoutes,
			RequireActor:       defaultPublishRequireActor,
			RequireRequestID:   defaultPublishRequireRequestID,
			FailClosed:         defaultPublishFailClosed,
		},
	}

	if in == nil {
		return out, res
	}

	if in.MaxBodySet {
		raw := strings.TrimSpace(resolveValue(in.MaxBody, "defaults.max_body", &res))
		size, err := parseByteSize(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("defaults.max_body %s", err.Error()))
		} else {
			out.MaxBodyBytes = size
		}
	}

	if in.MaxHeadersSet {
		raw := strings.TrimSpace(resolveValue(in.MaxHeaders, "defaults.max_headers", &res))
		size, err := parseByteSize(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("defaults.max_headers %s", err.Error()))
		} else if size > int64(^uint(0)>>1) {
			res.Errors = append(res.Errors, "defaults.max_headers must fit in int")
		} else {
			out.MaxHeaderBytes = int(size)
		}
	}

	if in.Deliver != nil {
		if in.Deliver.Retry != nil {
			retry, ok := compileRetry("defaults.deliver.retry", in.Deliver.Retry, out.DeliverRetry, &res)
			if ok {
				out.DeliverRetry = retry
			}
		}
		if in.Deliver.TimeoutSet {
			raw := strings.TrimSpace(resolveValue(in.Deliver.Timeout, "defaults.deliver.timeout", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("defaults.deliver.timeout %s", err.Error()))
			} else {
				out.DeliverTimeout = d
			}
		}
		if in.Deliver.ConcurrencySet {
			raw := strings.TrimSpace(resolveValue(in.Deliver.Concurrency, "defaults.deliver.concurrency", &res))
			if raw == "" {
				res.Errors = append(res.Errors, "defaults.deliver.concurrency must not be empty")
			} else {
				v, err := strconv.Atoi(raw)
				if err != nil || v <= 0 {
					res.Errors = append(res.Errors, "defaults.deliver.concurrency must be a positive integer")
				} else {
					out.DeliverConcurrency = v
				}
			}
		}
	}

	if in.TrendSignals != nil {
		block := in.TrendSignals
		if block.WindowSet {
			raw := strings.TrimSpace(resolveValue(block.Window, "defaults.trend_signals.window", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("defaults.trend_signals.window %s", err.Error()))
			} else {
				out.TrendSignals.Window = d
			}
		}
		if block.ExpectedCaptureIntervalSet {
			raw := strings.TrimSpace(resolveValue(block.ExpectedCaptureInterval, "defaults.trend_signals.expected_capture_interval", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("defaults.trend_signals.expected_capture_interval %s", err.Error()))
			} else {
				out.TrendSignals.ExpectedCaptureInterval = d
			}
		}
		if block.StaleGraceFactorSet {
			raw := strings.TrimSpace(resolveValue(block.StaleGraceFactor, "defaults.trend_signals.stale_grace_factor", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.stale_grace_factor", 1, 60, &res); ok {
				out.TrendSignals.StaleGraceFactor = v
			}
		}
		if block.SustainedGrowthConsecutiveSet {
			raw := strings.TrimSpace(resolveValue(block.SustainedGrowthConsecutive, "defaults.trend_signals.sustained_growth_consecutive", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.sustained_growth_consecutive", 1, 1000, &res); ok {
				out.TrendSignals.SustainedGrowthConsecutive = v
			}
		}
		if block.SustainedGrowthMinSamplesSet {
			raw := strings.TrimSpace(resolveValue(block.SustainedGrowthMinSamples, "defaults.trend_signals.sustained_growth_min_samples", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.sustained_growth_min_samples", 1, 1000000, &res); ok {
				out.TrendSignals.SustainedGrowthMinSamples = v
			}
		}
		if block.SustainedGrowthMinDeltaSet {
			raw := strings.TrimSpace(resolveValue(block.SustainedGrowthMinDelta, "defaults.trend_signals.sustained_growth_min_delta", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.sustained_growth_min_delta", 1, 1000000000, &res); ok {
				out.TrendSignals.SustainedGrowthMinDelta = v
			}
		}
		if block.RecentSurgeMinTotalSet {
			raw := strings.TrimSpace(resolveValue(block.RecentSurgeMinTotal, "defaults.trend_signals.recent_surge_min_total", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.recent_surge_min_total", 1, 1000000000, &res); ok {
				out.TrendSignals.RecentSurgeMinTotal = v
			}
		}
		if block.RecentSurgeMinDeltaSet {
			raw := strings.TrimSpace(resolveValue(block.RecentSurgeMinDelta, "defaults.trend_signals.recent_surge_min_delta", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.recent_surge_min_delta", 1, 1000000000, &res); ok {
				out.TrendSignals.RecentSurgeMinDelta = v
			}
		}
		if block.RecentSurgePercentSet {
			raw := strings.TrimSpace(resolveValue(block.RecentSurgePercent, "defaults.trend_signals.recent_surge_percent", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.recent_surge_percent", 1, 10000, &res); ok {
				out.TrendSignals.RecentSurgePercent = v
			}
		}
		if block.DeadShareHighMinTotalSet {
			raw := strings.TrimSpace(resolveValue(block.DeadShareHighMinTotal, "defaults.trend_signals.dead_share_high_min_total", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.dead_share_high_min_total", 1, 1000000000, &res); ok {
				out.TrendSignals.DeadShareHighMinTotal = v
			}
		}
		if block.DeadShareHighPercentSet {
			raw := strings.TrimSpace(resolveValue(block.DeadShareHighPercent, "defaults.trend_signals.dead_share_high_percent", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.dead_share_high_percent", 1, 100, &res); ok {
				out.TrendSignals.DeadShareHighPercent = v
			}
		}
		if block.QueuedPressureMinTotalSet {
			raw := strings.TrimSpace(resolveValue(block.QueuedPressureMinTotal, "defaults.trend_signals.queued_pressure_min_total", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.queued_pressure_min_total", 1, 1000000000, &res); ok {
				out.TrendSignals.QueuedPressureMinTotal = v
			}
		}
		if block.QueuedPressurePercentSet {
			raw := strings.TrimSpace(resolveValue(block.QueuedPressurePercent, "defaults.trend_signals.queued_pressure_percent", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.queued_pressure_percent", 1, 100, &res); ok {
				out.TrendSignals.QueuedPressurePercent = v
			}
		}
		if block.QueuedPressureLeasedMultiplierSet {
			raw := strings.TrimSpace(resolveValue(block.QueuedPressureLeasedMultiplier, "defaults.trend_signals.queued_pressure_leased_multiplier", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.trend_signals.queued_pressure_leased_multiplier", 1, 1000000, &res); ok {
				out.TrendSignals.QueuedPressureLeasedMultiplier = v
			}
		}
	}

	if in.AdaptiveBackpressure != nil {
		block := in.AdaptiveBackpressure
		if block.EnabledSet {
			raw := strings.TrimSpace(resolveValue(block.Enabled, "defaults.adaptive_backpressure.enabled", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.adaptive_backpressure.enabled must be on|off|true|false|1|0")
			} else {
				out.AdaptiveBackpressure.Enabled = val
			}
		}
		if block.MinTotalSet {
			raw := strings.TrimSpace(resolveValue(block.MinTotal, "defaults.adaptive_backpressure.min_total", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.adaptive_backpressure.min_total", 1, 1000000000, &res); ok {
				out.AdaptiveBackpressure.MinTotal = v
			}
		}
		if block.QueuedPercentSet {
			raw := strings.TrimSpace(resolveValue(block.QueuedPercent, "defaults.adaptive_backpressure.queued_percent", &res))
			if v, ok := parsePositiveIntInRange(raw, "defaults.adaptive_backpressure.queued_percent", 1, 100, &res); ok {
				out.AdaptiveBackpressure.QueuedPercent = v
			}
		}
		if block.ReadyLagSet {
			raw := strings.TrimSpace(resolveValue(block.ReadyLag, "defaults.adaptive_backpressure.ready_lag", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("defaults.adaptive_backpressure.ready_lag %s", err.Error()))
			} else {
				out.AdaptiveBackpressure.ReadyLag = d
			}
		}
		if block.OldestQueuedAgeSet {
			raw := strings.TrimSpace(resolveValue(block.OldestQueuedAge, "defaults.adaptive_backpressure.oldest_queued_age", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("defaults.adaptive_backpressure.oldest_queued_age %s", err.Error()))
			} else {
				out.AdaptiveBackpressure.OldestQueuedAge = d
			}
		}
		if block.SustainedGrowthSet {
			raw := strings.TrimSpace(resolveValue(block.SustainedGrowth, "defaults.adaptive_backpressure.sustained_growth", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.adaptive_backpressure.sustained_growth must be on|off|true|false|1|0")
			} else {
				out.AdaptiveBackpressure.SustainedGrowth = val
			}
		}
	}

	if in.Egress != nil {
		if in.Egress.HTTPSOnlySet {
			raw := strings.TrimSpace(resolveValue(in.Egress.HTTPSOnly, "defaults.egress.https_only", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.egress.https_only must be on|off|true|false|1|0")
			} else {
				out.EgressPolicy.HTTPSOnly = val
			}
		}
		if in.Egress.RedirectsSet {
			raw := strings.TrimSpace(resolveValue(in.Egress.Redirects, "defaults.egress.redirects", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.egress.redirects must be on|off|true|false|1|0")
			} else {
				out.EgressPolicy.Redirects = val
			}
		}
		if in.Egress.DNSRebindProtectionSet {
			raw := strings.TrimSpace(resolveValue(in.Egress.DNSRebindProtection, "defaults.egress.dns_rebind_protection", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.egress.dns_rebind_protection must be on|off|true|false|1|0")
			} else {
				out.EgressPolicy.DNSRebindProtection = val
			}
		}
		if len(in.Egress.Allow) > 0 {
			out.EgressPolicy.Allow = append(out.EgressPolicy.Allow, compileEgressRules("defaults.egress.allow", in.Egress.Allow, &res)...)
		}
		if len(in.Egress.Deny) > 0 {
			out.EgressPolicy.Deny = append(out.EgressPolicy.Deny, compileEgressRules("defaults.egress.deny", in.Egress.Deny, &res)...)
		}
	}

	if in.PublishPolicy != nil {
		if in.PublishPolicy.DirectSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.Direct, "defaults.publish_policy.direct", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.direct must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.DirectEnabled = val
			}
		}
		if in.PublishPolicy.ManagedSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.Managed, "defaults.publish_policy.managed", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.managed must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.ManagedEnabled = val
			}
		}
		if in.PublishPolicy.AllowPullRoutesSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.AllowPullRoutes, "defaults.publish_policy.allow_pull_routes", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.allow_pull_routes must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.AllowPullRoutes = val
			}
		}
		if in.PublishPolicy.AllowDeliverRoutesSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.AllowDeliverRoutes, "defaults.publish_policy.allow_deliver_routes", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.allow_deliver_routes must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.AllowDeliverRoutes = val
			}
		}
		if in.PublishPolicy.RequireActorSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.RequireActor, "defaults.publish_policy.require_actor", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.require_actor must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.RequireActor = val
			}
		}
		if in.PublishPolicy.RequireRequestIDSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.RequireRequestID, "defaults.publish_policy.require_request_id", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.require_request_id must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.RequireRequestID = val
			}
		}
		if in.PublishPolicy.FailClosedSet {
			raw := strings.TrimSpace(resolveValue(in.PublishPolicy.FailClosed, "defaults.publish_policy.fail_closed", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "defaults.publish_policy.fail_closed must be on|off|true|false|1|0")
			} else {
				out.PublishPolicy.FailClosed = val
			}
		}
		if len(in.PublishPolicy.ActorAllow) > 0 {
			seen := make(map[string]struct{}, len(in.PublishPolicy.ActorAllow))
			for i, rawValue := range in.PublishPolicy.ActorAllow {
				actor := strings.TrimSpace(resolveValue(rawValue, fmt.Sprintf("defaults.publish_policy.actor_allow[%d]", i), &res))
				if actor == "" {
					res.Errors = append(res.Errors, "defaults.publish_policy.actor_allow entries must not be empty")
					continue
				}
				if _, exists := seen[actor]; exists {
					continue
				}
				seen[actor] = struct{}{}
				out.PublishPolicy.ActorAllowlist = append(out.PublishPolicy.ActorAllowlist, actor)
			}
		}
		if len(in.PublishPolicy.ActorPrefix) > 0 {
			seen := make(map[string]struct{}, len(in.PublishPolicy.ActorPrefix))
			for i, rawValue := range in.PublishPolicy.ActorPrefix {
				prefix := strings.TrimSpace(resolveValue(rawValue, fmt.Sprintf("defaults.publish_policy.actor_prefix[%d]", i), &res))
				if prefix == "" {
					res.Errors = append(res.Errors, "defaults.publish_policy.actor_prefix entries must not be empty")
					continue
				}
				if _, exists := seen[prefix]; exists {
					continue
				}
				seen[prefix] = struct{}{}
				out.PublishPolicy.ActorPrefixes = append(out.PublishPolicy.ActorPrefixes, prefix)
			}
		}
	}

	return out, res
}

func compileSecrets(in *SecretsBlock) (map[string]SecretConfig, ValidationResult) {
	var res ValidationResult
	if in == nil || len(in.Items) == 0 {
		return nil, res
	}

	out := make(map[string]SecretConfig, len(in.Items))
	for i, s := range in.Items {
		id := strings.TrimSpace(resolveValue(s.ID, fmt.Sprintf("secrets.items[%d].id", i), &res))
		if id == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("secrets.items[%d].id must not be empty", i))
			continue
		}
		if _, ok := out[id]; ok {
			res.Errors = append(res.Errors, fmt.Sprintf("duplicate secret %q", id))
			continue
		}

		if !s.ValueSet {
			res.Errors = append(res.Errors, fmt.Sprintf("secret %q value is required", id))
			continue
		}
		value := strings.TrimSpace(resolveValue(s.Value, fmt.Sprintf("secret %q value", id), &res))
		if value == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("secret %q value must not be empty", id))
			continue
		}
		if err := secrets.ValidateRef(value); err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("secret %q value %v", id, err))
			continue
		}

		if !s.ValidFromSet {
			res.Errors = append(res.Errors, fmt.Sprintf("secret %q valid_from is required", id))
			continue
		}
		rawFrom := strings.TrimSpace(resolveValue(s.ValidFrom, fmt.Sprintf("secret %q valid_from", id), &res))
		validFrom, err := parseTimestampValue(rawFrom)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("secret %q valid_from %s", id, err.Error()))
			continue
		}

		var validUntil time.Time
		hasUntil := false
		if s.ValidUntilSet {
			rawUntil := strings.TrimSpace(resolveValue(s.ValidUntil, fmt.Sprintf("secret %q valid_until", id), &res))
			if rawUntil == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("secret %q valid_until must not be empty", id))
				continue
			}
			t, err := parseTimestampValue(rawUntil)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("secret %q valid_until %s", id, err.Error()))
				continue
			}
			if !t.After(validFrom) {
				res.Errors = append(res.Errors, fmt.Sprintf("secret %q valid_until must be after valid_from", id))
				continue
			}
			validUntil = t
			hasUntil = true
		}

		out[id] = SecretConfig{
			ID:         id,
			ValueRef:   value,
			ValidFrom:  validFrom,
			ValidUntil: validUntil,
			HasUntil:   hasUntil,
		}
	}

	return out, res
}

func compileObservability(in *ObservabilityBlock) (ObservabilityConfig, ValidationResult) {
	var res ValidationResult
	out := ObservabilityConfig{
		AccessLogEnabled: true,
		AccessLogOutput:  "stderr",
		RuntimeLogOutput: "stderr",
	}

	if in == nil {
		return out, res
	}

	if in.AccessLogSet {
		raw := strings.TrimSpace(resolveValue(in.AccessLog, "observability.access_log", &res))
		val, ok := parseBoolValue(raw)
		if !ok {
			res.Errors = append(res.Errors, "observability.access_log must be on|off|true|false|1|0")
		} else {
			out.AccessLogEnabled = val
		}
	}
	if in.AccessLogBlock != nil {
		if in.AccessLogBlock.EnabledSet {
			raw := strings.TrimSpace(resolveValue(in.AccessLogBlock.Enabled, "observability.access_log.enabled", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "observability.access_log.enabled must be on|off|true|false|1|0")
			} else {
				out.AccessLogEnabled = val
			}
		}
		out.AccessLogOutput, out.AccessLogPath = compileLogSink(
			"observability.access_log",
			in.AccessLogBlock.OutputSet,
			in.AccessLogBlock.Output,
			in.AccessLogBlock.PathSet,
			in.AccessLogBlock.Path,
			&res,
		)
		compileLogFormat("observability.access_log", in.AccessLogBlock.FormatSet, in.AccessLogBlock.Format, &res)
	}

	if in.RuntimeLogSet {
		out.RuntimeLogSet = true
		raw := strings.TrimSpace(resolveValue(in.RuntimeLog, "observability.runtime_log", &res))
		level, disabled, ok := compileRuntimeLogLevel(raw)
		if !ok {
			res.Errors = append(res.Errors, "observability.runtime_log must be debug|info|warn|error|off")
		} else {
			out.RuntimeLogLevel = level
			out.RuntimeLogDisabled = disabled
		}
	}
	if in.RuntimeLogBlock != nil {
		out.RuntimeLogSet = true
		if in.RuntimeLogBlock.LevelSet {
			raw := strings.TrimSpace(resolveValue(in.RuntimeLogBlock.Level, "observability.runtime_log.level", &res))
			level, disabled, ok := compileRuntimeLogLevel(raw)
			if !ok {
				res.Errors = append(res.Errors, "observability.runtime_log.level must be debug|info|warn|error|off")
			} else {
				out.RuntimeLogLevel = level
				out.RuntimeLogDisabled = disabled
			}
		}
		out.RuntimeLogOutput, out.RuntimeLogPath = compileLogSink(
			"observability.runtime_log",
			in.RuntimeLogBlock.OutputSet,
			in.RuntimeLogBlock.Output,
			in.RuntimeLogBlock.PathSet,
			in.RuntimeLogBlock.Path,
			&res,
		)
		compileLogFormat("observability.runtime_log", in.RuntimeLogBlock.FormatSet, in.RuntimeLogBlock.Format, &res)
	}

	if in.Metrics != nil {
		enabled := true
		if in.Metrics.EnabledSet {
			raw := strings.TrimSpace(resolveValue(in.Metrics.Enabled, "observability.metrics.enabled", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "observability.metrics.enabled must be on|off|true|false|1|0")
			} else {
				enabled = val
			}
		}
		out.Metrics.Enabled = enabled
		if !enabled {
			if in.Metrics.ListenSet {
				res.Warnings = append(res.Warnings, "observability.metrics.listen ignored because metrics.enabled is off")
			}
			if in.Metrics.PrefixSet {
				res.Warnings = append(res.Warnings, "observability.metrics.prefix ignored because metrics.enabled is off")
			}
		}

		listen := ""
		prefix := ""
		if enabled && in.Metrics.ListenSet {
			listen = strings.TrimSpace(resolveValue(in.Metrics.Listen, "observability.metrics.listen", &res))
		}
		if enabled && in.Metrics.PrefixSet {
			prefix = strings.TrimSpace(resolveValue(in.Metrics.Prefix, "observability.metrics.prefix", &res))
		}

		if enabled && listen == "" {
			listen = defaultMetricsListen
			res.Warnings = append(res.Warnings, fmt.Sprintf("observability.metrics.listen not set; using default %q", defaultMetricsListen))
		}
		if enabled && prefix == "" {
			prefix = defaultMetricsPrefix
			res.Warnings = append(res.Warnings, fmt.Sprintf("observability.metrics.prefix not set; using default %q", defaultMetricsPrefix))
		}

		if enabled {
			pfx, err := normalizePrefixValue(prefix)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("observability.metrics.prefix: %s", err.Error()))
			} else {
				prefix = pfx
			}
		}

		out.Metrics.Listen = listen
		out.Metrics.Prefix = prefix
	}

	if in.Tracing != nil {
		if in.Tracing.EnabledSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.Enabled, "observability.tracing.enabled", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "observability.tracing.enabled must be on|off|true|false|1|0")
			} else {
				out.TracingEnabled = val
			}
		} else {
			res.Warnings = append(res.Warnings, "observability.tracing.enabled not set; using default off")
		}

		if in.Tracing.CollectorSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.Collector, "observability.tracing.collector", &res))
			if raw == "" {
				res.Errors = append(res.Errors, "observability.tracing.collector must not be empty")
			} else {
				u, err := url.Parse(raw)
				if err != nil || !u.IsAbs() || u.Host == "" {
					res.Errors = append(res.Errors, "observability.tracing.collector must be an absolute http(s) URL")
				} else if u.Scheme != "http" && u.Scheme != "https" {
					res.Errors = append(res.Errors, "observability.tracing.collector must use http or https")
				} else {
					out.TracingCollector = raw
				}
			}
		}
		if in.Tracing.URLPathSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.URLPath, "observability.tracing.url_path", &res))
			pfx, err := normalizePathValue(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.url_path: %s", err.Error()))
			} else {
				out.TracingURLPath = pfx
			}
		}
		if in.Tracing.TimeoutSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.Timeout, "observability.tracing.timeout", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.timeout %s", err.Error()))
			} else {
				out.TracingTimeout = d
				out.TracingTimeoutSet = true
			}
		}
		if in.Tracing.CompressionSet {
			raw := strings.ToLower(strings.TrimSpace(resolveValue(in.Tracing.Compression, "observability.tracing.compression", &res)))
			switch raw {
			case "":
				res.Errors = append(res.Errors, "observability.tracing.compression must not be empty")
			case "none", "gzip":
				out.TracingCompression = raw
			default:
				res.Errors = append(res.Errors, "observability.tracing.compression must be none|gzip")
			}
		}
		if in.Tracing.InsecureSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.Insecure, "observability.tracing.insecure", &res))
			val, ok := parseBoolValue(raw)
			if !ok {
				res.Errors = append(res.Errors, "observability.tracing.insecure must be on|off|true|false|1|0")
			} else {
				out.TracingInsecure = val
			}
		}
		if in.Tracing.ProxyURLSet {
			raw := strings.TrimSpace(resolveValue(in.Tracing.ProxyURL, "observability.tracing.proxy_url", &res))
			if raw == "" {
				res.Errors = append(res.Errors, "observability.tracing.proxy_url must not be empty")
			} else {
				u, err := url.Parse(raw)
				if err != nil || !u.IsAbs() || u.Host == "" {
					res.Errors = append(res.Errors, "observability.tracing.proxy_url must be an absolute http(s) URL")
				} else if u.Scheme != "http" && u.Scheme != "https" {
					res.Errors = append(res.Errors, "observability.tracing.proxy_url must use http or https")
				} else {
					out.TracingProxyURL = raw
				}
			}
		}
		if in.Tracing.TLS != nil {
			t := in.Tracing.TLS
			if t.CAFileSet {
				raw := strings.TrimSpace(resolveValue(t.CAFile, "observability.tracing.tls.ca_file", &res))
				if raw == "" {
					res.Errors = append(res.Errors, "observability.tracing.tls.ca_file must not be empty")
				} else {
					out.TracingTLSCAFile = raw
				}
			}
			if t.CertFileSet {
				raw := strings.TrimSpace(resolveValue(t.CertFile, "observability.tracing.tls.cert_file", &res))
				if raw == "" {
					res.Errors = append(res.Errors, "observability.tracing.tls.cert_file must not be empty")
				} else {
					out.TracingTLSCertFile = raw
				}
			}
			if t.KeyFileSet {
				raw := strings.TrimSpace(resolveValue(t.KeyFile, "observability.tracing.tls.key_file", &res))
				if raw == "" {
					res.Errors = append(res.Errors, "observability.tracing.tls.key_file must not be empty")
				} else {
					out.TracingTLSKeyFile = raw
				}
			}
			if (out.TracingTLSCertFile == "") != (out.TracingTLSKeyFile == "") {
				res.Errors = append(res.Errors, "observability.tracing.tls.cert_file and key_file must be set together")
			}
			if t.ServerNameSet {
				raw := strings.TrimSpace(resolveValue(t.ServerName, "observability.tracing.tls.server_name", &res))
				if raw == "" {
					res.Errors = append(res.Errors, "observability.tracing.tls.server_name must not be empty")
				} else {
					out.TracingTLSServerName = raw
				}
			}
			if t.InsecureSkipVerifySet {
				raw := strings.TrimSpace(resolveValue(t.InsecureSkipVerify, "observability.tracing.tls.insecure_skip_verify", &res))
				val, ok := parseBoolValue(raw)
				if !ok {
					res.Errors = append(res.Errors, "observability.tracing.tls.insecure_skip_verify must be on|off|true|false|1|0")
				} else {
					out.TracingTLSInsecureSkipVerify = val
				}
			}
		}
		if out.TracingInsecure && (out.TracingTLSCAFile != "" || out.TracingTLSCertFile != "" || out.TracingTLSKeyFile != "" || out.TracingTLSServerName != "" || out.TracingTLSInsecureSkipVerify) {
			res.Errors = append(res.Errors, "observability.tracing.insecure cannot be combined with TLS transport options")
		}
		if in.Tracing.Retry != nil {
			retryCfg := TracingRetryConfig{
				Enabled:         defaultTracingRetryEnabled,
				InitialInterval: defaultTracingRetryInitialInterval,
				MaxInterval:     defaultTracingRetryMaxInterval,
				MaxElapsedTime:  defaultTracingRetryMaxElapsedTime,
			}
			if in.Tracing.Retry.EnabledSet {
				raw := strings.TrimSpace(resolveValue(in.Tracing.Retry.Enabled, "observability.tracing.retry.enabled", &res))
				val, ok := parseBoolValue(raw)
				if !ok {
					res.Errors = append(res.Errors, "observability.tracing.retry.enabled must be on|off|true|false|1|0")
				} else {
					retryCfg.Enabled = val
				}
			}
			if in.Tracing.Retry.InitialIntervalSet {
				raw := strings.TrimSpace(resolveValue(in.Tracing.Retry.InitialInterval, "observability.tracing.retry.initial_interval", &res))
				d, err := parsePositiveDuration(raw)
				if err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.retry.initial_interval %s", err.Error()))
				} else {
					retryCfg.InitialInterval = d
				}
			}
			if in.Tracing.Retry.MaxIntervalSet {
				raw := strings.TrimSpace(resolveValue(in.Tracing.Retry.MaxInterval, "observability.tracing.retry.max_interval", &res))
				d, err := parsePositiveDuration(raw)
				if err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.retry.max_interval %s", err.Error()))
				} else {
					retryCfg.MaxInterval = d
				}
			}
			if in.Tracing.Retry.MaxElapsedTimeSet {
				raw := strings.TrimSpace(resolveValue(in.Tracing.Retry.MaxElapsedTime, "observability.tracing.retry.max_elapsed_time", &res))
				d, off, err := parseDurationValue(raw)
				if err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.retry.max_elapsed_time %s", err.Error()))
				} else if off {
					retryCfg.MaxElapsedTime = 0
				} else {
					retryCfg.MaxElapsedTime = d
				}
			}
			if retryCfg.MaxInterval < retryCfg.InitialInterval {
				res.Errors = append(res.Errors, "observability.tracing.retry.max_interval must be >= initial_interval")
			}
			if retryCfg.MaxElapsedTime > 0 && retryCfg.MaxElapsedTime < retryCfg.InitialInterval {
				res.Errors = append(res.Errors, "observability.tracing.retry.max_elapsed_time must be >= initial_interval (or off)")
			}
			out.TracingRetry = &retryCfg
		}

		seenHeaders := make(map[string]struct{}, len(in.Tracing.Headers))
		for i, h := range in.Tracing.Headers {
			nameRaw := resolveValue(h.Name, fmt.Sprintf("observability.tracing.header[%d].name", i), &res)
			valRaw := resolveValue(h.Value, fmt.Sprintf("observability.tracing.header[%d].value", i), &res)
			if strings.TrimSpace(nameRaw) == "" || strings.TrimSpace(valRaw) == "" {
				res.Errors = append(res.Errors, "observability.tracing.header must include name and value")
				continue
			}
			if err := httpheader.ValidateMap(map[string]string{nameRaw: valRaw}); err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.header[%d] %s", i, err.Error()))
				continue
			}
			name := http.CanonicalHeaderKey(strings.TrimSpace(nameRaw))
			key := strings.ToLower(name)
			if _, ok := seenHeaders[key]; ok {
				res.Errors = append(res.Errors, fmt.Sprintf("observability.tracing.header duplicate %q", name))
				continue
			}
			seenHeaders[key] = struct{}{}
			out.TracingHeaders = append(out.TracingHeaders, TracingHeaderConfig{Name: name, Value: strings.TrimSpace(valRaw)})
		}
	}

	return out, res
}

func compileRuntimeLogLevel(raw string) (level string, disabled bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", false, false
	case "off":
		return "", true, true
	case "debug", "info", "warn", "warning", "error":
		if strings.EqualFold(raw, "warning") {
			return "warn", false, true
		}
		return strings.ToLower(raw), false, true
	default:
		return "", false, false
	}
}

func compileLogSink(prefix string, outputSet bool, outputRaw string, pathSet bool, pathRaw string, res *ValidationResult) (output string, path string) {
	output = "stderr"
	if outputSet {
		raw := strings.ToLower(strings.TrimSpace(resolveValue(outputRaw, prefix+".output", res)))
		switch raw {
		case "":
			res.Errors = append(res.Errors, prefix+".output must not be empty")
		case "stdout", "stderr", "file":
			output = raw
		default:
			res.Errors = append(res.Errors, prefix+".output must be stdout|stderr|file")
		}
	}
	if pathSet {
		path = strings.TrimSpace(resolveValue(pathRaw, prefix+".path", res))
		if path == "" {
			res.Errors = append(res.Errors, prefix+".path must not be empty")
		}
	}
	if output == "file" {
		if !pathSet || path == "" {
			res.Errors = append(res.Errors, prefix+".path is required when output is file")
		}
	} else if pathSet {
		res.Errors = append(res.Errors, prefix+".path requires output file")
	}
	return output, path
}

func compileLogFormat(prefix string, set bool, formatRaw string, res *ValidationResult) {
	if !set {
		return
	}
	raw := strings.ToLower(strings.TrimSpace(resolveValue(formatRaw, prefix+".format", res)))
	if raw == "" {
		res.Errors = append(res.Errors, prefix+".format must not be empty")
		return
	}
	if raw != "json" {
		res.Errors = append(res.Errors, prefix+".format must be json")
		return
	}
}

func compileMatch(routePath string, in *MatchBlock, res *ValidationResult) (MatchConfig, bool) {
	var out MatchConfig
	if in == nil {
		return out, true
	}

	ok := true

	for i, m := range in.Methods {
		raw := strings.TrimSpace(resolveValue(m, fmt.Sprintf("route %q match method[%d]", routePath, i), res))
		if raw == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match method must not be empty", routePath))
			ok = false
			continue
		}
		raw = strings.ToUpper(raw)
		if !isMethodToken(raw) {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match method %q must be a valid token", routePath, raw))
			ok = false
			continue
		}
		out.Methods = append(out.Methods, raw)
	}

	for i, h := range in.Hosts {
		raw := strings.TrimSpace(resolveValue(h, fmt.Sprintf("route %q match host[%d]", routePath, i), res))
		if raw == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match host must not be empty", routePath))
			ok = false
			continue
		}
		norm, err := normalizeHostMatch(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match host %s", routePath, err.Error()))
			ok = false
			continue
		}
		out.Hosts = append(out.Hosts, norm)
	}

	for i, h := range in.Headers {
		name := strings.TrimSpace(resolveValue(h.Name, fmt.Sprintf("route %q match header[%d].name", routePath, i), res))
		val := strings.TrimSpace(resolveValue(h.Value, fmt.Sprintf("route %q match header[%d].value", routePath, i), res))
		if name == "" || val == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match header must include name and value", routePath))
			ok = false
			continue
		}
		name = http.CanonicalHeaderKey(name)
		out.Headers = append(out.Headers, HeaderMatchConfig{Name: name, Value: val})
	}
	for i, h := range in.HeaderExists {
		name := strings.TrimSpace(resolveValue(h, fmt.Sprintf("route %q match header_exists[%d]", routePath, i), res))
		if name == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match header_exists must include name", routePath))
			ok = false
			continue
		}
		if !isMethodToken(name) {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match header_exists %q must be a valid token", routePath, name))
			ok = false
			continue
		}
		out.HeaderExists = append(out.HeaderExists, http.CanonicalHeaderKey(name))
	}

	for i, q := range in.Query {
		name := strings.TrimSpace(resolveValue(q.Name, fmt.Sprintf("route %q match query[%d].name", routePath, i), res))
		val := strings.TrimSpace(resolveValue(q.Value, fmt.Sprintf("route %q match query[%d].value", routePath, i), res))
		if name == "" || val == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match query must include name and value", routePath))
			ok = false
			continue
		}
		out.Query = append(out.Query, QueryMatchConfig{Name: name, Value: val})
	}

	for i, rawIP := range in.RemoteIPs {
		raw := strings.TrimSpace(resolveValue(rawIP, fmt.Sprintf("route %q match remote_ip[%d]", routePath, i), res))
		if raw == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match remote_ip must include an IP or CIDR", routePath))
			ok = false
			continue
		}
		pfx, err := parseRemoteIPPrefix(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match remote_ip[%d] %s", routePath, i, err.Error()))
			ok = false
			continue
		}
		out.RemoteIPs = append(out.RemoteIPs, pfx)
	}

	for i, q := range in.QueryExists {
		name := strings.TrimSpace(resolveValue(q, fmt.Sprintf("route %q match query_exists[%d]", routePath, i), res))
		if name == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("route %q match query_exists must include name", routePath))
			ok = false
			continue
		}
		out.QueryExists = append(out.QueryExists, name)
	}

	return out, ok
}

func compileQueueRetention(in *QueueRetentionBlock) (QueueRetentionConfig, ValidationResult) {
	var res ValidationResult
	out := QueueRetentionConfig{
		MaxAge:        defaultQueueRetentionMaxAge,
		PruneInterval: defaultQueueRetentionPruneInterval,
		Enabled:       true,
	}

	if in == nil {
		return out, res
	}

	if in.MaxAgeSet {
		raw := strings.TrimSpace(resolveValue(in.MaxAge, "queue_retention.max_age", &res))
		d, off, err := parseDurationValue(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("queue_retention.max_age %s", err.Error()))
		} else if off || d == 0 {
			out.MaxAge = 0
			out.Enabled = false
		} else {
			out.MaxAge = d
		}
	}

	if in.PruneIntervalSet {
		raw := strings.TrimSpace(resolveValue(in.PruneInterval, "queue_retention.prune_interval", &res))
		d, off, err := parseDurationValue(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("queue_retention.prune_interval %s", err.Error()))
		} else if off || d == 0 {
			out.PruneInterval = 0
		} else {
			out.PruneInterval = d
		}
	}

	return out, res
}

func compileDeliveredRetention(in *DeliveredRetentionBlock) (DeliveredRetentionConfig, ValidationResult) {
	var res ValidationResult
	out := DeliveredRetentionConfig{
		MaxAge:  defaultDeliveredRetentionMaxAge,
		Enabled: defaultDeliveredRetentionMaxAge > 0,
	}

	if in == nil {
		return out, res
	}

	if in.MaxAgeSet {
		raw := strings.TrimSpace(resolveValue(in.MaxAge, "delivered_retention.max_age", &res))
		d, off, err := parseDurationValue(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("delivered_retention.max_age %s", err.Error()))
		} else if off || d == 0 {
			out.MaxAge = 0
			out.Enabled = false
		} else {
			out.MaxAge = d
			out.Enabled = true
		}
	}

	return out, res
}

func compileDLQRetention(in *DLQRetentionBlock) (DLQRetentionConfig, ValidationResult) {
	var res ValidationResult
	out := DLQRetentionConfig{
		MaxAge:   defaultDLQRetentionMaxAge,
		MaxDepth: defaultDLQRetentionMaxDepth,
		Enabled:  true,
	}

	if in == nil {
		return out, res
	}

	if in.MaxAgeSet {
		raw := strings.TrimSpace(resolveValue(in.MaxAge, "dlq_retention.max_age", &res))
		d, off, err := parseDurationValue(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("dlq_retention.max_age %s", err.Error()))
		} else if off || d == 0 {
			out.MaxAge = 0
		} else {
			out.MaxAge = d
		}
	}

	if in.MaxDepthSet {
		raw := strings.TrimSpace(resolveValue(in.MaxDepth, "dlq_retention.max_depth", &res))
		if raw == "" {
			res.Errors = append(res.Errors, "dlq_retention.max_depth must not be empty")
		} else if strings.EqualFold(raw, "off") {
			out.MaxDepth = 0
		} else {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 {
				res.Errors = append(res.Errors, "dlq_retention.max_depth must be a non-negative integer")
			} else {
				out.MaxDepth = v
			}
		}
	}

	out.Enabled = out.MaxAge > 0 || out.MaxDepth > 0
	return out, res
}

func compileQueueLimits(in *QueueLimitsBlock) (QueueLimitsConfig, ValidationResult) {
	var res ValidationResult
	out := QueueLimitsConfig{
		MaxDepth:   defaultQueueMaxDepth,
		DropPolicy: defaultQueueDropPolicy,
	}

	if in == nil {
		return out, res
	}

	if in.MaxDepthSet {
		raw := strings.TrimSpace(resolveValue(in.MaxDepth, "queue_limits.max_depth", &res))
		if raw == "" {
			res.Errors = append(res.Errors, "queue_limits.max_depth must not be empty")
		} else {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 {
				res.Errors = append(res.Errors, "queue_limits.max_depth must be a non-negative integer")
			} else {
				out.MaxDepth = v
			}
		}
	}

	if in.DropPolicySet {
		raw := strings.TrimSpace(resolveValue(in.DropPolicy, "queue_limits.drop_policy", &res))
		switch strings.ToLower(raw) {
		case "reject", "drop_oldest":
			out.DropPolicy = strings.ToLower(raw)
		case "":
			res.Errors = append(res.Errors, "queue_limits.drop_policy must not be empty")
		default:
			res.Errors = append(res.Errors, "queue_limits.drop_policy must be reject|drop_oldest")
		}
	}

	return out, res
}

func compileIngress(in *IngressBlock, defaultListen string) (IngressConfig, ValidationResult) {
	var res ValidationResult
	out := IngressConfig{
		Listen: defaultListen,
	}

	if in == nil {
		return out, res
	}

	if strings.TrimSpace(in.Listen) != "" {
		listen := strings.TrimSpace(resolveValue(in.Listen, "ingress.listen", &res))
		if listen == "" {
			res.Warnings = append(res.Warnings, fmt.Sprintf("ingress.listen resolved empty; using default %q", defaultListen))
		} else {
			out.Listen = listen
		}
	} else {
		res.Warnings = append(res.Warnings, fmt.Sprintf("ingress.listen not set; using default %q", defaultListen))
	}

	out.TLS = compileTLSBlock("ingress", in.TLS, &res)
	out.RateLimit = compileRateLimitConfig("ingress.rate_limit", in.RateLimit, &res)

	return out, res
}

func compileRateLimitConfig(field string, in *RateLimitBlock, res *ValidationResult) RateLimitConfig {
	out := RateLimitConfig{}
	if in == nil {
		return out
	}

	out.Enabled = true

	if !in.RPSSet {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.rps must be set", field))
		out.Enabled = false
		return out
	}

	rawRPS := strings.TrimSpace(resolveValue(in.RPS, field+".rps", res))
	if rawRPS == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.rps must not be empty", field))
		out.Enabled = false
		return out
	}
	rps, err := strconv.ParseFloat(rawRPS, 64)
	if err != nil || rps <= 0 {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.rps must be a positive number", field))
		out.Enabled = false
		return out
	}
	out.RPS = rps

	if in.BurstSet {
		rawBurst := strings.TrimSpace(resolveValue(in.Burst, field+".burst", res))
		if rawBurst == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.burst must not be empty", field))
			out.Enabled = false
			return out
		}
		burst, err := strconv.Atoi(rawBurst)
		if err != nil || burst <= 0 {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.burst must be a positive integer", field))
			out.Enabled = false
			return out
		}
		out.Burst = burst
		return out
	}

	defaultBurst := int(math.Ceil(rps))
	if defaultBurst < 1 {
		defaultBurst = 1
	}
	out.Burst = defaultBurst
	return out
}

func compileForwardAuthConfig(field string, in *ForwardAuthBlock, res *ValidationResult) ForwardAuthConfig {
	out := ForwardAuthConfig{}
	if in == nil {
		return out
	}
	out.Enabled = true
	out.Timeout = defaultForwardAuthTimeout

	ok := true

	rawURL := strings.TrimSpace(resolveValue(in.URL, field+".url", res))
	if rawURL == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.url must not be empty", field))
		ok = false
	} else {
		u, err := url.Parse(rawURL)
		if err != nil || u == nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.url must be a valid URL", field))
			ok = false
		} else if u.Scheme != "http" && u.Scheme != "https" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.url must use http or https", field))
			ok = false
		} else if strings.TrimSpace(u.Host) == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.url must include host", field))
			ok = false
		} else {
			out.URL = u.String()
		}
	}

	if in.TimeoutSet {
		rawTimeout := strings.TrimSpace(resolveValue(in.Timeout, field+".timeout", res))
		timeout, err := parsePositiveDuration(rawTimeout)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.timeout %s", field, err.Error()))
			ok = false
		} else {
			out.Timeout = timeout
		}
	}

	if len(in.CopyHeaders) > 0 {
		seen := make(map[string]struct{}, len(in.CopyHeaders))
		for i, rawName := range in.CopyHeaders {
			name := strings.TrimSpace(resolveValue(rawName, fmt.Sprintf("%s.copy_headers[%d]", field, i), res))
			if name == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.copy_headers must not include empty header names", field))
				ok = false
				continue
			}
			if !isMethodToken(name) {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.copy_headers %q must be a valid header token", field, name))
				ok = false
				continue
			}
			name = http.CanonicalHeaderKey(name)
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			out.CopyHeaders = append(out.CopyHeaders, name)
		}
	}

	if in.BodyLimitSet {
		rawLimit := strings.TrimSpace(resolveValue(in.BodyLimit, field+".body_limit", res))
		switch strings.ToLower(rawLimit) {
		case "", "off", "0":
			out.BodyLimitBytes = 0
		default:
			size, err := parseByteSize(rawLimit)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.body_limit %s", field, err.Error()))
				ok = false
			} else {
				out.BodyLimitBytes = size
			}
		}
	}

	if !ok {
		out.Enabled = false
	}
	return out
}

func compileAPI(name string, in *APIBlock, defaultListen string) (APIConfig, ValidationResult) {
	var res ValidationResult
	out := APIConfig{
		Listen: defaultListen,
		Prefix: "",
	}
	if name == "pull_api" {
		out.MaxBatch = defaultPullAPIMaxBatch
		out.DefaultLeaseTTL = defaultPullAPIDefaultLeaseTTL
	}

	if in == nil {
		return out, res
	}

	if strings.TrimSpace(in.Listen) != "" {
		listen := strings.TrimSpace(resolveValue(in.Listen, fmt.Sprintf("%s.listen", name), &res))
		if listen == "" {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s.listen resolved empty; using default %q", name, defaultListen))
		} else {
			out.Listen = listen
		}
	} else {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%s.listen not set; using default %q", name, defaultListen))
	}

	if strings.TrimSpace(in.Prefix) != "" {
		raw := strings.TrimSpace(resolveValue(in.Prefix, fmt.Sprintf("%s.prefix", name), &res))
		pfx, err := normalizePrefixValue(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.prefix: %s", name, err.Error()))
		} else {
			out.Prefix = pfx
		}
	}

	out.TLS = compileTLSBlock(name, in.TLS, &res)

	for i, t := range in.AuthTokens {
		t = strings.TrimSpace(resolveValue(t, fmt.Sprintf("%s.auth token[%d]", name, i), &res))
		if t == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.auth token must not be empty", name))
			continue
		}
		if err := secrets.ValidateRef(t); err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.auth token[%d] %v", name, i, err))
			continue
		}
		out.AuthTokens = append(out.AuthTokens, t)
	}

	if name == "pull_api" {
		if in.MaxBatchSet {
			raw := strings.TrimSpace(resolveValue(in.MaxBatch, "pull_api.max_batch", &res))
			if raw == "" {
				res.Errors = append(res.Errors, "pull_api.max_batch must not be empty")
			} else {
				v, err := strconv.Atoi(raw)
				if err != nil || v <= 0 {
					res.Errors = append(res.Errors, "pull_api.max_batch must be a positive integer")
				} else {
					out.MaxBatch = v
				}
			}
		}
		if in.GRPCListenSet {
			raw := strings.TrimSpace(resolveValue(in.GRPCListen, "pull_api.grpc_listen", &res))
			if raw == "" {
				res.Errors = append(res.Errors, "pull_api.grpc_listen must not be empty")
			} else {
				out.GRPCListen = raw
			}
		}
		if in.DefaultLeaseTTLSet {
			raw := strings.TrimSpace(resolveValue(in.DefaultLeaseTTL, "pull_api.default_lease_ttl", &res))
			d, err := parsePositiveDuration(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("pull_api.default_lease_ttl %s", err.Error()))
			} else {
				out.DefaultLeaseTTL = d
			}
		}
		if in.MaxLeaseTTLSet {
			raw := strings.TrimSpace(resolveValue(in.MaxLeaseTTL, "pull_api.max_lease_ttl", &res))
			d, off, err := parseDurationValue(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("pull_api.max_lease_ttl %s", err.Error()))
			} else if off || d == 0 {
				out.MaxLeaseTTL = 0
			} else {
				out.MaxLeaseTTL = d
			}
		}
		if in.DefaultMaxWaitSet {
			raw := strings.TrimSpace(resolveValue(in.DefaultMaxWait, "pull_api.default_max_wait", &res))
			d, off, err := parseDurationValue(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("pull_api.default_max_wait %s", err.Error()))
			} else if off {
				out.DefaultMaxWait = 0
			} else {
				out.DefaultMaxWait = d
			}
		}
		if in.MaxWaitSet {
			raw := strings.TrimSpace(resolveValue(in.MaxWait, "pull_api.max_wait", &res))
			d, off, err := parseDurationValue(raw)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("pull_api.max_wait %s", err.Error()))
			} else if off {
				out.MaxWait = 0
			} else {
				out.MaxWait = d
			}
		}
		if in.DefaultLeaseTTLSet && in.MaxLeaseTTLSet && out.MaxLeaseTTL > 0 && out.DefaultLeaseTTL > out.MaxLeaseTTL {
			res.Errors = append(res.Errors, "pull_api.default_lease_ttl must not exceed pull_api.max_lease_ttl")
		}
		if in.DefaultMaxWaitSet && in.MaxWaitSet && out.MaxWait > 0 && out.DefaultMaxWait > out.MaxWait {
			res.Errors = append(res.Errors, "pull_api.default_max_wait must not exceed pull_api.max_wait")
		}
	}

	return out, res
}

func compileTLSBlock(name string, in *TLSBlock, res *ValidationResult) TLSConfig {
	if in == nil {
		return TLSConfig{}
	}
	cert := ""
	if in.CertFileSet {
		cert = strings.TrimSpace(resolveValue(in.CertFile, fmt.Sprintf("%s.tls.cert_file", name), res))
	}
	key := ""
	if in.KeyFileSet {
		key = strings.TrimSpace(resolveValue(in.KeyFile, fmt.Sprintf("%s.tls.key_file", name), res))
	}
	clientCA := ""
	if in.ClientCASet {
		clientCA = strings.TrimSpace(resolveValue(in.ClientCA, fmt.Sprintf("%s.tls.client_ca", name), res))
	}
	clientAuth := tls.NoClientCert
	if in.ClientAuthSet {
		raw := strings.TrimSpace(resolveValue(in.ClientAuth, fmt.Sprintf("%s.tls.client_auth", name), res))
		val, ok := parseClientAuthValue(raw)
		if !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.tls.client_auth must be none|request|require|verify_if_given|require_and_verify", name))
		} else {
			clientAuth = val
		}
	} else if clientCA != "" {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	if cert == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.tls.cert_file must not be empty", name))
	}
	if key == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.tls.key_file must not be empty", name))
	}
	if in.ClientCASet && clientCA == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s.tls.client_ca must not be empty", name))
	}
	if clientAuth == tls.VerifyClientCertIfGiven || clientAuth == tls.RequireAndVerifyClientCert {
		if clientCA == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.tls.client_ca is required for client_auth %s", name, formatClientAuth(clientAuth)))
		}
	}
	if clientCA != "" && (clientAuth == tls.NoClientCert || clientAuth == tls.RequestClientCert || clientAuth == tls.RequireAnyClientCert) {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%s.tls.client_ca set but client_auth is %s (client certs will not be verified)", name, formatClientAuth(clientAuth)))
	}
	if cert != "" && key != "" {
		return TLSConfig{
			Enabled:    true,
			CertFile:   cert,
			KeyFile:    key,
			ClientCA:   clientCA,
			ClientAuth: clientAuth,
		}
	}
	return TLSConfig{}
}

func parseBoolValue(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on":
		return true, true
	case "0", "false", "off":
		return false, true
	default:
		return false, false
	}
}

func parseClientAuthValue(raw string) (tls.ClientAuthType, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "none", "off":
		return tls.NoClientCert, true
	case "request":
		return tls.RequestClientCert, true
	case "require_any":
		return tls.RequireAnyClientCert, true
	case "verify_if_given":
		return tls.VerifyClientCertIfGiven, true
	case "require", "require_and_verify":
		return tls.RequireAndVerifyClientCert, true
	default:
		return tls.NoClientCert, false
	}
}

func formatClientAuth(auth tls.ClientAuthType) string {
	switch auth {
	case tls.NoClientCert:
		return "none"
	case tls.RequestClientCert:
		return "request"
	case tls.RequireAnyClientCert:
		return "require_any"
	case tls.VerifyClientCertIfGiven:
		return "verify_if_given"
	case tls.RequireAndVerifyClientCert:
		return "require"
	default:
		return "unknown"
	}
}

func parseDurationValue(raw string) (time.Duration, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, fmt.Errorf("must not be empty")
	}
	if strings.EqualFold(raw, "off") || raw == "0" {
		return 0, true, nil
	}

	rawLower := strings.ToLower(raw)
	if strings.HasSuffix(rawLower, "d") {
		num := strings.TrimSuffix(rawLower, "d")
		if num == "" {
			return 0, false, fmt.Errorf("must be a duration like 5m, 2h, 7d, or off")
		}
		v, err := strconv.Atoi(num)
		if err != nil || v < 0 {
			return 0, false, fmt.Errorf("must be a non-negative duration")
		}
		return time.Duration(v) * 24 * time.Hour, false, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, false, fmt.Errorf("must be a duration like 5m, 2h, 7d, or off")
	}
	if d < 0 {
		return 0, false, fmt.Errorf("must be a non-negative duration")
	}
	return d, false, nil
}

func parseTimestampValue(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("must not be empty")
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("must be RFC3339 timestamp")
}

func normalizeHostMatch(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("must not be empty")
	}
	if strings.Contains(raw, "://") {
		return "", fmt.Errorf("must be a host, not a URL")
	}
	host := strings.ToLower(raw)
	host = strings.TrimSuffix(host, ".")
	if strings.Contains(host, "*") {
		if host == "*" {
			return host, nil
		}
		if strings.HasPrefix(host, "*.") && strings.Count(host, "*") == 1 {
			suffix := strings.TrimPrefix(host, "*.")
			if suffix == "" || strings.Contains(suffix, "*") {
				return "", fmt.Errorf("wildcard host must be \"*\" or \"*.example.com\"")
			}
			return host, nil
		}
		return "", fmt.Errorf("wildcard host must be \"*\" or \"*.example.com\"")
	}
	host = stripHostPort(host)
	if host == "" {
		return "", fmt.Errorf("invalid host")
	}
	return host, nil
}

func stripHostPort(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if strings.HasPrefix(host, "[") {
		if h, _, err := net.SplitHostPort(host); err == nil {
			return strings.Trim(h, "[]")
		}
		return strings.Trim(host, "[]")
	}
	if strings.Count(host, ":") > 1 {
		return strings.Trim(host, "[]")
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func isMethodToken(raw string) bool {
	if raw == "" {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if !isTokenChar(raw[i]) {
			return false
		}
	}
	return true
}

func isTokenChar(ch byte) bool {
	if ch >= 'A' && ch <= 'Z' {
		return true
	}
	if ch >= 'a' && ch <= 'z' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	switch ch {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	d, off, err := parseDurationValue(raw)
	if err != nil {
		return 0, err
	}
	if off || d <= 0 {
		return 0, fmt.Errorf("must be a positive duration like 5s")
	}
	return d, nil
}

func parsePositiveIntInRange(raw string, field string, min int, max int, res *ValidationResult) (int, bool) {
	if raw == "" {
		res.Errors = append(res.Errors, field+" must not be empty")
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		res.Errors = append(res.Errors, field+" must be an integer")
		return 0, false
	}
	if v < min || v > max {
		res.Errors = append(res.Errors, fmt.Sprintf("%s must be between %d and %d", field, min, max))
		return 0, false
	}
	return v, true
}

func compileEgressRules(field string, values []string, res *ValidationResult) []EgressRule {
	out := make([]EgressRule, 0, len(values))
	for i, raw := range values {
		v := strings.TrimSpace(resolveValue(raw, fmt.Sprintf("%s[%d]", field, i), res))
		if v == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s[%d] must not be empty", field, i))
			continue
		}
		rule, ok := parseEgressRule(v)
		if !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("%s[%d] must be a host, IP, or CIDR", field, i))
			continue
		}
		out = append(out, rule)
	}
	return out
}

func compileVars(in *VarsBlock) (map[string]string, ValidationResult) {
	var res ValidationResult
	if in == nil || len(in.Items) == 0 {
		return nil, res
	}

	raw := make(map[string]string, len(in.Items))
	for i, item := range in.Items {
		name := strings.TrimSpace(resolveValue(item.Name, fmt.Sprintf("vars[%d].name", i), &res))
		if name == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("vars[%d].name must not be empty", i))
			continue
		}
		if !varNamePattern.MatchString(name) {
			res.Errors = append(res.Errors, fmt.Sprintf("vars[%d].name must match %s", i, varNamePattern.String()))
			continue
		}
		if _, ok := raw[name]; ok {
			res.Errors = append(res.Errors, fmt.Sprintf("vars duplicate name %q", name))
			continue
		}

		val := resolveValue(item.Value, fmt.Sprintf("vars[%q]", name), &res)
		raw[name] = val
	}

	if len(raw) == 0 {
		return nil, res
	}

	resolved := make(map[string]string, len(raw))
	for name := range raw {
		if _, ok := resolved[name]; ok {
			continue
		}
		v, ok := resolveVarValue(name, raw, resolved, nil, &res)
		if !ok {
			continue
		}
		resolved[name] = v
	}

	return resolved, res
}

func resolveVarValue(name string, raw map[string]string, resolved map[string]string, stack []string, res *ValidationResult) (string, bool) {
	if v, ok := resolved[name]; ok {
		return v, true
	}

	for i, s := range stack {
		if s != name {
			continue
		}
		cycle := append(append([]string(nil), stack[i:]...), name)
		res.Errors = append(res.Errors, fmt.Sprintf("vars cycle detected: %s", strings.Join(cycle, " -> ")))
		return "", false
	}

	current, ok := raw[name]
	if !ok {
		res.Errors = append(res.Errors, fmt.Sprintf("vars[%q] not defined", name))
		return "", false
	}

	var out strings.Builder
	out.Grow(len(current))

	expanded := true
	for i := 0; i < len(current); {
		if !strings.HasPrefix(current[i:], "{vars.") {
			out.WriteByte(current[i])
			i++
			continue
		}

		end := strings.IndexByte(current[i+6:], '}')
		if end == -1 {
			res.Errors = append(res.Errors, fmt.Sprintf("vars[%q]: unterminated {vars.*} placeholder", name))
			out.WriteString(current[i:])
			expanded = false
			break
		}

		ref := current[i+6 : i+6+end]
		if strings.TrimSpace(ref) == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("vars[%q]: empty name in {vars.*} placeholder", name))
			expanded = false
			i += 6 + end + 1
			continue
		}

		if _, ok := raw[ref]; !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("vars[%q]: unknown var %q", name, ref))
			expanded = false
			i += 6 + end + 1
			continue
		}

		refVal, ok := resolveVarValue(ref, raw, resolved, append(stack, name), res)
		if !ok {
			expanded = false
		}
		out.WriteString(refVal)
		i += 6 + end + 1
	}

	val := out.String()
	if expanded {
		resolved[name] = val
	}
	return val, expanded
}

func cloneConfig(cfg *Config) (*Config, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var out Config
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func applyVarsToConfig(cfg *Config, vars map[string]string, res *ValidationResult) {
	if cfg == nil || len(vars) == 0 {
		return
	}
	applyVarsValue(reflect.ValueOf(cfg), "config", vars, res)
}

func applyVarsValue(v reflect.Value, field string, vars map[string]string, res *ValidationResult) {
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		applyVarsValue(v.Elem(), field, vars, res)
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			name := t.Field(i).Name
			if field == "config" && (name == "Vars" || name == "Preamble") {
				continue
			}
			applyVarsValue(v.Field(i), field+"."+name, vars, res)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			applyVarsValue(v.Index(i), fmt.Sprintf("%s[%d]", field, i), vars, res)
		}
	case reflect.String:
		if !v.CanSet() {
			return
		}
		v.SetString(expandVarsInString(v.String(), vars, field, res))
	}
}

func expandVarsInString(in string, vars map[string]string, field string, res *ValidationResult) string {
	if !strings.Contains(in, "{vars.") {
		return in
	}

	var out strings.Builder
	out.Grow(len(in))

	for i := 0; i < len(in); {
		if !strings.HasPrefix(in[i:], "{vars.") {
			out.WriteByte(in[i])
			i++
			continue
		}

		end := strings.IndexByte(in[i+6:], '}')
		if end == -1 {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: unterminated {vars.*} placeholder", field))
			out.WriteString(in[i:])
			break
		}

		name := in[i+6 : i+6+end]
		if strings.TrimSpace(name) == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: empty name in {vars.*} placeholder", field))
			i += 6 + end + 1
			continue
		}

		val, ok := vars[name]
		if !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: unknown var %q", field, name))
			i += 6 + end + 1
			continue
		}

		out.WriteString(val)
		i += 6 + end + 1
	}

	return out.String()
}

func parseEgressRule(raw string) (EgressRule, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.Contains(raw, "://") {
		return EgressRule{}, false
	}
	if pfx, err := netip.ParsePrefix(raw); err == nil {
		return EgressRule{CIDR: pfx, IsCIDR: true}, true
	}
	if addr, err := netip.ParseAddr(raw); err == nil {
		return EgressRule{CIDR: netip.PrefixFrom(addr, addr.BitLen()), IsCIDR: true}, true
	}

	subdomains := false
	host := raw
	if strings.HasPrefix(host, "*.") {
		subdomains = true
		host = strings.TrimPrefix(host, "*.")
	}
	norm, err := normalizeHostMatch(host)
	if err != nil {
		return EgressRule{}, false
	}
	return EgressRule{Host: norm, Subdomains: subdomains}, true
}

func parseRemoteIPPrefix(raw string) (netip.Prefix, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return netip.Prefix{}, fmt.Errorf("must be an IP or CIDR")
	}
	if pfx, err := netip.ParsePrefix(raw); err == nil {
		return pfx.Masked(), nil
	}
	if addr, err := netip.ParseAddr(raw); err == nil {
		return netip.PrefixFrom(addr, addr.BitLen()), nil
	}
	return netip.Prefix{}, fmt.Errorf("%q must be an IP or CIDR", raw)
}

func parseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("must not be empty")
	}
	if strings.EqualFold(raw, "off") || raw == "0" {
		return 0, fmt.Errorf("must be a positive size like 64kb or 2mb")
	}

	lower := strings.ToLower(raw)
	mult := int64(1)

	switch {
	case strings.HasSuffix(lower, "kb"):
		mult = 1024
		lower = strings.TrimSuffix(lower, "kb")
	case strings.HasSuffix(lower, "k"):
		mult = 1024
		lower = strings.TrimSuffix(lower, "k")
	case strings.HasSuffix(lower, "mb"):
		mult = 1024 * 1024
		lower = strings.TrimSuffix(lower, "mb")
	case strings.HasSuffix(lower, "m"):
		mult = 1024 * 1024
		lower = strings.TrimSuffix(lower, "m")
	case strings.HasSuffix(lower, "gb"):
		mult = 1024 * 1024 * 1024
		lower = strings.TrimSuffix(lower, "gb")
	case strings.HasSuffix(lower, "g"):
		mult = 1024 * 1024 * 1024
		lower = strings.TrimSuffix(lower, "g")
	case strings.HasSuffix(lower, "b"):
		lower = strings.TrimSuffix(lower, "b")
	}

	lower = strings.TrimSpace(lower)
	if lower == "" {
		return 0, fmt.Errorf("must be a positive size like 64kb or 2mb")
	}

	v, err := strconv.ParseInt(lower, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("must be a positive size like 64kb or 2mb")
	}

	size := v * mult
	if size <= 0 {
		return 0, fmt.Errorf("must be a positive size like 64kb or 2mb")
	}
	return size, nil
}

func compileRetry(field string, in *RetryBlock, base RetryConfig, res *ValidationResult) (RetryConfig, bool) {
	out := base
	ok := true
	if in == nil {
		return out, true
	}

	typ := strings.TrimSpace(resolveValue(in.Type, field, res))
	if typ == "" {
		res.Errors = append(res.Errors, fmt.Sprintf("%s type must not be empty", field))
		ok = false
	} else if strings.EqualFold(typ, "exponential") {
		out.Type = "exponential"
	} else {
		res.Errors = append(res.Errors, fmt.Sprintf("%s type must be exponential", field))
		ok = false
	}

	if in.MaxSet {
		raw := strings.TrimSpace(resolveValue(in.Max, field+".max", res))
		if raw == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.max must not be empty", field))
			ok = false
		} else {
			v, err := strconv.Atoi(raw)
			if err != nil || v <= 0 {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.max must be a positive integer", field))
				ok = false
			} else {
				out.Max = v
			}
		}
	}

	if in.BaseSet {
		raw := strings.TrimSpace(resolveValue(in.Base, field+".base", res))
		d, err := parsePositiveDuration(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.base %s", field, err.Error()))
			ok = false
		} else {
			out.Base = d
		}
	}

	if in.CapSet {
		raw := strings.TrimSpace(resolveValue(in.Cap, field+".cap", res))
		d, err := parsePositiveDuration(raw)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.cap %s", field, err.Error()))
			ok = false
		} else {
			out.Cap = d
		}
	}

	if in.JitterSet {
		raw := strings.TrimSpace(resolveValue(in.Jitter, field+".jitter", res))
		if raw == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s.jitter must not be empty", field))
			ok = false
		} else {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil || v < 0 || v > 1 {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.jitter must be a number between 0 and 1", field))
				ok = false
			} else {
				out.Jitter = v
			}
		}
	}

	if out.Base > out.Cap {
		res.Errors = append(res.Errors, fmt.Sprintf("%s base must be <= cap", field))
		ok = false
	}

	return out, ok
}

func normalizePrefixValue(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if !strings.HasPrefix(s, "/") {
		return "", fmt.Errorf("must start with '/'")
	}
	c := path.Clean(s)
	if c == "/" {
		// "/" is equivalent to "no prefix"; keep it empty.
		return "", nil
	}
	if !strings.HasPrefix(c, "/") {
		return "", fmt.Errorf("must start with '/'")
	}
	return c, nil
}

func normalizePathValue(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("must not be empty")
	}
	if !strings.HasPrefix(s, "/") {
		return "", fmt.Errorf("must start with '/'")
	}
	c := path.Clean(s)
	if !strings.HasPrefix(c, "/") {
		// Should not happen due to the leading '/' check, but keep it safe.
		c = "/" + c
	}
	return c, nil
}

func hasPathPrefix(p, prefix string) bool {
	if prefix == "" {
		return true
	}
	if p == prefix {
		return true
	}
	if strings.HasPrefix(p, prefix+"/") {
		return true
	}
	return false
}
