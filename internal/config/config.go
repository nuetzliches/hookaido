package config

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Config is the parsed, user-authored configuration file.
//
// It intentionally keeps optional blocks as pointers so we can distinguish
// between "not set" (use defaults) and "set but empty" (usually invalid).
type Config struct {
	// Preamble holds leading comment lines (including the leading '#').
	// It is preserved by `config fmt` to avoid losing file headers.
	Preamble []string

	Ingress *IngressBlock

	Defaults *DefaultsBlock
	Vars     *VarsBlock

	Secrets *SecretsBlock

	PullAPI            *APIBlock
	AdminAPI           *APIBlock
	Observability      *ObservabilityBlock
	QueueRetention     *QueueRetentionBlock
	DeliveredRetention *DeliveredRetentionBlock
	DLQRetention       *DLQRetentionBlock
	QueueLimits        *QueueLimitsBlock

	NamedMatchers []NamedMatcher

	// Routes are evaluated top-down (first match wins).
	Routes []Route
}

type IngressBlock struct {
	Listen       string
	ListenQuoted bool
	TLS          *TLSBlock
	RateLimit    *RateLimitBlock
}

type DefaultsBlock struct {
	MaxBody       string
	MaxBodyQuoted bool
	MaxBodySet    bool

	MaxHeaders       string
	MaxHeadersQuoted bool
	MaxHeadersSet    bool

	Egress        *EgressBlock
	PublishPolicy *PublishPolicyBlock

	Deliver      *DeliverBlock
	TrendSignals *TrendSignalsBlock

	AdaptiveBackpressure *AdaptiveBackpressureBlock
}

type VarsBlock struct {
	Items []VarItem
}

type VarItem struct {
	Name        string
	NameQuoted  bool
	Value       string
	ValueQuoted bool
}

type SecretsBlock struct {
	Items []SecretBlock
}

type SecretBlock struct {
	ID       string
	IDQuoted bool

	Value       string
	ValueQuoted bool
	ValueSet    bool

	ValidFrom       string
	ValidFromQuoted bool
	ValidFromSet    bool

	ValidUntil       string
	ValidUntilQuoted bool
	ValidUntilSet    bool
}

type APIBlock struct {
	Listen       string
	ListenQuoted bool
	Prefix       string
	PrefixQuoted bool

	TLS *TLSBlock

	// AuthTokens holds token references for Pull/Admin API auth.
	// For now, only Pull API tokens are used by the runtime.
	AuthTokens       []string
	AuthTokensQuoted []bool

	MaxBatch       string
	MaxBatchQuoted bool
	MaxBatchSet    bool

	GRPCListen       string
	GRPCListenQuoted bool
	GRPCListenSet    bool

	DefaultLeaseTTL       string
	DefaultLeaseTTLQuoted bool
	DefaultLeaseTTLSet    bool

	MaxLeaseTTL       string
	MaxLeaseTTLQuoted bool
	MaxLeaseTTLSet    bool

	DefaultMaxWait       string
	DefaultMaxWaitQuoted bool
	DefaultMaxWaitSet    bool

	MaxWait       string
	MaxWaitQuoted bool
	MaxWaitSet    bool
}

type ObservabilityBlock struct {
	AccessLog       string
	AccessLogQuoted bool
	AccessLogSet    bool
	AccessLogBlock  *AccessLogBlock

	RuntimeLog       string
	RuntimeLogQuoted bool
	RuntimeLogSet    bool
	RuntimeLogBlock  *RuntimeLogBlock

	Metrics *MetricsBlock
	Tracing *TracingBlock
}

type AccessLogBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	Output       string
	OutputQuoted bool
	OutputSet    bool

	Path       string
	PathQuoted bool
	PathSet    bool

	Format       string
	FormatQuoted bool
	FormatSet    bool
}

type RuntimeLogBlock struct {
	Level       string
	LevelQuoted bool
	LevelSet    bool

	Output       string
	OutputQuoted bool
	OutputSet    bool

	Path       string
	PathQuoted bool
	PathSet    bool

	Format       string
	FormatQuoted bool
	FormatSet    bool
}

type MetricsBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	Listen       string
	ListenQuoted bool
	ListenSet    bool
	Prefix       string
	PrefixQuoted bool
	PrefixSet    bool

	// shorthand tracks `observability { metrics on|off }` style so fmt can
	// preserve compact DSL form.
	shorthand bool
}

type TracingBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	Collector       string
	CollectorQuoted bool
	CollectorSet    bool

	URLPath       string
	URLPathQuoted bool
	URLPathSet    bool

	Timeout       string
	TimeoutQuoted bool
	TimeoutSet    bool

	Compression       string
	CompressionQuoted bool
	CompressionSet    bool

	Insecure       string
	InsecureQuoted bool
	InsecureSet    bool

	ProxyURL       string
	ProxyURLQuoted bool
	ProxyURLSet    bool

	TLS *TracingTLSBlock

	Retry *TracingRetryBlock

	Headers []TracingHeader

	// shorthand tracks `observability { tracing on|off }` style so fmt can
	// preserve compact DSL form.
	shorthand bool
}

type TracingHeader struct {
	Name        string
	NameQuoted  bool
	Value       string
	ValueQuoted bool
}

type TracingRetryBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	InitialInterval       string
	InitialIntervalQuoted bool
	InitialIntervalSet    bool

	MaxInterval       string
	MaxIntervalQuoted bool
	MaxIntervalSet    bool

	MaxElapsedTime       string
	MaxElapsedTimeQuoted bool
	MaxElapsedTimeSet    bool
}

type QueueLimitsBlock struct {
	MaxDepth         string
	MaxDepthQuoted   bool
	MaxDepthSet      bool
	DropPolicy       string
	DropPolicyQuoted bool
	DropPolicySet    bool
}

type QueueRetentionBlock struct {
	MaxAge              string
	MaxAgeQuoted        bool
	MaxAgeSet           bool
	PruneInterval       string
	PruneIntervalQuoted bool
	PruneIntervalSet    bool
}

type DeliveredRetentionBlock struct {
	MaxAge       string
	MaxAgeQuoted bool
	MaxAgeSet    bool
}

type DLQRetentionBlock struct {
	MaxAge         string
	MaxAgeQuoted   bool
	MaxAgeSet      bool
	MaxDepth       string
	MaxDepthQuoted bool
	MaxDepthSet    bool
}

// ChannelType determines which directives are allowed on a route.
type ChannelType string

const (
	// ChannelDefault is the zero value — bare top-level routes (implicit inbound).
	ChannelDefault  ChannelType = ""
	ChannelInbound  ChannelType = "inbound"
	ChannelOutbound ChannelType = "outbound"
	ChannelInternal ChannelType = "internal"
)

type Route struct {
	ChannelType ChannelType

	Path       string
	PathQuoted bool

	Application       string
	ApplicationQuoted bool
	ApplicationSet    bool

	EndpointName       string
	EndpointNameQuoted bool
	EndpointNameSet    bool

	Match     *MatchBlock
	MatchRefs []string
	RateLimit *RateLimitBlock

	// Pull and deliver are mutually exclusive in the current MVP slice.
	Pull *Pull

	Queue *QueueBlock

	Deliveries []Deliver

	DeliverConcurrency       string
	DeliverConcurrencyQuoted bool
	DeliverConcurrencySet    bool

	MaxBody       string
	MaxBodyQuoted bool
	MaxBodySet    bool

	MaxHeaders       string
	MaxHeadersQuoted bool
	MaxHeadersSet    bool

	Publish *PublishBlock

	AuthBasic []BasicAuth

	AuthForward *ForwardAuthBlock

	// AuthHMACSecrets holds secret references for ingress HMAC verification.
	AuthHMACSecrets       []string
	AuthHMACSecretsQuoted []bool
	AuthHMACSecretIsRef   []bool
	AuthHMACBlockSet      bool

	AuthHMACSignatureHeader       string
	AuthHMACSignatureHeaderQuoted bool
	AuthHMACSignatureHeaderSet    bool

	AuthHMACTimestampHeader       string
	AuthHMACTimestampHeaderQuoted bool
	AuthHMACTimestampHeaderSet    bool

	AuthHMACNonceHeader       string
	AuthHMACNonceHeaderQuoted bool
	AuthHMACNonceHeaderSet    bool

	AuthHMACTolerance       string
	AuthHMACToleranceQuoted bool
	AuthHMACToleranceSet    bool
}

type MatchBlock struct {
	Methods       []string
	MethodsQuoted []bool

	Hosts       []string
	HostsQuoted []bool

	Headers []HeaderMatch
	Query   []QueryMatch

	RemoteIPs       []string
	RemoteIPsQuoted []bool

	HeaderExists       []string
	HeaderExistsQuoted []bool

	QueryExists       []string
	QueryExistsQuoted []bool
}

type RateLimitBlock struct {
	RPS       string
	RPSQuoted bool
	RPSSet    bool

	Burst       string
	BurstQuoted bool
	BurstSet    bool
}

type NamedMatcher struct {
	Name  string
	Match *MatchBlock
}

type HeaderMatch struct {
	Name        string
	NameQuoted  bool
	Value       string
	ValueQuoted bool
}

type QueryMatch struct {
	Name        string
	NameQuoted  bool
	Value       string
	ValueQuoted bool
}

type BasicAuth struct {
	User       string
	UserQuoted bool
	Pass       string
	PassQuoted bool
}

type ForwardAuthBlock struct {
	URL       string
	URLQuoted bool
	BlockSet  bool

	Timeout       string
	TimeoutQuoted bool
	TimeoutSet    bool

	CopyHeaders       []string
	CopyHeadersQuoted []bool

	BodyLimit       string
	BodyLimitQuoted bool
	BodyLimitSet    bool
}

type QueueBlock struct {
	Backend       string
	BackendQuoted bool
	BackendSet    bool

	// shorthand tracks `queue sqlite|memory|postgres` style so fmt can preserve compact
	// DSL form.
	shorthand bool
}

type Pull struct {
	Path       string
	PathQuoted bool

	AuthTokens       []string
	AuthTokensQuoted []bool
}

type TLSBlock struct {
	CertFile       string
	CertFileQuoted bool
	CertFileSet    bool

	KeyFile       string
	KeyFileQuoted bool
	KeyFileSet    bool

	ClientCA       string
	ClientCAQuoted bool
	ClientCASet    bool

	ClientAuth       string
	ClientAuthQuoted bool
	ClientAuthSet    bool
}

type EgressBlock struct {
	Allow       []string
	AllowQuoted []bool

	Deny       []string
	DenyQuoted []bool

	HTTPSOnly       string
	HTTPSOnlyQuoted bool
	HTTPSOnlySet    bool

	Redirects       string
	RedirectsQuoted bool
	RedirectsSet    bool

	DNSRebindProtection       string
	DNSRebindProtectionQuoted bool
	DNSRebindProtectionSet    bool
}

type TracingTLSBlock struct {
	CAFile       string
	CAFileQuoted bool
	CAFileSet    bool

	CertFile       string
	CertFileQuoted bool
	CertFileSet    bool

	KeyFile       string
	KeyFileQuoted bool
	KeyFileSet    bool

	ServerName       string
	ServerNameQuoted bool
	ServerNameSet    bool

	InsecureSkipVerify       string
	InsecureSkipVerifyQuoted bool
	InsecureSkipVerifySet    bool
}

type PublishBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	Direct       string
	DirectQuoted bool
	DirectSet    bool

	Managed       string
	ManagedQuoted bool
	ManagedSet    bool

	// shorthand tracks `publish on|off` style so fmt can preserve compact form.
	shorthand bool
	// dotNotation tracks `publish.direct`/`publish.managed` style so fmt can preserve it.
	dotNotation bool
}

type PublishPolicyBlock struct {
	Direct       string
	DirectQuoted bool
	DirectSet    bool

	Managed       string
	ManagedQuoted bool
	ManagedSet    bool

	AllowPullRoutes       string
	AllowPullRoutesQuoted bool
	AllowPullRoutesSet    bool

	AllowDeliverRoutes       string
	AllowDeliverRoutesQuoted bool
	AllowDeliverRoutesSet    bool

	RequireActor       string
	RequireActorQuoted bool
	RequireActorSet    bool

	RequireRequestID       string
	RequireRequestIDQuoted bool
	RequireRequestIDSet    bool

	FailClosed       string
	FailClosedQuoted bool
	FailClosedSet    bool

	ActorAllow       []string
	ActorAllowQuoted []bool

	ActorPrefix       []string
	ActorPrefixQuoted []bool
}

type Deliver struct {
	URL       string
	URLQuoted bool

	Retry   *RetryBlock
	Timeout string

	TimeoutQuoted bool
	TimeoutSet    bool

	SignHMACSecret       string
	SignHMACSecretQuoted bool
	SignHMACSecretSet    bool

	SignHMACSecretRefs       []string
	SignHMACSecretRefsQuoted []bool

	SignHMACSignatureHeader       string
	SignHMACSignatureHeaderQuoted bool
	SignHMACSignatureHeaderSet    bool

	SignHMACTimestampHeader       string
	SignHMACTimestampHeaderQuoted bool
	SignHMACTimestampHeaderSet    bool

	SignHMACSecretSelection       string
	SignHMACSecretSelectionQuoted bool
	SignHMACSecretSelectionSet    bool
}

type DeliverBlock struct {
	Retry   *RetryBlock
	Timeout string

	TimeoutQuoted bool
	TimeoutSet    bool

	Concurrency       string
	ConcurrencyQuoted bool
	ConcurrencySet    bool
}

type TrendSignalsBlock struct {
	Window       string
	WindowQuoted bool
	WindowSet    bool

	ExpectedCaptureInterval       string
	ExpectedCaptureIntervalQuoted bool
	ExpectedCaptureIntervalSet    bool

	StaleGraceFactor       string
	StaleGraceFactorQuoted bool
	StaleGraceFactorSet    bool

	SustainedGrowthConsecutive       string
	SustainedGrowthConsecutiveQuoted bool
	SustainedGrowthConsecutiveSet    bool

	SustainedGrowthMinSamples       string
	SustainedGrowthMinSamplesQuoted bool
	SustainedGrowthMinSamplesSet    bool

	SustainedGrowthMinDelta       string
	SustainedGrowthMinDeltaQuoted bool
	SustainedGrowthMinDeltaSet    bool

	RecentSurgeMinTotal       string
	RecentSurgeMinTotalQuoted bool
	RecentSurgeMinTotalSet    bool

	RecentSurgeMinDelta       string
	RecentSurgeMinDeltaQuoted bool
	RecentSurgeMinDeltaSet    bool

	RecentSurgePercent       string
	RecentSurgePercentQuoted bool
	RecentSurgePercentSet    bool

	DeadShareHighMinTotal       string
	DeadShareHighMinTotalQuoted bool
	DeadShareHighMinTotalSet    bool

	DeadShareHighPercent       string
	DeadShareHighPercentQuoted bool
	DeadShareHighPercentSet    bool

	QueuedPressureMinTotal       string
	QueuedPressureMinTotalQuoted bool
	QueuedPressureMinTotalSet    bool

	QueuedPressurePercent       string
	QueuedPressurePercentQuoted bool
	QueuedPressurePercentSet    bool

	QueuedPressureLeasedMultiplier       string
	QueuedPressureLeasedMultiplierQuoted bool
	QueuedPressureLeasedMultiplierSet    bool
}

type AdaptiveBackpressureBlock struct {
	Enabled       string
	EnabledQuoted bool
	EnabledSet    bool

	MinTotal       string
	MinTotalQuoted bool
	MinTotalSet    bool

	QueuedPercent       string
	QueuedPercentQuoted bool
	QueuedPercentSet    bool

	ReadyLag       string
	ReadyLagQuoted bool
	ReadyLagSet    bool

	OldestQueuedAge       string
	OldestQueuedAgeQuoted bool
	OldestQueuedAgeSet    bool

	SustainedGrowth       string
	SustainedGrowthQuoted bool
	SustainedGrowthSet    bool
}

type RetryBlock struct {
	Type       string
	TypeQuoted bool

	Max       string
	MaxQuoted bool
	MaxSet    bool

	Base       string
	BaseQuoted bool
	BaseSet    bool

	Cap       string
	CapQuoted bool
	CapSet    bool

	Jitter       string
	JitterQuoted bool
	JitterSet    bool
}

func Parse(input []byte) (*Config, error) {
	norm := normalizeInput(input)
	p := newParser(string(norm))
	cfg, err := p.parse()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, errors.New("empty config")
	}
	return cfg, nil
}

// Format returns a deterministic representation of the parsed config.
//
// The formatter does not expand defaults; it formats only what is present in
// the input file.
func Format(cfg *Config) ([]byte, error) {
	if cfg == nil {
		return nil, errors.New("nil config")
	}
	out, err := format(cfg)
	if err != nil {
		return nil, err
	}
	return canonicalize(out), nil
}

// Validate checks whether the config can be compiled for runtime.
func Validate(cfg *Config) error {
	_, res := Compile(cfg)
	if res.OK {
		return nil
	}
	if len(res.Errors) == 0 {
		return errors.New("invalid config")
	}
	return errors.New(res.Errors[0])
}

type ValidationResult struct {
	OK       bool     `json:"ok"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

type ValidationOptions struct {
	// SecretPreflight loads all compiled secret refs (`env:`, `file:`, `vault:`,
	// `raw:`) to catch missing/unreachable secrets during validation.
	SecretPreflight bool
}

func ValidateWithResult(cfg *Config) ValidationResult {
	return ValidateWithResultOptions(cfg, ValidationOptions{})
}

func ValidateWithResultOptions(cfg *Config, options ValidationOptions) ValidationResult {
	compiled, res := Compile(cfg)
	if !res.OK || !options.SecretPreflight {
		return res
	}
	for _, errMsg := range validateSecretPreflight(compiled) {
		res.Errors = append(res.Errors, errMsg)
	}
	if len(res.Errors) > 0 {
		res.OK = false
	}
	return res
}

func FormatValidationJSON(res ValidationResult) (string, error) {
	out, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func FormatValidationText(res ValidationResult) string {
	if res.OK {
		if len(res.Warnings) == 0 {
			return "config ok"
		}
		return fmt.Sprintf("config ok (warnings: %d)", len(res.Warnings))
	}
	if len(res.Errors) == 0 {
		return "config invalid"
	}
	return fmt.Sprintf("config invalid: %s", res.Errors[0])
}
