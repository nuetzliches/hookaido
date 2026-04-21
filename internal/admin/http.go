package admin

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
	"github.com/nuetzliches/hookaido/internal/queue"
)

const (
	defaultListLimit           = 100
	healthTrendSignalSamples   = 500
	defaultQueueHealthCacheTTL = time.Second

	auditReasonHeader     = "X-Hookaido-Audit-Reason"
	auditActorHeader      = "X-Hookaido-Audit-Actor"
	auditRequestIDHeader  = "X-Request-ID"
	maxAuditReasonLength  = 512
	maxAuditActorLength   = 256
	maxAuditRequestIDSize = 256
	defaultMaxBodyBytes   = 2 << 20 // 2 MiB
	defaultMaxHeaderBytes = 64 << 10

	publishCodeAuditReasonRequired         = "audit_reason_required"
	publishCodeAuditActorRequired          = "audit_actor_required"
	publishCodeAuditActorNotAllowed        = "audit_actor_not_allowed"
	publishCodeAuditRequestIDRequired      = "audit_request_id_required"
	publishCodeInvalidBody                 = "invalid_body"
	publishCodeManagedResolverMissing      = "managed_resolver_missing"
	publishCodeManagedEndpointNotFound     = "managed_endpoint_not_found"
	publishCodeRouteMismatch               = "route_mismatch"
	publishCodeManagedSelectorRequired     = "managed_selector_required"
	publishCodeRouteResolverMissing        = "route_resolver_missing"
	publishCodeManagedTargetMismatch       = "managed_target_mismatch"
	publishCodeRouteNotFound               = "route_not_found"
	publishCodeTargetUnresolvable          = "target_unresolvable"
	publishCodeInvalidReceivedAt           = "invalid_received_at"
	publishCodeInvalidNextRunAt            = "invalid_next_run_at"
	publishCodeInvalidPayload              = "invalid_payload_b64"
	publishCodeInvalidHeader               = "invalid_header"
	publishCodePayloadTooLarge             = "payload_too_large"
	publishCodeHeadersTooLarge             = "headers_too_large"
	publishCodeDuplicateID                 = "duplicate_id"
	publishCodeQueueFull                   = "queue_full"
	publishCodeStoreUnavailable            = "store_unavailable"
	publishCodeScopedSelectorMismatch      = "selector_scope_mismatch"
	publishCodeScopedSelectorForbidden     = "selector_scope_forbidden"
	publishCodeManagedEndpointNoTargets    = "managed_endpoint_no_targets"
	publishCodeScopedPublishRequired       = "scoped_publish_required"
	publishCodeGlobalPublishDisabled       = "global_publish_disabled"
	publishCodeScopedPublishDisabled       = "scoped_publish_disabled"
	publishCodePullRoutePublishDisabled    = "pull_route_publish_disabled"
	publishCodeDeliverRoutePublishDisabled = "deliver_route_publish_disabled"
	publishCodeRoutePublishDisabled        = "route_publish_disabled"

	managementCodeUnavailable      = "management_unavailable"
	managementCodeRouteNotFound    = "management_route_not_found"
	managementCodeEndpointNotFound = "management_endpoint_not_found"
	managementCodeRouteMapped      = "management_route_already_mapped"
	managementCodeRoutePublishOff  = "management_route_publish_disabled"
	managementCodeRouteTargetDrift = "management_route_target_mismatch"
	managementCodeRouteBacklog     = "management_route_backlog_active"
	managementCodeConflict         = "management_conflict"

	secretCodeUnavailable    = "secret_management_unavailable"
	secretCodePoolNotFound   = "secret_pool_not_found"
	secretCodeNotRuntime     = "secret_not_runtime"
	secretCodeValueEmpty     = "secret_value_empty"
	secretCodeInvalidWindow  = "secret_invalid_window"
	secretCodePoolFull       = "secret_pool_full"
	secretCodeDuplicateID    = "secret_duplicate_id"
	secretCodeIDNotFound     = "secret_id_not_found"
	secretCodeSealFailure    = "secret_seal_failure"
	secretCodePersistFailure = "secret_persist_failure"
	readCodeUnauthorized           = "unauthorized"
	readCodeMethodNotAllowed       = "method_not_allowed"
	readCodeInvalidQuery           = "invalid_query"
	readCodeNotFound               = "not_found"
	readCodeBacklogUnavailable     = "backlog_unavailable"
	readCodeManagementUnavailable  = "management_model_unavailable"
)

type Authorizer func(r *http.Request) bool

var (
	ErrManagementRouteNotFound    = errors.New("management route not found")
	ErrManagementEndpointNotFound = errors.New("management endpoint not found")
	ErrManagementConflict         = errors.New("management conflict")
	errManagedPolicyLookupMissing = errors.New("managed policy lookup is unavailable")
	errRequestBodyTooLarge        = errors.New("request body too large")
)

type managedOwnershipMismatchError struct {
	detail string
}

func (e *managedOwnershipMismatchError) Error() string {
	if e == nil || strings.TrimSpace(e.detail) == "" {
		return "managed route ownership mapping is out of sync"
	}
	return strings.TrimSpace(e.detail)
}

func BearerTokenAuthorizer(tokens [][]byte) Authorizer {
	allowed := make([][]byte, 0, len(tokens))
	for _, t := range tokens {
		if len(t) == 0 {
			continue
		}
		cp := make([]byte, len(t))
		copy(cp, t)
		allowed = append(allowed, cp)
	}

	return func(r *http.Request) bool {
		if len(allowed) == 0 {
			return true
		}

		h := r.Header.Get("Authorization")
		if h == "" {
			return false
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(h, prefix) {
			return false
		}
		got := strings.TrimSpace(strings.TrimPrefix(h, prefix))
		if got == "" {
			return false
		}
		gb := []byte(got)
		for _, want := range allowed {
			if subtle.ConstantTimeCompare(gb, want) == 1 {
				return true
			}
		}
		return false
	}
}

type Server struct {
	Store                              queue.Store
	Authorize                          Authorizer
	HealthDiagnostics                  func() map[string]any
	ResolveTrendSignalConfig           func() queue.BacklogTrendSignalConfig
	ResolveManaged                     func(application, endpointName string) (route string, targets []string, ok bool)
	ManagedRouteInfoForRoute           func(route string) (application, endpointName string, managed bool, available bool)
	ManagedRouteSet                    func() (routes map[string]struct{}, available bool)
	TargetsForRoute                    func(route string) []string
	ModeForRoute                       func(route string) string
	PublishEnabledForRoute             func(route string) bool
	PublishDirectEnabledForRoute       func(route string) bool
	PublishManagedEnabledForRoute      func(route string) bool
	LimitsForRoute                     func(route string) (maxBodyBytes int64, maxHeaderBytes int)
	ManagementModel                    func() ManagementModel
	RequireManagementAuditReason       bool
	MaxBodyBytes                       int64
	MaxHeaderBytes                     int
	PublishGlobalDirectEnabled         bool
	PublishScopedManagedEnabled        bool
	PublishAllowPullRoutes             bool
	PublishAllowDeliverRoutes          bool
	PublishRequireAuditActor           bool
	PublishRequireAuditRequestID       bool
	PublishScopedManagedFailClosed     bool
	PublishScopedManagedActorAllowlist []string
	PublishScopedManagedActorPrefixes  []string
	AuditManagementMutation            func(event ManagementMutationAuditEvent)
	ObservePublishResult               func(event PublishResultEvent)
	UpsertManagedEndpoint              func(req ManagementEndpointUpsertRequest) (ManagementEndpointMutationResult, error)
	DeleteManagedEndpoint              func(req ManagementEndpointDeleteRequest) (ManagementEndpointMutationResult, error)

	// Runtime secret rotation (see handle_secrets.go).
	SecretLookup        SecretLookup
	PersistSecret       func(rec SecretRecord) error
	DeleteSecretRecord  func(poolName, id string) (bool, error)
	SealSecret          func(plain []byte) ([]byte, error)
	AuditSecretMutation func(event SecretMutationAuditEvent)

	queueHealth struct {
		mu         sync.Mutex
		ttl        time.Duration
		cached     map[string]any
		cachedAt   time.Time
		refreshing bool
	}
}

type PublishResultEvent struct {
	Accepted int
	Rejected int
	Code     string
	Scoped   bool
}

func NewServer(store queue.Store) *Server {
	s := &Server{
		Store:                          store,
		RequireManagementAuditReason:   true,
		MaxBodyBytes:                   defaultMaxBodyBytes,
		MaxHeaderBytes:                 defaultMaxHeaderBytes,
		PublishGlobalDirectEnabled:     true,
		PublishScopedManagedEnabled:    true,
		PublishAllowPullRoutes:         true,
		PublishAllowDeliverRoutes:      true,
		PublishRequireAuditActor:       false,
		PublishRequireAuditRequestID:   false,
		PublishScopedManagedFailClosed: false,
	}
	s.queueHealth.ttl = defaultQueueHealthCacheTTL
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Authorize != nil && !s.Authorize(r) {
		writeManagementError(w, http.StatusUnauthorized, readCodeUnauthorized, "request is not authorized")
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if strings.HasPrefix(cleanPath, "/applications/") {
		s.handleApplicationResource(w, r, cleanPath)
		return
	}
	if strings.HasPrefix(cleanPath, "/secrets/") {
		s.handleSecretResource(w, r, cleanPath)
		return
	}

	switch cleanPath {
	case "/healthz":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleHealthz(w, r)
		return
	case "/dlq":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleDLQ(w, r)
		return
	case "/backlog/top_queued":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogTopQueued(w, r)
		return
	case "/backlog/oldest_queued":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogOldestQueued(w, r)
		return
	case "/backlog/aging_summary":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogAgingSummary(w, r)
		return
	case "/backlog/trends":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleBacklogTrends(w, r)
		return
	case "/messages":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleMessages(w, r)
		return
	case "/messages/publish":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesPublish(w, r)
		return
	case "/dlq/requeue":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleDLQRequeue(w, r)
		return
	case "/dlq/delete":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleDLQDelete(w, r)
		return
	case "/messages/cancel":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesCancel(w, r)
		return
	case "/messages/cancel_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesCancelByFilter(w, r)
		return
	case "/messages/requeue":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesRequeue(w, r)
		return
	case "/messages/resume":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesResume(w, r)
		return
	case "/messages/requeue_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesRequeueByFilter(w, r)
		return
	case "/messages/resume_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleMessagesResumeByFilter(w, r)
		return
	case "/attempts":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleAttempts(w, r)
		return
	case "/management/model":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleManagementModel(w, r)
		return
	case "/applications":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplications(w, r)
		return
	default:
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
}

func (s *Server) handleApplicationResource(w http.ResponseWriter, r *http.Request, cleanPath string) {
	application, endpointName, resource, ok := parseApplicationResourcePath(cleanPath)
	if !ok {
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
	if !config.IsValidManagementLabel(application) {
		writeManagementError(
			w,
			http.StatusBadRequest,
			publishCodeInvalidBody,
			fmt.Sprintf("application path segment must match %s", config.ManagementLabelPattern()),
		)
		return
	}
	if endpointName != "" && !config.IsValidManagementLabel(endpointName) {
		writeManagementError(
			w,
			http.StatusBadRequest,
			publishCodeInvalidBody,
			fmt.Sprintf("endpoint_name path segment must match %s", config.ManagementLabelPattern()),
		)
		return
	}

	switch resource {
	case "endpoints":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplicationEndpoints(w, application)
		return
	case "endpoint":
		switch r.Method {
		case http.MethodGet:
			s.handleApplicationEndpoint(w, application, endpointName)
			return
		case http.MethodPut:
			s.handleApplicationEndpointUpsert(w, r, application, endpointName)
			return
		case http.MethodDelete:
			s.handleApplicationEndpointDelete(w, r, application, endpointName)
			return
		default:
			writeMethodNotAllowed(w, "GET|PUT|DELETE")
			return
		}
	case "messages":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleApplicationEndpointMessages(w, r, application, endpointName)
		return
	case "messages_publish":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointPublish(w, r, application, endpointName)
		return
	case "messages_cancel_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointCancelByFilter(w, r, application, endpointName)
		return
	case "messages_requeue_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointRequeueByFilter(w, r, application, endpointName)
		return
	case "messages_resume_by_filter":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleApplicationEndpointResumeByFilter(w, r, application, endpointName)
		return
	default:
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
}
