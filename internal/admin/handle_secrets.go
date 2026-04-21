package admin

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/config"
)

// SecretPool is the admin-facing view of a runtime-mutable secret pool. The
// host app wires this by adapting *secrets.Pool so admin does not import
// internal/secrets directly.
type SecretPool interface {
	Name() string
	Runtime() bool
	// Add inserts a new version into the pool. Implementations must return
	// errors matching ErrSecret{Duplicate,PoolFull,Invalid} so the handler can
	// map them to HTTP responses.
	Add(id string, value []byte, notBefore, notAfter time.Time) error
	Remove(id string) (removed bool)
	ListMetadata() []SecretVersionMetadata
}

type SecretLookup func(name string) (SecretPool, bool)

type SecretVersionMetadata struct {
	ID        string    `json:"id"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after,omitempty"`
}

// SecretRecord is the persisted shape handed to PersistSecret. The admin
// package mirrors secrets.Record shape so internal/secrets stays decoupled.
type SecretRecord struct {
	PoolName  string
	ID        string
	Sealed    []byte
	NotBefore time.Time
	NotAfter  time.Time
	CreatedAt time.Time
}

type SecretMutationAuditEvent struct {
	At        time.Time
	Operation string // "add" or "delete"
	PoolName  string
	SecretID  string
	NotBefore time.Time
	NotAfter  time.Time
	Reason    string
	Actor     string
	RequestID string
}

// Sentinel errors the SecretPool.Add implementation should return. The host
// app's adapter translates internal/secrets errors to these.
var (
	ErrSecretDuplicate = errors.New("admin: duplicate secret id")
	ErrSecretPoolFull  = errors.New("admin: secret pool is full")
	ErrSecretInvalid   = errors.New("admin: invalid secret version")
)

type secretUpsertRequest struct {
	Value     string    `json:"value"`
	NotBefore time.Time `json:"not_before,omitempty"`
	NotAfter  time.Time `json:"not_after,omitempty"`
}

type secretAddResponse struct {
	ID        string    `json:"id"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type secretListResponse struct {
	Name     string                  `json:"name"`
	Versions []SecretVersionMetadata `json:"versions"`
}

func (s *Server) handleSecretResource(w http.ResponseWriter, r *http.Request, cleanPath string) {
	name, id, ok := parseSecretResourcePath(cleanPath)
	if !ok {
		writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
		return
	}
	switch r.Method {
	case http.MethodPost:
		if id != "" {
			writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
			return
		}
		s.handleSecretAdd(w, r, name)
	case http.MethodGet:
		if id != "" {
			writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
			return
		}
		s.handleSecretList(w, r, name)
	case http.MethodDelete:
		if id == "" {
			writeManagementError(w, http.StatusNotFound, readCodeNotFound, "resource was not found")
			return
		}
		s.handleSecretDelete(w, r, name, id)
	default:
		writeMethodNotAllowed(w, http.MethodPost+", "+http.MethodGet+", "+http.MethodDelete)
	}
}

func parseSecretResourcePath(cleanPath string) (name, id string, ok bool) {
	if !strings.HasPrefix(cleanPath, "/secrets/") {
		return "", "", false
	}
	rest := strings.TrimPrefix(cleanPath, "/secrets/")
	if rest == "" {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || len(parts) > 2 {
		return "", "", false
	}
	n, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", false
	}
	if !config.IsValidManagementLabel(n) {
		return "", "", false
	}
	if len(parts) == 1 {
		return n, "", true
	}
	i, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	if !config.IsValidManagementLabel(i) {
		return "", "", false
	}
	return n, i, true
}

func (s *Server) handleSecretAdd(w http.ResponseWriter, r *http.Request, name string) {
	if s.SecretLookup == nil || s.PersistSecret == nil || s.SealSecret == nil {
		writeManagementError(w, http.StatusServiceUnavailable, secretCodeUnavailable, "runtime secret rotation is not configured")
		return
	}
	pool, ok := s.SecretLookup(name)
	if !ok {
		writeManagementError(w, http.StatusNotFound, secretCodePoolNotFound, fmt.Sprintf("secret pool %q is not declared runtime=true", name))
		return
	}
	if !pool.Runtime() {
		writeManagementError(w, http.StatusBadRequest, secretCodeNotRuntime, "secret pool is not runtime-mutable")
		return
	}

	body, detail, ok := parseSecretUpsertBody(r, s.MaxBodyBytes)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeInvalidBody, detail)
		return
	}
	if strings.TrimSpace(body.Value) == "" {
		writeManagementError(w, http.StatusBadRequest, secretCodeValueEmpty, "value must not be empty")
		return
	}
	now := time.Now().UTC()
	if body.NotBefore.IsZero() {
		body.NotBefore = now
	} else {
		body.NotBefore = body.NotBefore.UTC()
	}
	if !body.NotAfter.IsZero() {
		body.NotAfter = body.NotAfter.UTC()
		if !body.NotAfter.After(body.NotBefore) {
			writeManagementError(w, http.StatusBadRequest, secretCodeInvalidWindow, "not_after must be after not_before")
			return
		}
	}

	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}
	if code, msg := s.mutationAuditPolicyError(audit, true, "secret mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, msg)
		return
	}

	id := newSecretID()
	sealed, err := s.SealSecret([]byte(body.Value))
	if err != nil {
		writeManagementError(w, http.StatusInternalServerError, secretCodeSealFailure, "failed to seal secret value")
		return
	}
	rec := SecretRecord{
		PoolName:  name,
		ID:        id,
		Sealed:    sealed,
		NotBefore: body.NotBefore,
		NotAfter:  body.NotAfter,
		CreatedAt: now,
	}
	if err := s.PersistSecret(rec); err != nil {
		writeManagementError(w, http.StatusInternalServerError, secretCodePersistFailure, "failed to persist secret")
		return
	}
	if err := pool.Add(id, []byte(body.Value), body.NotBefore, body.NotAfter); err != nil {
		// Roll back persistence on in-memory rejection to keep them in sync.
		if s.DeleteSecretRecord != nil {
			_, _ = s.DeleteSecretRecord(name, id)
		}
		status, code, msg := mapSecretAddError(err)
		writeManagementError(w, status, code, msg)
		return
	}

	s.emitSecretAudit(SecretMutationAuditEvent{
		At:        now,
		Operation: "add",
		PoolName:  name,
		SecretID:  id,
		NotBefore: body.NotBefore,
		NotAfter:  body.NotAfter,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(secretAddResponse{
		ID:        id,
		NotBefore: body.NotBefore,
		NotAfter:  body.NotAfter,
		CreatedAt: now,
	})
}

func (s *Server) handleSecretList(w http.ResponseWriter, r *http.Request, name string) {
	if s.SecretLookup == nil {
		writeManagementError(w, http.StatusServiceUnavailable, secretCodeUnavailable, "runtime secret rotation is not configured")
		return
	}
	pool, ok := s.SecretLookup(name)
	if !ok {
		writeManagementError(w, http.StatusNotFound, secretCodePoolNotFound, fmt.Sprintf("secret pool %q is not declared runtime=true", name))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(secretListResponse{Name: name, Versions: pool.ListMetadata()})
}

func (s *Server) handleSecretDelete(w http.ResponseWriter, r *http.Request, name, id string) {
	if s.SecretLookup == nil || s.DeleteSecretRecord == nil {
		writeManagementError(w, http.StatusServiceUnavailable, secretCodeUnavailable, "runtime secret rotation is not configured")
		return
	}
	pool, ok := s.SecretLookup(name)
	if !ok {
		writeManagementError(w, http.StatusNotFound, secretCodePoolNotFound, fmt.Sprintf("secret pool %q is not declared runtime=true", name))
		return
	}
	if !pool.Runtime() {
		writeManagementError(w, http.StatusBadRequest, secretCodeNotRuntime, "secret pool is not runtime-mutable")
		return
	}

	audit, ok := parseManagementAudit(r, s.RequireManagementAuditReason)
	if !ok {
		writeManagementError(w, http.StatusBadRequest, publishCodeAuditReasonRequired, "invalid or missing audit headers")
		return
	}
	if code, msg := s.mutationAuditPolicyError(audit, true, "secret mutation"); code != "" {
		writeManagementError(w, http.StatusBadRequest, code, msg)
		return
	}

	removed := pool.Remove(id)
	existed, err := s.DeleteSecretRecord(name, id)
	if err != nil {
		writeManagementError(w, http.StatusInternalServerError, secretCodePersistFailure, "failed to delete persisted secret")
		return
	}
	if !removed && !existed {
		writeManagementError(w, http.StatusNotFound, secretCodeIDNotFound, fmt.Sprintf("secret id %q was not found", id))
		return
	}

	s.emitSecretAudit(SecretMutationAuditEvent{
		At:        time.Now().UTC(),
		Operation: "delete",
		PoolName:  name,
		SecretID:  id,
		Reason:    audit.Reason,
		Actor:     audit.Actor,
		RequestID: audit.RequestID,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) emitSecretAudit(evt SecretMutationAuditEvent) {
	if s == nil || s.AuditSecretMutation == nil {
		return
	}
	s.AuditSecretMutation(evt)
}

func parseSecretUpsertBody(r *http.Request, maxBodyBytes int64) (secretUpsertRequest, string, bool) {
	if r.Body == nil {
		return secretUpsertRequest{}, "request body is empty", false
	}
	body := r.Body
	if maxBodyBytes > 0 {
		body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return secretUpsertRequest{}, "request body read error", false
	}
	if len(data) == 0 {
		return secretUpsertRequest{}, "request body is empty", false
	}
	var req secretUpsertRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return secretUpsertRequest{}, "request body is not valid JSON", false
	}
	return req, "", true
}

func newSecretID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failing is essentially impossible on supported platforms;
		// fall back to a coarse timestamp rather than a panic so a surface-level
		// disruption doesn't take down admin endpoints.
		return fmt.Sprintf("sec_%016x", time.Now().UnixNano())
	}
	return "sec_" + hex.EncodeToString(buf[:])
}

func mapSecretAddError(err error) (int, string, string) {
	switch {
	case errors.Is(err, ErrSecretDuplicate):
		return http.StatusConflict, secretCodeDuplicateID, err.Error()
	case errors.Is(err, ErrSecretPoolFull):
		return http.StatusRequestEntityTooLarge, secretCodePoolFull, err.Error()
	case errors.Is(err, ErrSecretInvalid):
		return http.StatusBadRequest, secretCodeInvalidWindow, err.Error()
	default:
		return http.StatusInternalServerError, secretCodePersistFailure, err.Error()
	}
}
