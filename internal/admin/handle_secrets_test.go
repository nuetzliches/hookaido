package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

// stubSecretPool is a minimal in-memory SecretPool for handler tests.
type stubSecretPool struct {
	mu       sync.Mutex
	name     string
	runtime  bool
	cap      int
	addErr   error
	versions []SecretVersionMetadata
	stored   map[string][]byte
}

func newStubPool(name string, runtime bool) *stubSecretPool {
	return &stubSecretPool{name: name, runtime: runtime, cap: 4, stored: map[string][]byte{}}
}

func (p *stubSecretPool) Name() string  { return p.name }
func (p *stubSecretPool) Runtime() bool { return p.runtime }

func (p *stubSecretPool) Add(id string, value []byte, nb, na time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.addErr != nil {
		return p.addErr
	}
	for _, v := range p.versions {
		if v.ID == id {
			return ErrSecretDuplicate
		}
	}
	if len(p.versions) >= p.cap {
		return ErrSecretPoolFull
	}
	p.versions = append(p.versions, SecretVersionMetadata{ID: id, NotBefore: nb, NotAfter: na})
	p.stored[id] = append([]byte(nil), value...)
	return nil
}

func (p *stubSecretPool) Remove(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, v := range p.versions {
		if v.ID == id {
			p.versions = append(p.versions[:i], p.versions[i+1:]...)
			delete(p.stored, id)
			return true
		}
	}
	return false
}

func (p *stubSecretPool) ListMetadata() []SecretVersionMetadata {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SecretVersionMetadata, len(p.versions))
	copy(out, p.versions)
	return out
}

type secretTestFixture struct {
	server *Server
	pool   *stubSecretPool
	// last admin-side sealed bytes observed via PersistSecret
	persisted  []SecretRecord
	deleted    []string
	auditCalls []SecretMutationAuditEvent
}

func newSecretTestFixture(t *testing.T, runtime bool) *secretTestFixture {
	t.Helper()
	srv := NewServer(queue.NewMemoryStore())
	srv.RequireManagementAuditReason = true
	pool := newStubPool("cituro", runtime)
	fx := &secretTestFixture{server: srv, pool: pool}
	srv.SecretLookup = func(name string) (SecretPool, bool) {
		if name != pool.name {
			return nil, false
		}
		return pool, true
	}
	srv.SealSecret = func(plain []byte) ([]byte, error) {
		// Deterministic pseudo-seal for tests: prefix with 0x01 so shape matches.
		return append([]byte{0x01}, plain...), nil
	}
	srv.PersistSecret = func(rec SecretRecord) error {
		fx.persisted = append(fx.persisted, rec)
		return nil
	}
	srv.DeleteSecretRecord = func(pool, id string) (bool, error) {
		fx.deleted = append(fx.deleted, pool+"/"+id)
		return true, nil
	}
	srv.AuditSecretMutation = func(evt SecretMutationAuditEvent) {
		fx.auditCalls = append(fx.auditCalls, evt)
	}
	return fx
}

func secretPostBody(value string) []byte {
	body, _ := json.Marshal(secretUpsertRequest{Value: value})
	return body
}

func secretPostRequest(t *testing.T, path string, body []byte, reason string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://example"+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if reason != "" {
		req.Header.Set("X-Hookaido-Audit-Reason", reason)
	}
	return req
}

func TestHandleSecretAdd_Success(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("whs_secret"), "rotate-test")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp secretAddResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasPrefix(resp.ID, "sec_") {
		t.Fatalf("expected sec_ prefix, got %q", resp.ID)
	}
	if len(fx.persisted) != 1 {
		t.Fatalf("expected 1 persisted record, got %d", len(fx.persisted))
	}
	if !bytes.Equal(fx.persisted[0].Sealed, append([]byte{0x01}, []byte("whs_secret")...)) {
		t.Fatalf("unexpected sealed content: %v", fx.persisted[0].Sealed)
	}
	if len(fx.auditCalls) != 1 || fx.auditCalls[0].Operation != "add" {
		t.Fatalf("audit: %+v", fx.auditCalls)
	}
	if fx.pool.ListMetadata()[0].ID != resp.ID {
		t.Fatalf("pool should contain the new id")
	}
}

func TestHandleSecretAdd_EmptyValue(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody(""), "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeValueEmpty {
		t.Fatalf("code=%q want %q", errResp.Code, secretCodeValueEmpty)
	}
}

func TestHandleSecretAdd_InvalidWindow(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	now := time.Now().UTC()
	body, _ := json.Marshal(secretUpsertRequest{
		Value:     "x",
		NotBefore: now,
		NotAfter:  now.Add(-time.Hour),
	})
	req := secretPostRequest(t, "/secrets/cituro", body, "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeInvalidWindow {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretAdd_MissingAuditReason(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("x"), "")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != publishCodeAuditReasonRequired {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretAdd_NonRuntimePool(t *testing.T) {
	fx := newSecretTestFixture(t, false) // Runtime=false
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("x"), "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeNotRuntime {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretAdd_UnknownPool(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	// Reuse fixture's SecretLookup; "unknown" will not match fx.pool.name.
	req := secretPostRequest(t, "/secrets/unknown", secretPostBody("x"), "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodePoolNotFound {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretAdd_PoolFull_RollsBackPersist(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	fx.pool.addErr = ErrSecretPoolFull
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("x"), "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodePoolFull {
		t.Fatalf("code=%q", errResp.Code)
	}
	// Persistence rollback: PersistSecret + DeleteSecretRecord both called.
	if len(fx.persisted) != 1 || len(fx.deleted) != 1 {
		t.Fatalf("expected persist+delete to both run (persist=%d delete=%d)",
			len(fx.persisted), len(fx.deleted))
	}
}

func TestHandleSecretAdd_Duplicate(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	fx.pool.addErr = ErrSecretDuplicate
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("x"), "r")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeDuplicateID {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretAdd_Unavailable(t *testing.T) {
	srv := NewServer(queue.NewMemoryStore())
	// No SecretLookup wired.
	req := secretPostRequest(t, "/secrets/cituro", secretPostBody("x"), "r")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeUnavailable {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestHandleSecretList_OmitsValues(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	now := time.Now().UTC()
	_ = fx.pool.Add("sec_a", []byte("va"), now, time.Time{})
	_ = fx.pool.Add("sec_b", []byte("vb"), now.Add(time.Hour), now.Add(2*time.Hour))

	req := httptest.NewRequest(http.MethodGet, "http://example/secrets/cituro", nil)
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp secretListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Name != "cituro" || len(resp.Versions) != 2 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	// Raw JSON must not contain any plaintext value.
	if bytes.Contains(rr.Body.Bytes(), []byte("va")) || bytes.Contains(rr.Body.Bytes(), []byte("vb")) {
		t.Fatalf("list output leaked plaintext value: %s", rr.Body.String())
	}
}

func TestHandleSecretDelete_Existing(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	_ = fx.pool.Add("sec_a", []byte("v"), time.Now(), time.Time{})
	req := httptest.NewRequest(http.MethodDelete, "http://example/secrets/cituro/sec_a", nil)
	req.Header.Set("X-Hookaido-Audit-Reason", "revoke")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if len(fx.deleted) != 1 || !strings.HasSuffix(fx.deleted[0], "/sec_a") {
		t.Fatalf("DeleteSecretRecord not called: %v", fx.deleted)
	}
	if len(fx.auditCalls) != 1 || fx.auditCalls[0].Operation != "delete" {
		t.Fatalf("audit: %+v", fx.auditCalls)
	}
}

func TestHandleSecretDelete_MissingID_Reports404(t *testing.T) {
	fx := newSecretTestFixture(t, true)
	// Make DeleteSecretRecord say it found nothing.
	fx.server.DeleteSecretRecord = func(pool, id string) (bool, error) { return false, nil }

	req := httptest.NewRequest(http.MethodDelete, "http://example/secrets/cituro/sec_missing", nil)
	req.Header.Set("X-Hookaido-Audit-Reason", "revoke")
	rr := httptest.NewRecorder()
	fx.server.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	errResp := decodeManagementError(t, rr)
	if errResp.Code != secretCodeIDNotFound {
		t.Fatalf("code=%q", errResp.Code)
	}
}

func TestParseSecretResourcePath(t *testing.T) {
	cases := []struct {
		path    string
		okName  string
		okID    string
		wantOK  bool
	}{
		{"/secrets/cituro", "cituro", "", true},
		{"/secrets/cituro/sec_abc", "cituro", "sec_abc", true},
		{"/secrets/", "", "", false},
		{"/secrets", "", "", false},
		{"/secrets/a/b/c", "", "", false},
		{"/secrets/bad name!", "", "", false},
		{"/secrets/ok/bad name!", "", "", false},
	}
	for _, c := range cases {
		n, id, ok := parseSecretResourcePath(c.path)
		if ok != c.wantOK || n != c.okName || id != c.okID {
			t.Errorf("parse %q: got (%q, %q, %v); want (%q, %q, %v)",
				c.path, n, id, ok, c.okName, c.okID, c.wantOK)
		}
	}
}

func TestMapSecretAddError(t *testing.T) {
	if status, code, _ := mapSecretAddError(ErrSecretDuplicate); status != http.StatusConflict || code != secretCodeDuplicateID {
		t.Fatalf("duplicate mapping wrong: %d %q", status, code)
	}
	if status, code, _ := mapSecretAddError(ErrSecretPoolFull); status != http.StatusRequestEntityTooLarge || code != secretCodePoolFull {
		t.Fatalf("poolfull mapping wrong: %d %q", status, code)
	}
	if status, code, _ := mapSecretAddError(ErrSecretInvalid); status != http.StatusBadRequest || code != secretCodeInvalidWindow {
		t.Fatalf("invalid mapping wrong: %d %q", status, code)
	}
	if status, code, _ := mapSecretAddError(errors.New("other")); status != http.StatusInternalServerError || code != secretCodePersistFailure {
		t.Fatalf("default mapping wrong: %d %q", status, code)
	}
}
