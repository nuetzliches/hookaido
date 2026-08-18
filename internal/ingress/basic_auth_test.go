package ingress

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuth_Verify(t *testing.T) {
	auth := NewBasicAuth(map[string]string{"ci": "s3cret", "ops": "other"})

	tests := []struct {
		name    string
		setAuth bool
		user    string
		pass    string
		want    bool
	}{
		{name: "no credentials", setAuth: false, want: false},
		{name: "correct", setAuth: true, user: "ci", pass: "s3cret", want: true},
		{name: "second user", setAuth: true, user: "ops", pass: "other", want: true},
		{name: "wrong password", setAuth: true, user: "ci", pass: "nope", want: false},
		{name: "wrong password same length", setAuth: true, user: "ci", pass: "s3cre7", want: false},
		{name: "unknown user", setAuth: true, user: "nobody", pass: "s3cret", want: false},
		{name: "empty password", setAuth: true, user: "ci", pass: "", want: false},
		{name: "user swapped", setAuth: true, user: "ops", pass: "s3cret", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example/hook", nil)
			if tc.setAuth {
				req.SetBasicAuth(tc.user, tc.pass)
			}
			if got := auth.Verify(req); got != tc.want {
				t.Fatalf("Verify = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBasicAuth_NilAndEmptyAllow(t *testing.T) {
	var nilAuth *BasicAuth
	req := httptest.NewRequest(http.MethodPost, "http://example/hook", nil)
	if !nilAuth.Verify(req) {
		t.Fatal("a nil BasicAuth must not reject")
	}
	if NewBasicAuth(nil) != nil {
		t.Fatal("NewBasicAuth with no users must return nil")
	}
}

// The credential check on the ingress path must not return early for an unknown
// user, and must not compare values whose length depends on the configured
// password: both leak through response timing. Storing digests is what makes
// the compare fixed-width, so this asserts the passwords are not kept at all.
func TestBasicAuth_StoresDigestsNotPasswords(t *testing.T) {
	auth := NewBasicAuth(map[string]string{"ci": "s3cret"})

	want := sha256.Sum256([]byte("s3cret"))
	got, ok := auth.users["ci"]
	if !ok {
		t.Fatal("user missing from the credential map")
	}
	if got != want {
		t.Fatal("stored credential is not the SHA-256 digest of the password")
	}
	if auth.decoy == got {
		t.Fatal("the decoy must not equal a real digest")
	}
	if auth.decoy == ([sha256.Size]byte{}) {
		t.Fatal("the decoy must be initialized, or an unknown user compares against zeroes")
	}
}
