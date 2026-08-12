package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// fastRetry returns a config with tiny backoff so retry tests stay quick.
func fastRetry(url string) *Config {
	return &Config{
		URL:            url,
		MaxRetries:     3,
		Timeout:        "2s",
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
	}
}

func newOutput(t *testing.T, cfg *Config) *Output {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	o, err := NewFactory().Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	out := o.(*Output)
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = out.Shutdown(context.Background()) })
	return out
}

func TestConsume_PostsBodyWithAuthAndHeaders(t *testing.T) {
	var gotBody []byte
	var gotAuth, gotCT, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotCustom = r.Header.Get("X-Tenant")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := fastRetry(srv.URL)
	cfg.Token = "tok"
	cfg.ContentType = "application/vnd.apache.parquet"
	cfg.Headers = map[string]string{"X-Tenant": "acme"}
	out := newOutput(t, cfg)

	block := data.EncodedBlock{Format: "parquet", Bytes: []byte("PAR1data"), Rows: 3}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if string(gotBody) != "PAR1data" {
		t.Fatalf("body = %q", gotBody)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if gotCT != "application/vnd.apache.parquet" {
		t.Fatalf("content-type = %q", gotCT)
	}
	if gotCustom != "acme" {
		t.Fatalf("custom header = %q", gotCustom)
	}
}

func TestConsume_TokenFileReadFreshPerAttempt(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// A projected ServiceAccount token rotates on disk while the process runs,
	// so the bearer must be re-read from the file on every send, not cached at
	// construction. Trailing whitespace/newlines are stripped.
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("tok-v1\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	cfg := fastRetry(srv.URL)
	cfg.TokenFile = tokenPath
	out := newOutput(t, cfg)

	block := data.EncodedBlock{Bytes: []byte("x")}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume v1: %v", err)
	}
	if gotAuth != "Bearer tok-v1" {
		t.Fatalf("auth v1 = %q, want Bearer tok-v1", gotAuth)
	}

	// Rotate the file; the next send must pick up the new token.
	if err := os.WriteFile(tokenPath, []byte("tok-v2"), 0o600); err != nil {
		t.Fatalf("rotate token: %v", err)
	}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume v2: %v", err)
	}
	if gotAuth != "Bearer tok-v2" {
		t.Fatalf("auth v2 = %q, want Bearer tok-v2 (token file not re-read)", gotAuth)
	}
}

func TestConsume_RetriesOn5xxThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out := newOutput(t, fastRetry(srv.URL))
	if err := out.Consume(context.Background(), data.EncodedBlock{Bytes: []byte("x")}); err != nil {
		t.Fatalf("Consume should succeed after retries: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestConsume_GivesUpAfterMaxRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	out := newOutput(t, fastRetry(srv.URL)) // retry budget is three
	err := out.Consume(context.Background(), data.EncodedBlock{Bytes: []byte("x")})
	if err == nil {
		t.Fatal("Consume should fail after exhausting retries")
	}
	// 1 initial attempt + 3 retries = 4 requests.
	if got := attempts.Load(); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestConsume_DoesNotRetry4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	out := newOutput(t, fastRetry(srv.URL))
	if err := out.Consume(context.Background(), data.EncodedBlock{Bytes: []byte("x")}); err == nil {
		t.Fatal("401 should be a permanent error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (4xx must not retry)", got)
	}
}

func TestConsume_DoesNotRetry499(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(499) // client closed — must not stampede retries
	}))
	defer srv.Close()

	out := newOutput(t, fastRetry(srv.URL))
	if err := out.Consume(context.Background(), data.EncodedBlock{Bytes: []byte("x")}); err == nil {
		t.Fatal("499 should be a permanent error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (499 must not retry)", got)
	}
}

func TestConsume_EmptyBlockIsNoOp(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
	}))
	defer srv.Close()

	out := newOutput(t, fastRetry(srv.URL))
	if err := out.Consume(context.Background(), data.EncodedBlock{}); err != nil {
		t.Fatalf("empty block: %v", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("attempts = %d, want 0 for empty block", got)
	}
}

func TestConsume_TLSWithSkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := fastRetry(srv.URL)
	cfg.TLS = &tlsConfig{InsecureSkipVerify: true}
	out := newOutput(t, cfg)
	if err := out.Consume(context.Background(), data.EncodedBlock{Bytes: []byte("x")}); err != nil {
		t.Fatalf("TLS post failed: %v", err)
	}
}

func TestConsume_ContextCancelStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := fastRetry(srv.URL)
	cfg.MaxRetries = 1000000
	cfg.InitialBackoff = "50ms"
	cfg.MaxBackoff = "50ms"
	out := newOutput(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: Consume must return promptly, not spin retries
	if err := out.Consume(ctx, data.EncodedBlock{Bytes: []byte("x")}); err == nil {
		t.Fatal("cancelled context should abort Consume with an error")
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := map[string]*Config{
		"no url":       {},
		"bad method":   {URL: "http://x", Method: "FETCH"},
		"neg retries":  {URL: "http://x", MaxRetries: -1},
		"bad timeout":  {URL: "http://x", Timeout: "nope"},
		"bad backoff":  {URL: "http://x", InitialBackoff: "nope"},
		"tls half key": {URL: "http://x", TLS: &tlsConfig{Cert: "c.pem"}},
		"token and token_file": {
			URL:       "http://x",
			Token:     "inline",
			TokenFile: "/var/run/token",
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if err := cfg.Validate(); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
}
