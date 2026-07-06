package prometheus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/component"
)

// startAndFirstBatch starts the input and returns its first scraped batch,
// then cancels and drains so no goroutine lingers.
func startAndFirstBatch(t *testing.T, in *Input) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := in.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-in.Batches():
	case <-time.After(3 * time.Second):
		t.Fatal("no batch scraped within timeout")
	}
	cancel()
	for range in.Batches() {
	}
}

func TestScrape_BearerTokenHeader(t *testing.T) {
	gotAuth := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case gotAuth <- r.Header.Get("Authorization"):
		default:
		}
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = []string{srv.URL}
	cfg.Interval = "1h"
	cfg.BearerToken = "s3cr3t"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	in, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	startAndFirstBatch(t, in.(*Input))

	select {
	case auth := <-gotAuth:
		if auth != "Bearer s3cr3t" {
			t.Fatalf("Authorization = %q, want %q", auth, "Bearer s3cr3t")
		}
	case <-time.After(time.Second):
		t.Fatal("server never observed a request")
	}
}

func TestScrape_BasicAuthHeader(t *testing.T) {
	type creds struct{ user, pass string }
	got := make(chan creds, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		select {
		case got <- creds{u, p}:
		default:
		}
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = []string{srv.URL}
	cfg.Interval = "1h"
	cfg.BasicAuth = &BasicAuth{Username: "alice", Password: "pw"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	in, _ := f.Create(cfg, component.Settings{})
	startAndFirstBatch(t, in.(*Input))

	select {
	case c := <-got:
		if c.user != "alice" || c.pass != "pw" {
			t.Fatalf("basic auth = %q/%q, want alice/pw", c.user, c.pass)
		}
	case <-time.After(time.Second):
		t.Fatal("server never observed a request")
	}
}

func TestScrape_TLSInsecureSkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = []string{srv.URL}
	cfg.Interval = "1h"
	cfg.TLS = &TLSConfig{InsecureSkipVerify: true}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	in, _ := f.Create(cfg, component.Settings{})
	// A self-signed TLS server is unreachable without skip-verify; the test
	// passes only because the client honored InsecureSkipVerify.
	startAndFirstBatch(t, in.(*Input))
}

func TestValidate_AuthErrors(t *testing.T) {
	cases := map[string]*Config{
		"basic and bearer both set": {
			Targets:     []string{"http://x"},
			BasicAuth:   &BasicAuth{Username: "u", Password: "p"},
			BearerToken: "t",
		},
		"basic auth missing username": {
			Targets:   []string{"http://x"},
			BasicAuth: &BasicAuth{Password: "p"},
		},
		"tls cert without key": {
			Targets: []string{"http://x"},
			TLS:     &TLSConfig{Cert: "c.pem"},
		},
		"tls key without cert": {
			Targets: []string{"http://x"},
			TLS:     &TLSConfig{Key: "k.pem"},
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

func TestStart_TLSMissingCAFile(t *testing.T) {
	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = []string{"https://example.invalid/metrics"}
	cfg.TLS = &TLSConfig{CA: "/no/such/ca.pem"}
	in, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := in.Start(context.Background(), nil); err == nil {
		t.Fatal("Start should fail when the CA file is unreadable")
	}
}
