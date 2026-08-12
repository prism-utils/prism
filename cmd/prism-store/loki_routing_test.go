package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
)

const lokiRouteTenant = "user-6f3a9c2b-apps"

func lokiTestConfig(t *testing.T, enabled bool) (*serverConfig, *engine.Engine, *slog.Logger) {
	t.Helper()
	dir := t.TempDir()
	// A provisioned tenant root makes a mounted Loki route answer 200, so a 404
	// unambiguously means "route not registered".
	if err := os.MkdirAll(filepath.Join(dir, lokiRouteTenant), 0o750); err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &serverConfig{
		dataDir:          dir,
		allowedArtifacts: []string{"metrics-raw", "logs-summary"},
		authMode:         "none",
		lokiAPIEnabled:   enabled,
		sqlAPIMaxRows:    100,
		sqlAPITimeout:    30 * time.Second,
	}
	return cfg, eng, logger
}

// TestLokiRoutesRegistered proves every advertised Loki pattern is mounted on the
// admin plane when the API is enabled.
func TestLokiRoutesRegistered(t *testing.T) {
	cfg, eng, logger := lokiTestConfig(t, true)
	mux := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, nil)
	for _, path := range []string{
		"/" + lokiRouteTenant + "/loki/api/v1/query_range",
		"/" + lokiRouteTenant + "/loki/api/v1/labels",
		"/" + lokiRouteTenant + "/loki/api/v1/label/format/values",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if rec.Code == http.StatusNotFound {
			t.Fatalf("admin mux must serve %s", path)
		}
	}
}

func TestLokiRoutesDisabled(t *testing.T) {
	cfg, eng, logger := lokiTestConfig(t, false)
	mux := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet,
		"/"+lokiRouteTenant+"/loki/api/v1/labels", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("LOKI_API_ENABLED=false must not register routes, got %d", rec.Code)
	}
}

// TestLokiRoutePatternsMatchMux pins the wiring contract: the patterns the store
// mounts are exactly the patterns a cluster coordinator forwards.
func TestLokiRoutePatternsMatchMux(t *testing.T) {
	patterns := query.LokiRoutePatterns("")
	if len(patterns) == 0 {
		t.Fatal("no Loki route patterns")
	}
	cfg, eng, logger := lokiTestConfig(t, true)
	mux := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, nil)
	for _, p := range patterns {
		method, path, ok := splitPattern(p)
		if !ok {
			t.Fatalf("pattern %q is not \"METHOD /path\"", p)
		}
		req := httptest.NewRequestWithContext(context.Background(), method,
			replaceNS(path, lokiRouteTenant), nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Fatalf("pattern %q not mounted", p)
		}
	}
}

func TestLokiAPIEnabledDefaultsTrue(t *testing.T) {
	cfg := loadConfig()
	if !cfg.lokiAPIEnabled {
		t.Fatal("LOKI_API_ENABLED must default to true")
	}
	t.Setenv("LOKI_API_ENABLED", "false")
	if loadConfig().lokiAPIEnabled {
		t.Fatal("LOKI_API_ENABLED=false must disable the API")
	}
}

func splitPattern(p string) (method, path string, ok bool) {
	for i := 0; i < len(p); i++ {
		if p[i] == ' ' {
			return p[:i], p[i+1:], true
		}
	}
	return "", "", false
}

// replaceNS substitutes a concrete tenant for the {ns} and {name} wildcards so a
// pattern can be exercised as a real request path.
func replaceNS(path, ns string) string {
	out := ""
	i := 0
	for i < len(path) {
		if path[i] == '{' {
			j := i
			for j < len(path) && path[j] != '}' {
				j++
			}
			switch path[i : j+1] {
			case "{ns}":
				out += ns
			default:
				out += "format"
			}
			i = j + 1
			continue
		}
		out += string(path[i])
		i++
	}
	return out
}
