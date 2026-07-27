package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elk-utilities/prism/internal/alert/config"
)

func TestVersionLine(t *testing.T) {
	assert.Contains(t, versionLine(), "prism-alert")
}

func TestParseFlagsOverrideEnvDefaults(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.StoreBaseURL = "http://env-store:8080"
	cfg.ListenAddr = ":8080"

	err := parseFlags(&cfg, []string{
		"-listen", ":9091",
		"-store-base-url", "http://flag-store:8080",
		"-evaluation-interval", "15s",
	})
	require.NoError(t, err)
	assert.Equal(t, ":9091", cfg.ListenAddr)
	assert.Equal(t, "http://flag-store:8080", cfg.StoreBaseURL)
	assert.Equal(t, 15*time.Second, cfg.EvaluationInterval)
}

func TestParseFlagsRejectsUnknown(t *testing.T) {
	cfg := config.DefaultConfig()
	err := parseFlags(&cfg, []string{"-nope", "x"})
	require.Error(t, err)
}

func TestHealthEndpoints(t *testing.T) {
	mux := healthMux()
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code, path)
		assert.NotEmpty(t, rec.Body.String(), path)
	}
}

func TestDispatcherOptionsFromConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	opts := dispatcherOptions(&cfg)
	assert.Equal(t, cfg.Receiver, opts.Receiver)
	assert.Equal(t, cfg.GroupBy, opts.GroupBy)
	assert.Equal(t, cfg.RepeatInterval, opts.RepeatInterval)
	assert.Equal(t, cfg.ResolveTimeout, opts.ResolveTimeout)
}
