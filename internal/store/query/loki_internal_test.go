package query

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLokiConfigValidate(t *testing.T) {
	base := LokiConfig{MaxEntries: 10, Timeout: time.Second}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*LokiConfig){
		"loki.max_entries": func(c *LokiConfig) { c.MaxEntries = 0 },
		"loki.timeout":     func(c *LokiConfig) { c.Timeout = 0 },
	}
	for wantPath, mutate := range cases {
		t.Run(wantPath, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("%s: invalid config accepted", wantPath)
			}
			if !strings.Contains(err.Error(), wantPath) {
				t.Fatalf("error %q must name the config path %q", err, wantPath)
			}
		})
	}
}

func TestLokiConfigWithDefaults(t *testing.T) {
	cfg := LokiConfig{}
	cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must produce a valid config: %v", err)
	}
	if cfg.MaxEntries != defaultLokiMaxEntries || cfg.Timeout != defaultLokiTimeout {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestLokiAPIEnabledFromEnv(t *testing.T) {
	cases := map[string]struct {
		set  bool
		val  string
		want bool
	}{
		"unset_defaults_true": {set: false, want: true},
		"empty_defaults_true": {set: true, val: "", want: true},
		"false":               {set: true, val: "false", want: false},
		"zero":                {set: true, val: "0", want: false},
		"true":                {set: true, val: "true", want: true},
		"garbage_is_true":     {set: true, val: "maybe", want: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.set {
				t.Setenv("LOKI_API_ENABLED", tc.val)
			}
			if got := LokiAPIEnabledFromEnv(); got != tc.want {
				t.Fatalf("LokiAPIEnabledFromEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLokiRoutePatterns(t *testing.T) {
	want := []string{
		"GET /{ns}/loki/api/v1/query_range",
		"POST /{ns}/loki/api/v1/query_range",
		"GET /{ns}/loki/api/v1/labels",
		"POST /{ns}/loki/api/v1/labels",
		"GET /{ns}/loki/api/v1/label/{name}/values",
		"POST /{ns}/loki/api/v1/label/{name}/values",
	}
	got := LokiRoutePatterns("")
	if len(got) != len(want) {
		t.Fatalf("patterns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pattern %d = %q, want %q", i, got[i], want[i])
		}
	}
	prefixed := LokiRoutePatterns("/prism-proxy/")
	if prefixed[0] != "GET /prism-proxy/{ns}/loki/api/v1/query_range" {
		t.Fatalf("prefixed pattern = %q", prefixed[0])
	}
}

func TestLokiTimeParsing(t *testing.T) {
	rfc := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	cases := []struct {
		name string
		in   string
		want int64
	}{
		{"nanoseconds", "1770000000000000000", 1770000000000000000},
		{"fractional_seconds", "1770000000.5", 1770000000500000000},
		{"rfc3339", rfc.Format(time.RFC3339), rfc.UnixNano()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLokiTimeNanos(tc.in, 0)
			if err != nil {
				t.Fatalf("parseLokiTimeNanos(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseLokiTimeNanos(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
	if got, err := parseLokiTimeNanos("", 42); err != nil || got != 42 {
		t.Fatalf("empty must yield the default: got=%d err=%v", got, err)
	}
	if _, err := parseLokiTimeNanos("not-a-time", 0); err == nil {
		t.Fatal("malformed time accepted")
	}
}

func TestLokiExecErrorCanceledIs499(t *testing.T) {
	h := &lokiHandler{cfg: &LokiConfig{}}
	e := h.execError(context.Canceled)
	if e.status != 499 {
		t.Fatalf("status=%d want 499", e.status)
	}
}

func TestLokiExecErrorDeadlineStill503(t *testing.T) {
	h := &lokiHandler{cfg: &LokiConfig{}}
	e := h.execError(context.DeadlineExceeded)
	if e.status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", e.status)
	}
}
