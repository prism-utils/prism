package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalEnv returns the smallest env map that yields a valid Config.
func minimalEnv() map[string]string {
	return map[string]string{
		"STORE_BASE_URL":       "http://prism-store:8080",
		"TENANT_NS":            "team-a",
		"NOTIFIER_WEBHOOK_URL": "http://notifier:8080/webhook",
		"WEBHOOK_SECRET":       "s3cret",
	}
}

func lookup(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(lookup(minimalEnv()))
	require.NoError(t, err)

	assert.Equal(t, "http://prism-store:8080", cfg.StoreBaseURL)
	assert.Equal(t, "team-a", cfg.TenantNS)
	assert.Equal(t, "http://notifier:8080/webhook", cfg.NotifierWebhookURL)
	assert.Equal(t, "/etc/prism-alert/rules", cfg.RulesDir)
	assert.Equal(t, 60*time.Second, cfg.EvaluationInterval)
	assert.Equal(t, []string{"alertname", "severity"}, cfg.GroupBy)
	assert.Equal(t, 30*time.Second, cfg.GroupWait)
	assert.Equal(t, 5*time.Minute, cfg.GroupInterval)
	assert.Equal(t, 4*time.Hour, cfg.RepeatInterval)
	assert.Equal(t, 5*time.Minute, cfg.ResolveTimeout)
	assert.Equal(t, "tenant-webhook", cfg.Receiver)
	assert.Equal(t, ":8080", cfg.ListenAddr)
	assert.True(t, cfg.QueryHotOnly, "hot-only is the default ruler scope")
	assert.Empty(t, cfg.ExternalURL)
	assert.Empty(t, cfg.RoutePrefix)
}

func TestLoadQueryHotOnlyToggle(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"false", false}, {"0", false}, {"off", false}, {"no", false},
		{"true", true}, {"1", true}, {"on", true}, {"yes", true},
	} {
		env := minimalEnv()
		env["QUERY_HOT_ONLY"] = tc.val
		cfg, err := Load(lookup(env))
		require.NoError(t, err)
		assert.Equal(t, tc.want, cfg.QueryHotOnly, "QUERY_HOT_ONLY=%q", tc.val)
	}
}

func TestLoadRejectsBadHotOnly(t *testing.T) {
	env := minimalEnv()
	env["QUERY_HOT_ONLY"] = "maybe"
	_, err := Load(lookup(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QUERY_HOT_ONLY")
}

func TestLoadOverrides(t *testing.T) {
	env := minimalEnv()
	env["RULES_DIR"] = "/rules"
	env["EVALUATION_INTERVAL"] = "15s"
	env["GROUP_BY"] = "alertname, severity ,cluster"
	env["GROUP_WAIT"] = "10s"
	env["GROUP_INTERVAL"] = "2m"
	env["REPEAT_INTERVAL"] = "1h"
	env["RESOLVE_TIMEOUT"] = "3m"
	env["EXTERNAL_URL"] = "https://prism.example/alerts"
	env["RECEIVER"] = "custom-receiver"
	env["ROUTE_PREFIX"] = "/prism"
	env["LISTEN_ADDR"] = ":9091"
	env["STORE_TOKEN_FILE"] = "/var/run/token"

	cfg, err := Load(lookup(env))
	require.NoError(t, err)

	assert.Equal(t, "/rules", cfg.RulesDir)
	assert.Equal(t, 15*time.Second, cfg.EvaluationInterval)
	assert.Equal(t, []string{"alertname", "severity", "cluster"}, cfg.GroupBy)
	assert.Equal(t, 10*time.Second, cfg.GroupWait)
	assert.Equal(t, 2*time.Minute, cfg.GroupInterval)
	assert.Equal(t, time.Hour, cfg.RepeatInterval)
	assert.Equal(t, 3*time.Minute, cfg.ResolveTimeout)
	assert.Equal(t, "https://prism.example/alerts", cfg.ExternalURL)
	assert.Equal(t, "custom-receiver", cfg.Receiver)
	assert.Equal(t, "/prism", cfg.RoutePrefix)
	assert.Equal(t, ":9091", cfg.ListenAddr)
	assert.Equal(t, "/var/run/token", cfg.StoreTokenFile)
}

func TestLoadRejectsBadDuration(t *testing.T) {
	env := minimalEnv()
	env["EVALUATION_INTERVAL"] = "not-a-duration"
	_, err := Load(lookup(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EVALUATION_INTERVAL")
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"ok", func(*Config) {}, ""},
		{"missing store url", func(c *Config) { c.StoreBaseURL = "" }, "STORE_BASE_URL"},
		{"bad store url", func(c *Config) { c.StoreBaseURL = "://nope" }, "STORE_BASE_URL"},
		{"non-abs store url", func(c *Config) { c.StoreBaseURL = "prism-store:8080" }, "STORE_BASE_URL"},
		{"missing tenant", func(c *Config) { c.TenantNS = "" }, "TENANT_NS"},
		{"bad tenant", func(c *Config) { c.TenantNS = "Bad NS" }, "TENANT_NS"},
		{"missing notifier url", func(c *Config) { c.NotifierWebhookURL = "" }, "NOTIFIER_WEBHOOK_URL"},
		{"bad notifier url", func(c *Config) { c.NotifierWebhookURL = "://x" }, "NOTIFIER_WEBHOOK_URL"},
		{"missing secret", func(c *Config) { c.WebhookSecret = "" }, "WEBHOOK_SECRET"},
		{"zero eval interval", func(c *Config) { c.EvaluationInterval = 0 }, "EVALUATION_INTERVAL"},
		{"negative group wait", func(c *Config) { c.GroupWait = -1 }, "GROUP_WAIT"},
		{"zero group interval", func(c *Config) { c.GroupInterval = 0 }, "GROUP_INTERVAL"},
		{"zero repeat interval", func(c *Config) { c.RepeatInterval = 0 }, "REPEAT_INTERVAL"},
		{"zero resolve timeout", func(c *Config) { c.ResolveTimeout = 0 }, "RESOLVE_TIMEOUT"},
		{"empty group by", func(c *Config) { c.GroupBy = nil }, "GROUP_BY"},
		{"empty rules dir", func(c *Config) { c.RulesDir = "" }, "RULES_DIR"},
		{"empty listen addr", func(c *Config) { c.ListenAddr = "" }, "LISTEN_ADDR"},
		{"empty receiver", func(c *Config) { c.Receiver = "" }, "RECEIVER"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(lookup(minimalEnv()))
			require.NoError(t, err)
			tc.mutate(&cfg)
			err = cfg.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Load must run Validate so an invalid environment fails fast at startup.
func TestLoadValidates(t *testing.T) {
	env := minimalEnv()
	delete(env, "WEBHOOK_SECRET")
	_, err := Load(lookup(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WEBHOOK_SECRET")
}
