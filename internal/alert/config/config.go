// Package config holds prism-alert's typed configuration: the ruler evaluation
// cadence, the prism-store PromQL endpoint, the Alertmanager-style route knobs,
// and the notifier webhook target. Every field has a default from
// DefaultConfig, is loaded from the environment (secrets via ${ENV}), and is
// validated with a path-named error before the process starts.
package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// tenantPattern mirrors the namespace grammar prism-store enforces, so a
// prism-alert instance can only ever address a well-formed tenant.
var tenantPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// Config is the fully-resolved prism-alert configuration.
type Config struct {
	// StoreBaseURL is the prism-store query base URL (scheme://host[:port]).
	StoreBaseURL string `json:"store_base_url"`
	// TenantNS is the tenant namespace this instance rules for.
	TenantNS string `json:"tenant_ns"`
	// RoutePrefix is prism-store's optional path prefix (ROUTE_PREFIX).
	RoutePrefix string `json:"route_prefix"`
	// StoreTokenFile is the path to the reader JWT; read fresh per request so
	// rotation needs no restart. Empty means no Authorization header.
	StoreTokenFile string `json:"store_token_file"`

	// RulesDir is the directory of Prometheus rule-group YAML files.
	RulesDir string `json:"rules_dir"`
	// EvaluationInterval is the ruler eval cadence.
	EvaluationInterval time.Duration `json:"evaluation_interval"`

	// NotifierWebhookURL is the notifier /webhook endpoint.
	NotifierWebhookURL string `json:"notifier_webhook_url"`
	// WebhookSecret is the bearer token presented to the notifier. It is never
	// serialized (json:"-") so it cannot leak through a config dump.
	WebhookSecret string `json:"-"`
	// Receiver is the receiver name stamped on every emitted payload.
	Receiver string `json:"receiver"`
	// ExternalURL is the payload externalURL (links back to a UI); may be empty.
	ExternalURL string `json:"external_url"`

	// GroupBy is the set of labels alerts are grouped on before notifying.
	GroupBy []string `json:"group_by"`
	// GroupWait delays the first notification for a new group so related alerts
	// can accumulate into one webhook.
	GroupWait time.Duration `json:"group_wait"`
	// GroupInterval is the minimum spacing between notifications for a group
	// once it has fired, when its alert set changes.
	GroupInterval time.Duration `json:"group_interval"`
	// RepeatInterval is how often an unchanged firing group re-notifies.
	RepeatInterval time.Duration `json:"repeat_interval"`
	// ResolveTimeout is how long after the last firing evaluation an alert is
	// considered resolved when the rule stops producing it.
	ResolveTimeout time.Duration `json:"resolve_timeout"`

	// ListenAddr is the health/probe HTTP listen address.
	ListenAddr string `json:"listen_addr"`
}

// DefaultConfig returns the zero value with every non-secret default applied.
func DefaultConfig() Config {
	return Config{
		RulesDir:           "/etc/prism-alert/rules",
		EvaluationInterval: 60 * time.Second,
		Receiver:           "tenant-webhook",
		GroupBy:            []string{"alertname", "severity"},
		GroupWait:          30 * time.Second,
		GroupInterval:      5 * time.Minute,
		RepeatInterval:     4 * time.Hour,
		ResolveTimeout:     5 * time.Minute,
		ListenAddr:         ":8080",
	}
}

// Load builds a Config from the given environment lookup, applying defaults for
// unset keys, then validates it. Durations parse with time.ParseDuration.
func Load(getenv func(string) string) (Config, error) {
	c := DefaultConfig()

	c.StoreBaseURL = getenv("STORE_BASE_URL")
	c.TenantNS = getenv("TENANT_NS")
	c.NotifierWebhookURL = getenv("NOTIFIER_WEBHOOK_URL")
	c.WebhookSecret = getenv("WEBHOOK_SECRET")
	c.StoreTokenFile = getenv("STORE_TOKEN_FILE")

	if v := getenv("RULES_DIR"); v != "" {
		c.RulesDir = v
	}
	if v := getenv("RECEIVER"); v != "" {
		c.Receiver = v
	}
	if v := getenv("ROUTE_PREFIX"); v != "" {
		c.RoutePrefix = v
	}
	if v := getenv("EXTERNAL_URL"); v != "" {
		c.ExternalURL = v
	}
	if v := getenv("LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := getenv("GROUP_BY"); v != "" {
		c.GroupBy = parseList(v)
	}

	durations := []struct {
		key string
		dst *time.Duration
	}{
		{"EVALUATION_INTERVAL", &c.EvaluationInterval},
		{"GROUP_WAIT", &c.GroupWait},
		{"GROUP_INTERVAL", &c.GroupInterval},
		{"REPEAT_INTERVAL", &c.RepeatInterval},
		{"RESOLVE_TIMEOUT", &c.ResolveTimeout},
	}
	for _, d := range durations {
		v := getenv(d.key)
		if v == "" {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("%s: invalid duration %q: %w", d.key, v, err)
		}
		*d.dst = parsed
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate is total: it rejects any field that would make the ruler or the
// notifier client misbehave, naming the offending environment variable.
func (c *Config) Validate() error {
	if c.StoreBaseURL == "" {
		return fmt.Errorf("STORE_BASE_URL: required")
	}
	if err := validateAbsURL(c.StoreBaseURL); err != nil {
		return fmt.Errorf("STORE_BASE_URL: %w", err)
	}
	if c.TenantNS == "" {
		return fmt.Errorf("TENANT_NS: required")
	}
	if !tenantPattern.MatchString(c.TenantNS) {
		return fmt.Errorf("TENANT_NS: %q must match %s", c.TenantNS, tenantPattern.String())
	}
	if c.NotifierWebhookURL == "" {
		return fmt.Errorf("NOTIFIER_WEBHOOK_URL: required")
	}
	if err := validateAbsURL(c.NotifierWebhookURL); err != nil {
		return fmt.Errorf("NOTIFIER_WEBHOOK_URL: %w", err)
	}
	if c.WebhookSecret == "" {
		return fmt.Errorf("WEBHOOK_SECRET: required")
	}
	if c.Receiver == "" {
		return fmt.Errorf("RECEIVER: required")
	}
	if c.RulesDir == "" {
		return fmt.Errorf("RULES_DIR: required")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("LISTEN_ADDR: required")
	}
	if len(c.GroupBy) == 0 {
		return fmt.Errorf("GROUP_BY: at least one label required")
	}
	positives := []struct {
		key string
		val time.Duration
	}{
		{"EVALUATION_INTERVAL", c.EvaluationInterval},
		{"GROUP_WAIT", c.GroupWait},
		{"GROUP_INTERVAL", c.GroupInterval},
		{"REPEAT_INTERVAL", c.RepeatInterval},
		{"RESOLVE_TIMEOUT", c.ResolveTimeout},
	}
	for _, p := range positives {
		// GROUP_WAIT may legitimately be zero (notify immediately); everything
		// else must be strictly positive to avoid a busy loop or instant resolve.
		if p.key == "GROUP_WAIT" {
			if p.val < 0 {
				return fmt.Errorf("GROUP_WAIT: must be >= 0, got %s", p.val)
			}
			continue
		}
		if p.val <= 0 {
			return fmt.Errorf("%s: must be > 0, got %s", p.key, p.val)
		}
	}
	return nil
}

func validateAbsURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		// Do not echo the raw value: it may contain embedded credentials.
		return fmt.Errorf("invalid URL: %w", err)
	}
	// Reject userinfo so a credential can never ride in the URL (and so it never
	// lands in logs or error messages); auth is the bearer token / reader JWT.
	if u.User != nil {
		return fmt.Errorf("must not embed credentials in the URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("must be an http(s) URL")
	}
	if u.Host == "" {
		return fmt.Errorf("must be an absolute URL with a host")
	}
	return nil
}

func parseList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
