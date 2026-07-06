// Package prometheus implements an Input that scrapes one or more Prometheus
// /metrics endpoints on a fixed interval. Each scrape's exposition body is
// split into line records and emitted as a RawBatch (Source = target URL) for
// the prometheus parser to structure. Scraping stops on ctx cancellation.
package prometheus

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

const (
	// Type is the config identifier for this input.
	Type = "prometheus"

	defaultInterval = 15 * time.Second
	defaultTimeout  = 10 * time.Second
)

// Config configures the prometheus scrape input.
type Config struct {
	// Targets are the /metrics URLs to scrape. At least one is required.
	Targets []string `json:"targets"`
	// Interval is the scrape period as a Go duration (default 15s).
	Interval string `json:"interval"`
	// Timeout bounds each scrape request as a Go duration (default 10s).
	Timeout string `json:"timeout"`
	// BasicAuth attaches HTTP Basic credentials to every scrape. Mutually
	// exclusive with BearerToken. Secrets should come from ${ENV}.
	BasicAuth *BasicAuth `json:"basic_auth,omitempty"`
	// BearerToken is sent as "Authorization: Bearer <token>". Mutually
	// exclusive with BasicAuth. Should come from ${ENV}.
	BearerToken string `json:"bearer_token,omitempty"`
	// TLS configures transport security for https targets (custom CA, client
	// mTLS, SNI override, or verification skip for self-signed endpoints).
	TLS *TLSConfig `json:"tls,omitempty"`
}

// BasicAuth holds HTTP Basic credentials for scraping.
type BasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// TLSConfig configures transport security for scraping https endpoints. File
// fields are paths read at Start; secrets in them stay off the config file.
type TLSConfig struct {
	// CA is a PEM bundle used to verify the server certificate (optional; the
	// system roots are used when empty).
	CA string `json:"ca"`
	// Cert and Key are the client certificate/key for mTLS (both or neither).
	Cert string `json:"cert"`
	Key  string `json:"key"`
	// ServerName overrides the SNI/verification hostname (optional).
	ServerName string `json:"server_name"`
	// InsecureSkipVerify disables certificate verification (self-signed dev
	// endpoints only — never in production).
	InsecureSkipVerify bool `json:"insecure_skip_verify"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if len(c.Targets) == 0 {
		return fmt.Errorf("prometheus.targets: at least one required")
	}
	for i, t := range c.Targets {
		if t == "" {
			return fmt.Errorf("prometheus.targets[%d]: must not be empty", i)
		}
	}
	if _, err := parseDur(c.Interval, defaultInterval); err != nil {
		return fmt.Errorf("prometheus.interval: %w", err)
	}
	if _, err := parseDur(c.Timeout, defaultTimeout); err != nil {
		return fmt.Errorf("prometheus.timeout: %w", err)
	}
	if c.BasicAuth != nil && c.BearerToken != "" {
		return fmt.Errorf("prometheus: basic_auth and bearer_token are mutually exclusive")
	}
	if c.BasicAuth != nil && c.BasicAuth.Username == "" {
		return fmt.Errorf("prometheus.basic_auth.username: required when basic_auth is set")
	}
	if c.TLS != nil {
		if (c.TLS.Cert == "") != (c.TLS.Key == "") {
			return fmt.Errorf("prometheus.tls: cert and key must be set together")
		}
	}
	return nil
}

func parseDur(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be > 0, got %q", s)
	}
	return d, nil
}

type factory struct{}

// NewFactory returns the prometheus input factory.
func NewFactory() component.Factory[component.Input] { return factory{} }

func (factory) Type() string { return Type }
func (factory) DefaultConfig() component.Config {
	return &Config{Interval: "15s", Timeout: "10s"}
}

func (factory) Create(cfg component.Config, set component.Settings) (component.Input, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("input/prometheus: unexpected config type %T", cfg)
	}
	interval, _ := parseDur(c.Interval, defaultInterval)
	timeout, _ := parseDur(c.Timeout, defaultTimeout)
	return &Input{
		targets:     append([]string(nil), c.Targets...),
		interval:    interval,
		timeout:     timeout,
		basicAuth:   c.BasicAuth,
		bearerToken: c.BearerToken,
		tls:         c.TLS,
		batches:     make(chan data.RawBatch, 1),
		log:         set.Logger,
	}, nil
}

// Input scrapes targets on an interval and emits their bodies as RawBatches.
type Input struct {
	targets     []string
	interval    time.Duration
	timeout     time.Duration
	basicAuth   *BasicAuth
	bearerToken string
	tls         *TLSConfig
	client      *http.Client
	batches     chan data.RawBatch
	log         *slog.Logger
}

// Batches returns the channel of scraped RawBatches; closed on ctx cancel.
func (in *Input) Batches() <-chan data.RawBatch { return in.batches }

// Start builds the HTTP client (reading any TLS material) and launches the
// scrape loop. It scrapes once immediately, then every interval, so a
// short-lived run still produces data.
func (in *Input) Start(ctx context.Context, _ component.Host) error {
	client := &http.Client{Timeout: in.timeout}
	if in.tls != nil {
		tlsCfg, err := buildTLSConfig(in.tls)
		if err != nil {
			return fmt.Errorf("input/prometheus: tls: %w", err)
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	in.client = client
	go in.loop(ctx)
	return nil
}

// buildTLSConfig reads the configured CA/client-cert material into a tls.Config.
func buildTLSConfig(c *TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // opt-in for self-signed dev endpoints; documented as unsafe
	}
	if c.CA != "" {
		pem, err := os.ReadFile(c.CA)
		if err != nil {
			return nil, fmt.Errorf("read ca %q: %w", c.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca %q: no certificates found", c.CA)
		}
		cfg.RootCAs = pool
	}
	if c.Cert != "" {
		pair, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// Shutdown is a no-op; the loop stops on ctx cancellation.
func (in *Input) Shutdown(context.Context) error { return nil }

func (in *Input) loop(ctx context.Context) {
	defer close(in.batches)
	ticker := time.NewTicker(in.interval)
	defer ticker.Stop()

	in.scrapeAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			in.scrapeAll(ctx)
		}
	}
}

func (in *Input) scrapeAll(ctx context.Context) {
	for _, target := range in.targets {
		body, err := in.scrape(ctx, target)
		if err != nil {
			if in.log != nil {
				in.log.Warn("prometheus: scrape failed", "target", target, "err", err)
			}
			continue
		}
		batch := data.RawBatch{Source: target, Records: splitLines(body)}
		if len(batch.Records) == 0 {
			continue
		}
		select {
		case in.batches <- batch:
		case <-ctx.Done():
			return
		}
	}
}

func (in *Input) scrape(ctx context.Context, target string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	switch {
	case in.bearerToken != "":
		req.Header.Set("Authorization", "Bearer "+in.bearerToken)
	case in.basicAuth != nil:
		req.SetBasicAuth(in.basicAuth.Username, in.basicAuth.Password)
	}
	resp, err := in.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// splitLines splits an exposition body into per-line records, dropping blanks.
func splitLines(body []byte) [][]byte {
	raw := bytes.Split(body, []byte("\n"))
	out := make([][]byte, 0, len(raw))
	for _, l := range raw {
		l = bytes.TrimRight(l, "\r")
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		out = append(out, l)
	}
	return out
}
