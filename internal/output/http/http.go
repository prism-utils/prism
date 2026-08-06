// Package http implements an Output that POSTs each encoded block to an HTTP(S)
// endpoint with optional bearer auth, custom headers, and client TLS, retrying
// transient failures with capped exponential backoff and giving up with a typed
// error after a bounded number of attempts.
//
// It is the authenticated egress that reaches a tenant ingress fronted by a
// Bearer-checking reverse proxy (e.g. Traefik ForwardAuth): the block bytes —
// typically a self-contained Parquet window — become the request body, so the
// receiver lands columnar artifacts directly. The bearer token comes from
// ${ENV} (static) or a token_file re-read per send (for rotating credentials),
// so nothing sensitive lives in the config file.
package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/tlsconf"
)

// Type is the config identifier for this output.
const Type = "http"

const (
	defaultMethod         = http.MethodPost
	defaultContentType    = "application/octet-stream"
	defaultTimeout        = 30 * time.Second
	defaultMaxRetries     = 5
	defaultInitialBackoff = 500 * time.Millisecond
	defaultMaxBackoff     = 30 * time.Second
)

// ErrGiveUp is returned when all attempts are exhausted without success.
var ErrGiveUp = errors.New("output/http: gave up after retries")

// tlsConfig is the shared client-TLS block.
type tlsConfig = tlsconf.Config

// Config configures the http output.
type Config struct {
	// URL is the endpoint each block is POSTed to. Required.
	URL string `json:"url"`
	// Method overrides the HTTP method (default POST).
	Method string `json:"method"`
	// Headers are extra request headers (e.g. a tenant id). Values may use ${ENV}.
	Headers map[string]string `json:"headers"`
	// Token, when set, is sent as "Authorization: Bearer <token>". Use ${ENV}.
	Token string `json:"token"`
	// TokenFile is a path whose contents are sent as "Authorization: Bearer
	// <contents>". Unlike Token it is re-read on every send, so a bearer that
	// rotates on disk (e.g. a Kubernetes projected ServiceAccount token) stays
	// current without restarting the process. Trailing whitespace is trimmed.
	// Mutually exclusive with Token.
	TokenFile string `json:"token_file"`
	// ContentType sets the request Content-Type (default application/octet-stream).
	ContentType string `json:"content_type"`
	// TLS configures transport security for https endpoints.
	TLS *tlsConfig `json:"tls,omitempty"`
	// MaxRetries bounds the retries after the first attempt (default 5).
	MaxRetries int `json:"max_retries"`
	// Timeout bounds each individual attempt as a Go duration (default 30s).
	Timeout string `json:"timeout"`
	// InitialBackoff is the first retry delay (default 500ms).
	InitialBackoff string `json:"initial_backoff"`
	// MaxBackoff caps the retry delay (default 30s).
	MaxBackoff string `json:"max_backoff"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("http.url: required, must not be empty")
	}
	if c.Token != "" && c.TokenFile != "" {
		return fmt.Errorf("http.token / http.token_file: set at most one, not both")
	}
	switch c.Method {
	case "", http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return fmt.Errorf("http.method: must be POST, PUT, or PATCH, got %q", c.Method)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("http.max_retries: must be >= 0")
	}
	if _, err := parseDur(c.Timeout, defaultTimeout); err != nil {
		return fmt.Errorf("http.timeout: %w", err)
	}
	if _, err := parseDur(c.InitialBackoff, defaultInitialBackoff); err != nil {
		return fmt.Errorf("http.initial_backoff: %w", err)
	}
	if _, err := parseDur(c.MaxBackoff, defaultMaxBackoff); err != nil {
		return fmt.Errorf("http.max_backoff: %w", err)
	}
	if err := c.TLS.Validate("http.tls"); err != nil {
		return fmt.Errorf("%w", err)
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

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

type factory struct{}

// NewFactory returns the http output factory.
func NewFactory() component.Factory[component.Output] { return factory{} }

func (factory) Type() string { return Type }
func (factory) DefaultConfig() component.Config {
	return &Config{Method: defaultMethod}
}

func (factory) Create(cfg component.Config, _ component.Settings) (component.Output, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("output/http: unexpected config type %T", cfg)
	}
	timeout, _ := parseDur(c.Timeout, defaultTimeout)
	initial, _ := parseDur(c.InitialBackoff, defaultInitialBackoff)
	maxb, _ := parseDur(c.MaxBackoff, defaultMaxBackoff)
	retries := c.MaxRetries
	if retries == 0 {
		retries = defaultMaxRetries
	}
	return &Output{
		cfg:            *c,
		method:         orDefault(c.Method, defaultMethod),
		timeout:        timeout,
		maxRetries:     uint64(retries),
		initialBackoff: initial,
		maxBackoff:     maxb,
	}, nil
}

// Output POSTs blocks to an HTTP endpoint with retry.
type Output struct {
	cfg            Config
	method         string
	timeout        time.Duration
	maxRetries     uint64
	initialBackoff time.Duration
	maxBackoff     time.Duration
	client         *http.Client
}

// Start builds the HTTP client, reading any TLS material.
func (o *Output) Start(_ context.Context, _ component.Host) error {
	client := &http.Client{Timeout: o.timeout}
	if o.cfg.TLS != nil {
		tlsCfg, err := o.cfg.TLS.Build()
		if err != nil {
			return fmt.Errorf("output/http: tls: %w", err)
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	o.client = client
	return nil
}

// Shutdown releases idle connections.
func (o *Output) Shutdown(context.Context) error {
	if o.client != nil {
		o.client.CloseIdleConnections()
	}
	return nil
}

// Consume POSTs the block, retrying transient failures with capped exponential
// backoff. A 4xx (other than 429) is permanent and fails immediately; 429 and
// 5xx and transport errors are retried until success or the retry budget runs
// out, at which point it returns ErrGiveUp wrapping the last failure.
func (o *Output) Consume(ctx context.Context, block data.EncodedBlock) error {
	if len(block.Bytes) == 0 {
		return nil // empty window: nothing to ship
	}
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = o.initialBackoff
	bo.MaxInterval = o.maxBackoff
	bo.MaxElapsedTime = 0 // bounded by MaxRetries, not wall clock

	var lastErr error
	op := func() error {
		err := o.attempt(ctx, block)
		if err != nil {
			lastErr = err
		}
		return err
	}
	policy := backoff.WithContext(backoff.WithMaxRetries(bo, o.maxRetries), ctx)
	if err := backoff.Retry(op, policy); err != nil {
		var perm *backoff.PermanentError
		if errors.As(err, &perm) {
			return fmt.Errorf("output/http: permanent failure: %w", perm.Err)
		}
		return fmt.Errorf("%w: %w", ErrGiveUp, lastErr)
	}
	return nil
}

// contentTypeFor picks the request Content-Type: an explicit config value wins;
// otherwise duckdb windows use application/vnd.duckdb and everything else uses
// application/octet-stream.
func (o *Output) contentTypeFor(block data.EncodedBlock) string {
	if o.cfg.ContentType != "" {
		return o.cfg.ContentType
	}
	if block.Format == "duckdb" {
		return duckdbfile.ContentType
	}
	return defaultContentType
}

// attempt performs one POST. It returns a backoff.Permanent error for a
// non-retryable status so the retry loop stops immediately.
func (o *Output) attempt(ctx context.Context, block data.EncodedBlock) error {
	req, err := http.NewRequestWithContext(ctx, o.method, o.cfg.URL, bytes.NewReader(block.Bytes))
	if err != nil {
		return backoff.Permanent(fmt.Errorf("build request: %w", err))
	}
	req.Header.Set("Content-Type", o.contentTypeFor(block))
	for k, v := range o.cfg.Headers {
		req.Header.Set(k, v)
	}
	if o.cfg.TokenFile != "" {
		// Re-read on every attempt: a projected SA token rotates on disk, and a
		// value cached at construction would go stale and start returning 401.
		raw, err := os.ReadFile(o.cfg.TokenFile)
		if err != nil {
			return fmt.Errorf("read token_file %q: %w", o.cfg.TokenFile, err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(raw)))
	} else if o.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+o.cfg.Token)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err // transport error: retryable
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("status %d", resp.StatusCode) // retryable
	default:
		return backoff.Permanent(fmt.Errorf("status %d", resp.StatusCode))
	}
}
