// Package prometheus implements an Input that scrapes one or more Prometheus
// /metrics endpoints on a fixed interval. Each scrape's exposition body is
// split into line records and emitted as a RawBatch (Source = target URL) for
// the prometheus parser to structure. Scraping stops on ctx cancellation.
package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
		targets:  append([]string(nil), c.Targets...),
		interval: interval,
		client:   &http.Client{Timeout: timeout},
		batches:  make(chan data.RawBatch, 1),
		log:      set.Logger,
	}, nil
}

// Input scrapes targets on an interval and emits their bodies as RawBatches.
type Input struct {
	targets  []string
	interval time.Duration
	client   *http.Client
	batches  chan data.RawBatch
	log      *slog.Logger
}

// Batches returns the channel of scraped RawBatches; closed on ctx cancel.
func (in *Input) Batches() <-chan data.RawBatch { return in.batches }

// Start launches the scrape loop. It scrapes once immediately, then every
// interval, so a short-lived run still produces data.
func (in *Input) Start(ctx context.Context, _ component.Host) error {
	go in.loop(ctx)
	return nil
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
