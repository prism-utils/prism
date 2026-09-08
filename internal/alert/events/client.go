package events

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/alert/notify"
)

const ingestArtifact = "alert-events"

// Client POSTs encoded alert_events parquet windows to prism-store ingest.
// Failures are logged and dropped (fail-open): a store blip must not block
// ruler evaluation or webhook delivery.
type Client struct {
	endpoint   string
	tokenFile  string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewClient builds a client targeting
// {storeBaseURL}{routePrefix}/{tenant}/ingest/alert-events. A nil httpClient
// gets a 10s-timeout default that never follows redirects.
func NewClient(storeBaseURL, routePrefix, tenant, tokenFile string, hc *http.Client, logger *slog.Logger) (*Client, error) {
	base, err := url.Parse(storeBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse store base url: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + ingestPath(routePrefix, tenant)
	if hc == nil {
		hc = &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Client{endpoint: base.String(), tokenFile: tokenFile, httpClient: hc, logger: logger}, nil
}

func ingestPath(routePrefix, tenant string) string {
	prefix := "/" + strings.Trim(routePrefix, "/")
	if prefix == "/" {
		prefix = ""
	}
	return prefix + "/" + strings.Trim(tenant, "/") + "/ingest/" + ingestArtifact
}

// Persist encodes alerts and POSTs them. An empty batch is a no-op. Transport
// and non-2xx responses are logged; the method never returns an error so the
// ruler can keep evaluating (fail-open).
func (c *Client) Persist(ctx context.Context, alerts []notify.Alert) {
	if len(alerts) == 0 {
		return
	}
	body, err := Encode(alerts)
	if err != nil {
		c.logger.Warn("alert events encode failed", "err", err)
		return
	}
	if len(body) == 0 {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		c.logger.Warn("alert events request failed", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.tokenFile != "" {
		raw, err := os.ReadFile(c.tokenFile)
		if err != nil {
			c.logger.Warn("alert events token read failed", "err", err)
			return
		}
		if token := strings.TrimSpace(string(raw)); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Warn("alert events ingest failed", "err", err)
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.logger.Warn("alert events ingest rejected", "status", resp.StatusCode)
	}
}
