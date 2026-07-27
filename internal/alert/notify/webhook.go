package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// maxWebhookBodyBytes is the notifier's per-request body cap (256 KiB). A group
// whose JSON exceeds this is split across several POSTs rather than truncated.
const maxWebhookBodyBytes = 256 * 1024

// WebhookClient POSTs Alertmanager v4 payloads to the notifier /webhook with a
// bearer token, splitting oversized groups and retrying transient failures with
// bounded exponential backoff.
type WebhookClient struct {
	url        string
	secret     string
	httpClient *http.Client
	maxElapsed time.Duration
	maxBody    int
	logger     *slog.Logger
}

// WebhookConfig configures a WebhookClient.
type WebhookConfig struct {
	URL        string
	Secret     string
	HTTPClient *http.Client
	// MaxElapsed caps total retry time for a single POST; 0 uses a 30s default.
	MaxElapsed time.Duration
}

// NewWebhookClient builds a WebhookClient. A nil HTTPClient gets a 10s-timeout
// default; a nil logger becomes a discard logger.
func NewWebhookClient(cfg WebhookConfig, logger *slog.Logger) *WebhookClient {
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	maxElapsed := cfg.MaxElapsed
	if maxElapsed <= 0 {
		maxElapsed = 30 * time.Second
	}
	return &WebhookClient{
		url:        cfg.URL,
		secret:     cfg.Secret,
		httpClient: hc,
		maxElapsed: maxElapsed,
		maxBody:    maxWebhookBodyBytes,
		logger:     logger,
	}
}

var _ Sender = (*WebhookClient)(nil)

// Send delivers a payload, chunking it into ≤256 KiB batches first.
func (c *WebhookClient) Send(ctx context.Context, payload WebhookPayload) error {
	for _, batch := range c.chunk(payload) {
		body, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("marshal webhook payload: %w", err)
		}
		if err := c.postWithRetry(ctx, body); err != nil {
			return err
		}
	}
	return nil
}

// chunk splits a payload so each emitted payload's JSON body is ≤ maxBody. A
// single alert that alone exceeds the cap is emitted as-is (the notifier will
// reject it, which surfaces the misconfiguration rather than hiding data).
func (c *WebhookClient) chunk(payload WebhookPayload) []WebhookPayload {
	body, err := json.Marshal(payload)
	if err == nil && len(body) <= c.maxBody {
		return []WebhookPayload{payload}
	}
	if len(payload.Alerts) <= 1 {
		return []WebhookPayload{payload}
	}
	mid := len(payload.Alerts) / 2
	left := payload
	left.Alerts = payload.Alerts[:mid]
	right := payload
	right.Alerts = payload.Alerts[mid:]
	return append(c.chunk(left), c.chunk(right)...)
}

// postWithRetry POSTs one JSON body, retrying transport errors, 429, and 5xx
// with capped exponential backoff. 4xx (other than 429) are permanent.
func (c *WebhookClient) postWithRetry(ctx context.Context, body []byte) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = c.maxElapsed
	op := func() error { return c.postOnce(ctx, body) }
	return backoff.Retry(op, backoff.WithContext(bo, ctx))
}

func (c *WebhookClient) postOnce(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return backoff.Permanent(fmt.Errorf("build webhook request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("notifier returned retryable status %d", resp.StatusCode)
	default:
		return backoff.Permanent(fmt.Errorf("notifier returned status %d", resp.StatusCode))
	}
}
