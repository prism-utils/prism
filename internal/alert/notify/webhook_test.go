package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func samplePayload(nAlerts int) WebhookPayload {
	alerts := make([]WebhookAlert, nAlerts)
	for i := range alerts {
		alerts[i] = WebhookAlert{
			Status:      statusFiring,
			Labels:      map[string]string{"alertname": "A", "instance": strings.Repeat("x", 8)},
			Annotations: map[string]string{"summary": "s"},
			StartsAt:    time.Unix(0, 0).UTC().Format(time.RFC3339),
		}
	}
	return WebhookPayload{Version: "4", Status: statusFiring, Receiver: "r", Alerts: alerts}
}

func TestWebhookSendsBearerAndJSON(t *testing.T) {
	var gotAuth, gotType string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewWebhookClient(WebhookConfig{URL: srv.URL, Secret: "s3cret"}, nil)
	require.NoError(t, c.Send(context.Background(), samplePayload(1)))

	assert.Equal(t, "Bearer s3cret", gotAuth)
	assert.Equal(t, "application/json", gotType)
	var decoded WebhookPayload
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "4", decoded.Version)
	require.Len(t, decoded.Alerts, 1)
}

func TestWebhookRetriesTransient5xxThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewWebhookClient(WebhookConfig{URL: srv.URL, Secret: "s", MaxElapsed: 5 * time.Second}, nil)
	require.NoError(t, c.Send(context.Background(), samplePayload(1)))
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(3))
}

func TestWebhookDoesNotRetryPermanent4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewWebhookClient(WebhookConfig{URL: srv.URL, Secret: "s", MaxElapsed: 5 * time.Second}, nil)
	err := c.Send(context.Background(), samplePayload(1))
	require.Error(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "4xx must not be retried")
}

func TestWebhookSplitsOversizedBatch(t *testing.T) {
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewWebhookClient(WebhookConfig{URL: srv.URL, Secret: "s"}, nil)
	// Enough alerts that the JSON body exceeds 256 KiB and must be split.
	require.NoError(t, c.Send(context.Background(), samplePayload(4000)))
	require.Greater(t, len(bodies), 1, "oversized payload must be split into multiple POSTs")
	for _, b := range bodies {
		assert.LessOrEqual(t, len(b), maxWebhookBodyBytes)
	}
}

func TestWebhookHonorsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := NewWebhookClient(WebhookConfig{URL: srv.URL, Secret: "s", MaxElapsed: time.Minute}, nil)
	err := c.Send(ctx, samplePayload(1))
	require.Error(t, err)
}
