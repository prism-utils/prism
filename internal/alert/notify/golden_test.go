package notify

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "regenerate golden files")

// notifierWebhook mirrors the notifier's AlertmanagerWebhook TypeScript type
// (services/notifier/app/src/format.ts) field-for-field. Decoding the emitted
// payload into it proves the wire contract stays compatible; a drift here means
// the notifier would silently miss fields.
type notifierWebhook struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []struct {
		Status       string            `json:"status"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     string            `json:"startsAt"`
		EndsAt       string            `json:"endsAt"`
		GeneratorURL string            `json:"generatorURL"`
		Fingerprint  string            `json:"fingerprint"`
	} `json:"alerts"`
}

func goldenPayload() WebhookPayload {
	now := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	start := now.Add(-5 * time.Minute)
	return buildPayload(
		"tenant-webhook",
		"https://prism.example/alerts",
		map[string]string{"alertname": "HighCPU", "severity": "warning"},
		[]Alert{
			{
				Labels:       map[string]string{"alertname": "HighCPU", "severity": "warning", "instance": "node-a"},
				Annotations:  map[string]string{"summary": "CPU > 90%", "description": "CPU has been over 90% for 5m"},
				StartsAt:     start,
				GeneratorURL: "https://prism.example/alerts/graph?g0.expr=cpu",
			},
			{
				Labels:       map[string]string{"alertname": "HighCPU", "severity": "warning", "instance": "node-b"},
				Annotations:  map[string]string{"summary": "CPU > 90%", "description": "CPU has been over 90% for 5m"},
				StartsAt:     start,
				GeneratorURL: "https://prism.example/alerts/graph?g0.expr=cpu",
			},
		},
		now, 5*time.Minute,
	)
}

func TestWebhookPayloadGolden(t *testing.T) {
	got, err := json.MarshalIndent(goldenPayload(), "", "  ")
	require.NoError(t, err)
	got = append(got, '\n')

	golden := filepath.Join("testdata", "golden", "firing_webhook.json")
	if *update {
		require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0o750))
		require.NoError(t, os.WriteFile(golden, got, 0o640))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got), "payload drifted from golden; run: make golden-update")
}

func TestPayloadDecodesIntoNotifierContract(t *testing.T) {
	body, err := json.Marshal(goldenPayload())
	require.NoError(t, err)

	var wh notifierWebhook
	require.NoError(t, json.Unmarshal(body, &wh))

	assert.Equal(t, "4", wh.Version)
	assert.Equal(t, statusFiring, wh.Status)
	assert.Equal(t, "tenant-webhook", wh.Receiver)
	require.Len(t, wh.Alerts, 2, "notifier requires a populated alerts[] array")
	assert.Equal(t, "warning", wh.CommonLabels["severity"])
	assert.NotContains(t, wh.CommonLabels, "instance", "per-alert labels must not leak into commonLabels")
	for _, a := range wh.Alerts {
		assert.Equal(t, statusFiring, a.Status)
		assert.NotEmpty(t, a.Fingerprint)
		assert.NotEmpty(t, a.StartsAt)
		assert.NotEmpty(t, a.EndsAt)
	}
}
