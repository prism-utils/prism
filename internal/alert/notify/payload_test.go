package notify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupKeyForIsDeterministicAndSorted(t *testing.T) {
	a := groupKeyFor(map[string]string{"severity": "warning", "alertname": "HighCPU"})
	b := groupKeyFor(map[string]string{"alertname": "HighCPU", "severity": "warning"})
	assert.Equal(t, a, b)
	assert.Equal(t, `{alertname="HighCPU",severity="warning"}`, a)
}

func TestFingerprintStableAndLabelOrderIndependent(t *testing.T) {
	f1 := fingerprint(map[string]string{"alertname": "A", "instance": "n1"})
	f2 := fingerprint(map[string]string{"instance": "n1", "alertname": "A"})
	assert.Equal(t, f1, f2)
	assert.Len(t, f1, 16) // 64-bit fingerprint rendered as 16 hex digits
	assert.NotEqual(t, f1, fingerprint(map[string]string{"alertname": "A", "instance": "n2"}))
}

func TestGroupLabelsForProjectsOnlyPresentLabels(t *testing.T) {
	gl := groupLabelsFor(map[string]string{"alertname": "A", "instance": "n1"}, []string{"alertname", "severity"})
	assert.Equal(t, map[string]string{"alertname": "A"}, gl)
}

func TestCommonPairs(t *testing.T) {
	got := commonPairs([]map[string]string{
		{"alertname": "A", "severity": "warning", "instance": "n1"},
		{"alertname": "A", "severity": "warning", "instance": "n2"},
	})
	assert.Equal(t, map[string]string{"alertname": "A", "severity": "warning"}, got)
	assert.Equal(t, map[string]string{}, commonPairs(nil))
}

func TestBuildPayloadFiring(t *testing.T) {
	now := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	p := buildPayload("tenant-webhook", "https://ext/", map[string]string{"alertname": "HighCPU", "severity": "warning"},
		[]Alert{{
			Labels:       map[string]string{"alertname": "HighCPU", "severity": "warning", "instance": "n1"},
			Annotations:  map[string]string{"summary": "cpu high"},
			StartsAt:     now.Add(-5 * time.Minute),
			GeneratorURL: "https://store/graph",
		}},
		now, 5*time.Minute)

	assert.Equal(t, "4", p.Version)
	assert.Equal(t, statusFiring, p.Status)
	assert.Equal(t, "tenant-webhook", p.Receiver)
	assert.Equal(t, "https://ext/", p.ExternalURL)
	assert.Equal(t, map[string]string{"alertname": "HighCPU", "severity": "warning"}, p.GroupLabels)
	require.Len(t, p.Alerts, 1)
	a := p.Alerts[0]
	assert.Equal(t, statusFiring, a.Status)
	assert.Equal(t, "2026-04-19T09:55:00Z", a.StartsAt)
	// Firing alert auto-resolves at now+resolveTimeout so the receiver expires it.
	assert.Equal(t, "2026-04-19T10:05:00Z", a.EndsAt)
	assert.Equal(t, "https://store/graph", a.GeneratorURL)
	assert.NotEmpty(t, a.Fingerprint)
}

func TestBuildPayloadResolved(t *testing.T) {
	now := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	p := buildPayload("r", "", map[string]string{"alertname": "HighCPU"},
		[]Alert{{
			Resolved:    true,
			Labels:      map[string]string{"alertname": "HighCPU"},
			Annotations: map[string]string{},
			StartsAt:    now.Add(-10 * time.Minute),
			ResolvedAt:  now.Add(-1 * time.Minute),
		}},
		now, 5*time.Minute)

	assert.Equal(t, statusResolved, p.Status)
	require.Len(t, p.Alerts, 1)
	assert.Equal(t, statusResolved, p.Alerts[0].Status)
	assert.Equal(t, "2026-04-19T09:59:00Z", p.Alerts[0].EndsAt)
}

func TestBuildPayloadMixedStatusIsFiring(t *testing.T) {
	now := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	p := buildPayload("r", "", map[string]string{},
		[]Alert{
			{Labels: map[string]string{"alertname": "A"}, StartsAt: now},
			{Resolved: true, Labels: map[string]string{"alertname": "A", "instance": "n2"}, StartsAt: now, ResolvedAt: now},
		},
		now, time.Minute)
	assert.Equal(t, statusFiring, p.Status)
}
