package events

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prism-utils/prism/internal/alert/notify"
)

func TestEncodeParquetSchemaAndFingerprint(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	alerts := []notify.Alert{
		{
			Labels: map[string]string{
				"alertname": "HighCPU",
				"severity":  "warning",
				"instance":  "n1",
			},
			Annotations: map[string]string{
				"summary":     "CPU > 90%",
				"description": "hot",
				"runbook":     "https://runbooks/cpu",
			},
			StartsAt: now,
		},
		{
			Resolved: true,
			Labels: map[string]string{
				"alertname": "HighCPU",
				"severity":  "warning",
				"instance":  "n1",
			},
			Annotations: map[string]string{
				"summary":        "CPU > 90%",
				"recommendation": "scale",
			},
			StartsAt:   now,
			ResolvedAt: now.Add(time.Minute),
		},
	}

	body, err := Encode(alerts)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(body, []byte("PAR1")), "parquet magic")

	rows, err := DecodeForTest(body)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	fp := notify.Fingerprint(alerts[0].Labels)
	assert.Len(t, fp, 16)
	assert.Equal(t, fp, rows[0].Fingerprint)
	assert.Equal(t, "HighCPU", rows[0].Alertname)
	assert.Equal(t, "warning", rows[0].Severity)
	assert.Equal(t, statusFiring, rows[0].Status)
	assert.Equal(t, "CPU > 90%", rows[0].Summary)
	assert.Equal(t, "hot", rows[0].Description)
	assert.Equal(t, "https://runbooks/cpu", rows[0].Recommendation)
	assert.True(t, rows[0].EndsAt.IsZero(), "firing ends_at must be null/zero")
	assert.Contains(t, rows[0].Labels, `"alertname":"HighCPU"`)

	assert.Equal(t, statusResolved, rows[1].Status)
	assert.Equal(t, "scale", rows[1].Recommendation, "recommendation annotation wins over runbook")
	assert.False(t, rows[1].EndsAt.IsZero())
}

func TestEncodeEmptyIsEmpty(t *testing.T) {
	body, err := Encode(nil)
	require.NoError(t, err)
	assert.Empty(t, body)
}

func TestRecommendationPrefersExplicitThenRunbook(t *testing.T) {
	assert.Equal(t, "from-rec", recommendationOf(map[string]string{
		"recommendation": "from-rec",
		"runbook":        "from-runbook",
	}))
	assert.Equal(t, "from-runbook", recommendationOf(map[string]string{"runbook": "from-runbook"}))
	assert.Equal(t, "", recommendationOf(nil))
}
