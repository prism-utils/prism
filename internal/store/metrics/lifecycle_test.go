package metrics_test

import (
	"errors"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metrics"
)

func TestTickObservationsCountRunsAndErrors(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObserveTick("merge", 250*time.Millisecond, nil)
	reg.ObserveTick("merge", 10*time.Millisecond, errors.New("scan tiers"))

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_lifecycle_ticks_total{job="merge"} 2`,
		`prism_store_lifecycle_tick_errors_total{job="merge"} 1`,
		`prism_store_lifecycle_tick_duration_seconds_count{job="merge"} 2`,
	)
}

func TestLastSuccessTimestampOnlyMovesOnSuccess(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObserveTick("flush", time.Millisecond, nil)
	first := gaugeValue(t, scrape(t, reg), `prism_store_lifecycle_last_success_timestamp_seconds{job="flush"}`)
	if first <= 0 {
		t.Fatalf("last success timestamp = %v, want a unix timestamp", first)
	}

	reg.ObserveTick("flush", time.Millisecond, errors.New("boom"))
	after := gaugeValue(t, scrape(t, reg), `prism_store_lifecycle_last_success_timestamp_seconds{job="flush"}`)
	if after != first {
		t.Fatalf("failed tick moved last success timestamp: %v -> %v", first, after)
	}
}

func TestFileGaugesTrackLatestTickObservation(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObserveTierSegments("user-6f3a9c2b-apps", 12)
	reg.ObserveLogLandingFiles("user-6f3a9c2b-apps", "logs-summary", 5)
	reg.SetLogLandingLimit(64)

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_tier_segment_files{tenant="user-6f3a9c2b-apps"} 12`,
		`prism_store_log_landing_files{artifact="logs-summary",tenant="user-6f3a9c2b-apps"} 5`,
		"prism_store_log_landing_files_limit 64",
	)

	// A gauge reports the newest tick, not a running total.
	reg.ObserveTierSegments("user-6f3a9c2b-apps", 3)
	assertContains(t, scrape(t, reg), `prism_store_tier_segment_files{tenant="user-6f3a9c2b-apps"} 3`)
}

func TestCompactionSecondsAccumulate(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObserveCompactionSeconds("user-6f3a9c2b-apps", 1.5)
	reg.ObserveCompactionSeconds("user-6f3a9c2b-apps", 0.5)

	assertContains(t, scrape(t, reg), `prism_store_compaction_cpu_seconds_total{tenant="user-6f3a9c2b-apps"} 2`)
}

func TestTenantLifecycleSeriesAbsentWhenPerTenantDisabled(t *testing.T) {
	cfg := enabledConfig()
	cfg.PerTenant = false
	reg := metrics.New(cfg)
	reg.ObserveTierSegments("user-6f3a9c2b-apps", 12)
	reg.ObserveLogLandingFiles("user-6f3a9c2b-apps", "logs-summary", 5)
	reg.ObserveCompactionSeconds("user-6f3a9c2b-apps", 1)
	reg.ObserveTick("merge", time.Millisecond, nil)

	body := scrape(t, reg)
	assertAbsent(t, body,
		"prism_store_tier_segment_files",
		"prism_store_log_landing_files{",
		"prism_store_compaction_cpu_seconds_total",
	)
	// Job-scoped lifecycle health carries no tenant, so it stays on.
	assertContains(t, body, `prism_store_lifecycle_ticks_total{job="merge"} 1`)
}

func TestObservePromoteCounts(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObservePromote(3, 2, 1, 4096, 4)
	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_promote_attempts_total 3`,
		`prism_store_promote_successes_total 2`,
		`prism_store_promote_retries_total 1`,
		`prism_store_promote_bytes_total 4096`,
		`prism_store_promote_tmp_files 4`,
	)
}
