package logmeta_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/logmeta"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const indexTenant = "user-label-9f31"

// The label index answers Loki's label/<name>/values, so it has to describe the
// same searchable set the logs relation scans: a value that only exists in the
// landing buffer would offer a Grafana dropdown entry that query_range cannot
// match until the next refresh.
func TestLabelValuesOmitsLandingOnlyValues(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, indexTenant, "logs", "logs-raw")
	testparquet.WriteLogsRawFile(t,
		filepath.Join(artifactDir, layout.SegmentName(time.Unix(100, 0).UTC())),
		[]testparquet.LogRow{{Message: "buffered", Format: "buffered-format"}},
	)
	testparquet.WriteLogsRawFile(t,
		filepath.Join(artifactDir, "tiers", "L0", layout.SegmentName(time.Unix(200, 0).UTC())),
		[]testparquet.LogRow{{Message: "refreshed", Format: "refreshed-format"}},
	)
	if err := logmeta.Bump(dataDir, indexTenant); err != nil {
		t.Fatal(err)
	}
	if err := logmeta.SyncManifest(dataDir, indexTenant, "logs-raw"); err != nil {
		t.Fatal(err)
	}

	vals, err := logmeta.LabelValues(dataDir, indexTenant, "format", 0)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(vals) != 1 || vals[0] != "refreshed-format" {
		t.Fatalf("format values = %v, want only the refreshed tier value", vals)
	}
}

// A refresh is what publishes buffered values, so the same buffer must show up
// once its window reaches a tier.
func TestLabelValuesIncludesValuesAfterRefresh(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, indexTenant, "logs", "logs-raw")
	testparquet.WriteLogsRawFile(t,
		filepath.Join(artifactDir, layout.SegmentName(time.Unix(100, 0).UTC())),
		[]testparquet.LogRow{{Message: "buffered", Format: "buffered-format"}},
	)
	testparquet.PromoteLandedLogsToTier(t, dataDir, indexTenant, "logs-raw")

	vals, err := logmeta.LabelValues(dataDir, indexTenant, "format", 0)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(vals) != 1 || vals[0] != "buffered-format" {
		t.Fatalf("format values = %v, want the refreshed value", vals)
	}
}
