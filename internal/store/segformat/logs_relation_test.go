package segformat_test

import (
	"testing"

	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/store/segformat"
)

func TestLogsRelationForPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		path string
		want string
	}{
		{"/data/ns/logs/logs-summary/178-abc.duckdb", duckdbfile.Table},
		{"/data/ns/logs/logs-raw/178-abc.duckdb", duckdbfile.Table},
		{"/data/ns/logs/logs-summary/tiers/L0/178-abc.duckdb", segformat.LogsTable},
		{"/data/ns/logs/logs-template/tiers/L1/178-abc.duckdb", segformat.LogsTable},
	}
	for _, tc := range cases {
		if got := segformat.LogsRelationForPath(tc.path); got != tc.want {
			t.Fatalf("LogsRelationForPath(%q)=%q, want %q", tc.path, got, tc.want)
		}
	}
}
