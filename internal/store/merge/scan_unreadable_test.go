package merge

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestScanTierOmitsUnreadableSegment(t *testing.T) {
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		file     string
		optional bool
		plant    func(t *testing.T, path string)
	}{
		{
			name: "empty",
			file: "garbage.parquet",
			plant: func(t *testing.T, path string) {
				writeBytes(t, path, nil)
			},
		},
		{
			name: "truncated",
			file: "truncated.parquet",
			plant: func(t *testing.T, path string) {
				testparquet.WriteSegmentWithTs(t, path, base, "up", 1)
				if err := os.Truncate(path, 16); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:     "duckdb-magic",
			file:     "garbage.duckdb",
			optional: true,
			plant: func(t *testing.T, path string) {
				body := append([]byte("DUCK"), make([]byte, 64)...)
				writeBytes(t, path, body)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			tenant := "user-scanomit01-apps"
			tierDir := layout.TierDir(dataDir, tenant, 0)
			if err := os.MkdirAll(tierDir, 0o750); err != nil {
				t.Fatal(err)
			}
			live := filepath.Join(tierDir, "live.parquet")
			testparquet.WriteSegmentWithTs(t, live, base, "up", 2)
			garbage := filepath.Join(tierDir, tc.file)
			tc.plant(t, garbage)
			if _, err := StatSegment(garbage, 0, DuckDBCaps{}); err == nil {
				if tc.optional {
					t.Skip("planted file is readable")
				}
				t.Fatal("planted file is readable")
			}

			segs, err := ScanTier(dataDir, tenant, 0, DuckDBCaps{})
			if err != nil {
				t.Fatalf("ScanTier: %v", err)
			}
			if len(segs) != 1 || segs[0].Path != live {
				t.Fatalf("ScanTier = %v, want only %s", segs, live)
			}
			if _, err := os.Stat(garbage); err != nil {
				t.Fatalf("unreadable file was removed: %v", err)
			}
		})
	}
}

func TestScanTierEmptyDir(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-scanempty01-apps"
	if err := os.MkdirAll(layout.TierDir(dataDir, tenant, 0), 0o750); err != nil {
		t.Fatal(err)
	}
	segs, err := ScanTier(dataDir, tenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatalf("ScanTier empty dir: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("ScanTier empty dir = %v, want none", segs)
	}
}

func writeBytes(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}
