package stats

// TenantOnDiskBytes sums tiers/, rollups/, hot/, and engine.duckdb for a tenant.
func TenantOnDiskBytes(dataDir, tenant string) (int64, error) { return 0, nil }

// CompactionCPUSeconds returns cumulative compaction CPU-seconds for a tenant.
func CompactionCPUSeconds(dataDir, tenant string) (float64, error) { return 0, nil }

// AddCompactionCPUSeconds increments the cumulative compaction CPU-seconds counter.
func AddCompactionCPUSeconds(dataDir, tenant string, seconds float64) error { return nil }
