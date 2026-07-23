package layout

import (
	"fmt"
	"path/filepath"
)

// TierDir returns the on-disk directory for tier L{tier}.
func TierDir(dataDir, tenant string, tier int) string {
	return filepath.Join(dataDir, tenant, "tiers", fmt.Sprintf("L%d", tier))
}

// RollupDir returns the on-disk directory for rollup step {step}.
func RollupDir(dataDir, tenant, step string) string {
	return filepath.Join(dataDir, tenant, "rollups", step)
}

// ToSlash normalizes a filesystem path for DuckDB read_parquet literals.
func ToSlash(path string) string {
	return filepath.ToSlash(path)
}
