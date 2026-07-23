package layout

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"
)

// SegmentName builds a collision-free parquet filename. The ingest instant keeps
// segments time-ordered, and a random suffix stops two writes that land within
// the same clock resolution from overwriting each other's output on rename.
func SegmentName(now time.Time) string {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("%d-%s.parquet", now.UnixNano(), hex.EncodeToString(buf[:]))
}

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
