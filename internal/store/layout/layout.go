package layout

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
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

// LogsLandingDir returns the on-disk landing directory for a logs artifact.
func LogsLandingDir(dataDir, tenant, artifact string) string {
	return filepath.Join(dataDir, tenant, "logs", artifact)
}

// LogsTierDir returns the on-disk directory for log tier L{tier} under an artifact.
func LogsTierDir(dataDir, tenant, artifact string, tier int) string {
	return filepath.Join(dataDir, tenant, "logs", artifact, "tiers", fmt.Sprintf("L%d", tier))
}

// WindowIDNanos parses the leading unix-nanosecond window id from a segment
// filename (<unix_ns>-<suffix>.parquet). Returns ok=false when absent.
func WindowIDNanos(path string) (int64, bool) {
	base := filepath.Base(path)
	dash := strings.IndexByte(base, '-')
	if dash <= 0 {
		return 0, false
	}
	ns, err := strconv.ParseInt(base[:dash], 10, 64)
	if err != nil || ns <= 0 {
		return 0, false
	}
	return ns, true
}

// ToSlash normalizes a filesystem path for DuckDB read_parquet literals.
func ToSlash(path string) string {
	return filepath.ToSlash(path)
}
