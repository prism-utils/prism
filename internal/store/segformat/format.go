// Package segformat names on-disk hot snapshot and merge segment formats.
package segformat

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/prism-utils/prism/internal/duckdbfile"
)

// Format is the on-disk encoding for hot snapshots and merge segments.
type Format string

const (
	// Parquet is the portable default (agent ingest + cold default).
	Parquet Format = "parquet"
	// DuckDB is a checkpointed single-file DuckDB database (ATTACH).
	DuckDB Format = "duckdb"
)

// DefaultStorageVersion pins newly created .duckdb files to a compatibility
// line readable by the bundled go-duckdb. Operators may override via env.
const DefaultStorageVersion = "v1.0.0"

// MinUsableBytes is the smallest on-disk segment that can hold a parquet magic
// pair or a duckdb header. Crash leftovers below this fail ATTACH / read_parquet
// for every sibling in the same open set.
const MinUsableBytes int64 = 8

// TooSmall reports whether a file is too small to open as a parquet or duckdb
// segment. Callers skip these paths; they do not rename them (a read-only query
// plane cannot).
func TooSmall(n int64) bool {
	return n < MinUsableBytes
}

const parquetMagic = "PAR1"

// Payload sniffs on-disk magic and ignores the filename extension. PAR1 at
// head and tail is parquet; DuckDB's DUCK marker at offset 8 is duckdb.
func Payload(path string) Format {
	f, err := os.Open(path) //nolint:gosec // G304: path is a server-owned segment
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, duckdbfile.MagicPeek)
	n, err := io.ReadFull(f, head)
	if err != nil && n < 4 {
		return ""
	}
	if n >= 4 && string(head[:4]) == parquetMagic {
		st, err := f.Stat()
		if err != nil || st.Size() < 8 {
			return ""
		}
		tail := make([]byte, 4)
		if _, err := f.ReadAt(tail, st.Size()-4); err != nil {
			return ""
		}
		if string(tail) == parquetMagic {
			return Parquet
		}
		return ""
	}
	if duckdbfile.HasMagic(head[:n]) {
		return DuckDB
	}
	return ""
}

// SkipOpen reports whether a segment should be omitted from query and merge
// scans. Size below a header is always unusable. Unknown payload (neither
// parquet nor duckdb magic) is unusable even when large enough. DuckDB bytes
// at a .parquet name stay in the open set so ATTACH can read them.
func SkipOpen(path string, size int64) bool {
	if TooSmall(size) {
		return true
	}
	return Payload(path) == ""
}

// MetricsTable is the relation name inside metrics-plane .duckdb segments and
// hot/current.duckdb exports.
const MetricsTable = "metrics"

// LogsTable is the relation name inside logs-plane .duckdb tier segments
// written by MERGE_SEGMENT_FORMAT=duckdb.
const LogsTable = "logs"

// AgentLogsTable is the relation name inside agent-emitted log windows
// (encoder/duckdb → duckdbfile.Table).
const AgentLogsTable = duckdbfile.Table

// LogsRelationForPath returns the DuckDB relation name for a logs .duckdb file.
// Agent landing windows use duckdbfile.Table ("data"); compacted tier segments
// use LogsTable ("logs").
func LogsRelationForPath(path string) string {
	clean := filepath.ToSlash(path)
	if strings.Contains(clean, "/tiers/") {
		return LogsTable
	}
	return AgentLogsTable
}

// Parse validates a format env value (case-insensitive). Empty defaults to Parquet.
func Parse(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", string(Parquet):
		return Parquet, nil
	case string(DuckDB):
		return DuckDB, nil
	default:
		return "", fmt.Errorf("must be parquet or duckdb, got %q", s)
	}
}

// Ext returns the file extension without the leading dot.
func (f Format) Ext() string {
	switch f {
	case DuckDB:
		return "duckdb"
	default:
		return "parquet"
	}
}

// DotExt returns the extension with a leading dot (".parquet" / ".duckdb").
func (f Format) DotExt() string {
	return "." + f.Ext()
}
