// Package segformat names on-disk hot snapshot and merge segment formats.
package segformat

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/elk-utilities/prism/internal/duckdbfile"
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
