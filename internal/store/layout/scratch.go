package layout

import (
	"strings"
	"unicode"
)

const (
	hotSnapshotParquetPrefix = "current.parquet."
	hotSnapshotDuckDBPrefix  = "current.duckdb."
	hotScratchTmpSuffix      = ".tmp"
	hotScratchWalSuffix      = ".tmp.wal"
	hotReadPinPrefix         = ".read-"
	engineSpillName          = "engine.duckdb.tmp"
)

// IsHotScratch reports whether a hot-directory entry is reclaimable scratch:
// an unfinished snapshot export, that export's WAL sibling, or a leftover
// query pin. Live snapshot names and unrelated temps are not scratch.
func IsHotScratch(name string) bool {
	if isHotReadPin(name) {
		return true
	}
	return isHotSnapshotTmp(name, hotSnapshotParquetPrefix) ||
		isHotSnapshotTmp(name, hotSnapshotDuckDBPrefix)
}

// IsEngineScratch reports whether a tenant-root entry is DuckDB spill. The
// live catalog and its WAL use different names and are not scratch.
func IsEngineScratch(name string) bool {
	return name == engineSpillName
}

// IsMaterializeScratch reports whether a materialization-directory entry is
// an unfinished COPY dest. Live parquet/duckdb and unrelated temps are not
// scratch.
func IsMaterializeScratch(name string) bool {
	return strings.HasSuffix(name, ".parquet.tmp") || strings.HasSuffix(name, ".duckdb.tmp")
}

func isHotReadPin(name string) bool {
	return len(name) > len(hotReadPinPrefix) && strings.HasPrefix(name, hotReadPinPrefix)
}

func isHotSnapshotTmp(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	rest := name[len(prefix):]
	id, ok := strings.CutSuffix(rest, hotScratchWalSuffix)
	if !ok {
		id, ok = strings.CutSuffix(rest, hotScratchTmpSuffix)
	}
	if !ok || id == "" {
		return false
	}
	return isHexID(id)
}

func isHexID(s string) bool {
	for _, r := range s {
		if !unicode.Is(unicode.ASCII_Hex_Digit, r) {
			return false
		}
	}
	return true
}
