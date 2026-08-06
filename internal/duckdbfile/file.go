// Package duckdbfile names the on-the-wire DuckDB window format shared by the
// agent encoder and store ingest (Content-Type, magic sniff, table name, and
// storage-version pin).
package duckdbfile

import (
	"bytes"
	"strings"
)

const (
	// ContentType is the HTTP Content-Type for a sealed .duckdb window body.
	ContentType = "application/vnd.duckdb"

	// Ext is the file extension without a leading dot.
	Ext = "duckdb"

	// Table is the single relation name inside an agent-emitted window database.
	Table = "data"

	// DefaultStorageVersion pins newly created .duckdb windows to a
	// compatibility line readable by the store's bundled go-duckdb.
	DefaultStorageVersion = "v1.0.0"

	// Magic is the DuckDB file header marker.
	Magic = "DUCK"

	// MagicOffset is where Magic begins in a DuckDB database file.
	MagicOffset = 8

	// FormatMeta is the Flight app-metadata / path token that marks an opaque
	// duckdb DoPut (as opposed to Arrow IPC).
	FormatMeta = "format=duckdb"
)

// HasMagic reports whether b looks like a DuckDB database file header.
func HasMagic(b []byte) bool {
	if len(b) < MagicOffset+len(Magic) {
		return false
	}
	return string(b[MagicOffset:MagicOffset+len(Magic)]) == Magic
}

// LooksLikeParquet reports the Parquet magic at offset 0 ("PAR1").
func LooksLikeParquet(b []byte) bool {
	return len(b) >= 4 && string(b[:4]) == "PAR1"
}

// DetectHTTP classifies an ingest body from Content-Type and optional magic.
// Explicit application/vnd.duckdb wins; octet-stream/empty fall back to magic;
// Parquet magic or other CTs are not duckdb.
func DetectHTTP(contentType string, body []byte) (isDuckDB bool) {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case ContentType:
		return true
	case "", "application/octet-stream":
		return HasMagic(body)
	default:
		return false
	}
}

// FormatFromFlightMeta reports whether Flight descriptor app metadata or path
// carries format=duckdb.
func FormatFromFlightMeta(appMeta []byte, path []string) bool {
	if bytes.Equal(bytes.TrimSpace(appMeta), []byte(FormatMeta)) {
		return true
	}
	for _, p := range path {
		if strings.EqualFold(strings.TrimSpace(p), FormatMeta) {
			return true
		}
	}
	return false
}
