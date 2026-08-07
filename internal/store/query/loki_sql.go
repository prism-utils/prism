package query

import (
	"fmt"
	"time"
)

// lokiTSColumn is the ingest-time column on every Loki log row (nanoseconds).
// Parsers must not emit event timestamps; storage stamps ingest time at land/merge
// so charts stay honest after compaction. Prefer a per-row column already present
// in the file; fall back to the segment filename window id for legacy files.
const lokiTSColumn = "__prism_ts_ns"

// lokiLineColumns are the columns that can carry the log line text, in the order
// they are preferred: an explicit message, else the mined template.
var lokiLineColumns = []string{"message", "template"}

// emptyLokiLogsViewSQL is the body of the logs relation when a tenant has no
// landed log parquet: zero rows with the guaranteed logs columns plus the
// ingest-time column, so a query against a logs-empty tenant returns empty.
const emptyLokiLogsViewSQL = `SELECT ` +
	`CAST(NULL AS VARCHAR) AS message, ` +
	`CAST(NULL AS VARCHAR) AS format, ` +
	`CAST(NULL AS VARCHAR) AS template, ` +
	`CAST(NULL AS BIGINT) AS count, ` +
	`CAST(NULL AS BIGINT) AS ` + lokiTSColumn + ` ` +
	`WHERE 1=0`

// sandboxLokiLogsSQL builds the Loki logs relation: shared list read_parquet plus
// ingest-time column, optionally time-pruned and optionally without message.
func sandboxLokiLogsSQL(tenantRoot string, startNs, endNs int64, omitMessage bool, recentLookback time.Duration) (string, []logFileMeta, error) {
	opts := logsCatalogOpts{
		StartNs:        startNs,
		EndNs:          endNs,
		WithIngestTS:   true,
		OmitMessage:    omitMessage,
		RecentOnly:     recentLookback > 0 && endNs <= startNs,
		RecentLookback: recentLookback,
		Now:            time.Now().UTC(),
	}
	// When start is omitted (0), a configured lookback raises the open-set floor
	// so label browsers default to recent segments. Explicit start>0 keeps cold
	// history reachable.
	if recentLookback > 0 && startNs == 0 {
		opts.RecentOnly = true
	} else {
		opts.RecentOnly = false
	}
	sqlText, files, err := sandboxLogsRelationSQL(tenantRoot, opts)
	if err != nil {
		return "", nil, err
	}
	return sqlText, files, nil
}

// sandboxLogsUnionSQL builds the /sql `logs` relation (no ingest-ts column).
func sandboxLogsUnionSQL(tenantRoot string) (string, error) {
	sqlText, _, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	return sqlText, err
}

// logsRelationFingerprint returns the normalized path list shared by /sql and Loki
// so tests can prove both planners open the same files.
func logsRelationFingerprint(tenantRoot string) (string, error) {
	_, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		return "", err
	}
	parts := make([]string, len(files))
	for i, f := range files {
		parts[i] = f.Path
	}
	return fmt.Sprintf("%d:%s", len(parts), joinComma(parts)), nil
}

func joinComma(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	n := 0
	for _, p := range parts {
		n += len(p) + 1
	}
	b := make([]byte, 0, n)
	for i, p := range parts {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, p...)
	}
	return string(b)
}
